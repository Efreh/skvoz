//! Linux descriptor ownership, TUN packet I/O, and bounded Unix control framing.
#![deny(unsafe_op_in_unsafe_fn)]
#![cfg(target_os = "linux")]

mod control;
mod process;
mod state;
mod tun;

pub use control::{CONTROL_BODY_MAX, ControlFrame, FramedUnix, IncrementalUnix, PeerCredentials};
pub use process::{inherit_control, restrict_helper_caps};
pub use state::{SecureStateDir, read_private_config, read_root_config};
pub use tun::TunDevice;

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::time::Instant;

/// Duplicate a borrowed descriptor without changing its open-file flags.
pub fn duplicate_cloexec(fd: BorrowedFd<'_>) -> io::Result<OwnedFd> {
    duplicate_inherited(fd.as_raw_fd())
}

/// Duplicate an inherited descriptor; the caller retains the original ownership.
/// An invalid descriptor is rejected by the kernel. No ownership of `fd` is taken.
pub fn duplicate_inherited(fd: RawFd) -> io::Result<OwnedFd> {
    // SAFETY: fcntl does not dereference a pointer; its returned new descriptor
    // has unique ownership, even when the input number is invalid or races a close.
    let copied = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if copied < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful F_DUPFD_CLOEXEC produced a new owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(copied) })
}

/// Change nonblocking mode only on an owned pipe/socket whose other users agree.
pub fn set_nonblocking(fd: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: F_GETFL/F_SETFL accept scalar arguments, never ownership or pointers.
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags < 0
        || unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Detect peer shutdown without consuming any protocol bytes. POLLRDHUP still
/// reports EOF when buffered bytes remain, unlike a one-byte MSG_PEEK check.
pub fn owner_closed(fd: BorrowedFd<'_>) -> io::Result<bool> {
    let mut item = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLRDHUP,
        revents: 0,
    };
    // SAFETY: item is a valid writable single-element pollfd array.
    let result = unsafe { libc::poll(&mut item, 1, 0) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(item.revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0)
}

/// Half-close one owned socket's write direction without closing its read side.
pub fn shutdown_write(fd: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: shutdown takes only scalar descriptor and direction arguments.
    if unsafe { libc::shutdown(fd.as_raw_fd(), libc::SHUT_WR) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn set_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_GETFD/F_SETFD have scalar arguments and the descriptor stays owned.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn require_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: F_GETFL has no pointer argument; it cannot change ownership.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::O_NONBLOCK == 0 {
        return Err(invalid("descriptor must already be nonblocking"));
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn wait_ready(fd: RawFd, events: libc::c_short, deadline: Instant) -> io::Result<()> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "I/O deadline elapsed",
            ));
        }
        let timeout = remaining
            .as_millis()
            .saturating_add(1)
            .min(i32::MAX as u128) as i32;
        let mut item = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: item is a valid writable pollfd for the full call duration.
        let result = unsafe { libc::poll(&mut item, 1, timeout) };
        if result > 0 {
            if item.revents & libc::POLLNVAL != 0 {
                return Err(io::Error::from_raw_os_error(libc::EBADF));
            }
            return Ok(());
        }
        if result == 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Wait for read/write readiness with an absolute caller-owned deadline.
pub fn wait_interest(
    fd: BorrowedFd<'_>,
    read: bool,
    write: bool,
    deadline: Instant,
) -> io::Result<()> {
    let events = if read { libc::POLLIN } else { 0 } | if write { libc::POLLOUT } else { 0 };
    wait_ready(fd.as_raw_fd(), events, deadline)
}

/// Validate a systemd inherited AF_UNIX listener and its root-anchored per-UID
/// endpoint. The descriptor is adopted; errors close it. The bound socket must
/// be uid0600 under the fixed root-owned, nonwritable runtime parents.
pub fn activated_client_listener(
    fd: OwnedFd,
) -> io::Result<(std::os::unix::net::UnixListener, u32)> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let listener = std::os::unix::net::UnixListener::from(fd);
    let address = listener.local_addr()?;
    let path = address
        .as_pathname()
        .ok_or_else(|| invalid("unnamed helper listener"))?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("invalid helper endpoint"))?;
    let uid = parent
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| invalid("invalid helper uid"))?
        .parse::<u32>()
        .map_err(|_| invalid("invalid helper uid"))?;
    if uid == 0
        || path != std::path::PathBuf::from(format!("/run/skvoz-network-helper/{uid}/control.sock"))
    {
        return Err(invalid("invalid helper endpoint"));
    }
    state::validate_root_parent(parent)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.uid() != uid
        || metadata.mode() & 0o7777 != 0o600
        || !metadata.file_type().is_socket()
    {
        return Err(invalid("insecure helper endpoint"));
    }
    // Confirm SOCK_STREAM and SO_ACCEPTCONN rather than trusting the conversion.
    for (option, expected) in [(libc::SO_TYPE, libc::SOCK_STREAM), (libc::SO_ACCEPTCONN, 1)] {
        let mut value: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
        // SAFETY: the output scalar and length are valid for the getsockopt call.
        if unsafe {
            libc::getsockopt(
                listener.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&mut value as *mut libc::c_int).cast(),
                &mut length,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        if value != expected {
            return Err(invalid("invalid helper listener type"));
        }
    }
    listener.set_nonblocking(true)?;
    Ok((listener, uid))
}

/// Request bounded kernel socket buffers and return the observed kernel values
/// (Linux usually reports twice the requested size). These are separate from
/// userspace reservations and do not prove process RSS limits.
pub fn configure_socket_buffers(fd: BorrowedFd<'_>, bytes: usize) -> io::Result<(usize, usize)> {
    if bytes == 0 || bytes > 128 * 1024 {
        return Err(invalid("invalid socket buffer request"));
    }
    let value = bytes as libc::c_int;
    let mut observed = [0usize; 2];
    for (index, option) in [libc::SO_SNDBUF, libc::SO_RCVBUF].into_iter().enumerate() {
        // SAFETY: scalar option data remains readable for its advertised size.
        if unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&value as *const libc::c_int).cast(),
                std::mem::size_of_val(&value) as libc::socklen_t,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut actual: libc::c_int = 0;
        let mut length = std::mem::size_of_val(&actual) as libc::socklen_t;
        // SAFETY: output scalar and its length remain valid for this call.
        if unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                option,
                (&mut actual as *mut libc::c_int).cast(),
                &mut length,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        if actual <= 0 {
            return Err(invalid("invalid observed socket buffer"));
        }
        observed[index] = actual as usize;
    }
    Ok((observed[0], observed[1]))
}

/// Adopt an explicitly transferred inherited CLI descriptor. Duplicate first so
/// CLOEXEC validation failures preserve the original; successful transfer closes
/// the original exactly once. Embedding/FFI borrowed callers use duplicate instead.
pub fn adopt_inherited(fd: RawFd) -> io::Result<OwnedFd> {
    if fd < 3 {
        return Err(invalid("invalid transferred descriptor"));
    }
    let owned = duplicate_inherited(fd)?;
    // SAFETY: CLI transfer grants ownership of this exact inherited descriptor.
    // No retry on EINTR: Linux has already released the descriptor number.
    if unsafe { libc::close(fd) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(owned)
}
