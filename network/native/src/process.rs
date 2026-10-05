//! Child descriptor setup stays within the Linux syscall boundary.
use std::{
    io,
    os::fd::{AsRawFd, BorrowedFd},
    os::unix::process::CommandExt,
    process::Command,
};

/// Arrange one borrowed connected channel as a child descriptor. A private
/// CLOEXEC duplicate is captured until spawn; the caller may close its original
/// after this function returns. No parent flags or descriptors are changed.
/// Drop the Command after spawning to release its captured parent duplicate.
pub fn inherit_control(
    command: &mut Command,
    source: BorrowedFd<'_>,
    target: i32,
) -> io::Result<()> {
    if !(3..=63).contains(&target) {
        return Err(crate::invalid("invalid child descriptor"));
    }
    // Captured sources are above every permitted target, so multiple mappings
    // cannot overwrite another hook's pending source descriptor.
    // SAFETY: scalar fcntl duplicates a live borrowed FD with independent ownership.
    let raw = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 64) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: successful F_DUPFD_CLOEXEC returned a new uniquely owned descriptor.
    let source = unsafe { OwnedFd::from_raw_fd(raw) };
    // SAFETY: the child hook performs only async-signal-safe scalar FD syscalls.
    // The captured OwnedFd is valid until exec or child hook completion. Parent
    // ownership is unchanged; a failed hook prevents execution of the child.
    unsafe {
        command.pre_exec(move || {
            let raw = source.as_raw_fd();
            if raw != target {
                if libc::dup2(raw, target) < 0 {
                    return Err(io::Error::last_os_error());
                }
            } else {
                let flags = libc::fcntl(target, libc::F_GETFD);
                if flags < 0 || libc::fcntl(target, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    Ok(())
}

/// Retain exactly CAP_NET_ADMIN in effective/permitted sets and remove all
/// inheritable/ambient capabilities. The root bootstrap must already limit the
/// bounding set to CAP_NET_ADMIN; this function verifies that condition first.
pub fn restrict_helper_caps() -> io::Result<()> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    for key in ["CapBnd:", "CapAmb:"] {
        let line = status
            .lines()
            .find(|l| l.starts_with(key))
            .ok_or_else(|| crate::invalid("missing capability status"))?;
        let bits = u64::from_str_radix(
            line.split_whitespace()
                .nth(1)
                .ok_or_else(|| crate::invalid("invalid capability status"))?,
            16,
        )
        .map_err(|_| crate::invalid("invalid capability status"))?;
        let expected = if key == "CapBnd:" { 1u64 << 12 } else { 0 };
        if bits != expected {
            return Err(crate::invalid(
                "invalid helper bounding or ambient capabilities",
            ));
        }
    }
    #[repr(C)]
    struct Header {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    struct Data {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    let header = Header {
        version: 0x2008_0522,
        pid: 0,
    };
    let data = [
        Data {
            effective: 1 << 12,
            permitted: 1 << 12,
            inheritable: 0,
        },
        Data {
            effective: 0,
            permitted: 0,
            inheritable: 0,
        },
    ];
    // SAFETY: Linux capset v3 reads one repr(C) header and two repr(C) data
    // records for this calling thread. All pointers remain live for the syscall.
    if unsafe { libc::syscall(libc::SYS_capset, &header, data.as_ptr()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: prctl scalar operation forbids future exec privilege gains.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
