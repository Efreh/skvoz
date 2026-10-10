use crate::{invalid, require_nonblocking, set_cloexec, wait_ready};
use std::io;
use std::mem::{size_of, zeroed};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

pub const CONTROL_BODY_MAX: usize = 32768;
const RECEIVE_DEADLINE: Duration = Duration::from_secs(5);
const SEND_DEADLINE: Duration = Duration::from_secs(3);
const CONTROL_WORDS: usize = 32;
const MAX_RECEIVED_RIGHTS: usize =
    (CONTROL_WORDS * size_of::<usize>() - size_of::<libc::cmsghdr>()) / size_of::<RawFd>();

#[derive(Debug)]
pub struct ControlFrame {
    pub body: Vec<u8>,
    pub fd: Option<OwnedFd>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub pid: i32,
    pub uid: u32,
    pub gid: u32,
}

/// Serialized frame I/O. Any read/write framing error makes this channel terminal.
/// JSON validation and fd_count admission belong to the owning protocol adapter.
#[derive(Debug)]
pub struct FramedUnix {
    fd: OwnedFd,
    terminal: bool,
}

impl FramedUnix {
    pub fn from_owned_fd(fd: OwnedFd) -> io::Result<Self> {
        require_nonblocking(fd.as_raw_fd())?;
        let domain = socket_option(fd.as_raw_fd(), libc::SO_DOMAIN)?;
        let kind = socket_option(fd.as_raw_fd(), libc::SO_TYPE)?;
        if domain != libc::AF_UNIX || kind != libc::SOCK_STREAM {
            return Err(invalid("control descriptor must be AF_UNIX SOCK_STREAM"));
        }
        // A successful credential query also verifies that the socket is connected.
        peer_credentials(fd.as_raw_fd())?;
        set_cloexec(fd.as_raw_fd())?;
        Ok(Self {
            fd,
            terminal: false,
        })
    }

    pub fn pair() -> io::Result<(Self, Self)> {
        let (left, right) = UnixStream::pair()?;
        left.set_nonblocking(true)?;
        right.set_nonblocking(true)?;
        Ok((
            Self::from_owned_fd(left.into())?,
            Self::from_owned_fd(right.into())?,
        ))
    }

    pub fn peer_credentials(&self) -> io::Result<PeerCredentials> {
        peer_credentials(self.as_raw_fd())
    }

    /// Blocking bounded operation for a dedicated control owner thread.
    /// Idle returns WouldBlock without closing the owner. After the first byte,
    /// the whole frame must finish within five seconds.
    pub fn receive_frame(&mut self) -> io::Result<ControlFrame> {
        self.require_live()?;
        let mut header = [0u8; 4];
        let fd = match receive(self.as_raw_fd(), &mut header[..1], true) {
            Ok((1, fd)) => fd,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                return Err(error);
            }
            Ok(_) => {
                self.close_on_error();
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "control frame EOF",
                ));
            }
            Err(error) => {
                self.close_on_error();
                return Err(error);
            }
        };
        let result = self.receive_remainder(header, fd, Instant::now() + RECEIVE_DEADLINE);
        if result.is_err() {
            self.close_on_error();
        }
        result
    }

    pub fn receive_frame_until(&mut self, deadline: Instant) -> io::Result<ControlFrame> {
        self.require_live()?;
        let result = self.receive_inner(deadline.min(Instant::now() + RECEIVE_DEADLINE));
        if result.is_err() {
            self.close_on_error();
        }
        result
    }

    fn receive_inner(&self, deadline: Instant) -> io::Result<ControlFrame> {
        // Only the first byte may carry rights. Reading exactly one byte prevents
        // rights associated with a later stream byte from being misattributed.
        let mut header = [0u8; 4];
        let (_, fd) = self.receive_chunk(&mut header[..1], true, deadline)?;
        self.receive_remainder(header, fd, deadline)
    }

    fn receive_remainder(
        &self,
        mut header: [u8; 4],
        fd: Option<OwnedFd>,
        deadline: Instant,
    ) -> io::Result<ControlFrame> {
        self.receive_exact(&mut header[1..], deadline)?;
        let size = u32::from_be_bytes(header) as usize;
        if size == 0 || size > CONTROL_BODY_MAX {
            return Err(invalid("invalid control frame length"));
        }
        let mut body = vec![0; size];
        self.receive_exact(&mut body, deadline)?;
        check_deadline(deadline)?;
        Ok(ControlFrame { body, fd })
    }

    fn receive_exact(&self, mut buffer: &mut [u8], deadline: Instant) -> io::Result<()> {
        while !buffer.is_empty() {
            let (count, _) = self.receive_chunk(buffer, false, deadline)?;
            buffer = &mut buffer[count..];
        }
        check_deadline(deadline)?;
        Ok(())
    }

    fn receive_chunk(
        &self,
        buffer: &mut [u8],
        allow_fd: bool,
        deadline: Instant,
    ) -> io::Result<(usize, Option<OwnedFd>)> {
        loop {
            check_deadline(deadline)?;
            match receive(self.as_raw_fd(), buffer, allow_fd) {
                Ok((0, _)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "control frame EOF",
                    ));
                }
                Ok(result) => {
                    check_deadline(deadline)?;
                    return Ok(result);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_ready(self.as_raw_fd(), libc::POLLIN, deadline)?
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "control read deadline elapsed",
                ));
            }
        }
    }

    /// Send an owned frame with rights on its first byte. Original FD is retained.
    pub fn send_frame(&mut self, body: &[u8], fd: Option<BorrowedFd<'_>>) -> io::Result<()> {
        self.require_live()?;
        if body.is_empty() || body.len() > CONTROL_BODY_MAX {
            return Err(invalid("invalid control frame length"));
        }
        let result = self.send_inner(body, fd, Instant::now() + SEND_DEADLINE);
        if result.is_err() {
            self.close_on_error();
        }
        result
    }

    fn send_inner(
        &self,
        body: &[u8],
        fd: Option<BorrowedFd<'_>>,
        deadline: Instant,
    ) -> io::Result<()> {
        let header = (body.len() as u32).to_be_bytes();
        loop {
            check_deadline(deadline)?;
            match send_first(self.as_raw_fd(), header[0], fd) {
                Ok(1) => {
                    check_deadline(deadline)?;
                    break;
                }
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "control first byte write failed",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    wait_ready(self.as_raw_fd(), libc::POLLOUT, deadline)?
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        self.send_all(&header[1..], deadline)?;
        self.send_all(body, deadline)
    }

    fn send_all(&self, mut bytes: &[u8], deadline: Instant) -> io::Result<()> {
        while !bytes.is_empty() {
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "control write deadline elapsed",
                ));
            }
            // SAFETY: bytes is a valid readable slice; MSG_NOSIGNAL prevents process signals.
            let count = unsafe {
                libc::send(
                    self.as_raw_fd(),
                    bytes.as_ptr().cast(),
                    bytes.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if count > 0 {
                bytes = &bytes[count as usize..];
                continue;
            }
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "control frame write failed",
                ));
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::WouldBlock => wait_ready(self.as_raw_fd(), libc::POLLOUT, deadline)?,
                io::ErrorKind::Interrupted => {}
                _ => return Err(error),
            }
        }
        check_deadline(deadline)?;
        Ok(())
    }

    fn require_live(&self) -> io::Result<()> {
        if self.terminal {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "control channel is terminal",
            ));
        }
        Ok(())
    }

    fn close_on_error(&mut self) {
        self.terminal = true;
        // SAFETY: shutdown takes scalar arguments and fd remains owned for Drop.
        unsafe {
            libc::shutdown(self.as_raw_fd(), libc::SHUT_RDWR);
        }
    }
}

impl AsRawFd for FramedUnix {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
impl AsFd for FramedUnix {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

fn socket_option(fd: RawFd, option: i32) -> io::Result<i32> {
    let mut value = 0i32;
    let mut size = size_of::<i32>() as libc::socklen_t;
    // SAFETY: value and size are correctly aligned, sized writable scalar buffers.
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            (&mut value as *mut i32).cast(),
            &mut size,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    if size as usize != size_of::<i32>() {
        return Err(invalid("unexpected socket option size"));
    }
    Ok(value)
}

fn peer_credentials(fd: RawFd) -> io::Result<PeerCredentials> {
    // SAFETY: zeroed ucred contains only integer scalar fields.
    let mut credentials: libc::ucred = unsafe { zeroed() };
    let mut size = size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: credentials and size are writable buffers of the advertised sizes.
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut size,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    if size as usize != size_of::<libc::ucred>() || credentials.pid <= 0 {
        return Err(invalid("control socket is not connected"));
    }
    Ok(PeerCredentials {
        pid: credentials.pid,
        uid: credentials.uid,
        gid: credentials.gid,
    })
}

fn receive(fd: RawFd, buffer: &mut [u8], allow_fd: bool) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut control = [0usize; CONTROL_WORDS];
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    // SAFETY: zeroed msghdr has valid null pointers until initialized below.
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = size_of_val(&control);
    // SAFETY: all message pointers refer to live, correctly sized writable buffers.
    let count = unsafe {
        libc::recvmsg(
            fd,
            &mut message,
            libc::MSG_CMSG_CLOEXEC | libc::MSG_DONTWAIT,
        )
    };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    if message.msg_controllen > size_of_val(&control) {
        return Err(invalid("ancillary length exceeds storage"));
    }
    let mut rights = Vec::with_capacity(MAX_RECEIVED_RIGHTS);
    let mut malformed = false;
    // SAFETY: recvmsg initialized the ancillary buffer; CMSG macros walk only
    // msg_controllen bytes. Payload is read unaligned after length validation.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            let Some(offset) = (header as usize).checked_sub(control.as_ptr() as usize) else {
                malformed = true;
                break;
            };
            let Some(remaining) = message.msg_controllen.checked_sub(offset) else {
                malformed = true;
                break;
            };
            if remaining < size_of::<libc::cmsghdr>() {
                malformed = true;
                break;
            }
            let item = &*header;
            let base = libc::CMSG_LEN(0) as usize;
            if item.cmsg_len < base || item.cmsg_len > remaining {
                malformed = true;
                break;
            }
            let length = item.cmsg_len - base;
            if item.cmsg_level == libc::SOL_SOCKET && item.cmsg_type == libc::SCM_RIGHTS {
                if !length.is_multiple_of(size_of::<RawFd>()) {
                    malformed = true;
                }
                for index in 0..length / size_of::<RawFd>() {
                    let raw = libc::CMSG_DATA(header)
                        .add(index * size_of::<RawFd>())
                        .cast::<RawFd>()
                        .read_unaligned();
                    // SCM_RIGHTS installs fresh owned descriptors. Immediate
                    // ownership ensures all are closed on every later reject.
                    rights.push(OwnedFd::from_raw_fd(raw));
                }
            } else {
                malformed = true;
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if malformed
        || message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
        || rights.len() > usize::from(allow_fd)
    {
        return Err(invalid("invalid or truncated control ancillary data"));
    }
    Ok((count as usize, rights.pop()))
}

fn check_deadline(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "control frame deadline elapsed",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct PendingRead {
    header: [u8; 4],
    header_used: usize,
    body: Vec<u8>,
    body_used: usize,
    fd: Option<OwnedFd>,
    deadline: Instant,
}

#[derive(Debug)]
struct PendingWrite {
    bytes: Vec<u8>,
    used: usize,
    fd: Option<OwnedFd>,
    deadline: Instant,
}

/// Nonblocking, cancellation-safe framing for one serialized protocol owner.
/// WouldBlock retains all accepted bytes and rights. Fatal errors close the
/// channel and release pending descriptors. The owner must arm next_deadline()
/// even when no further socket readiness arrives.
#[derive(Debug)]
pub struct IncrementalUnix {
    channel: FramedUnix,
    reading: Option<PendingRead>,
    writing: Option<PendingWrite>,
}

impl IncrementalUnix {
    pub fn from_owned_fd(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            channel: FramedUnix::from_owned_fd(fd)?,
            reading: None,
            writing: None,
        })
    }

    pub fn peer_credentials(&self) -> io::Result<PeerCredentials> {
        self.channel.peer_credentials()
    }

    pub fn write_pending(&self) -> bool {
        self.writing.is_some()
    }

    /// An accepted first byte arms the existing bounded receive deadline.
    pub fn read_pending(&self) -> bool {
        self.reading.is_some()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.reading
            .as_ref()
            .map(|r| r.deadline)
            .into_iter()
            .chain(self.writing.as_ref().map(|w| w.deadline))
            .min()
    }

    pub fn queue_frame(&mut self, body: Vec<u8>, fd: Option<OwnedFd>) -> io::Result<()> {
        self.channel.require_live()?;
        if body.is_empty() || body.len() > CONTROL_BODY_MAX {
            return Err(invalid("invalid control frame length"));
        }
        if self.writing.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "control write already pending",
            ));
        }
        let mut bytes = Vec::with_capacity(4 + body.len());
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        self.writing = Some(PendingWrite {
            bytes,
            used: 0,
            fd,
            deadline: Instant::now() + SEND_DEADLINE,
        });
        Ok(())
    }

    /// Check both frame timers; safe to call from an owner's timer branch.
    pub fn check_deadlines(&mut self) -> io::Result<()> {
        self.channel.require_live()?;
        if let Some(deadline) = self.next_deadline()
            && let Err(error) = check_deadline(deadline)
        {
            self.fail();
            return Err(error);
        }
        Ok(())
    }

    pub fn try_flush(&mut self) -> io::Result<()> {
        self.check_deadlines()?;
        let result = self.flush_inner();
        self.finish_io(result)
    }

    fn flush_inner(&mut self) -> io::Result<()> {
        let socket = self.channel.as_raw_fd();
        let Some(write) = self.writing.as_mut() else {
            return Ok(());
        };
        while write.used < write.bytes.len() {
            check_deadline(write.deadline)?;
            let result = if write.used == 0 {
                send_first(socket, write.bytes[0], write.fd.as_ref().map(AsFd::as_fd))
            } else {
                let bytes = &write.bytes[write.used..];
                // SAFETY: bytes remains a live readable slice through send;
                // nonblocking send never retains this pointer.
                let count = unsafe {
                    libc::send(
                        socket,
                        bytes.as_ptr().cast(),
                        bytes.len(),
                        libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                    )
                };
                if count < 0 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(count as usize)
                }
            };
            match result {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "control frame write failed",
                    ));
                }
                Ok(count) => {
                    write.used += count;
                    write.fd.take();
                    check_deadline(write.deadline)?;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        self.writing = None;
        Ok(())
    }

    pub fn try_receive_frame(&mut self) -> io::Result<ControlFrame> {
        self.check_deadlines()?;
        let result = self.receive_inner();
        self.finish_io(result)
    }

    fn receive_inner(&mut self) -> io::Result<ControlFrame> {
        let socket = self.channel.as_raw_fd();
        if self.reading.is_none() {
            let mut header = [0u8; 4];
            let (count, fd) = receive(socket, &mut header[..1], true)?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "control frame EOF",
                ));
            }
            self.reading = Some(PendingRead {
                header,
                header_used: 1,
                body: Vec::new(),
                body_used: 0,
                fd,
                deadline: Instant::now() + RECEIVE_DEADLINE,
            });
        }
        let read = self
            .reading
            .as_mut()
            .expect("first byte installed read state");
        loop {
            check_deadline(read.deadline)?;
            if read.header_used == 4 && read.body.is_empty() {
                let size = u32::from_be_bytes(read.header) as usize;
                if size == 0 || size > CONTROL_BODY_MAX {
                    return Err(invalid("invalid control frame length"));
                }
                read.body.resize(size, 0);
            }
            if !read.body.is_empty() && read.body_used == read.body.len() {
                break;
            }
            let buffer = if read.header_used < 4 {
                &mut read.header[read.header_used..]
            } else {
                &mut read.body[read.body_used..]
            };
            match receive(socket, buffer, false) {
                Ok((0, _)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "control frame EOF",
                    ));
                }
                Ok((count, _)) => {
                    if read.header_used < 4 {
                        read.header_used += count;
                    } else {
                        read.body_used += count;
                    }
                    check_deadline(read.deadline)?;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        let read = self.reading.take().expect("completed read state");
        Ok(ControlFrame {
            body: read.body,
            fd: read.fd,
        })
    }

    fn finish_io<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if let Err(error) = &result
            && !matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            )
        {
            self.fail();
        }
        result
    }

    fn fail(&mut self) {
        self.channel.close_on_error();
        self.reading = None;
        self.writing = None;
    }
}

impl AsRawFd for IncrementalUnix {
    fn as_raw_fd(&self) -> RawFd {
        self.channel.as_raw_fd()
    }
}
impl AsFd for IncrementalUnix {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.channel.as_fd()
    }
}

fn send_first(socket: RawFd, first: u8, fd: Option<BorrowedFd<'_>>) -> io::Result<usize> {
    let mut control = [0usize; 4];
    let mut iov = libc::iovec {
        iov_base: (&first as *const u8).cast_mut().cast(),
        iov_len: 1,
    };
    // SAFETY: zeroed msghdr has valid null pointers before initialization below.
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    if let Some(fd) = fd {
        message.msg_control = control.as_mut_ptr().cast();
        // SAFETY: CMSG_SPACE/LEN have scalar lengths; aligned control is large
        // enough for one header plus RawFd. Every pointer stays live through sendmsg.
        unsafe {
            message.msg_controllen = libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize;
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize;
            libc::CMSG_DATA(header)
                .cast::<RawFd>()
                .write_unaligned(fd.as_raw_fd());
        }
    }
    // SAFETY: msghdr points to live readable byte/ancillary allocations.
    let count = unsafe { libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL | libc::MSG_DONTWAIT) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(count as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn send_rights(socket: RawFd, fd: RawFd, count: usize) {
        let byte = 0u8;
        let mut control = [0usize; 128];
        let mut iov = libc::iovec {
            iov_base: (&byte as *const u8).cast_mut().cast(),
            iov_len: 1,
        };
        // SAFETY: zeroed msghdr has valid null/zero fields before setup below.
        let mut message: libc::msghdr = unsafe { zeroed() };
        message.msg_iov = &mut iov;
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast();
        // SAFETY: aligned ancillary storage covers at most 80 RawFd entries;
        // header/data writes stay inside that buffer and all pointers stay live.
        unsafe {
            message.msg_controllen = libc::CMSG_SPACE((count * size_of::<RawFd>()) as u32) as usize;
            assert!(message.msg_controllen <= size_of_val(&control));
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN((count * size_of::<RawFd>()) as u32) as usize;
            for index in 0..count {
                libc::CMSG_DATA(header)
                    .add(index * size_of::<RawFd>())
                    .cast::<RawFd>()
                    .write_unaligned(fd);
            }
            assert_eq!(libc::sendmsg(socket, &message, libc::MSG_NOSIGNAL), 1);
        }
    }

    #[test]
    fn extra_rights_and_truncated_ancillary_close_every_received_fd() {
        for count in [2, 80] {
            let (sender, mut receiver) = FramedUnix::pair().unwrap();
            let (stream, mut peer) = UnixStream::pair().unwrap();
            send_rights(sender.as_raw_fd(), stream.as_raw_fd(), count);
            drop(stream);
            assert!(receiver.receive_frame().is_err());
            peer.set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            assert_eq!(
                peer.read(&mut [0]).unwrap(),
                0,
                "all duplicated rights must close on rejection"
            );
        }
    }

    #[test]
    fn incremental_deadline_releases_retained_rights_without_readiness() {
        let (sender, receiver) = FramedUnix::pair().unwrap();
        let mut receiver = IncrementalUnix::from_owned_fd(receiver.fd).unwrap();
        let (stream, mut peer) = UnixStream::pair().unwrap();
        send_rights(sender.as_raw_fd(), stream.as_raw_fd(), 1);
        drop(stream);
        assert_eq!(
            receiver.try_receive_frame().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        receiver.reading.as_mut().unwrap().deadline = Instant::now();
        assert_eq!(
            receiver.check_deadlines().unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        peer.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        assert_eq!(
            peer.read(&mut [0]).unwrap(),
            0,
            "pending received FD must close on deadline"
        );
        assert_eq!(
            receiver.try_receive_frame().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}
