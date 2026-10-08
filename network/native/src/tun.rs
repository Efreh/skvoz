use crate::{invalid, require_nonblocking, set_cloexec};
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

/// One nonpersistent, single-queue Linux TUN carrying complete IP packets.
#[derive(Debug)]
pub struct TunDevice {
    fd: OwnedFd,
    name: String,
    mtu: usize,
}

impl TunDevice {
    /// Create a TUN inside the caller's namespace. Requires CAP_NET_ADMIN.
    /// Only the interface and MTU are changed; addresses/routes belong to the host.
    #[cfg(target_os = "linux")]
    pub fn create(name: &str, mtu: usize) -> io::Result<Self> {
        use std::os::fd::FromRawFd;
        validate_mtu(mtu)?;
        let mut request = interface_request(name)?;
        request.ifr_ifru.ifru_flags = (libc::IFF_TUN | libc::IFF_NO_PI | libc::IFF_TUN_EXCL) as i16;
        let fd: OwnedFd = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open("/dev/net/tun")?
            .into();
        // SAFETY: request is a correctly sized initialized ifreq; fd owns a TUN file.
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETIFF, &mut request) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: TUNSETOFFLOAD accepts scalar flags; zero disables all offloads.
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNSETOFFLOAD, 0) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let device = Self::from_owned_fd(fd, mtu)?;
        let mut request = interface_request(device.name())?;
        request.ifr_ifru.ifru_mtu = mtu as i32;
        // SAFETY: socket takes scalar arguments and returns a uniquely owned FD.
        let socket =
            unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if socket < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful socket returned a new descriptor owned by this scope.
        let socket = unsafe { OwnedFd::from_raw_fd(socket) };
        // SAFETY: SIOCSIFMTU reads the initialized ifreq; socket lives for the call.
        if unsafe { libc::ioctl(socket.as_raw_fd(), libc::SIOCSIFMTU, &request) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(device)
    }

    /// Adopt a nonblocking TUN. Rejection closes this owned descriptor.
    /// O_NONBLOCK is checked, never changed on a caller-shared open-file description.
    pub fn from_owned_fd(fd: OwnedFd, mtu: usize) -> io::Result<Self> {
        validate_mtu(mtu)?;
        require_nonblocking(fd.as_raw_fd())?;
        let mut request = interface_request("unused")?;
        // SAFETY: TUNGETIFF writes only the valid initialized ifreq allocation.
        if unsafe { libc::ioctl(fd.as_raw_fd(), libc::TUNGETIFF, &mut request) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: TUNGETIFF initialized the flags member of the ifreq union.
        let flags = unsafe { request.ifr_ifru.ifru_flags } as i32;
        // TUNGETIFF reports IFF_NOFILTER when no socket filter is installed.
        // It is a kernel status bit, independent of PI/VNET/offload framing.
        if flags != libc::IFF_TUN | libc::IFF_NO_PI | libc::IFF_NOFILTER {
            return Err(invalid(
                "TUN must use exactly IFF_TUN|IFF_NO_PI without offload or multiqueue flags",
            ));
        }
        let bytes: Vec<u8> = request
            .ifr_name
            .iter()
            .take_while(|c| **c != 0)
            .map(|c| *c as u8)
            .collect();
        let name = String::from_utf8(bytes).map_err(|_| invalid("invalid TUN interface name"))?;
        interface_request(&name)?;
        set_cloexec(fd.as_raw_fd())?;
        Ok(Self { fd, name, mtu })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn mtu(&self) -> usize {
        self.mtu
    }
    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }

    /// Read one packet. `buffer` needs MTU+1 bytes to detect oversize/truncation.
    /// WouldBlock is returned without waiting; an idle reader has no deadline.
    pub fn try_read_packet(&self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.len() <= self.mtu {
            return Err(invalid("packet read buffer must exceed MTU"));
        }
        // SAFETY: buffer is exclusively borrowed and valid for its length; fd stays owned.
        let count =
            unsafe { libc::read(self.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        if count == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "TUN closed"));
        }
        if count as usize > self.mtu {
            return Err(invalid("TUN packet exceeds negotiated MTU"));
        }
        Ok(count as usize)
    }

    /// Write exactly one packet. A short write is terminal; no suffix is retried.
    pub fn try_write_packet(&self, packet: &[u8]) -> io::Result<()> {
        if packet.is_empty() || packet.len() > self.mtu {
            return Err(invalid("invalid TUN packet size"));
        }
        // SAFETY: packet is a readable borrowed slice; fd remains owned through write.
        let count = unsafe { libc::write(self.as_raw_fd(), packet.as_ptr().cast(), packet.len()) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        require_atomic_write(count as usize, packet.len())
    }
}

impl AsRawFd for TunDevice {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}
impl AsFd for TunDevice {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

fn validate_mtu(mtu: usize) -> io::Result<()> {
    if !(576..=1500).contains(&mtu) {
        return Err(invalid("TUN MTU must be 576..1500"));
    }
    Ok(())
}

fn interface_request(name: &str) -> io::Result<libc::ifreq> {
    if name.is_empty()
        || name.len() >= libc::IFNAMSIZ
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return Err(invalid("invalid interface name"));
    }
    // SAFETY: all-zero is valid for ifreq, whose union contains scalar fields/pointers.
    let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
    for (dst, src) in request.ifr_name.iter_mut().zip(name.bytes()) {
        *dst = src as libc::c_char;
    }
    Ok(request)
}

fn require_atomic_write(written: usize, expected: usize) -> io::Result<()> {
    if written != expected {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short atomic TUN write",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_names_mtu_and_short_writes() {
        for name in ["", "tun%d", "../tun", "tun\0", "sixteencharacters"] {
            assert!(interface_request(name).is_err());
        }
        assert!(interface_request("skvoz-tun0").is_ok());
        for mtu in [0, 575, 1501, usize::MAX] {
            assert!(validate_mtu(mtu).is_err());
        }
        assert!(require_atomic_write(12, 13).is_err());
        assert!(require_atomic_write(0, 13).is_err());
        assert!(require_atomic_write(13, 13).is_ok());
    }
}
