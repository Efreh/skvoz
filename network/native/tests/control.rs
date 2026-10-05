#![cfg(target_os = "linux")]
use skvoz_network_native::{
    CONTROL_BODY_MAX, FramedUnix, TunDevice, duplicate_cloexec, duplicate_inherited,
};
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

#[test]
fn rights_are_cloexec_and_original_ownership_is_retained() {
    let (mut sender, mut receiver) = FramedUnix::pair().unwrap();
    let (mut original, mut peer) = UnixStream::pair().unwrap();
    sender.send_frame(b"hello", Some(original.as_fd())).unwrap();
    let frame = receiver.receive_frame().unwrap();
    assert_eq!(frame.body, b"hello");
    let fd = frame.fd.unwrap();
    // SAFETY: fcntl has only scalar arguments and the descriptor is borrowed.
    assert_ne!(
        unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
        0
    );
    let mut received = UnixStream::from(fd);
    received.write_all(b"first").unwrap();
    let mut bytes = [0; 5];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"first");
    drop(received);
    original.write_all(b"again").unwrap();
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"again");
}

#[test]
fn coalesced_and_split_frames_keep_boundaries() {
    let (mut sender, mut receiver) = FramedUnix::pair().unwrap();
    sender.send_frame(b"one", None).unwrap();
    sender.send_frame(b"two", None).unwrap();
    assert_eq!(receiver.receive_frame().unwrap().body, b"one");
    assert_eq!(receiver.receive_frame().unwrap().body, b"two");
    let (socket, mut peer) = UnixStream::pair().unwrap();
    socket.set_nonblocking(true).unwrap();
    let mut receiver = FramedUnix::from_owned_fd(socket.into()).unwrap();
    let thread = std::thread::spawn(move || {
        for byte in [0, 0, 0, 3, b'a', b'b', b'c'] {
            peer.write_all(&[byte]).unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
    });
    assert_eq!(
        receiver
            .receive_frame_until(Instant::now() + Duration::from_secs(1))
            .unwrap()
            .body,
        b"abc"
    );
    thread.join().unwrap();
}

#[test]
fn idle_is_nonterminal_and_deadline_is_bounded() {
    let (mut sender, mut receiver) = FramedUnix::pair().unwrap();
    assert_eq!(
        receiver.receive_frame().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    sender.send_frame(b"alive", None).unwrap();
    assert_eq!(receiver.receive_frame().unwrap().body, b"alive");
    let start = Instant::now();
    assert_eq!(
        receiver
            .receive_frame_until(start + Duration::from_millis(10))
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(
        receiver.receive_frame().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn expired_deadline_rejects_even_a_fully_ready_frame() {
    let (mut sender, mut receiver) = FramedUnix::pair().unwrap();
    sender.send_frame(b"already ready", None).unwrap();
    let deadline = Instant::now() - Duration::from_millis(1);
    assert_eq!(
        receiver.receive_frame_until(deadline).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(
        receiver.receive_frame().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn reject_invalid_fd_types_and_borrowed_copy_never_closes_original() {
    assert!(duplicate_inherited(-1).is_err());
    let file = File::open("/dev/null").unwrap();
    assert!(FramedUnix::from_owned_fd(duplicate_cloexec(file.as_fd()).unwrap()).is_err());
    assert!(TunDevice::from_owned_fd(duplicate_cloexec(file.as_fd()).unwrap(), 1500).is_err());
    assert!(file.metadata().is_ok());
    let (socket, _peer) = UnixStream::pair().unwrap();
    assert!(FramedUnix::from_owned_fd(socket.into()).is_err());
}

#[test]
fn malformed_lengths_and_eof_make_channel_terminal() {
    for bytes in [
        vec![0, 0, 0, 0],
        ((CONTROL_BODY_MAX + 1) as u32).to_be_bytes().to_vec(),
        vec![0, 0, 0, 2, b'a'],
        vec![0],
    ] {
        let (socket, mut peer) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        let mut receiver = FramedUnix::from_owned_fd(socket.into()).unwrap();
        peer.write_all(&bytes).unwrap();
        drop(peer);
        assert!(receiver.receive_frame().is_err());
        assert_eq!(
            receiver.receive_frame().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }
}

#[test]
fn invalid_outgoing_frame_does_not_poison_channel() {
    let (mut sender, mut receiver) = FramedUnix::pair().unwrap();
    assert!(sender.send_frame(&[], None).is_err());
    assert!(
        sender
            .send_frame(&vec![0; CONTROL_BODY_MAX + 1], None)
            .is_err()
    );
    sender.send_frame(b"ok", None).unwrap();
    assert_eq!(receiver.receive_frame().unwrap().body, b"ok");
}

#[test]
fn late_rights_are_rejected_and_received_copy_closed() {
    let (sender, mut receiver) = FramedUnix::pair().unwrap();
    let mut wire = UnixStream::from(duplicate_cloexec(sender.as_fd()).unwrap());
    wire.write_all(&[0]).unwrap();
    // Valid sender starts its own frame with rights; in receiver this is a
    // forbidden header continuation, regardless of its eventual JSON fd_count.
    let (stream, mut peer) = UnixStream::pair().unwrap();
    let mut sender = sender;
    sender.send_frame(b"bad", Some(stream.as_fd())).unwrap();
    drop(stream);
    assert!(receiver.receive_frame().is_err());
    peer.set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}
