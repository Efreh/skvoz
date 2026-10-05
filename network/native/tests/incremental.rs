use skvoz_network_native::{IncrementalUnix, duplicate_cloexec};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;

fn pair() -> (UnixStream, IncrementalUnix) {
    let (left, right) = UnixStream::pair().unwrap();
    right.set_nonblocking(true).unwrap();
    (left, IncrementalUnix::from_owned_fd(right.into()).unwrap())
}

#[test]
fn retained_prefix_and_exact_frame_boundary() {
    let (mut peer, mut framed) = pair();
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(framed.next_deadline().is_none());
    peer.write_all(&[0, 0]).unwrap();
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(framed.next_deadline().is_some());
    peer.write_all(&[0, 3, b'a']).unwrap();
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    peer.write_all(b"bc\0\0\0\x01z").unwrap();
    assert_eq!(framed.try_receive_frame().unwrap().body, b"abc");
    assert_eq!(framed.try_receive_frame().unwrap().body, b"z");
    assert!(framed.next_deadline().is_none());
}

#[test]
fn queued_rights_preserve_sender_ownership_and_reject_second_write() {
    let (left, right) = UnixStream::pair().unwrap();
    left.set_nonblocking(true).unwrap();
    right.set_nonblocking(true).unwrap();
    let mut sender = IncrementalUnix::from_owned_fd(left.into()).unwrap();
    let mut receiver = IncrementalUnix::from_owned_fd(right.into()).unwrap();
    let (original, mut other) = UnixStream::pair().unwrap();
    sender
        .queue_frame(
            b"hello".to_vec(),
            Some(duplicate_cloexec(original.as_fd()).unwrap()),
        )
        .unwrap();
    assert_eq!(
        sender.queue_frame(b"x".to_vec(), None).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    sender.try_flush().unwrap();
    assert!(!sender.write_pending());
    let frame = receiver.try_receive_frame().unwrap();
    assert_eq!(frame.body, b"hello");
    drop(frame);
    (&original).write_all(b"x").unwrap();
    let mut byte = [0];
    other.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [b'x']);
}

#[test]
fn eof_midframe_is_terminal() {
    let (mut peer, mut framed) = pair();
    peer.write_all(&[0, 0]).unwrap();
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(peer);
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(framed.next_deadline().is_none());
    assert_eq!(
        framed.try_receive_frame().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}
