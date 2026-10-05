use skvoz_core::{
    Config, Error, Frame, Manager, ManagerConfig, PeerId, PeerLimits, ProtocolError, SendOutcome,
    Stream, StreamKey,
};

fn offer(limits: PeerLimits) -> Frame {
    Frame::Open {
        receive_window: limits.receive_window,
        max_frame: limits.max_frame,
        metadata: Box::new([]),
    }
}

fn accept(limits: PeerLimits) -> Frame {
    Frame::Accept {
        receive_window: limits.receive_window,
        max_frame: limits.max_frame,
        metadata: Box::new([]),
    }
}

#[test]
fn incoming_limits_are_known_before_accept_without_extracting_the_open() {
    let mut stream = Stream::new(Config::default()).unwrap();
    assert_eq!(stream.peer_limits(), None);
    let limits = PeerLimits {
        receive_window: 4096,
        max_frame: 128,
    };
    stream.receive(&offer(limits), 0).unwrap();
    let before = stream.snapshot();
    assert_eq!(stream.peer_limits(), Some(limits));
    assert_eq!(stream.snapshot(), before);
    assert!(
        stream
            .poll_events(1)
            .iter()
            .any(|event| matches!(event, skvoz_core::Event::IncomingOpen { .. }))
    );
    stream.accept(b"").unwrap();
    assert_eq!(stream.peer_limits(), Some(limits));
    assert!(matches!(
        stream.poll_frames(1).as_slice(),
        [Frame::Accept { .. }]
    ));
}

#[test]
fn outgoing_limits_wait_for_accept_and_preserve_small_valid_peer_send_policy() {
    let mut stream = Stream::new(Config::default()).unwrap();
    stream.open(b"", 0).unwrap();
    assert_eq!(stream.peer_limits(), None);
    assert!(matches!(
        stream.poll_frames(1).as_slice(),
        [Frame::Open { .. }]
    ));
    assert_eq!(stream.peer_limits(), None);
    let limits = PeerLimits {
        receive_window: 65536,
        max_frame: 1,
    };
    stream.receive(&accept(limits), 0).unwrap();
    assert_eq!(stream.peer_limits(), Some(limits));
    assert_eq!(stream.send(b"abc").unwrap(), SendOutcome::Accepted(1));
    let before = stream.snapshot();
    assert_eq!(stream.peer_limits(), Some(limits));
    assert_eq!(stream.snapshot(), before);
    assert!(
        matches!(stream.poll_frames(1).as_slice(), [Frame::Data { offset: 0, bytes }] if bytes.as_ref() == b"a")
    );
    assert_eq!(stream.snapshot().send_unacknowledged_bytes, 1);
}

#[test]
fn invalid_open_and_accept_never_publish_unvalidated_limits() {
    let invalid = PeerLimits {
        receive_window: 64,
        max_frame: 65,
    };
    let mut incoming = Stream::new(Config::default()).unwrap();
    assert_eq!(
        incoming.receive(&offer(invalid), 0),
        Err(Error::Protocol(ProtocolError::InvalidLimits))
    );
    assert_eq!(incoming.peer_limits(), None);
    let mut outgoing = Stream::new(Config::default()).unwrap();
    outgoing.open(b"", 0).unwrap();
    outgoing.poll_frames(1);
    assert_eq!(
        outgoing.receive(&accept(invalid), 0),
        Err(Error::Protocol(ProtocolError::InvalidLimits))
    );
    assert_eq!(outgoing.peer_limits(), None);
}

#[test]
fn manager_reports_only_the_requested_stream_without_ownership_changes() {
    let mut manager = Manager::new(ManagerConfig::default()).unwrap();
    manager.register_peer(PeerId(1), false).unwrap();
    let incoming = StreamKey {
        peer: PeerId(1),
        stream_id: 3,
    };
    let unknown = StreamKey {
        peer: PeerId(2),
        stream_id: 3,
    };
    assert_eq!(manager.peer_limits(incoming), None);
    assert_eq!(manager.peer_limits(unknown), None);
    let outgoing = manager.open(PeerId(1), b"", 0).unwrap();
    assert_eq!(manager.peer_limits(outgoing), None);
    let limits = PeerLimits {
        receive_window: 2048,
        max_frame: 64,
    };
    manager.receive(incoming, &offer(limits), 0).unwrap();
    let before = manager.snapshot(incoming);
    let resources = manager.resources();
    assert_eq!(manager.peer_limits(incoming), Some(limits));
    assert_eq!(manager.peer_limits(outgoing), None);
    assert_eq!(manager.peer_limits(unknown), None);
    assert_eq!(manager.snapshot(incoming), before);
    assert_eq!(manager.resources(), resources);
    assert!(
        manager
            .poll_frames(1)
            .iter()
            .any(|frame| frame.key == outgoing && matches!(frame.frame, Frame::Open { .. }))
    );
    assert!(
        manager
            .poll_events(8)
            .iter()
            .any(|event| event.key == incoming
                && matches!(event.event, skvoz_core::Event::IncomingOpen { .. }))
    );
}
