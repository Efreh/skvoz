use skvoz_core::*;
fn config() -> ManagerConfig {
    ManagerConfig {
        stream: Config {
            receive_window: 8,
            max_frame: 4,
            max_pending_frames: 2,
            max_metadata: 8,
            open_timeout_ms: 10,
        },
        max_peers: 3,
        max_streams: 4,
        max_streams_per_peer: 2,
        receive_budget: 32,
        receive_budget_per_peer: 16,
        send_budget: 4,
        send_budget_per_peer: 4,
    }
}
fn manager() -> Manager {
    let mut m = Manager::new(config()).unwrap();
    m.register_peer(PeerId(1), false).unwrap();
    m.register_peer(PeerId(2), false).unwrap();
    m
}
fn establish(m: &mut Manager, p: PeerId) -> StreamKey {
    let k = m.open(p, b"", 0).unwrap();
    let fs = m.poll_frames(256);
    assert!(
        fs.iter()
            .any(|f| f.key == k && matches!(f.frame, Frame::Open { .. }))
    );
    m.receive(
        k,
        &Frame::Accept {
            receive_window: 1024,
            max_frame: 4,
            metadata: Box::new([]),
        },
        0,
    )
    .unwrap();
    m.receive(
        StreamKey {
            peer: p,
            stream_id: 0,
        },
        &Frame::PeerGrant {
            epoch: 0,
            consumed_bytes: 0,
            limit_bytes: 4096,
            consumed_records: 0,
            limit_records: 1024,
            probe: 0,
        },
        0,
    )
    .unwrap();
    m.poll_events(256);
    k
}
#[test]
fn owners_isolate_same_ids_and_global_budget_not_peer_window() {
    let mut m = manager();
    let a = establish(&mut m, PeerId(1));
    let b = establish(&mut m, PeerId(2));
    assert_eq!(a.stream_id, b.stream_id);
    assert_eq!(m.send(a, b"1234").unwrap(), SendOutcome::Accepted(4));
    assert_eq!(m.send(b, b"x").unwrap(), SendOutcome::WouldBlock);
    assert_eq!(m.resources().pending_send_bytes, 4);
    m.poll_frames(1);
    assert!(
        m.poll_events(8)
            .iter()
            .any(|e| e.key == b && e.event == Event::Writable)
    );
    assert_eq!(m.send(b, b"x").unwrap(), SendOutcome::Accepted(1));
    m.receive(
        a,
        &Frame::Data {
            offset: 0,
            bytes: b"a".as_slice().into(),
        },
        0,
    )
    .unwrap();
    m.receive(
        b,
        &Frame::Data {
            offset: 0,
            bytes: b"b".as_slice().into(),
        },
        0,
    )
    .unwrap();
    let events = m.poll_events(8);
    assert!(
        events
            .iter()
            .any(|e| e.key == a && matches!(&e.event,Event::Data{bytes,..} if &**bytes==b"a"))
    );
    assert!(
        events
            .iter()
            .any(|e| e.key == b && matches!(&e.event,Event::Data{bytes,..} if &**bytes==b"b"))
    );
    assert_eq!(m.resources().receive_unconsumed_bytes, 2);
}
#[test]
fn admission_reserves_promises_and_overload_replies_are_finite() {
    let mut m = manager();
    m.open(PeerId(1), b"", 0).unwrap();
    m.open(PeerId(1), b"", 0).unwrap();
    assert_eq!(m.open(PeerId(1), b"", 0), Err(ManagerError::Admission));
    assert_eq!(m.resources().receive_capacity_bytes, 0);
    assert_eq!(m.resources().reserved_receive_bytes, 2);
    for seq in 1..100 {
        m.receive(
            StreamKey {
                peer: PeerId(1),
                stream_id: seq * 2 + 1,
            },
            &Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            0,
        )
        .unwrap();
    }
    assert_eq!(m.resources().pending_rejections, 1);
    assert_eq!(m.resources().streams, 2);
    let frames = m.poll_frames(256);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame, Frame::Reject { .. }))
            .count(),
        1
    );
    m.receive(
        StreamKey {
            peer: PeerId(1),
            stream_id: 3,
        },
        &Frame::Open {
            receive_window: 8,
            max_frame: 4,
            metadata: Box::new([]),
        },
        0,
    )
    .unwrap();
    assert_eq!(m.resources().pending_rejections, 0);
}
#[test]
fn close_churn_reaps_only_after_terminal_work_and_replay_cannot_reopen() {
    let mut m = manager();
    for _ in 0..50 {
        let k = establish(&mut m, PeerId(1));
        m.close(k, CloseReason::Cancelled).unwrap();
        assert_eq!(m.resources().streams, 1);
        m.poll_events(8);
        assert_eq!(m.resources().streams, 1);
        m.poll_frames(8);
        assert_eq!(m.resources().streams, 0);
    }
    let k = StreamKey {
        peer: PeerId(2),
        stream_id: 3,
    };
    let open = Frame::Open {
        receive_window: 8,
        max_frame: 4,
        metadata: Box::new([]),
    };
    m.receive(k, &open, 0).unwrap();
    m.reject(k, b"no").unwrap();
    m.poll_frames(8);
    m.poll_events(8);
    m.receive(k, &open, 0).unwrap();
    assert_eq!(m.resources().streams, 0);
}
#[test]
fn round_robin_is_by_peer_then_stream_and_open_order_survives_abort() {
    let mut m = manager();
    m.poll_frames(256);
    let a = m.open(PeerId(1), b"", 0).unwrap();
    let b = m.open(PeerId(1), b"", 0).unwrap();
    let c = m.open(PeerId(2), b"", 0).unwrap();
    m.close(a, CloseReason::Cancelled).unwrap();
    let frames = m.poll_frames(3);
    assert_eq!(frames.iter().map(|f| f.key).collect::<Vec<_>>(), [a, c, b]);
    assert!(matches!(frames[0].frame, Frame::Close { .. }));
    assert!(matches!(frames[2].frame, Frame::Open { .. }));
}
#[test]
fn deadlines_are_indexed_and_failure_cleans_queued_terminal_frames() {
    let mut m = manager();
    let k = StreamKey {
        peer: PeerId(1),
        stream_id: 3,
    };
    m.receive(
        k,
        &Frame::Open {
            receive_window: 8,
            max_frame: 4,
            metadata: Box::new([]),
        },
        0,
    )
    .unwrap();
    assert_eq!(m.next_deadline(), Some(10));
    m.tick(10).unwrap();
    assert_eq!(m.snapshot(k).unwrap().state, State::Closed);
    m.transport_lost();
    assert!(m.poll_frames(8).is_empty());
    m.poll_events(8);
    assert_eq!(m.resources().streams, 0);
    assert_eq!(m.open(PeerId(1), b"", 10), Err(ManagerError::TransportLost));
}
#[test]
fn receive_capacity_is_lazy_and_bounded_for_irregular_windows() {
    for window in [1, 3, 9, 8193] {
        let c = Config {
            receive_window: window,
            max_frame: 1,
            max_metadata: 0,
            ..Config::default()
        };
        let mut a = Stream::new(c).unwrap();
        assert_eq!(a.snapshot().receive_capacity_bytes, 0);
        a.receive(
            &Frame::Open {
                receive_window: window,
                max_frame: 1,
                metadata: Box::new([]),
            },
            0,
        )
        .unwrap();
        a.accept(b"").unwrap();
        a.poll_frames(1);
        for i in 0..window {
            a.receive(
                &Frame::Data {
                    offset: i as u64,
                    bytes: Box::new([0]),
                },
                0,
            )
            .unwrap();
            assert!(a.snapshot().receive_capacity_bytes <= window as usize);
        }
    }
}

#[test]
fn peer_global_slot_and_receive_limits_are_independent() {
    let mut c = config();
    c.max_peers = 1;
    let mut m = Manager::new(c).unwrap();
    m.register_peer(PeerId(1), false).unwrap();
    assert_eq!(
        m.register_peer(PeerId(2), false),
        Err(ManagerError::Admission)
    );
    assert_eq!(m.resources().peers, 1);

    for (global_slots, peer_slots, global_credit, peer_credit, same_peer) in
        [(1, 4, 64, 64, false), (4, 1, 64, 64, true)]
    {
        let mut c = config();
        c.max_streams = global_slots;
        c.max_streams_per_peer = peer_slots;
        c.receive_budget = global_credit;
        c.receive_budget_per_peer = peer_credit;
        let mut m = Manager::new(c).unwrap();
        m.register_peer(PeerId(1), false).unwrap();
        m.register_peer(PeerId(2), false).unwrap();
        m.open(PeerId(1), b"", 0).unwrap();
        let peer = if same_peer { PeerId(1) } else { PeerId(2) };
        let before = m.resources();
        assert_eq!(m.open(peer, b"", 0), Err(ManagerError::Admission));
        assert_eq!(
            m.resources(),
            before,
            "failed local admission allocated resources"
        );
        m.receive(
            StreamKey { peer, stream_id: 3 },
            &Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            0,
        )
        .unwrap();
        assert_eq!(m.resources().streams, 1);
        assert!(m.resources().reserved_receive_bytes <= global_credit);
        assert_eq!(m.resources().receive_capacity_bytes, 0);
        assert_eq!(m.resources().pending_rejections, 1);
    }
}
#[test]
fn per_peer_send_limit_preserves_other_peer_and_wakes_blocked_stream() {
    let mut c = config();
    c.send_budget = 16;
    c.send_budget_per_peer = 4;
    let mut m = Manager::new(c).unwrap();
    m.register_peer(PeerId(1), false).unwrap();
    m.register_peer(PeerId(2), false).unwrap();
    let a = establish(&mut m, PeerId(1));
    let blocked = establish(&mut m, PeerId(1));
    let independent = establish(&mut m, PeerId(2));
    assert_eq!(m.send(a, b"1234").unwrap(), SendOutcome::Accepted(4));
    assert_eq!(m.send(blocked, b"x").unwrap(), SendOutcome::WouldBlock);
    assert_eq!(
        m.send(independent, b"other").unwrap(),
        SendOutcome::Accepted(4)
    );
    assert_eq!(m.resources().pending_send_bytes, 8);
    assert_eq!(m.poll_frames(1)[0].key, a);
    assert!(
        m.poll_events(8)
            .iter()
            .any(|e| e.key == blocked && e.event == Event::Writable)
    );
    assert_eq!(m.send(blocked, b"x").unwrap(), SendOutcome::Accepted(1));
}
#[test]
fn busy_streams_rotate_within_each_ready_peer_without_idle_work() {
    let mut c = config();
    c.send_budget = 64;
    c.send_budget_per_peer = 32;
    let mut m = Manager::new(c).unwrap();
    for id in 1..=3 {
        m.register_peer(PeerId(id), false).unwrap();
    }
    let a = establish(&mut m, PeerId(1));
    let b = establish(&mut m, PeerId(1));
    let c = establish(&mut m, PeerId(2));
    let d = establish(&mut m, PeerId(2));
    for _ in 0..8 {
        for k in [a, b, c, d] {
            assert_eq!(m.send(k, b"abcdefgh").unwrap(), SendOutcome::Accepted(4));
            assert_eq!(m.send(k, b"efgh").unwrap(), SendOutcome::Accepted(4));
        }
        let frames = m.poll_frames(8);
        assert_eq!(
            frames.iter().map(|f| f.key).collect::<Vec<_>>(),
            [a, c, b, d, a, c, b, d]
        );
        assert_eq!(m.resources().ready_output_peers, 0);
    }
}
