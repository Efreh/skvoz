//! Multi-peer ownership, admission and active-work scheduling without I/O.
use crate::credit::{Amount, ReceiveCredit, SendCredit};
use crate::{
    CloseReason, Config, Error, Event, Frame, MAX_BATCH_EVENTS, PeerLimits, SendOutcome, Snapshot,
    State, Stream,
};
use std::collections::{BTreeMap, BTreeSet, LinkedList, VecDeque};
use std::fmt;

const ACTIVE_KEY_INDEX_BACKING: usize = 384;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PeerId(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StreamKey {
    pub peer: PeerId,
    pub stream_id: u64,
}

#[cfg(test)]
mod credit_integration_tests {
    use super::*;
    fn profile(server: bool) -> ManagerConfig {
        ManagerConfig {
            stream: Config {
                receive_window: crate::MAX_RECEIVE_WINDOW,
                max_frame: 16384,
                max_pending_frames: 128,
                max_metadata: 512,
                open_timeout_ms: 15000,
            },
            max_peers: if server { 128 } else { 1 },
            max_streams: if server { 2048 } else { 512 },
            max_streams_per_peer: 512,
            receive_budget: if server { 128 << 20 } else { 32 << 20 },
            receive_budget_per_peer: 32 << 20,
            send_budget: 64 << 20,
            send_budget_per_peer: 2 << 20,
        }
    }
    fn pair() -> (Manager, Manager) {
        let mut a = Manager::new(profile(false)).unwrap();
        let mut b = Manager::new(profile(true)).unwrap();
        a.register_peer(PeerId(1), false).unwrap();
        b.register_peer(PeerId(1), true).unwrap();
        (a, b)
    }
    fn forward(a: &mut Manager, b: &mut Manager, now: u64) {
        for f in a.poll_frames(256) {
            b.receive(f.key, &f.frame, now).unwrap();
        }
    }
    fn established(a: &mut Manager, b: &mut Manager) -> StreamKey {
        let key = a.open(PeerId(1), b"", 0).unwrap();
        forward(a, b, 0);
        for e in b.poll_events(256) {
            if matches!(e.event, Event::IncomingOpen { .. }) {
                b.accept(e.key, b"").unwrap();
            }
        }
        forward(b, a, 0);
        a.poll_events(256);
        key
    }
    #[test]
    fn own_consumption_grows_a_sole_flow_and_does_not_grow_an_idle_neighbor() {
        let (mut a, mut b) = pair();
        let bulk = established(&mut a, &mut b);
        let neighbor = established(&mut a, &mut b);
        let mut received = 0u64;
        for now in 1..160 {
            for _ in 0..128 {
                if a.send(bulk, &[11; 16384]).unwrap() == SendOutcome::WouldBlock {
                    break;
                }
            }
            forward(&mut a, &mut b, now);
            for event in b.poll_events(256) {
                if let Event::Data { offset, bytes } = event.event {
                    assert_eq!(event.key, bulk);
                    assert_eq!(offset, received);
                    assert!(bytes.iter().all(|byte| *byte == 11));
                    received += bytes.len() as u64;
                    b.consume_through(bulk, received).unwrap();
                }
            }
            forward(&mut b, &mut a, now);
            a.poll_events(256);
        }
        assert!(received > 8 << 20);
        assert!(b.streams[&bulk].stream.receive_window() > 1 << 20);
        assert_eq!(b.streams[&neighbor].stream.receive_window(), 65536);
        assert!(b.pressure_deadlines.is_empty());
        assert!(b.receive_promises.bytes <= b.config.receive_budget as u64);
    }
    fn queued_growth_sample() -> (Manager, StreamKey) {
        let (mut sender, mut receiver) = pair();
        let key = established(&mut sender, &mut receiver);
        for _ in 0..3 {
            assert_eq!(
                sender.send(key, &[7; 16384]),
                Ok(SendOutcome::Accepted(16384))
            );
        }
        forward(&mut sender, &mut receiver, 1);
        receiver.poll_events(256);
        receiver
            .receive(
                StreamKey {
                    peer: key.peer,
                    stream_id: 0,
                },
                &Frame::PeerRequest {
                    bytes: 1048576,
                    records: 64,
                    probe: 0,
                    requester_stream_id: key.stream_id,
                    blocked: 0,
                },
                1,
            )
            .unwrap();
        receiver.peers.get_mut(&key.peer).unwrap().rtt_ms = 2;
        receiver.tick(1000).unwrap();
        (receiver, key)
    }
    #[test]
    fn consumption_growth_excludes_wait_before_own_processing_starts() {
        let (mut receiver, key) = queued_growth_sample();
        receiver.consume_through(key, 16384).unwrap();
        receiver.consume_through(key, 32768).unwrap();
        assert_eq!(receiver.streams[&key].stream.receive_window(), 131072);
        assert!(receiver.receive_promises.bytes <= receiver.config.receive_budget as u64);
    }
    #[test]
    fn consumption_growth_recovers_when_shared_owner_drains_the_stream() {
        let (mut receiver, key) = queued_growth_sample();
        receiver.consume_through(key, 16384).unwrap();
        receiver.tick(1100).unwrap();
        receiver.consume_through(key, 49152).unwrap();
        assert_eq!(receiver.streams[&key].stream.receive_window(), 131072);
        assert_eq!(
            receiver.streams[&key].received,
            receiver.streams[&key].consumed
        );
    }
    #[test]
    fn consumption_growth_still_rejects_slow_backlogged_processing() {
        let (mut receiver, key) = queued_growth_sample();
        receiver.consume_through(key, 16384).unwrap();
        receiver.tick(1100).unwrap();
        receiver.consume_through(key, 32768).unwrap();
        assert_eq!(receiver.streams[&key].stream.receive_window(), 65536);
    }
    #[test]
    fn retained_pending_and_dispatched_records_keep_same_peer_headroom() {
        let (mut a, mut b) = pair();
        let stalled = established(&mut a, &mut b);
        let healthy = established(&mut a, &mut b);
        for _ in 0..56 {
            assert_eq!(a.send(stalled, &[1]), Ok(SendOutcome::Accepted(1)));
        }
        assert_eq!(a.send(stalled, &[1]), Ok(SendOutcome::WouldBlock));
        assert_eq!(a.streams[&stalled].stream.retained_send_records(), 56);
        forward(&mut a, &mut b, 1);
        assert_eq!(a.pending_records, 0);
        assert_eq!(a.send(stalled, &[1]), Ok(SendOutcome::WouldBlock));
        assert_eq!(a.send(healthy, b"healthy"), Ok(SendOutcome::Accepted(7)));
        forward(&mut a, &mut b, 1);
        let mut exact = Vec::new();
        for event in b.poll_events(256) {
            if let Event::Data { offset, bytes } = event.event
                && event.key == healthy
            {
                assert_eq!(offset, 0);
                exact.extend_from_slice(&bytes);
                b.consume_through(healthy, bytes.len() as u64).unwrap();
            }
        }
        assert_eq!(exact, b"healthy");
        assert_eq!(b.peers[&PeerId(1)].rx.outstanding().records, 56);
    }
    #[test]
    fn byte_headroom_progresses_another_stream_and_partial_ack_wakes_own_gate() {
        let (mut a, mut b) = pair();
        let stalled = established(&mut a, &mut b);
        let healthy = established(&mut a, &mut b);
        let mut accepted = 0;
        while let SendOutcome::Accepted(n) = a.send(stalled, &[3; 16384]).unwrap() {
            accepted += n;
        }
        assert_eq!(accepted, 57344);
        assert_eq!(
            a.snapshot(stalled).unwrap().send_unacknowledged_bytes,
            57344
        );
        forward(&mut a, &mut b, 1);
        assert_eq!(a.send(stalled, &[3]), Ok(SendOutcome::WouldBlock));
        assert_eq!(
            a.send(healthy, &[9; 16384]),
            Ok(SendOutcome::Accepted(8192))
        );
        assert_eq!(a.send(healthy, &[9]), Ok(SendOutcome::WouldBlock));
        forward(&mut a, &mut b, 1);
        let mut host_owned = b.poll_events(256);
        assert_eq!(b.peers[&PeerId(1)].rx.outstanding().bytes, 65536);
        assert_eq!(b.peers[&PeerId(1)].rx.outstanding().records, 5);
        // Deliver aggregate ACK before the partial stream ACK deliberately.
        // Keep this fixture's peer allowance unchanged to isolate the own gate.
        let peer = b.peers.get_mut(&PeerId(1)).unwrap();
        peer.requested = peer.target;
        b.consume_through(stalled, 1).unwrap();
        let mut frames = b.poll_frames(256);
        let aggregate = frames
            .iter()
            .position(|f| matches!(f.frame, Frame::PeerGrant { .. }))
            .unwrap();
        let frame = frames.remove(aggregate);
        a.receive(frame.key, &frame.frame, 2).unwrap();
        a.poll_events(256);
        assert_eq!(a.send(stalled, &[3]), Ok(SendOutcome::WouldBlock));
        assert!(a.budget_waiters.contains(&stalled));
        for frame in frames {
            a.receive(frame.key, &frame.frame, 2).unwrap();
        }
        assert!(
            a.poll_events(256)
                .iter()
                .any(|e| e.key == stalled && e.event == Event::Writable)
        );
        assert_eq!(a.send(stalled, &[3]), Ok(SendOutcome::Accepted(1)));
        let exact = host_owned
            .iter()
            .find_map(|e| match &e.event {
                Event::Data { bytes, .. } if e.key == healthy => Some(bytes.as_ref()),
                _ => None,
            })
            .unwrap();
        assert_eq!(exact, &[9; 8192]);
        b.consume_through(healthy, 8192).unwrap();
        b.tick(1002).unwrap();
        assert_eq!(b.snapshot(stalled).unwrap().state, State::Open);
        assert_eq!(b.snapshot(healthy).unwrap().state, State::Open);
        host_owned.clear();
        b.close(stalled, CloseReason::Cancelled).unwrap();
        b.close(healthy, CloseReason::Cancelled).unwrap();
        assert_eq!(b.resources().receive_unconsumed_bytes, 0);
    }
    fn pressure_request(requester: StreamKey, blocked: u8) -> Frame {
        Frame::PeerRequest {
            bytes: 1 << 20,
            records: 128,
            probe: 0,
            requester_stream_id: requester.stream_id,
            blocked,
        }
    }
    #[test]
    fn byte_only_pressure_retires_largest_unread_flow_and_preserves_other_fin() {
        let (mut a, mut b) = pair();
        let largest = established(&mut a, &mut b);
        let smaller = established(&mut a, &mut b);
        let requester = established(&mut a, &mut b);
        for offset in [0, 16384, 32768] {
            b.receive(
                largest,
                &Frame::Data {
                    offset,
                    bytes: vec![3; 16384].into_boxed_slice(),
                },
                1,
            )
            .unwrap();
        }
        b.receive(
            smaller,
            &Frame::Data {
                offset: 0,
                bytes: vec![4; 16384].into_boxed_slice(),
            },
            1,
        )
        .unwrap();
        b.receive(
            smaller,
            &Frame::Fin {
                final_offset: 16384,
            },
            1,
        )
        .unwrap();
        let mut retained = b.poll_events(256);
        assert_eq!(
            b.peers[&largest.peer].rx.outstanding(),
            Amount {
                bytes: 65536,
                records: 4
            }
        );
        b.receive(
            StreamKey {
                peer: largest.peer,
                stream_id: 0,
            },
            &pressure_request(requester, 1),
            100,
        )
        .unwrap();
        b.tick(1099).unwrap();
        assert_eq!(b.snapshot(largest).unwrap().state, State::Open);
        b.tick(1100).unwrap();
        assert_eq!(b.snapshot(largest).unwrap().state, State::Closed);
        assert_eq!(b.snapshot(smaller).unwrap().state, State::HalfClosedRemote);
        assert_eq!(b.snapshot(requester).unwrap().state, State::Open);
        assert_eq!(
            b.peers[&largest.peer].rx.outstanding(),
            Amount {
                bytes: 16384,
                records: 1
            }
        );
        retained.retain(|event| event.key != largest);
        b.receive(
            requester,
            &Frame::Data {
                offset: 0,
                bytes: Box::new(*b"exact"),
            },
            1100,
        )
        .unwrap();
        let exact = b
            .poll_events(256)
            .into_iter()
            .find_map(|event| match event.event {
                Event::Data { offset, bytes } if event.key == requester => {
                    assert_eq!(offset, 0);
                    Some(bytes)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(&*exact, b"exact");
        b.consume_through(requester, 5).unwrap();
        b.consume_through(smaller, 16384).unwrap();
        assert_eq!(b.resources().receive_unconsumed_bytes, 0);
        assert!(b.unconsumed.is_empty());
        assert!(b.receive_promises.bytes <= b.config.receive_budget as u64);
    }
    #[test]
    fn transport_credit_keeps_open_order_without_blocking_smaller_live_frames() {
        let mut config = profile(false);
        config.max_peers = 2;
        let mut sender = Manager::new(config).unwrap();
        let mut receiver = Manager::new(profile(true)).unwrap();
        sender.register_peer(PeerId(1), false).unwrap();
        receiver.register_peer(PeerId(1), true).unwrap();
        let live = established(&mut sender, &mut receiver);
        let keys: Vec<_> = [400, 1, 100]
            .into_iter()
            .map(|size| sender.open(PeerId(1), &vec![7; size], 0).unwrap())
            .collect();
        let cancelled = sender.open(PeerId(1), b"cancel", 0).unwrap();
        sender.close(cancelled, CloseReason::Cancelled).unwrap();
        sender.register_peer(PeerId(2), false).unwrap();
        let other = sender.open(PeerId(2), b"other", 0).unwrap();
        sender.close(other, CloseReason::Cancelled).unwrap();
        assert_eq!(
            sender.send(live, b"tail").unwrap(),
            SendOutcome::Accepted(4)
        );
        sender.finish(live).unwrap();

        let mut attempts = 0;
        let mut emitted = Vec::new();
        for _ in 0..4 {
            emitted.extend(sender.poll_frames_with_budget(256, |_, size, _| {
                attempts += 1;
                size <= 64
            }));
        }
        assert!(attempts <= 4 * (256 + 6 + 2));
        assert!(
            !emitted
                .iter()
                .any(|f| matches!(f.frame, Frame::Open { .. }))
        );
        for key in [cancelled, other] {
            assert!(
                emitted
                    .iter()
                    .any(|f| f.key == key && matches!(f.frame, Frame::Close { .. }))
            );
        }
        assert!(
            emitted
                .iter()
                .any(|f| f.key.peer == PeerId(2) && matches!(f.frame, Frame::PeerGrant { .. }))
        );
        assert!(emitted.iter().any(|f| f.key == live
            && matches!(&f.frame, Frame::Data { bytes, .. } if &**bytes == b"tail")));
        assert!(
            emitted
                .iter()
                .any(|f| f.key == live && matches!(f.frame, Frame::Fin { final_offset: 4 }))
        );
        // A smaller, barred OPEN must not keep a recipient grant open when
        // its remainder cannot carry the oldest eligible OPEN.
        assert_eq!(
            sender.next_transport_weight(PeerId(1), 24),
            Some(24 + 28 + 400)
        );
        for frame in emitted.iter().filter(|f| f.key.peer == PeerId(1)) {
            receiver.receive(frame.key, &frame.frame, 0).unwrap();
        }
        let mut published = Vec::new();
        for _ in 0..3 {
            let frames = sender.poll_frames_with_budget(1, |_, _, _| true);
            assert_eq!(frames.len(), 1);
            assert!(matches!(frames[0].frame, Frame::Open { .. }));
            published.push(frames[0].key);
            receiver
                .receive(frames[0].key, &frames[0].frame, 0)
                .unwrap();
        }
        assert_eq!(published, keys);
        let incoming: Vec<_> = receiver
            .poll_events(256)
            .into_iter()
            .filter(|event| matches!(event.event, Event::IncomingOpen { .. }))
            .map(|event| event.key)
            .collect();
        assert_eq!(incoming, keys);
        for key in &incoming {
            receiver.accept(*key, b"").unwrap();
        }
        forward(&mut receiver, &mut sender, 0);
        assert_eq!(
            sender
                .poll_events(256)
                .into_iter()
                .filter(|event| matches!(event.event, Event::Opened { .. }))
                .count(),
            keys.len()
        );
    }

    #[test]
    fn delivery_frame_gate_preserves_pending_data_other_peer_cancel_and_freeze_order() {
        let mut sender = Manager::new(profile(true)).unwrap();
        let mut keys = Vec::new();
        for peer in [PeerId(1), PeerId(2)] {
            sender.register_peer(peer, false).unwrap();
            let key = sender.open(peer, b"", 0).unwrap();
            sender.poll_frames(256);
            sender
                .receive(
                    key,
                    &Frame::Accept {
                        receive_window: 32 << 20,
                        max_frame: 16384,
                        metadata: Box::new([]),
                    },
                    0,
                )
                .unwrap();
            sender
                .receive(
                    StreamKey { peer, stream_id: 0 },
                    &Frame::PeerGrant {
                        epoch: 0,
                        limit_bytes: 65536,
                        limit_records: 64,
                        consumed_bytes: 0,
                        consumed_records: 0,
                        probe: 0,
                    },
                    0,
                )
                .unwrap();
            sender.poll_events(256);
            keys.push(key);
        }
        sender.poll_frames(256);
        assert_eq!(
            sender.send(keys[0], &[1; 10000]).unwrap(),
            SendOutcome::Accepted(10000)
        );
        let before = sender.snapshot(keys[0]).unwrap();
        sender
            .receive(
                StreamKey {
                    peer: keys[0].peer,
                    stream_id: 0,
                },
                &Frame::PeerFreeze { epoch: 1 },
                1,
            )
            .unwrap();
        sender.close(keys[1], CloseReason::Cancelled).unwrap();
        let mut attempts = 0;
        let terminal = sender.poll_frames_with_budget(1, |_, _, data| {
            attempts += 1;
            !data
        });
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].key, keys[1]);
        assert!(matches!(
            terminal[0].frame,
            Frame::Close {
                reason: CloseReason::Cancelled
            }
        ));
        assert!(
            sender
                .poll_frames_with_budget(8, |_, _, data| {
                    attempts += 1;
                    !data
                })
                .is_empty()
        );
        assert!(attempts <= 1 + 2 + 2 + 8 + 2 + 2);
        assert_eq!(
            sender.snapshot(keys[0]).unwrap().pending_send_bytes,
            before.pending_send_bytes
        );
        assert_eq!(sender.peers[&keys[0].peer].tx.dispatched, Amount::default());
        assert!(!sender.peers[&keys[0].peer].tx.frozen);
        let frames = sender.poll_frames(8);
        assert!(matches!(frames[0].frame, Frame::Data { .. }));
        assert!(matches!(
            frames[1].frame,
            Frame::PeerFrozen {
                bytes: 10000,
                records: 1,
                ..
            }
        ));
        assert_eq!(sender.resources().pending_send_bytes, 0);
    }
    #[test]
    fn framed_demand_survives_data_scheduling_yields_without_a_rejection() {
        let (mut sender, mut recipient) = pair();
        let key = established(&mut sender, &mut recipient);
        sender.poll_frames(256);
        for _ in 0..3 {
            assert_eq!(
                sender.send(key, &[1; 16384]).unwrap(),
                SendOutcome::Accepted(16384)
            );
        }
        let before = sender.transport_demand(key.peer, 540, 3 << 20);
        let first = sender.poll_frames(1).pop().unwrap();
        assert!(matches!(first.frame, Frame::Data { .. }));
        assert!(sender.peers[&key.peer].reject_next);
        assert!(sender.peers[&key.peer].rejection.is_none());
        assert!(sender.next_transport_weight(key.peer, 540).is_some());
        let after = sender.transport_demand(key.peer, 540, 3 << 20);
        assert!(after > 0 && after < before);
        let next = sender.poll_frames(1).pop().unwrap();
        assert!(matches!(next.frame, Frame::Data { .. }));
    }
    #[test]
    fn requester_remote_fin_clears_pressure_without_retiring_unread_neighbor() {
        let (mut a, mut b) = pair();
        let stalled = established(&mut a, &mut b);
        let requester = established(&mut a, &mut b);
        for offset in [0, 16384, 32768, 49152] {
            b.receive(
                stalled,
                &Frame::Data {
                    offset,
                    bytes: vec![3; 16384].into_boxed_slice(),
                },
                1,
            )
            .unwrap();
        }
        let retained = b.poll_events(256);
        let outstanding = b.peers[&stalled.peer].rx.outstanding();
        b.receive(
            StreamKey {
                peer: stalled.peer,
                stream_id: 0,
            },
            &pressure_request(requester, 1),
            100,
        )
        .unwrap();
        assert!(b.peers[&stalled.peer].pressure.is_some());
        b.receive(requester, &Frame::Fin { final_offset: 0 }, 101)
            .unwrap();
        assert_eq!(
            b.snapshot(requester).unwrap().state,
            State::HalfClosedRemote
        );
        b.tick(1100).unwrap();
        assert_eq!(b.snapshot(stalled).unwrap().state, State::Open);
        assert!(b.peers[&stalled.peer].pressure.is_none());
        assert!(b.pressure_deadlines.is_empty());
        assert_eq!(b.peers[&stalled.peer].rx.outstanding(), outstanding);
        b.close(stalled, CloseReason::Cancelled).unwrap();
        b.close(requester, CloseReason::Cancelled).unwrap();
        drop(retained);
        assert_eq!(b.resources().receive_unconsumed_bytes, 0);
    }
    #[test]
    fn multiple_unread_streams_and_changing_requesters_have_a_finite_pressure_deadline() {
        let (mut a, mut b) = pair();
        let first = established(&mut a, &mut b);
        let second = established(&mut a, &mut b);
        let healthy = established(&mut a, &mut b);
        for _ in 0..56 {
            a.send(first, &[1]).unwrap();
        }
        for _ in 0..8 {
            a.send(second, &[2]).unwrap();
        }
        forward(&mut a, &mut b, 1);
        let mut host_owned = b.poll_events(256);
        assert_eq!(b.peers[&PeerId(1)].rx.outstanding().records, 64);
        assert_eq!(
            a.send(healthy, b"after-pressure"),
            Ok(SendOutcome::WouldBlock)
        );
        forward(&mut a, &mut b, 100);
        let control = StreamKey {
            peer: PeerId(1),
            stream_id: 0,
        };
        let deadline = b.peers[&PeerId(1)].pressure.unwrap().deadline;
        for now in 101..200 {
            b.receive(
                control,
                &pressure_request(
                    if now % 2 == 0 { healthy } else { second },
                    if now % 2 == 0 { 2 } else { 1 },
                ),
                now,
            )
            .unwrap();
            assert_eq!(b.peers[&PeerId(1)].pressure.unwrap().deadline, deadline);
        }
        // Keep the real healthy requester excluded from victim selection.
        b.receive(control, &pressure_request(healthy, 2), 200)
            .unwrap();
        assert_eq!(b.peers[&PeerId(1)].pressure.unwrap().blocked, 3);
        b.tick(deadline - 1).unwrap();
        assert_eq!(b.snapshot(first).unwrap().state, State::Open);
        b.tick(deadline).unwrap();
        assert_eq!(b.snapshot(first).unwrap().state, State::Closed);
        assert_eq!(b.snapshot(second).unwrap().state, State::Open);
        assert_eq!(b.snapshot(healthy).unwrap().state, State::Open);
        // The owning host releases cancelled buffers before receiving new ones.
        host_owned.retain(|event| event.key != first);
        forward(&mut b, &mut a, deadline);
        a.poll_events(256);
        assert_eq!(
            a.send(healthy, b"after-pressure"),
            Ok(SendOutcome::Accepted(14))
        );
        forward(&mut a, &mut b, deadline);
        let mut exact = Vec::new();
        for event in b.poll_events(256) {
            if let Event::Data { offset, bytes } = event.event
                && event.key == healthy
            {
                assert_eq!(offset, 0);
                exact.extend_from_slice(&bytes);
                b.consume_through(healthy, bytes.len() as u64).unwrap();
            }
        }
        assert_eq!(exact, b"after-pressure");
        assert_eq!(b.peers[&PeerId(1)].rx.outstanding().records, 8);
        assert!(b.receive_promises.bytes <= b.config.receive_budget as u64);
        assert!(b.receive_promises.records <= b.config.receive_record_budget() as u64);
        drop(host_owned);
        b.close(second, CloseReason::Cancelled).unwrap();
        b.close(healthy, CloseReason::Cancelled).unwrap();
        for _ in 0..4 {
            forward(&mut b, &mut a, deadline);
            forward(&mut a, &mut b, deadline);
            b.poll_events(256);
            a.poll_events(256);
        }
        assert_eq!(b.resources().receive_unconsumed_bytes, 0);
        assert_eq!(b.resources().streams, 0);
        assert!(b.unconsumed.is_empty());
    }
    #[test]
    fn own_request_cannot_retire_a_sole_paused_receiver() {
        let (mut a, mut b) = pair();
        let paused = established(&mut a, &mut b);
        // A legal full incoming peer allowance, independently of local send policy.
        for offset in 0..64 {
            b.receive(
                paused,
                &Frame::Data {
                    offset,
                    bytes: Box::new([7]),
                },
                1,
            )
            .unwrap();
        }
        b.receive(
            StreamKey {
                peer: paused.peer,
                stream_id: 0,
            },
            &pressure_request(paused, 2),
            2,
        )
        .unwrap();
        for now in [1002, 5000, 30000, 60000] {
            b.tick(now).unwrap();
        }
        assert_eq!(b.snapshot(paused).unwrap().state, State::Open);
        assert_eq!(b.peers[&paused.peer].rx.outstanding().records, 64);
        assert!(!b.peers[&paused.peer].failed);
    }
    #[test]
    fn fresh_data_on_an_old_idle_stream_does_not_inherit_stall_age() {
        let (mut a, mut b) = pair();
        let fresh = established(&mut a, &mut b);
        let requester = established(&mut a, &mut b);
        let control = StreamKey {
            peer: fresh.peer,
            stream_id: 0,
        };
        b.receive(control, &pressure_request(requester, 2), 50000)
            .unwrap();
        for offset in 0..64 {
            b.receive(
                fresh,
                &Frame::Data {
                    offset,
                    bytes: Box::new([8]),
                },
                50999,
            )
            .unwrap();
        }
        b.tick(51000).unwrap();
        assert_eq!(b.snapshot(fresh).unwrap().state, State::Open);
        assert_eq!(b.streams[&fresh].last_consumption, 50999);
        assert_eq!(b.streams[&fresh].growth_started, 50999);
        b.tick(51999).unwrap();
        // The rescheduled deadline remains bounded, rather than closing at age1ms.
        b.tick(52000).unwrap();
        assert_eq!(b.snapshot(fresh).unwrap().state, State::Closed);
    }
    #[test]
    fn pressure_request_identity_and_counter_boundaries_are_checked() {
        let (mut a, mut b) = pair();
        let key = established(&mut a, &mut b);
        let control = StreamKey {
            peer: key.peer,
            stream_id: 0,
        };
        let future = StreamKey {
            peer: key.peer,
            stream_id: key.stream_id + 2,
        };
        assert_eq!(
            b.receive(control, &pressure_request(future, 2), 1),
            Err(ManagerError::InvalidOrigin)
        );
        b.close(key, CloseReason::Cancelled).unwrap();
        assert_eq!(b.receive(control, &pressure_request(key, 2), 2), Ok(()));
        assert!(b.pressure_deadlines.is_empty());
        let (mut a, mut b) = pair();
        let key = established(&mut a, &mut b);
        assert_eq!(
            b.receive(control, &pressure_request(key, 2), u64::MAX),
            Err(ManagerError::Stream(Error::InvalidTime))
        );
    }
    #[test]
    fn duplicate_grants_preserve_request_suppression_but_thaw_renews_demand() {
        let (mut a, mut b) = pair();
        let first = established(&mut a, &mut b);
        let second = established(&mut a, &mut b);
        let requester = established(&mut a, &mut b);
        for _ in 0..56 {
            a.send(first, &[1]).unwrap();
        }
        for _ in 0..8 {
            a.send(second, &[2]).unwrap();
        }
        assert_eq!(a.send(requester, &[3]), Ok(SendOutcome::WouldBlock));
        forward(&mut a, &mut b, 1);
        let suppressed = a.peers[&requester.peer].last_request;
        assert!(suppressed.is_some());
        let control = StreamKey {
            peer: requester.peer,
            stream_id: 0,
        };
        a.receive(control, &b.peers[&requester.peer].rx.frame(0), 2)
            .unwrap();
        assert_eq!(a.peers[&requester.peer].last_request, suppressed);
        assert_eq!(a.send(requester, &[3]), Ok(SendOutcome::WouldBlock));
        assert!(
            !a.poll_frames(256)
                .iter()
                .any(|f| matches!(f.frame, Frame::PeerRequest { .. }))
        );
        let freeze = b
            .peers
            .get_mut(&requester.peer)
            .unwrap()
            .rx
            .freeze()
            .unwrap();
        a.receive(control, &freeze, 3).unwrap();
        let frozen = a
            .poll_frames(256)
            .into_iter()
            .find(|f| matches!(f.frame, Frame::PeerFrozen { .. }))
            .unwrap();
        // An old demand can expire while the direction is frozen. No arrived
        // DATA is freed, and the delayed ordered ACK retains the same totals.
        b.tick(1001).unwrap();
        assert!(b.peers[&requester.peer].pressure.is_none());
        b.receive(frozen.key, &frozen.frame, 1002).unwrap();
        a.receive(control, &b.peers[&requester.peer].rx.frame(0), 1002)
            .unwrap();
        assert!(a.peers[&requester.peer].last_request.is_none());
        assert_eq!(a.send(requester, &[3]), Ok(SendOutcome::WouldBlock));
        assert!(a.poll_frames(256).iter().any(|f|matches!(f.frame,
            Frame::PeerRequest{requester_stream_id,blocked:2,..} if requester_stream_id==requester.stream_id)));
        assert_eq!(b.peers[&requester.peer].rx.outstanding().records, 64);
        assert!(b.receive_promises.records <= b.config.receive_record_budget() as u64);
    }
    #[test]
    fn bounded_event_admission_retains_burst_and_progresses_other_streams() {
        let (mut a, mut b) = pair();
        let ip = established(&mut a, &mut b);
        let tcp = established(&mut a, &mut b);
        let terminal = established(&mut a, &mut b);
        // Actual consumption grows the adaptive stream/peer window first.
        let mut prefix = 0;
        for now in 1..24 {
            for _ in 0..128 {
                match a.send(ip, &[7; 16384]).unwrap() {
                    SendOutcome::Accepted(n) => prefix += n as u64,
                    SendOutcome::WouldBlock => break,
                }
            }
            forward(&mut a, &mut b, now);
            for event in b.poll_events(256) {
                if let Event::Data { offset, bytes } = event.event {
                    b.consume_through(event.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
            forward(&mut b, &mut a, now);
            a.poll_events(256);
        }
        for _ in 0..8 {
            assert_eq!(a.send(ip, &[9; 16384]), Ok(SendOutcome::Accepted(16384)));
        }
        assert_eq!(a.send(tcp, b"healthy"), Ok(SendOutcome::Accepted(7)));
        a.close(terminal, CloseReason::Cancelled).unwrap();
        forward(&mut a, &mut b, 24);
        let mut room = 65536;
        let events = b.poll_events_with_data_budget(256, |key, bytes| {
            if key != ip {
                return true;
            }
            if bytes > room {
                return false;
            }
            room -= bytes;
            true
        });
        assert_eq!(room, 0);
        assert!(events.iter().any(|e| e.key == tcp
            && matches!(&e.event, Event::Data { bytes, .. } if &**bytes == b"healthy")));
        assert!(
            events
                .iter()
                .any(|e| e.key == terminal && matches!(e.event, Event::Closed { .. }))
        );
        let mut received = 0;
        for event in events {
            if event.key == ip
                && let Event::Data { offset, bytes } = event.event
            {
                assert_eq!(offset, prefix + received);
                assert!(bytes.iter().all(|byte| *byte == 9));
                received += bytes.len() as u64;
                b.consume_through(ip, offset + bytes.len() as u64).unwrap();
            }
        }
        assert_eq!(received, 65536);
        let mut attempts = 0;
        assert!(
            b.poll_events_with_data_budget(8, |_, _| {
                attempts += 1;
                false
            })
            .is_empty()
        );
        assert!(attempts > 0 && attempts <= 8 + b.resources().streams);
        assert_eq!(b.snapshot(ip).unwrap().buffered_receive_bytes, 65536);
        for event in b.poll_events_with_data_budget(256, |_, _| true) {
            if event.key == ip
                && let Event::Data { offset, bytes } = event.event
            {
                assert_eq!(offset, prefix + received);
                assert!(bytes.iter().all(|byte| *byte == 9));
                received += bytes.len() as u64;
                b.consume_through(ip, offset + bytes.len() as u64).unwrap();
            }
        }
        assert_eq!(received, 131072);
    }
    #[test]
    fn smaller_shared_pool_leaves_active_growth_and_backing_for_future_peers() {
        let mut config = profile(true);
        config.stream.receive_window = 1 << 20;
        config.receive_budget = 8 << 20;
        config.receive_budget_per_peer = 1 << 20;
        let mut client_config = config;
        client_config.max_peers = 1;
        let mut a = Manager::new(client_config).unwrap();
        let mut b = Manager::new(config).unwrap();
        a.register_peer(PeerId(1), false).unwrap();
        b.register_peer(PeerId(1), true).unwrap();
        assert_eq!(b.minimum_credit().bytes, 8192);
        let key = a.open(PeerId(1), b"", 0).unwrap();
        forward(&mut a, &mut b, 0);
        for event in b.poll_events(256) {
            if matches!(event.event, Event::IncomingOpen { .. }) {
                b.accept(event.key, b"").unwrap();
            }
        }
        forward(&mut b, &mut a, 0);
        a.poll_events(256);
        let mut accepted = 0u64;
        let mut received = 0u64;
        for now in 1..8 {
            b.receive(
                StreamKey {
                    peer: PeerId(1),
                    stream_id: 0,
                },
                &Frame::PeerRequest {
                    bytes: 1 << 20,
                    records: 128,
                    probe: 0,
                    requester_stream_id: 2,
                    blocked: 0,
                },
                now,
            )
            .unwrap();
            let SendOutcome::Accepted(count) = a.send(key, &[0; 8192]).unwrap() else {
                panic!("consuming stream made no progress");
            };
            assert!((1..=8192).contains(&count));
            accepted += count as u64;
            forward(&mut a, &mut b, now);
            for event in b.poll_events(256) {
                if let Event::Data { offset, bytes } = event.event {
                    assert_eq!(event.key, key);
                    assert_eq!(offset, received);
                    assert!(bytes.iter().all(|byte| *byte == 0));
                    received += bytes.len() as u64;
                    b.consume_through(event.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
            forward(&mut b, &mut a, now);
            a.poll_events(256);
        }
        assert_eq!(received, accepted);
        assert_eq!(b.peers[&PeerId(1)].target.bytes, 1 << 20);
        b.register_peer(PeerId(2), true).unwrap();
        assert_eq!(b.peers[&PeerId(2)].reserved_receive.bytes, 8192);
        assert_eq!(b.peers[&PeerId(2)].reserved_receive.records, 64);
        assert!(b.receive_promises.bytes <= config.receive_budget as u64);
        assert!(b.receive_promises.records <= config.receive_record_budget() as u64);
    }
    #[test]
    fn idle_count_does_not_reserve_maximum_windows_and_bulk_grows_automatically() {
        let (mut a, mut b) = pair();
        let mut keys = Vec::new();
        for _ in 0..512 {
            keys.push(a.open(PeerId(1), b"", 0).unwrap());
        }
        assert_eq!(a.open(PeerId(1), b"", 0), Err(ManagerError::Admission));
        assert_eq!(a.resources().reserved_receive_bytes, 65536);
        assert_eq!(a.resources().receive_capacity_bytes, 0);
        for _ in 0..8 {
            forward(&mut a, &mut b, 0);
            for e in b.poll_events(256) {
                if matches!(e.event, Event::IncomingOpen { .. }) {
                    b.accept(e.key, b"").unwrap();
                }
            }
            forward(&mut b, &mut a, 0);
            a.poll_events(256);
        }
        assert_eq!(b.resources().streams, 512);
        let key = keys[0];
        let block = [37; 16384];
        let mut sent = 0;
        let mut received = 0;
        for now in 1..1000 {
            for _ in 0..32 {
                if sent >= 8 << 20 {
                    break;
                }
                match a
                    .send(key, &block[..block.len().min((8 << 20) - sent)])
                    .unwrap()
                {
                    SendOutcome::Accepted(n) => sent += n,
                    SendOutcome::WouldBlock => break,
                }
            }
            forward(&mut a, &mut b, now);
            for e in b.poll_events(256) {
                if let Event::Data { offset, bytes } = e.event {
                    assert_eq!(offset as usize, received);
                    assert!(bytes.iter().all(|b| *b == 37));
                    received += bytes.len();
                    b.consume_through(e.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
            forward(&mut b, &mut a, now);
            a.poll_events(256);
            assert!(b.receive_promises.bytes <= b.config.receive_budget as u64);
            assert!(b.receive_promises.records <= b.config.receive_record_budget() as u64);
            if received == 8 << 20 {
                break;
            }
        }
        assert_eq!(received, 8 << 20);
        assert_eq!(sent, received);
        assert!(b.peers[&PeerId(1)].target.bytes >= 1 << 20);
        assert_eq!(b.resources().buffered_receive_bytes, 0);
        assert_eq!(b.resources().receive_capacity_bytes, 0);
    }
    #[test]
    fn duplex_consumption_does_not_add_already_queued_send_bytes_again() {
        let (mut a, mut b) = pair();
        let key = established(&mut a, &mut b);
        assert_eq!(a.send(key, b"outbound"), Ok(SendOutcome::Accepted(8)));
        assert_eq!(b.send(key, b"inbound"), Ok(SendOutcome::Accepted(7)));
        forward(&mut b, &mut a, 1);
        let data = a
            .poll_events(256)
            .into_iter()
            .find_map(|e| match e.event {
                Event::Data { offset, bytes } => Some((offset, bytes)),
                _ => None,
            })
            .unwrap();
        a.consume_through(key, data.0 + data.1.len() as u64)
            .unwrap();
        assert_eq!(a.resources().pending_send_bytes, 8);
        assert_eq!(a.peers[&PeerId(1)].tx.pending, Amount::data(8));
        forward(&mut a, &mut b, 1);
        assert_eq!(a.resources().pending_send_bytes, 0);
        assert_eq!(a.pending_records, 0);
    }
    #[test]
    fn late_cancelled_data_is_accounted_without_recycling_unused_permission() {
        let (mut a, mut b) = pair();
        let key = established(&mut a, &mut b);
        a.send(key, b"late").unwrap();
        let data = a
            .poll_frames(256)
            .into_iter()
            .find(|f| matches!(f.frame, Frame::Data { .. }))
            .unwrap();
        let promised = b.receive_promises;
        b.close(key, CloseReason::Cancelled).unwrap();
        b.poll_events(256);
        b.poll_frames(256);
        assert_eq!(b.resources().streams, 0);
        assert_eq!(b.receive_promises, promised);
        b.receive(data.key, &data.frame, 1).unwrap();
        let p = &b.peers[&PeerId(1)];
        assert_eq!(p.rx.received, Amount::data(4));
        assert_eq!(p.rx.consumed, Amount::data(4));
        assert_eq!(b.receive_promises, promised);
        assert_eq!(b.resources().receive_capacity_bytes, 0);
    }
    #[test]
    fn all_registered_idle_peers_leave_record_and_byte_headroom_for_bulk() {
        let mut b = Manager::new(profile(true)).unwrap();
        for id in 1..=128 {
            b.register_peer(PeerId(id), true).unwrap();
        }
        assert_eq!(b.receive_promises.bytes, 8 << 20);
        assert_eq!(b.receive_promises.records, 8192);
        assert!(b.config.receive_record_budget() >= 16384);
        let mut a = Manager::new(profile(false)).unwrap();
        a.register_peer(PeerId(1), false).unwrap();
        let key = a.open(PeerId(1), b"", 0).unwrap();
        forward(&mut a, &mut b, 0);
        b.accept(key, b"").unwrap();
        for f in b.poll_frames(256) {
            if f.key.peer == PeerId(1) {
                a.receive(f.key, &f.frame, 0).unwrap();
            }
        }
        for now in 1..12 {
            b.receive(
                StreamKey {
                    peer: PeerId(1),
                    stream_id: 0,
                },
                &Frame::PeerRequest {
                    bytes: 32 << 20,
                    records: 2112,
                    probe: 0,
                    requester_stream_id: 2,
                    blocked: 0,
                },
                now,
            )
            .unwrap();
            assert_eq!(a.send(key, &[0; 16384]), Ok(SendOutcome::Accepted(16384)));
            forward(&mut a, &mut b, now);
            for e in b.poll_events(256) {
                if let Event::Data { offset, bytes } = e.event {
                    b.consume_through(e.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
            for f in b.poll_frames(256) {
                if f.key.peer == PeerId(1) {
                    a.receive(f.key, &f.frame, now).unwrap();
                }
            }
            a.poll_events(256);
        }
        assert_eq!(b.peers[&PeerId(1)].target.bytes, 32 << 20);
        assert!(b.peers[&PeerId(1)].target.records > 64);
        assert!(b.receive_promises.bytes <= 128 << 20);
        assert!(b.receive_promises.records <= b.config.receive_record_budget() as u64);
    }

    #[test]
    fn output_readiness_holds_grants_data_and_frozen_ack_without_extraction() {
        let (mut a, mut b) = pair();
        a.set_peer_output_enabled(PeerId(1), false).unwrap();
        let key = a.open(PeerId(1), b"", 0).unwrap();
        assert!(a.poll_frames(256).is_empty());
        assert_eq!(a.receive_promises.bytes, 65536);
        a.set_peer_output_enabled(PeerId(1), true).unwrap();
        forward(&mut a, &mut b, 0);
        b.accept(key, b"").unwrap();
        forward(&mut b, &mut a, 0);
        a.poll_events(256);
        a.set_peer_output_enabled(PeerId(1), false).unwrap();
        a.receive(
            StreamKey {
                peer: PeerId(1),
                stream_id: 0,
            },
            &Frame::PeerFreeze { epoch: 1 },
            1,
        )
        .unwrap();
        assert!(a.poll_frames(256).is_empty());
        a.set_peer_output_enabled(PeerId(1), true).unwrap();
        assert!(
            a.poll_frames(256)
                .iter()
                .any(|f| matches!(f.frame, Frame::PeerFrozen { epoch: 1, .. }))
        );
    }

    #[test]
    fn dispatched_send_metadata_is_bounded_with_withheld_ack_and_other_peer_progress() {
        let mut config = profile(true);
        config.receive_budget = 4 << 20;
        config.max_peers = 2;
        for late_join in [false, true] {
            let mut a = Manager::new(config).unwrap();
            a.register_peer(PeerId(1), false).unwrap();
            if !late_join {
                a.register_peer(PeerId(2), false).unwrap();
            }
            let mut keys = Vec::new();
            let setup = |a: &mut Manager, peer| {
                let key = a.open(peer, b"", 0).unwrap();
                a.poll_frames(256);
                a.receive(
                    key,
                    &Frame::Accept {
                        receive_window: 32 << 20,
                        max_frame: 16384,
                        metadata: Box::new([]),
                    },
                    0,
                )
                .unwrap();
                a.receive(
                    StreamKey { peer, stream_id: 0 },
                    &Frame::PeerGrant {
                        epoch: 0,
                        consumed_bytes: 0,
                        limit_bytes: 32 << 20,
                        consumed_records: 0,
                        limit_records: 65536,
                        probe: 0,
                    },
                    0,
                )
                .unwrap();
                a.poll_events(256);
                key
            };
            keys.push(setup(&mut a, PeerId(1)));
            if !late_join {
                keys.push(setup(&mut a, PeerId(2)));
            }
            let mut dispatched = 0;
            loop {
                match a.send(keys[0], b"x").unwrap() {
                    SendOutcome::Accepted(1) => {
                        dispatched += 1;
                        a.poll_frames(256);
                    }
                    SendOutcome::WouldBlock => break,
                    other => panic!("unexpected send {other:?}"),
                }
            }
            assert_eq!(a.pending_records, 0);
            assert_eq!(a.retained_send_records, dispatched);
            assert!(dispatched <= a.config.send_record_budget() - 64);
            if late_join {
                a.register_peer(PeerId(2), false).unwrap();
                keys.push(setup(&mut a, PeerId(2)));
            }
            assert_eq!(a.send(keys[1], b"healthy"), Ok(SendOutcome::Accepted(7)));
            a.poll_frames(256);
            assert!(a.retained_send_records <= a.config.send_record_budget());
            a.close(keys[0], CloseReason::Cancelled).unwrap();
            assert_eq!(a.retained_send_records, 1);
            assert_eq!(a.peers[&PeerId(1)].tx.dispatched.bytes, dispatched as u64);
        }
    }

    #[test]
    fn asymmetric_demand_uses_free_pool_then_reclaims_for_a_new_consuming_peer() {
        let mut config = profile(true);
        config.max_peers = 4;
        config.receive_budget = 32 << 20;
        let mut server = Manager::new(config).unwrap();
        let mut clients = BTreeMap::new();
        let mut keys = BTreeMap::new();
        fn drive(server: &mut Manager, clients: &mut BTreeMap<PeerId, Manager>, now: u64) {
            for client in clients.values_mut() {
                forward(client, server, now);
            }
            for event in server.poll_events(256) {
                match event.event {
                    Event::IncomingOpen { .. } => server.accept(event.key, b"").unwrap(),
                    Event::Data { offset, bytes } => server
                        .consume_through(event.key, offset + bytes.len() as u64)
                        .unwrap(),
                    _ => {}
                }
            }
            for frame in server.poll_frames(256) {
                clients
                    .get_mut(&frame.key.peer)
                    .unwrap()
                    .receive(frame.key, &frame.frame, now)
                    .unwrap();
            }
            for client in clients.values_mut() {
                client.poll_events(256);
            }
        }
        for id in 1..=3 {
            let peer = PeerId(id);
            server.register_peer(peer, true).unwrap();
            let mut client = Manager::new(profile(false)).unwrap();
            client.register_peer(peer, false).unwrap();
            keys.insert(peer, client.open(peer, b"", 0).unwrap());
            clients.insert(peer, client);
        }
        drive(&mut server, &mut clients, 0);
        for now in 1..=6 {
            for id in 1..=3 {
                let peer = PeerId(id);
                assert_eq!(
                    clients
                        .get_mut(&peer)
                        .unwrap()
                        .send(keys[&peer], &[7; 16384]),
                    Ok(SendOutcome::Accepted(16384))
                );
            }
            drive(&mut server, &mut clients, now);
            for id in 1..=3 {
                server
                    .receive(
                        StreamKey {
                            peer: PeerId(id),
                            stream_id: 0,
                        },
                        &Frame::PeerRequest {
                            bytes: if id == 3 { 32 << 20 } else { 128 << 10 },
                            records: if id == 3 { 2112 } else { 64 },
                            probe: 0,
                            requester_stream_id: 2,
                            blocked: 0,
                        },
                        now,
                    )
                    .unwrap();
            }
            drive(&mut server, &mut clients, now);
        }
        assert!(server.peers[&PeerId(3)].target.bytes > 16 << 20);
        assert_eq!(server.peers[&PeerId(1)].target.bytes, 128 << 10);
        assert_eq!(server.peers[&PeerId(2)].target.bytes, 128 << 10);
        assert!(server.receive_promises.bytes <= config.receive_budget as u64 - 65536);
        let peer = PeerId(4);
        server.register_peer(peer, true).unwrap();
        let mut client = Manager::new(profile(false)).unwrap();
        client.register_peer(peer, false).unwrap();
        let key = client.open(peer, b"", 7).unwrap();
        clients.insert(peer, client);
        drive(&mut server, &mut clients, 7);
        clients
            .get_mut(&peer)
            .unwrap()
            .send(key, &[8; 16384])
            .unwrap();
        drive(&mut server, &mut clients, 8);
        server
            .receive(
                StreamKey { peer, stream_id: 0 },
                &Frame::PeerRequest {
                    bytes: 1 << 20,
                    records: 128,
                    probe: 0,
                    requester_stream_id: 2,
                    blocked: 0,
                },
                8,
            )
            .unwrap();
        for now in 9..=12 {
            drive(&mut server, &mut clients, now);
        }
        assert!(server.peers[&peer].target.bytes > 65536);
        assert_eq!(server.peers[&PeerId(3)].rx.epoch, 1);
        assert!(server.peers.values().all(|peer| peer.rx.freezing.is_none()));
        assert!(server.receive_promises.bytes <= config.receive_budget as u64);
    }
    #[test]
    fn two_saturated_peers_reclaim_idle_allowance_then_progress_without_epoch_thrash() {
        let mut config = profile(true);
        config.max_peers = 2;
        config.receive_budget = 32 << 20;
        let mut server = Manager::new(config).unwrap();
        let mut clients = Vec::new();
        let mut keys = Vec::new();
        for id in 1..=2 {
            server.register_peer(PeerId(id), true).unwrap();
            let mut client = Manager::new(profile(false)).unwrap();
            client.register_peer(PeerId(id), false).unwrap();
            let key = client.open(PeerId(id), b"", 0).unwrap();
            forward(&mut client, &mut server, 0);
            server.accept(key, b"").unwrap();
            keys.push(key);
            clients.push(client);
        }
        for frame in server.poll_frames(256) {
            clients[frame.key.peer.0 as usize - 1]
                .receive(frame.key, &frame.frame, 0)
                .unwrap();
        }
        for client in &mut clients {
            client.poll_events(256);
        }
        let mut received = [0u64; 2];
        for now in 1..80 {
            let active = if now < 12 { 1 } else { 2 };
            for i in 0..active {
                let peer = PeerId(i as u64 + 1);
                server
                    .receive(
                        StreamKey { peer, stream_id: 0 },
                        &Frame::PeerRequest {
                            bytes: 32 << 20,
                            records: 2112,
                            probe: 0,
                            requester_stream_id: 2,
                            blocked: 0,
                        },
                        now,
                    )
                    .unwrap();
                match clients[i].send(keys[i], &[42; 16384]).unwrap() {
                    SendOutcome::Accepted(_) | SendOutcome::WouldBlock => {}
                }
                forward(&mut clients[i], &mut server, now);
            }
            for event in server.poll_events(256) {
                if let Event::Data { offset, bytes } = event.event {
                    let i = event.key.peer.0 as usize - 1;
                    assert_eq!(offset, received[i]);
                    received[i] += bytes.len() as u64;
                    server
                        .consume_through(event.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
            for frame in server.poll_frames(256) {
                clients[frame.key.peer.0 as usize - 1]
                    .receive(frame.key, &frame.frame, now)
                    .unwrap();
            }
            // A frozen donor may need a turn even when it submitted no new DATA.
            for client in &mut clients {
                forward(client, &mut server, now);
                client.poll_events(256);
            }
            assert!(server.receive_promises.bytes <= 32 << 20);
            assert!(
                server.receive_promises.records <= server.config.receive_record_budget() as u64
            );
        }
        assert!(received[0] > 1 << 20);
        assert!(received[1] > 512 << 10);
        for peer in [PeerId(1), PeerId(2)] {
            assert!(
                server.peers[&peer].target.bytes >= 1 << 20,
                "peer={peer:?} target={:?} epoch={} freezing={:?} consumed={} growth={} requested={:?}",
                server.peers[&peer].target,
                server.peers[&peer].rx.epoch,
                server.peers[&peer].rx.freezing,
                server.peers[&peer].rx.consumed.bytes,
                server.peers[&peer].growth_consumed,
                server.peers[&peer].requested
            );
            assert!(
                server.peers[&peer].rx.epoch <= 2,
                "unexpected repeated reclamation peer={peer:?} epoch={} target={:?} other={:?}",
                server.peers[&peer].rx.epoch,
                server.peers[&peer].target,
                server.peers[&PeerId(3 - peer.0)].target,
            );
            assert!(server.peers[&peer].rx.freezing.is_none());
        }
    }

    #[test]
    fn failed_barrier_deadline_and_counter_boundaries_retire_only_the_affected_peer() {
        let (mut a, mut b) = pair();
        let key = established(&mut a, &mut b);
        let frame = b.peers.get_mut(&PeerId(1)).unwrap().rx.freeze().unwrap();
        b.peers.get_mut(&PeerId(1)).unwrap().freeze_deadline = Some(10);
        b.queue_control(PeerId(1), frame);
        b.tick(10).unwrap();
        assert!(b.peers[&PeerId(1)].failed);
        assert_eq!(b.receive_promises, Amount::default());
        assert_eq!(b.snapshot(key).unwrap().state, State::Closed);

        let mut b = Manager::new(profile(true)).unwrap();
        b.register_peer(PeerId(1), true).unwrap();
        b.register_peer(PeerId(2), true).unwrap();
        b.now = 2000;
        let p = b.peers.get_mut(&PeerId(1)).unwrap();
        p.rx.epoch = u64::MAX;
        p.target.bytes = 1 << 20;
        let before = p.reserved_receive;
        p.rx.grant(p.target).unwrap();
        p.reserved_receive = p.rx.promise();
        b.receive_promises = b
            .receive_promises
            .sub(before)
            .unwrap()
            .add(p.reserved_receive)
            .unwrap();
        b.peers.get_mut(&PeerId(2)).unwrap().requested.bytes = 1 << 20;
        b.reclaim_credit(PeerId(2));
        assert!(b.peers[&PeerId(1)].failed);
        assert!(!b.peers[&PeerId(2)].failed);

        let p = b.peers.get_mut(&PeerId(2)).unwrap();
        let before = p.reserved_receive;
        p.rx.received.bytes = u64::MAX - 1;
        p.rx.consumed.bytes = u64::MAX - 1;
        p.rx.allowed.bytes = u64::MAX;
        p.reserved_receive = Amount {
            bytes: 65536,
            records: 64,
        };
        p.requested.bytes = 1 << 20;
        b.receive_promises = b
            .receive_promises
            .sub(before)
            .unwrap()
            .add(p.reserved_receive)
            .unwrap();
        b.grow_credit();
        assert!(b.peers[&PeerId(2)].failed);
        assert_eq!(b.receive_promises, Amount::default());
    }
}
#[derive(Debug)]
pub struct ManagedEvent {
    pub key: StreamKey,
    pub event: Event,
}
#[derive(Debug)]
pub struct RoutedFrame {
    pub key: StreamKey,
    pub frame: Frame,
}

#[derive(Clone, Copy, Debug)]
pub struct ManagerConfig {
    pub stream: Config,
    pub max_peers: usize,
    pub max_streams: usize,
    pub max_streams_per_peer: usize,
    pub receive_budget: usize,
    pub receive_budget_per_peer: usize,
    pub send_budget: usize,
    pub send_budget_per_peer: usize,
}
impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            stream: Config {
                receive_window: 8192,
                max_frame: 1024,
                max_pending_frames: 8,
                max_metadata: 256,
                open_timeout_ms: 5000,
            },
            max_peers: 128,
            max_streams: 8192,
            max_streams_per_peer: 128,
            receive_budget: 64 * 1024 * 1024,
            receive_budget_per_peer: 2 * 1024 * 1024,
            send_budget: 8 * 1024 * 1024,
            send_budget_per_peer: 256 * 1024,
        }
    }
}
impl ManagerConfig {
    pub fn receive_record_budget(self) -> usize {
        (self.receive_budget / self.stream.max_frame as usize)
            .saturating_add(self.max_peers.saturating_mul(64))
    }
    pub fn receive_records_per_peer(self) -> usize {
        (self.receive_budget_per_peer / self.stream.max_frame as usize)
            .saturating_add(64)
            .min(65536)
    }
    pub fn send_record_budget(self) -> usize {
        self.receive_record_budget()
    }
    pub fn send_records_per_peer(self) -> usize {
        (crate::MAX_RECEIVE_WINDOW as usize / self.stream.max_frame as usize)
            .saturating_add(64)
            .min(65536)
    }
    /// Payload is separate. Per record, 160 bytes conservatively cover the
    /// chunk/frame node, both end-offset nodes, alignment and allocator headers.
    /// Per stream, Entry plus 512 bytes cover BTree/deadline/waiter nodes, both
    /// ready indexes and four lifecycle event slots. Peer bookkeeping includes
    /// its four control slots and queue/index nodes. The two new key indexes
    /// have separate backing: 384 bytes/key covers a full 11-key/12-edge BTree
    /// node with 16-byte keys, headers, alignment and allocator overhead, even
    /// when sparsely occupied. This is ledger backing, not preallocation.
    pub fn metadata_backing(self) -> Option<usize> {
        self.receive_record_budget()
            .checked_add(self.send_record_budget())?
            .checked_mul(160)?
            .checked_add(
                self.max_streams.checked_mul(
                    std::mem::size_of::<Entry>()
                        .checked_add(512)?
                        .checked_add(self.stream.max_metadata.checked_mul(3)?)?,
                )?,
            )?
            .checked_add(
                self.max_peers
                    .checked_mul(std::mem::size_of::<Peer>().checked_add(512)?)?,
            )
            .and_then(|bytes| {
                bytes.checked_add(
                    self.max_streams
                        .checked_add(self.max_peers)?
                        .checked_mul(ACTIVE_KEY_INDEX_BACKING)?,
                )
            })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagerError {
    Stream(Error),
    InvalidConfig,
    UnknownPeer,
    UnknownStream,
    Admission,
    IdentityExhausted,
    InvalidOrigin,
    TransportLost,
    PeerBusy,
}
impl fmt::Display for ManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core manager error: {self:?}")
    }
}
impl std::error::Error for ManagerError {}
impl From<Error> for ManagerError {
    fn from(e: Error) -> Self {
        Self::Stream(e)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Resources {
    pub peers: usize,
    pub streams: usize,
    pub reserved_receive_bytes: usize,
    pub pending_send_bytes: usize,
    pub buffered_receive_bytes: usize,
    pub receive_capacity_bytes: usize,
    pub receive_unconsumed_bytes: u64,
    pub ready_output_peers: usize,
    pub ready_event_peers: usize,
    pub pending_rejections: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Aggregate {
    pub peers: usize,
    pub streams: usize,
    pub reserved_receive_bytes: usize,
    pub pending_send_bytes: usize,
    pub ready_output_peers: usize,
    pub ready_event_peers: usize,
}
struct Entry {
    stream: Stream,
    receive_ends: LinkedList<u64>,
    received: u64,
    consumed: u64,
    consumed_records: u64,
    growth_consumed: u64,
    growth_records: u64,
    growth_started: u64,
    last_consumption: u64,
    pending_records: usize,
    retained_send_records: usize,
    deadline: Option<u64>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CreditRequest {
    requester: u64,
    blocked: u8,
    consumed: Amount,
}
#[derive(Clone, Copy)]
struct Pressure {
    requester: u64,
    blocked: u8,
    deadline: u64,
}
struct Peer {
    tx: SendCredit,
    rx: ReceiveCredit,
    target: Amount,
    reserved_receive: Amount,
    requested: Amount,
    growth_consumed: u64,
    probe: u64,
    controls: VecDeque<Frame>,
    freeze_deadline: Option<u64>,
    last_progress: u64,
    last_request: Option<CreditRequest>,
    pressure: Option<Pressure>,
    retained_send_records: usize,
    output_enabled: bool,
    tx_probe: Option<u64>,
    rtt_ms: u64,
    ack_ms: u64,
    ack_bytes: u64,
    consumed_bytes_per_ms: u64,
    local_bit: u64,
    local_sequence: u64,
    remote_sequence: u64,
    count: usize,
    pending_send: usize,
    output: VecDeque<u64>,
    output_set: BTreeSet<u64>,
    events: VecDeque<u64>,
    event_set: BTreeSet<u64>,
    rejection: Option<RoutedFrame>,
    reject_next: bool,
    failed: bool,
}
/// A single owner drives this value. Returned buffers transfer to its caller.
pub struct Manager {
    config: ManagerConfig,
    peers: BTreeMap<PeerId, Peer>,
    streams: BTreeMap<StreamKey, Entry>,
    output: VecDeque<PeerId>,
    output_set: BTreeSet<PeerId>,
    events: VecDeque<PeerId>,
    event_set: BTreeSet<PeerId>,
    deadlines: BTreeSet<(u64, StreamKey)>,
    pressure_deadlines: BTreeSet<(u64, PeerId)>,
    unconsumed: BTreeSet<StreamKey>,
    budget_waiters: BTreeSet<StreamKey>,
    pending_send: usize,
    pending_records: usize,
    receive_promises: Amount,
    retained_send_records: usize,
    minimum_send_records: usize,
    failed_peers: BTreeSet<PeerId>,
    credit_cursor: Option<PeerId>,
    wake_cursor: Option<StreamKey>,
    now: u64,
    failed: bool,
}
impl Manager {
    pub fn new(config: ManagerConfig) -> Result<Self, ManagerError> {
        config.stream.validate()?;
        if config.max_peers == 0
            || config.max_streams == 0
            || config.max_streams_per_peer == 0
            || config.receive_budget == 0
            || config.receive_budget_per_peer == 0
            || config.send_budget == 0
            || config.send_budget_per_peer == 0
            || config.metadata_backing().is_none()
        {
            return Err(ManagerError::InvalidConfig);
        }
        Ok(Self {
            config,
            peers: BTreeMap::new(),
            streams: BTreeMap::new(),
            output: VecDeque::new(),
            output_set: BTreeSet::new(),
            events: VecDeque::new(),
            event_set: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            pressure_deadlines: BTreeSet::new(),
            unconsumed: BTreeSet::new(),
            budget_waiters: BTreeSet::new(),
            pending_send: 0,
            pending_records: 0,
            receive_promises: Amount::default(),
            retained_send_records: 0,
            minimum_send_records: 0,
            failed_peers: BTreeSet::new(),
            credit_cursor: None,
            wake_cursor: None,
            now: 0,
            failed: false,
        })
    }
    pub fn config(&self) -> ManagerConfig {
        self.config
    }
    /// The host must bind this registration to an authenticated peer session.
    pub fn register_peer(
        &mut self,
        id: PeerId,
        local_origin_bit: bool,
    ) -> Result<(), ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        if self.peers.contains_key(&id) || self.peers.len() >= self.config.max_peers {
            return Err(ManagerError::Admission);
        }
        let target = self.minimum_credit();
        let total = self.receive_promises.add(target).map_err(Error::Protocol)?;
        if total.bytes > self.config.receive_budget as u64
            || total.records > self.config.receive_record_budget() as u64
        {
            return Err(ManagerError::Admission);
        }
        let mut rx = ReceiveCredit::default();
        rx.grant(target).map_err(Error::Protocol)?;
        let initial = rx.frame(0);
        self.receive_promises = total;
        self.minimum_send_records += 64;
        self.peers.insert(
            id,
            Peer {
                tx: SendCredit::default(),
                rx,
                target,
                reserved_receive: target,
                requested: target,
                growth_consumed: 0,
                probe: 0,
                controls: VecDeque::from([initial]),
                freeze_deadline: None,
                last_progress: self.now,
                last_request: None,
                pressure: None,
                retained_send_records: 0,
                output_enabled: true,
                tx_probe: None,
                rtt_ms: 0,
                ack_ms: self.now,
                ack_bytes: 0,
                consumed_bytes_per_ms: 0,
                local_bit: u64::from(local_origin_bit),
                local_sequence: 0,
                remote_sequence: 0,
                count: 0,
                pending_send: 0,
                output: VecDeque::new(),
                output_set: BTreeSet::new(),
                events: VecDeque::new(),
                event_set: BTreeSet::new(),
                rejection: None,
                reject_next: true,
                failed: false,
            },
        );
        self.ready_peer(id, true);
        Ok(())
    }
    /// Retain output until the host has proved the authenticated peer lane ready.
    pub fn set_peer_output_enabled(
        &mut self,
        id: PeerId,
        enabled: bool,
    ) -> Result<(), ManagerError> {
        let p = self.peers.get_mut(&id).ok_or(ManagerError::UnknownPeer)?;
        p.output_enabled = enabled;
        if enabled {
            if !p.output.is_empty()
                || !p.controls.is_empty()
                || p.rejection.is_some()
                || (p.tx.freezing.is_some() && !p.tx.frozen && p.tx.pending == Amount::default())
            {
                self.ready_peer(id, true);
            }
        } else {
            self.output.retain(|peer| *peer != id);
            self.output_set.remove(&id);
        }
        Ok(())
    }
    pub fn remove_peer(&mut self, id: PeerId) -> Result<(), ManagerError> {
        let peer = self.peers.get(&id).ok_or(ManagerError::UnknownPeer)?;
        if peer.count != 0 || peer.rejection.is_some() || !peer.failed {
            return Err(ManagerError::PeerBusy);
        }
        self.peers.remove(&id);
        self.failed_peers.remove(&id);
        self.output.retain(|p| *p != id);
        self.output_set.remove(&id);
        self.events.retain(|p| *p != id);
        self.event_set.remove(&id);
        Ok(())
    }
    fn minimum_credit(&self) -> Amount {
        Amount {
            bytes: (self.config.stream.receive_window as usize)
                .min(65536)
                .min((self.config.receive_budget / self.config.max_peers / 8).max(1))
                .min(self.config.receive_budget_per_peer) as u64,
            records: 64.min(self.config.receive_records_per_peer()) as u64,
        }
    }
    fn queue_control(&mut self, peer: PeerId, frame: Frame) {
        let p = self.peers.get_mut(&peer).unwrap();
        let kind = std::mem::discriminant(&frame);
        if let Some(old) = p
            .controls
            .iter_mut()
            .find(|old| std::mem::discriminant(*old) == kind)
        {
            *old = frame;
        } else {
            assert!(p.controls.len() < 4, "bounded peer control slots");
            p.controls.push_back(frame);
        }
        self.ready_peer(peer, true);
    }
    fn release_received(&mut self, peer: PeerId, amount: Amount) -> Result<(), ManagerError> {
        let minimum = self.minimum_credit();
        let p = self.peers.get_mut(&peer).unwrap();
        let before = p.reserved_receive;
        p.rx.consume(amount).map_err(Error::Protocol)?;
        if amount.bytes != 0 {
            p.last_progress = self.now;
        }
        if !p.failed && p.rx.freezing.is_none() {
            p.rx.grant(p.target).map_err(Error::Protocol)?;
        }
        let promise = p.rx.promise();
        p.reserved_receive = Amount {
            bytes: promise.bytes.max(minimum.bytes),
            records: promise.records.max(minimum.records),
        };
        self.receive_promises = self
            .receive_promises
            .sub(before)
            .and_then(|a| a.add(p.reserved_receive))
            .map_err(Error::Protocol)?;
        if !p.failed && p.rx.freezing.is_none() {
            let frame = p.rx.frame(p.probe);
            self.queue_control(peer, frame);
        }
        Ok(())
    }
    fn request_credit(&mut self, key: StreamKey, blocked: u8) {
        let peer = key.peer;
        let p = self.peers.get_mut(&peer).unwrap();
        let request = CreditRequest {
            requester: key.stream_id,
            blocked,
            consumed: p.tx.consumed,
        };
        if p.tx.freezing.is_some() || p.last_request == Some(request) {
            return;
        }
        p.last_request = Some(request);
        let current = p.tx.allowed.bytes - p.tx.consumed.bytes;
        let bdp = p
            .consumed_bytes_per_ms
            .saturating_mul(p.rtt_ms)
            .saturating_mul(2);
        let bytes = current
            .saturating_mul(2)
            .max(bdp)
            .max(1048576)
            .min(crate::MAX_RECEIVE_WINDOW as u64) as u32;
        let records = (p.tx.allowed.records - p.tx.consumed.records)
            .saturating_mul(2)
            .max(64)
            .min(self.config.receive_records_per_peer() as u64) as u32;
        let probe = self.now.saturating_add(1);
        p.tx_probe = Some(probe);
        self.queue_control(
            peer,
            Frame::PeerRequest {
                bytes: bytes.max(1),
                records: records.max(1),
                probe,
                requester_stream_id: key.stream_id,
                blocked,
            },
        );
    }
    fn pressure_interval(&self, peer: PeerId) -> u64 {
        self.peers[&peer].rtt_ms.saturating_mul(4).max(1000)
    }
    fn set_pressure(
        &mut self,
        peer: PeerId,
        requester: u64,
        mut blocked: u8,
    ) -> Result<(), ManagerError> {
        if blocked == 0 {
            return Ok(());
        }
        let interval = self.pressure_interval(peer);
        let mut deadline = self.now.checked_add(interval).ok_or(Error::InvalidTime)?;
        let p = self.peers.get_mut(&peer).unwrap();
        if let Some(old) = p.pressure.take() {
            deadline = deadline.min(old.deadline);
            blocked |= old.blocked;
            self.pressure_deadlines.remove(&(old.deadline, peer));
        }
        p.pressure = Some(Pressure {
            requester,
            blocked,
            deadline,
        });
        self.pressure_deadlines.insert((deadline, peer));
        Ok(())
    }
    fn apply_pressure(&mut self, peer: PeerId) -> Result<(), ManagerError> {
        let Some(pressure) = self.peers[&peer].pressure else {
            return Ok(());
        };
        let requester = StreamKey {
            peer,
            stream_id: pressure.requester,
        };
        let p = &self.peers[&peer];
        let available = p.rx.allowed.sub(p.rx.received).map_err(Error::Protocol)?;
        let byte_deficit = pressure.blocked & 1 != 0 && available.bytes == 0;
        let record_deficit = pressure.blocked & 2 != 0 && available.records == 0;
        if !self
            .streams
            .get(&requester)
            .is_some_and(|e| matches!(e.stream.state(), State::Open | State::HalfClosedLocal))
            || (!byte_deficit && !record_deficit)
            || p.failed
            || p.rx.freezing.is_some()
        {
            self.peers.get_mut(&peer).unwrap().pressure = None;
            return Ok(());
        }
        let interval = self.pressure_interval(peer);
        // This index contains only actually received, unconsumed DATA. Idle
        // streams are never scanned on the normal drive/consumption hot path.
        let victim = self
            .unconsumed
            .range(
                StreamKey { peer, stream_id: 0 }..=StreamKey {
                    peer,
                    stream_id: u64::MAX,
                },
            )
            .filter(|key| **key != requester)
            .filter_map(|key| {
                let e = &self.streams[key];
                (self.now.saturating_sub(e.last_consumption) >= interval).then_some((
                    if record_deficit {
                        e.receive_ends.len() as u64
                    } else {
                        e.received - e.consumed
                    },
                    if byte_deficit {
                        e.received - e.consumed
                    } else {
                        e.receive_ends.len() as u64
                    },
                    *key,
                ))
            })
            .max()
            .map(|(_, _, key)| key);
        if let Some(key) = victim {
            self.close(key, CloseReason::Cancelled)?;
            self.peers.get_mut(&peer).unwrap().pressure = None;
        } else {
            self.peers.get_mut(&peer).unwrap().pressure = None;
            self.set_pressure(peer, pressure.requester, pressure.blocked)?;
        }
        Ok(())
    }
    fn receive_control(&mut self, peer: PeerId, frame: &Frame) -> Result<(), ManagerError> {
        match *frame {
            Frame::PeerGrant {
                epoch,
                consumed_bytes,
                limit_bytes,
                consumed_records,
                limit_records,
                probe,
            } => {
                let p = self.peers.get_mut(&peer).unwrap();
                if limit_bytes
                    .checked_sub(consumed_bytes)
                    .is_none_or(|n| n > crate::MAX_RECEIVE_WINDOW as u64)
                    || limit_records
                        .checked_sub(consumed_records)
                        .is_none_or(|n| n > 65536)
                    || (probe != 0 && probe - 1 > self.now)
                {
                    return Err(Error::Protocol(crate::ProtocolError::InvalidCredit).into());
                }
                let previous_epoch = p.tx.epoch;
                let previous_allowed = p.tx.allowed;
                p.tx.grant(
                    epoch,
                    Amount {
                        bytes: consumed_bytes,
                        records: consumed_records,
                    },
                    Amount {
                        bytes: limit_bytes,
                        records: limit_records,
                    },
                )
                .map_err(Error::Protocol)?;
                if p.tx.epoch != previous_epoch || p.tx.allowed != previous_allowed {
                    p.last_request = None;
                }
                if p.tx_probe == Some(probe) {
                    let sample = self.now.saturating_sub(probe.saturating_sub(1)).max(1);
                    p.rtt_ms = if p.rtt_ms == 0 {
                        sample
                    } else {
                        (p.rtt_ms.saturating_mul(3) + sample) / 4
                    };
                    p.tx_probe = None;
                }
                if self.now > p.ack_ms && consumed_bytes > p.ack_bytes {
                    let sample = (consumed_bytes - p.ack_bytes) / (self.now - p.ack_ms);
                    p.consumed_bytes_per_ms = if p.consumed_bytes_per_ms == 0 {
                        sample
                    } else {
                        (p.consumed_bytes_per_ms.saturating_mul(3) + sample) / 4
                    };
                    p.ack_ms = self.now;
                    p.ack_bytes = consumed_bytes;
                }
                self.wake_budget_waiters();
            }
            Frame::PeerRequest {
                bytes,
                records,
                probe,
                requester_stream_id,
                blocked,
            } => {
                if bytes == 0
                    || records == 0
                    || bytes > crate::MAX_RECEIVE_WINDOW
                    || requester_stream_id < 2
                    || blocked > 3
                {
                    return Err(Error::Protocol(crate::ProtocolError::InvalidCredit).into());
                }
                let key = StreamKey {
                    peer,
                    stream_id: requester_stream_id,
                };
                if self
                    .streams
                    .get(&key)
                    .is_none_or(|e| e.stream.state() == State::Closed)
                {
                    let p = &self.peers[&peer];
                    let sequence = if requester_stream_id & 1 == p.local_bit {
                        p.local_sequence
                    } else {
                        p.remote_sequence
                    };
                    if requester_stream_id >> 1 <= sequence {
                        return Ok(());
                    }
                    return Err(ManagerError::InvalidOrigin);
                }
                let p = self.peers.get_mut(&peer).unwrap();
                p.requested = Amount {
                    bytes: (bytes as u64).min(self.config.receive_budget_per_peer as u64),
                    records: (records as u64).min(self.config.receive_records_per_peer() as u64),
                };
                p.probe = probe;
                self.set_pressure(peer, requester_stream_id, blocked)?;
                self.grow_credit();
                let p = &self.peers[&peer];
                if p.rx.freezing.is_none() {
                    self.queue_control(peer, p.rx.frame(probe));
                }
            }
            Frame::PeerFreeze { epoch } => {
                self.peers
                    .get_mut(&peer)
                    .unwrap()
                    .tx
                    .freeze(epoch)
                    .map_err(Error::Protocol)?;
                self.ready_peer(peer, true);
            }
            Frame::PeerFrozen {
                epoch,
                bytes,
                records,
            } => {
                let minimum = self.minimum_credit();
                let fair = self.fair_target();
                let p = self.peers.get_mut(&peer).unwrap();
                if epoch <= p.rx.epoch {
                    return Ok(());
                }
                let before = p.reserved_receive;
                let outstanding = p.rx.outstanding();
                let spare = before.sub(outstanding).map_err(Error::Protocol)?;
                // Reclaim only excess from an actively consuming donor. Keeping
                // its contention share avoids repeated freeze/regrant cycles;
                // freely available backing has no such per-peer ceiling.
                let retained = if p.requested.bytes > minimum.bytes
                    && self.now.saturating_sub(p.last_progress)
                        <= self.config.stream.open_timeout_ms.min(1000)
                {
                    Amount {
                        bytes: fair.bytes.min(p.requested.bytes),
                        records: fair.records.min(p.requested.records),
                    }
                } else {
                    minimum
                };
                let headroom = Amount {
                    bytes: retained
                        .bytes
                        .saturating_sub(outstanding.bytes)
                        .min(spare.bytes),
                    records: retained
                        .records
                        .saturating_sub(outstanding.records)
                        .min(spare.records),
                };
                p.rx.frozen(epoch, Amount { bytes, records }, headroom)
                    .map_err(Error::Protocol)?;
                p.target = p.rx.promise();
                p.reserved_receive = p.target;
                p.growth_consumed = p.rx.consumed.bytes;
                p.freeze_deadline = None;
                self.receive_promises = self
                    .receive_promises
                    .sub(before)
                    .and_then(|a| a.add(p.target))
                    .map_err(Error::Protocol)?;
                let frame = p.rx.frame(p.probe);
                self.queue_control(peer, frame);
                self.grow_credit_with_reclaimed_pool(true);
            }
            _ => return Err(Error::Protocol(crate::ProtocolError::UnexpectedFrame).into()),
        }
        Ok(())
    }
    fn fair_target(&self) -> Amount {
        let minimum = self.minimum_credit();
        let active = self
            .peers
            .values()
            .filter(|p| {
                !p.failed
                    && p.requested.bytes > minimum.bytes
                    && self.now.saturating_sub(p.last_progress)
                        <= self.config.stream.open_timeout_ms.min(1000)
            })
            .count()
            .max(1);
        let idle = self.config.max_peers.saturating_sub(active);
        Amount {
            bytes: (self.config.receive_budget as u64).saturating_sub(minimum.bytes * idle as u64)
                / active as u64,
            records: (self.config.receive_record_budget() as u64)
                .saturating_sub(minimum.records * idle as u64)
                / active as u64,
        }
    }
    fn grow_credit(&mut self) {
        self.grow_credit_with_reclaimed_pool(false);
    }
    fn grow_credit_with_reclaimed_pool(&mut self, reclaimed: bool) {
        let minimum = self.minimum_credit();
        let mut peers: Vec<_> = self.peers.keys().copied().collect();
        if let Some(cursor) = self.credit_cursor {
            peers.sort_by_key(|id| (*id <= cursor, *id));
        }
        let eligible = |p: &Peer| {
            !p.failed && p.rx.freezing.is_none() && p.rx.consumed.bytes != p.growth_consumed
        };
        let mut byte_requests = self
            .peers
            .values()
            .filter(|p| eligible(p) && p.requested.bytes > p.target.bytes)
            .count();
        let mut record_requests = self
            .peers
            .values()
            .filter(|p| eligible(p) && p.requested.records > p.target.records)
            .count();
        for peer in peers {
            let p = &self.peers[&peer];
            if p.failed
                || p.rx.freezing.is_some()
                || p.rx.consumed.bytes == p.growth_consumed
                || (p.requested.bytes <= p.target.bytes && p.requested.records <= p.target.records)
            {
                continue;
            }
            let unregistered = self.config.max_peers - self.peers.len();
            let byte_free = (self.config.receive_budget as u64 - self.receive_promises.bytes)
                .saturating_sub(minimum.bytes * unregistered as u64);
            let record_free = (self.config.receive_record_budget() as u64
                - self.receive_promises.records)
                .saturating_sub(minimum.records * unregistered as u64);
            // Only competing unmet requests share newly free backing. Fulfilled
            // low-demand peers impose no window ceiling on a bulk consumer.
            let byte_share = byte_free / byte_requests.max(1) as u64;
            let record_share = record_free / record_requests.max(1) as u64;
            byte_requests =
                byte_requests.saturating_sub(usize::from(p.requested.bytes > p.target.bytes));
            record_requests =
                record_requests.saturating_sub(usize::from(p.requested.records > p.target.records));
            let bytes = p
                .requested
                .bytes
                .min(if reclaimed {
                    u64::MAX
                } else {
                    p.target.bytes.saturating_mul(4)
                })
                .saturating_sub(p.target.bytes)
                .min(byte_share);
            let records = p
                .requested
                .records
                .min(if reclaimed {
                    u64::MAX
                } else {
                    p.target.records.saturating_mul(4)
                })
                .saturating_sub(p.target.records)
                .min(record_share);
            if bytes == 0 && records == 0 {
                self.reclaim_credit(peer);
                continue;
            }
            let p = self.peers.get_mut(&peer).unwrap();
            let before = p.reserved_receive;
            p.target = p
                .target
                .add(Amount { bytes, records })
                .expect("bounded growth");
            if p.rx.grant(p.target).is_err() {
                self.peer_lost(peer);
                continue;
            }
            p.growth_consumed = p.rx.consumed.bytes;
            p.reserved_receive = p.rx.promise();
            self.receive_promises = self
                .receive_promises
                .sub(before)
                .and_then(|a| a.add(p.reserved_receive))
                .expect("aggregate growth conservation");
            let frame = p.rx.frame(p.probe);
            self.queue_control(peer, frame);
            self.credit_cursor = Some(peer);
        }
    }
    fn reclaim_credit(&mut self, requester: PeerId) {
        let minimum = self.minimum_credit();
        let fair = self.fair_target();
        if self.peers.values().any(|p| p.rx.freezing.is_some()) {
            return;
        }
        let byte_deficit =
            self.peers[&requester].requested.bytes > self.peers[&requester].target.bytes;
        let record_deficit =
            self.peers[&requester].requested.records > self.peers[&requester].target.records;
        let candidate = self
            .peers
            .iter()
            .filter(|(id, p)| **id != requester && !p.failed && p.rx.freezing.is_none())
            .filter(|(_, p)| {
                let idle = self.now.saturating_sub(p.last_progress)
                    > self.config.stream.open_timeout_ms.min(1000);
                (byte_deficit
                    && (p.target.bytes > fair.bytes || idle)
                    && p.rx
                        .promise()
                        .bytes
                        .saturating_sub(p.rx.outstanding().bytes)
                        > minimum.bytes)
                    || (record_deficit
                        && (p.target.records > fair.records || idle)
                        && p.rx
                            .promise()
                            .records
                            .saturating_sub(p.rx.outstanding().records)
                            > minimum.records)
            })
            .min_by_key(|(_, p)| p.last_progress)
            .map(|(id, _)| *id);
        if let Some(peer) = candidate {
            let p = self.peers.get_mut(&peer).unwrap();
            let frame = match p.rx.freeze() {
                Ok(frame) => frame,
                Err(_) => {
                    self.peer_lost(peer);
                    return;
                }
            };
            p.freeze_deadline = self.now.checked_add(self.config.stream.open_timeout_ms);
            if p.freeze_deadline.is_none() {
                self.peer_lost(peer);
                return;
            }
            self.queue_control(peer, frame);
        }
    }
    fn available_admission(&self, id: PeerId) -> Result<(), ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&id).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        if self.streams.len() >= self.config.max_streams
            || p.count >= self.config.max_streams_per_peer
        {
            return Err(ManagerError::Admission);
        }
        Ok(())
    }
    pub fn open(
        &mut self,
        peer: PeerId,
        metadata: &[u8],
        now: u64,
    ) -> Result<StreamKey, ManagerError> {
        self.tick(now)?;
        self.available_admission(peer)?;
        let p = self.peers.get(&peer).unwrap();
        let seq = p
            .local_sequence
            .checked_add(1)
            .ok_or(ManagerError::IdentityExhausted)?;
        let id = seq
            .checked_mul(2)
            .and_then(|n| n.checked_add(p.local_bit))
            .ok_or(ManagerError::IdentityExhausted)?;
        let mut stream = Stream::new(self.config.stream)?;
        stream.initial_receive_window(self.config.stream.receive_window.min(65536));
        stream.open(metadata, now)?;
        let key = StreamKey {
            peer,
            stream_id: id,
        };
        self.insert(key, stream, now)?;
        self.peers.get_mut(&peer).unwrap().local_sequence = seq;
        Ok(key)
    }
    fn insert(&mut self, key: StreamKey, stream: Stream, now: u64) -> Result<(), ManagerError> {
        let deadline = now
            .checked_add(self.config.stream.open_timeout_ms)
            .ok_or(Error::InvalidTime)?;
        self.deadlines.insert((deadline, key));
        self.streams.insert(
            key,
            Entry {
                stream,
                receive_ends: LinkedList::new(),
                received: 0,
                consumed: 0,
                consumed_records: 0,
                growth_consumed: 0,
                growth_records: 0,
                growth_started: now,
                last_consumption: now,
                pending_records: 0,
                retained_send_records: 0,
                deadline: Some(deadline),
            },
        );
        self.peers.get_mut(&key.peer).unwrap().count += 1;
        self.refresh(key, 0);
        Ok(())
    }
    /// Unknown non-OPEN packets are ignored. Excess OPEN receives at most one
    /// pending REJECT per peer; additional overload requests time out remotely.
    pub fn receive(&mut self, key: StreamKey, frame: &Frame, now: u64) -> Result<(), ManagerError> {
        self.tick(now)?;
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&key.peer).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        let local_bit = p.local_bit;
        let remote_sequence = p.remote_sequence;
        if frame.is_peer_control() {
            if key.stream_id != 0 {
                return Err(Error::Protocol(crate::ProtocolError::UnexpectedFrame).into());
            }
            return self.receive_control(key.peer, frame);
        }
        if key.stream_id == 0 {
            return Err(ManagerError::InvalidOrigin);
        }
        if let Frame::Data { bytes, .. } = frame {
            if bytes.is_empty() || bytes.len() > self.config.stream.max_frame as usize {
                return Err(Error::Protocol(crate::ProtocolError::InvalidDataSize).into());
            }
            self.peers
                .get_mut(&key.peer)
                .unwrap()
                .rx
                .receive(Amount::data(bytes.len()))
                .map_err(Error::Protocol)?;
            if !self.streams.contains_key(&key) {
                self.release_received(key.peer, Amount::data(bytes.len()))?;
                return Ok(());
            }
        }
        if !self.streams.contains_key(&key) {
            if !matches!(frame, Frame::Open { .. }) {
                return Ok(());
            }
            let seq = key.stream_id >> 1;
            if seq == 0 || key.stream_id & 1 == local_bit {
                return Err(ManagerError::InvalidOrigin);
            }
            if seq <= remote_sequence {
                return Ok(());
            }
            self.peers.get_mut(&key.peer).unwrap().remote_sequence = seq;
            if self.available_admission(key.peer).is_err() {
                let p = self.peers.get_mut(&key.peer).unwrap();
                if p.rejection.is_none() {
                    p.rejection = Some(RoutedFrame {
                        key,
                        frame: Frame::Reject {
                            reason: Box::new([]),
                        },
                    });
                }
                self.ready_peer(key.peer, true);
                return Ok(());
            }
            let mut stream = Stream::new(self.config.stream)?;
            stream.initial_receive_window(self.config.stream.receive_window.min(65536));
            self.insert(key, stream, now)?;
        }
        let before = self.streams[&key].stream.snapshot().pending_send_bytes;
        let result = self
            .streams
            .get_mut(&key)
            .unwrap()
            .stream
            .receive(frame, now);
        if let Frame::Data { offset, bytes } = frame {
            if result.is_ok() && self.streams[&key].stream.state() != State::Closed {
                let end = offset
                    .checked_add(bytes.len() as u64)
                    .ok_or(Error::OffsetExhausted)?;
                let e = self.streams.get_mut(&key).unwrap();
                if e.received == e.consumed {
                    e.last_consumption = now;
                    if e.growth_consumed == e.consumed && e.growth_records == e.consumed_records {
                        e.growth_started = now;
                    }
                }
                e.received = end;
                e.receive_ends.push_back(end);
                self.unconsumed.insert(key);
            } else {
                self.release_received(key.peer, Amount::data(bytes.len()))?;
            }
        }
        self.refresh(key, before);
        if matches!(
            frame,
            Frame::WindowUpdate { .. } | Frame::WindowGrant { .. }
        ) {
            // A partial byte ACK can free the per-stream occupancy gate while
            // retaining its end-offset node and all pending frame counts.
            self.wake_budget_waiters();
        }
        result.map_err(Into::into)
    }
    fn operation(
        &mut self,
        key: StreamKey,
        f: impl FnOnce(&mut Stream) -> Result<(), Error>,
    ) -> Result<(), ManagerError> {
        let e = self
            .streams
            .get_mut(&key)
            .ok_or(ManagerError::UnknownStream)?;
        let before = e.stream.snapshot().pending_send_bytes;
        let result = f(&mut e.stream);
        self.refresh(key, before);
        result.map_err(Into::into)
    }
    pub fn accept(&mut self, key: StreamKey, metadata: &[u8]) -> Result<(), ManagerError> {
        self.operation(key, |s| s.accept(metadata))
    }
    pub fn reject(&mut self, key: StreamKey, reason: &[u8]) -> Result<(), ManagerError> {
        self.operation(key, |s| s.reject(reason))
    }
    pub fn finish(&mut self, key: StreamKey) -> Result<(), ManagerError> {
        self.operation(key, Stream::finish)
    }
    pub fn consume_through(&mut self, key: StreamKey, offset: u64) -> Result<(), ManagerError> {
        let e = self
            .streams
            .get_mut(&key)
            .ok_or(ManagerError::UnknownStream)?;
        let before = e.stream.snapshot().pending_send_bytes;
        e.stream.consume_through(offset)?;
        let bytes = offset.saturating_sub(e.consumed);
        let mut records = 0;
        while e.receive_ends.front().is_some_and(|end| *end <= offset) {
            e.receive_ends.pop_front();
            records += 1;
        }
        if bytes != 0 && e.consumed == e.growth_consumed && e.consumed_records == e.growth_records {
            // Measure this stream's processing, not time waiting for its first
            // native/owner turn. Subsequent slow consumption still bounds growth.
            e.growth_started = self.now;
        }
        e.consumed = e.consumed.max(offset);
        e.consumed_records = e
            .consumed_records
            .checked_add(records)
            .ok_or(Error::OffsetExhausted)?;
        if bytes != 0 {
            e.last_consumption = self.now;
        }
        if e.received == e.consumed {
            self.unconsumed.remove(&key);
        }
        self.release_received(key.peer, Amount { bytes, records })?;
        self.grow_credit();
        if let Some(e) = self.streams.get_mut(&key)
            && matches!(
                e.stream.state(),
                State::Open | State::HalfClosedLocal | State::HalfClosedRemote | State::Draining
            )
        {
            let p = &self.peers[&key.peer];
            let current = e.stream.receive_window();
            let epoch_bytes = e.consumed - e.growth_consumed;
            let epoch_records = e.consumed_records - e.growth_records;
            if epoch_bytes >= (current as u64 / 2).max(1)
                || epoch_records >= (e.stream.receive_record_window() as u64 / 2).max(1)
            {
                // Only this stream's actual consumption can grow its flight.
                // A drained native queue is also proof of consumer progress.
                // Shared owner scheduling can exceed a low transport RTT even
                // when this stream leaves no work waiting for its consumer.
                let fast = e.consumed == e.received
                    || p.rtt_ms == 0
                    || self.now.saturating_sub(e.growth_started) <= p.rtt_ms.saturating_mul(2);
                if fast {
                    let ceiling = p
                        .target
                        .bytes
                        .saturating_sub(p.target.bytes / 8)
                        .min(self.config.stream.receive_window as u64)
                        as u32;
                    let window = current.saturating_mul(2).min(ceiling).max(current);
                    if window > current {
                        e.stream.grant_receive_window(window, p.probe)?;
                    }
                }
                e.growth_consumed = e.consumed;
                e.growth_records = e.consumed_records;
                e.growth_started = self.now;
            }
        }
        self.refresh(key, before);
        Ok(())
    }
    pub fn close(&mut self, key: StreamKey, reason: CloseReason) -> Result<(), ManagerError> {
        self.operation(key, |s| s.close(reason))
    }
    pub fn send(&mut self, key: StreamKey, bytes: &[u8]) -> Result<SendOutcome, ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&key.peer).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        let e = self
            .streams
            .get_mut(&key)
            .ok_or(ManagerError::UnknownStream)?;
        let before = e.stream.snapshot().pending_send_bytes;
        let credit = p.tx.room();
        let flight = p.tx.allowed.sub(p.tx.consumed).map_err(Error::Protocol)?;
        let byte_ceiling = flight.bytes.saturating_sub(flight.bytes / 8).max(1);
        let record_ceiling = flight.records.saturating_sub(flight.records / 8).max(1);
        let own_byte_room = byte_ceiling.saturating_sub(e.stream.send_unacknowledged_bytes());
        let own_records_full = e.stream.retained_send_records() as u64 >= record_ceiling;
        let room = (self.config.send_budget - self.pending_send)
            .min(self.config.send_budget_per_peer - p.pending_send)
            .min(credit.bytes as usize)
            .min(own_byte_room as usize);
        let own_minimum = 64usize.saturating_sub(p.retained_send_records);
        let spare_records = self
            .config
            .send_record_budget()
            .saturating_sub(self.retained_send_records)
            .saturating_sub(self.minimum_send_records - own_minimum)
            .saturating_sub((self.config.max_peers - self.peers.len()).saturating_mul(64));
        let room = if credit.records == 0
            || spare_records == 0
            || p.retained_send_records >= self.config.send_records_per_peer()
            || own_records_full
        {
            0
        } else {
            room
        };
        if room == 0
            && !bytes.is_empty()
            && matches!(e.stream.state(), State::Open | State::HalfClosedRemote)
        {
            e.stream.budget_blocked();
            self.budget_waiters.insert(key);
            if credit.bytes == 0 || credit.records == 0 {
                self.request_credit(
                    key,
                    u8::from(credit.bytes == 0) | (u8::from(credit.records == 0) << 1),
                );
            } else if own_byte_room == 0 || own_records_full {
                self.request_credit(key, 0);
            }
            return Ok(SendOutcome::WouldBlock);
        }
        let result = e.stream.send(&bytes[..bytes.len().min(room)]);
        if let Ok(SendOutcome::Accepted(count)) = result
            && count != 0
        {
            self.peers
                .get_mut(&key.peer)
                .unwrap()
                .tx
                .reserve(Amount::data(count))
                .map_err(Error::Protocol)?;
            self.pending_records += 1;
            self.streams.get_mut(&key).unwrap().pending_records += 1;
        }
        if matches!(result, Ok(SendOutcome::WouldBlock))
            && self.streams[&key].stream.credit_blocked()
        {
            self.request_credit(key, 0);
        }
        self.refresh(key, before);
        result.map_err(Into::into)
    }
    fn ready_peer(&mut self, id: PeerId, output: bool) {
        if output && !self.peers[&id].output_enabled {
            return;
        }
        let (q, set) = if output {
            (&mut self.output, &mut self.output_set)
        } else {
            (&mut self.events, &mut self.event_set)
        };
        if set.insert(id) {
            q.push_back(id);
        }
    }
    fn refresh(&mut self, key: StreamKey, before: usize) {
        let e = self.streams.get_mut(&key).unwrap();
        let snapshot = e.stream.snapshot();
        let retained_before = e.retained_send_records;
        let retained_after = e.stream.retained_send_records();
        e.retained_send_records = retained_after;
        let after = snapshot.pending_send_bytes;
        if snapshot.state == State::Closed {
            self.unconsumed.remove(&key);
            let amount = Amount {
                bytes: e.received - e.consumed,
                records: e.receive_ends.len() as u64,
            };
            e.consumed = e.received;
            e.receive_ends.clear();
            if amount != Amount::default() {
                self.release_received(key.peer, amount)
                    .expect("closed receive conservation");
            }
            let cancelled = before - after;
            if cancelled != 0 {
                // Every DATA node has one record; closed stream removed all its nodes.
                let e = self.streams.get_mut(&key).unwrap();
                let records = e.pending_records as u64;
                e.pending_records = 0;
                self.peers
                    .get_mut(&key.peer)
                    .unwrap()
                    .tx
                    .release_pending(Amount {
                        bytes: cancelled as u64,
                        records,
                    })
                    .expect("cancelled send conservation");
                self.pending_records -= records as usize;
            }
        }
        let e = self.streams.get_mut(&key).unwrap();
        let opening = matches!(e.stream.state(), State::Idle | State::Opening(_));
        if !opening && let Some(d) = e.deadline.take() {
            self.deadlines.remove(&(d, key));
        }
        let output = e.stream.has_frames();
        let events = e.stream.has_events();
        let p = self.peers.get_mut(&key.peer).unwrap();
        let previous_minimum = if p.failed {
            0
        } else {
            64usize.saturating_sub(p.retained_send_records)
        };
        p.retained_send_records = p.retained_send_records - retained_before + retained_after;
        self.retained_send_records = self.retained_send_records - retained_before + retained_after;
        let next_minimum = if p.failed {
            0
        } else {
            64usize.saturating_sub(p.retained_send_records)
        };
        self.minimum_send_records = self.minimum_send_records - previous_minimum + next_minimum;
        p.pending_send = p.pending_send - before + after;
        self.pending_send = self.pending_send - before + after;
        if output && p.output_set.insert(key.stream_id) {
            p.output.push_back(key.stream_id);
        }
        if events && p.event_set.insert(key.stream_id) {
            p.events.push_back(key.stream_id);
        }
        if output {
            self.ready_peer(key.peer, true);
        }
        if events {
            self.ready_peer(key.peer, false);
        }
        if after < before || retained_after < retained_before {
            self.wake_budget_waiters();
        }
    }
    fn wake_budget_waiters(&mut self) {
        if self.pending_send >= self.config.send_budget {
            return;
        }
        let mut keys: Vec<_> = match self.wake_cursor {
            Some(cursor) => self
                .budget_waiters
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(32)
                .copied()
                .collect(),
            None => Vec::new(),
        };
        if keys.len() < 32 {
            keys.extend(self.budget_waiters.iter().take(32 - keys.len()).copied());
        }
        self.wake_cursor = keys.last().copied();
        for key in keys {
            if self.peers[&key.peer].pending_send >= self.config.send_budget_per_peer {
                continue;
            }
            self.budget_waiters.remove(&key);
            if let Some(e) = self.streams.get_mut(&key) {
                e.stream.budget_writable();
                let p = self.peers.get_mut(&key.peer).unwrap();
                if e.stream.has_events() && p.event_set.insert(key.stream_id) {
                    p.events.push_back(key.stream_id);
                }
                if e.stream.has_events() {
                    self.ready_peer(key.peer, false);
                }
            }
        }
    }
    fn reap(&mut self, key: StreamKey) {
        if self.streams.get(&key).is_some_and(|e| {
            e.stream.state() == State::Closed && !e.stream.has_frames() && !e.stream.has_events()
        }) {
            let e = self.streams.remove(&key).unwrap();
            if let Some(d) = e.deadline {
                self.deadlines.remove(&(d, key));
            }
            let p = self.peers.get_mut(&key.peer).unwrap();
            p.count -= 1;
            p.output.retain(|id| *id != key.stream_id);
            p.output_set.remove(&key.stream_id);
            p.events.retain(|id| *id != key.stream_id);
            p.event_set.remove(&key.stream_id);
            self.budget_waiters.remove(&key);
        }
    }
    /// One frame per ready peer, rotating its ready streams. Caller must
    /// transmit in returned order and bound its own retained frame batches.
    pub fn poll_frames(&mut self, max: usize) -> Vec<RoutedFrame> {
        self.poll_frames_with_budget(max, |_, _, _| true)
    }
    /// Framed demand already retained in ready output. Does not extract,
    /// clone payloads, dispatch credit or alter stream/peer rotation.
    pub fn transport_demand(&self, peer: PeerId, overhead: usize, maximum: usize) -> usize {
        let Some(p) = self.peers.get(&peer) else {
            return 0;
        };
        let weight = |id, frame: &Frame| {
            crate::wire::encoded_size(id, frame)
                .unwrap_or(maximum)
                .saturating_add(overhead)
        };
        let mut bytes = 0usize;
        for frame in &p.controls {
            bytes = bytes.saturating_add(weight(0, frame));
            if bytes >= maximum {
                return maximum;
            }
        }
        if let Some(frame) = p.tx.peek_frozen_frame() {
            bytes = bytes.saturating_add(weight(0, &frame));
        }
        if let Some(frame) = &p.rejection {
            bytes = bytes.saturating_add(weight(frame.key.stream_id, &frame.frame));
        }
        for &id in &p.output {
            if bytes >= maximum {
                break;
            }
            if let Some(entry) = self.streams.get(&StreamKey {
                peer,
                stream_id: id,
            }) {
                bytes = bytes.saturating_add(entry.stream.transport_demand(
                    id,
                    overhead,
                    maximum - bytes,
                ));
            }
        }
        bytes.min(maximum)
    }
    pub fn next_transport_weight(&self, peer: PeerId, overhead: usize) -> Option<usize> {
        let p = self.peers.get(&peer)?;
        let weight = |id, frame: &Frame| {
            crate::wire::encoded_size(id, frame)
                .ok()?
                .checked_add(overhead)
        };
        if let Some(frame) = p.controls.front() {
            return weight(0, frame);
        }
        if let Some(frame) = p.tx.peek_frozen_frame() {
            return weight(0, &frame);
        }
        let rejected = p
            .rejection
            .as_ref()
            .and_then(|frame| weight(frame.key.stream_id, &frame.frame));
        if p.rejection.is_some() && (p.output.is_empty() || p.reject_next) {
            return rejected;
        }
        p.output
            .iter()
            .filter_map(|&id| {
                let key = StreamKey {
                    peer,
                    stream_id: id,
                };
                if self.open_output_blocked(key) {
                    return None;
                }
                let entry = self.streams.get(&key)?;
                let (size, _) = entry.stream.next_frame_size_for(id)?;
                size.checked_add(overhead)
            })
            .chain(rejected)
            .min()
    }
    fn open_output_blocked(&self, key: StreamKey) -> bool {
        self.streams
            .get(&key)
            .is_some_and(|entry| entry.stream.has_pending_open())
            && self.peers[&key.peer]
                .output_set
                .range(..key.stream_id)
                .any(|&id| {
                    self.streams
                        .get(&StreamKey {
                            peer: key.peer,
                            stream_id: id,
                        })
                        .is_some_and(|entry| entry.stream.has_pending_open())
                })
    }
    /// Admit an encoded Core frame before extraction. Denied work stays owned
    /// by Core; ready peers/streams rotate within a finite scan, never idle slots.
    /// The callback accounts cumulative admissions within this batch.
    pub fn poll_frames_with_budget(
        &mut self,
        max: usize,
        mut admit: impl FnMut(StreamKey, usize, bool) -> bool,
    ) -> Vec<RoutedFrame> {
        let mut result = Vec::new();
        let max = max.min(MAX_BATCH_EVENTS);
        let mut attempts = max
            .saturating_add(self.streams.len())
            .saturating_add(self.peers.len());
        while result.len() < max && attempts > 0 {
            attempts -= 1;
            let Some(peer) = self.output.pop_front() else {
                break;
            };
            self.output_set.remove(&peer);
            let p = &self.peers[&peer];
            let describe = |key: StreamKey, frame: &Frame| {
                crate::wire::encoded_size(key.stream_id, frame)
                    .ok()
                    .map(|size| (key, size, matches!(frame, Frame::Data { .. }), false))
            };
            let next = if let Some(frame) = p.controls.front() {
                describe(StreamKey { peer, stream_id: 0 }, frame)
            } else if let Some(frame) = p.tx.peek_frozen_frame() {
                describe(StreamKey { peer, stream_id: 0 }, &frame)
            } else if p.rejection.is_some() && (p.output.is_empty() || p.reject_next) {
                let routed = p.rejection.as_ref().unwrap();
                describe(routed.key, &routed.frame)
            } else {
                p.output.front().and_then(|&id| {
                    let key = StreamKey {
                        peer,
                        stream_id: id,
                    };
                    self.streams
                        .get(&key)?
                        .stream
                        .next_frame_size_for(id)
                        .map(|(size, data)| (key, size, data, true))
                })
            };
            // The receiver's replay watermark requires first OPEN publication
            // in identity order. Keep rotating denied work so smaller DATA,
            // FIN/CLOSE and peer controls can still use available credit.
            if let Some((key, size, data, stream)) = next
                && (self.open_output_blocked(key) || !admit(key, size, data))
            {
                if stream {
                    let p = self.peers.get_mut(&peer).unwrap();
                    p.reject_next = true;
                    let id = p.output.pop_front().unwrap();
                    p.output.push_back(id);
                }
                self.ready_peer(peer, true);
                continue;
            }
            if let Some(frame) = self.peers.get_mut(&peer).unwrap().controls.pop_front() {
                result.push(RoutedFrame {
                    key: StreamKey { peer, stream_id: 0 },
                    frame,
                });
            } else if let Some(frame) = self.peers.get_mut(&peer).unwrap().tx.frozen_frame() {
                result.push(RoutedFrame {
                    key: StreamKey { peer, stream_id: 0 },
                    frame,
                });
            } else if self.peers[&peer].rejection.is_some()
                && (self.peers[&peer].output.is_empty() || self.peers[&peer].reject_next)
            {
                let p = self.peers.get_mut(&peer).unwrap();
                result.push(p.rejection.take().unwrap());
                p.reject_next = false;
            } else {
                let p = self.peers.get_mut(&peer).unwrap();
                p.reject_next = true;
                if let Some(id) = p.output.pop_front() {
                    p.output_set.remove(&id);
                    let key = StreamKey {
                        peer,
                        stream_id: id,
                    };
                    if let Some(e) = self.streams.get_mut(&key) {
                        let before = e.stream.snapshot().pending_send_bytes;
                        if let Some(frame) = e.stream.poll_frames(1).pop() {
                            if let Frame::Data { bytes, .. } = &frame {
                                self.peers
                                    .get_mut(&peer)
                                    .unwrap()
                                    .tx
                                    .dispatch(Amount::data(bytes.len()))
                                    .expect("dispatch conservation");
                                self.pending_records -= 1;
                                self.streams.get_mut(&key).unwrap().pending_records -= 1;
                            }
                            result.push(RoutedFrame { key, frame });
                        }
                        self.refresh(key, before);
                        self.reap(key);
                    }
                }
            }
            let p = &self.peers[&peer];
            if !p.output.is_empty()
                || p.rejection.is_some()
                || !p.controls.is_empty()
                || (p.tx.freezing.is_some() && !p.tx.frozen && p.tx.pending == Amount::default())
            {
                self.ready_peer(peer, true);
            }
        }
        result
    }
    pub fn poll_events(&mut self, max: usize) -> Vec<ManagedEvent> {
        self.poll_events_with_data_budget(max, |_, _| true)
    }
    /// Transfer DATA only when its owner can retain the entire next chunk.
    /// The callback accounts cumulative admissions within this batch. Denied
    /// chunks remain owned by Core; other ready streams continue to be visited.
    pub fn poll_events_with_data_budget(
        &mut self,
        max: usize,
        mut admit: impl FnMut(StreamKey, usize) -> bool,
    ) -> Vec<ManagedEvent> {
        self.wake_budget_waiters();
        let mut result = Vec::new();
        let max = max.min(MAX_BATCH_EVENTS);
        let mut attempts = max.saturating_add(self.streams.len());
        while result.len() < max && attempts > 0 {
            attempts -= 1;
            let Some(peer) = self.events.pop_front() else {
                break;
            };
            self.event_set.remove(&peer);
            let p = self.peers.get_mut(&peer).unwrap();
            if let Some(id) = p.events.pop_front() {
                p.event_set.remove(&id);
                let key = StreamKey {
                    peer,
                    stream_id: id,
                };
                if let Some(e) = self.streams.get_mut(&key) {
                    let before = e.stream.snapshot().pending_send_bytes;
                    if e.stream
                        .next_event_data_bytes()
                        .is_none_or(|bytes| admit(key, bytes))
                        && let Some(event) = e.stream.poll_events(1).pop()
                    {
                        result.push(ManagedEvent { key, event });
                    }
                    self.refresh(key, before);
                    self.reap(key);
                }
            }
            if !self.peers[&peer].events.is_empty() {
                self.ready_peer(peer, false);
            }
        }
        result
    }
    pub fn tick(&mut self, now: u64) -> Result<(), ManagerError> {
        if now < self.now {
            return Err(Error::InvalidTime.into());
        }
        self.now = now;
        let expired: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, p)| p.freeze_deadline.is_some_and(|d| now >= d))
            .map(|(id, _)| *id)
            .collect();
        for peer in expired {
            self.peer_lost(peer);
        }
        self.grow_credit();
        while let Some(&(deadline, peer)) = self.pressure_deadlines.first() {
            if deadline > now {
                break;
            }
            self.pressure_deadlines.remove(&(deadline, peer));
            self.apply_pressure(peer)?;
        }
        while let Some(&(deadline, key)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            self.deadlines.remove(&(deadline, key));
            if let Some(e) = self.streams.get_mut(&key) {
                let before = e.stream.snapshot().pending_send_bytes;
                e.deadline = None;
                e.stream.tick(now)?;
                self.refresh(key, before);
            }
        }
        Ok(())
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.deadlines
            .first()
            .map(|(d, _)| *d)
            .into_iter()
            .chain(self.peers.values().filter_map(|p| p.freeze_deadline))
            .chain(
                self.pressure_deadlines
                    .first()
                    .map(|(deadline, _)| *deadline),
            )
            .min()
    }
    pub fn transport_lost(&mut self) {
        if self.failed {
            return;
        }
        self.failed = true;
        let peers: Vec<_> = self.peers.keys().copied().collect();
        for peer in peers {
            self.peer_lost(peer);
        }
    }
    pub fn protocol_error(&mut self, peer: PeerId) {
        let keys: Vec<_> = self
            .streams
            .range(
                StreamKey { peer, stream_id: 0 }..=StreamKey {
                    peer,
                    stream_id: u64::MAX,
                },
            )
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            let _ = self.close(key, CloseReason::ProtocolError);
        }
    }
    pub fn peer_lost(&mut self, peer: PeerId) {
        if let Some(p) = self.peers.get_mut(&peer) {
            if !p.failed {
                self.failed_peers.insert(peer);
                self.minimum_send_records -= 64usize.saturating_sub(p.retained_send_records);
            }
            p.failed = true;
            p.rejection = None;
            p.controls.clear();
            p.freeze_deadline = None;
            if let Some(pressure) = p.pressure.take() {
                self.pressure_deadlines.remove(&(pressure.deadline, peer));
            }
        }
        let keys: Vec<_> = self
            .streams
            .range(
                StreamKey { peer, stream_id: 0 }..=StreamKey {
                    peer,
                    stream_id: u64::MAX,
                },
            )
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            let e = self.streams.get_mut(&key).unwrap();
            let before = e.stream.snapshot().pending_send_bytes;
            e.stream.transport_lost();
            self.refresh(key, before);
        }
        if let Some(p) = self.peers.get_mut(&peer) {
            self.receive_promises = self
                .receive_promises
                .sub(p.reserved_receive)
                .expect("lost peer conservation");
            p.rx.allowed = p.rx.consumed;
            p.reserved_receive = Amount::default();
        }
    }
    #[cfg(feature = "nats")]
    pub(crate) fn poll_failed_peer(&mut self) -> Option<PeerId> {
        self.failed_peers.pop_first()
    }
    #[cfg(feature = "nats")]
    pub(crate) fn peer_failed(&self, peer: PeerId) -> bool {
        self.peers.get(&peer).is_some_and(|p| p.failed)
    }
    #[cfg(feature = "fault-injection")]
    pub(crate) fn expire_credit_freeze(&mut self, peer: PeerId) {
        if let Some(p) = self.peers.get_mut(&peer) {
            p.freeze_deadline = Some(self.now);
        }
    }
    pub fn snapshot(&self, key: StreamKey) -> Option<Snapshot> {
        self.streams.get(&key).map(|e| e.stream.snapshot())
    }
    /// Inspect validated peer limits without advancing stream ownership.
    /// Unknown streams and streams awaiting peer negotiation return None.
    pub fn peer_limits(&self, key: StreamKey) -> Option<PeerLimits> {
        self.streams.get(&key).and_then(|e| e.stream.peer_limits())
    }
    /// Constant-time counters; buffer occupancy remains a detailed diagnostic.
    pub fn aggregate(&self) -> Aggregate {
        Aggregate {
            peers: self.peers.len(),
            streams: self.streams.len(),
            reserved_receive_bytes: self.receive_promises.bytes as usize,
            pending_send_bytes: self.pending_send,
            ready_output_peers: self.output_set.len(),
            ready_event_peers: self.event_set.len(),
        }
    }
    pub fn peer_streams(&self, id: PeerId) -> Option<usize> {
        self.peers.get(&id).map(|p| p.count)
    }
    /// Resource inspection is explicit and O(stream count), never a driver turn.
    pub fn resources(&self) -> Resources {
        let mut r = Resources {
            peers: self.peers.len(),
            streams: self.streams.len(),
            reserved_receive_bytes: self.receive_promises.bytes as usize,
            pending_send_bytes: self.pending_send,
            ready_output_peers: self.output_set.len(),
            ready_event_peers: self.event_set.len(),
            pending_rejections: self
                .peers
                .values()
                .filter(|p| p.rejection.is_some())
                .count(),
            ..Resources::default()
        };
        for e in self.streams.values() {
            let s = e.stream.snapshot();
            r.buffered_receive_bytes += s.buffered_receive_bytes;
            r.receive_capacity_bytes += s.receive_capacity_bytes;
            r.receive_unconsumed_bytes += s.receive_unconsumed_bytes;
        }
        r
    }
}
