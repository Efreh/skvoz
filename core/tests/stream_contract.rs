use skvoz_core::{
    CloseReason, Config, Direction, Error, Event, Frame, MAX_BATCH_EVENTS, ProtocolError,
    SendOutcome, State, Stream,
};

fn small() -> Config {
    Config {
        receive_window: 8,
        max_frame: 4,
        max_pending_frames: 2,
        max_metadata: 8,
        open_timeout_ms: 10,
    }
}

fn pump(from: &mut Stream, to: &mut Stream) -> Vec<Frame> {
    let frames = from.poll_frames(usize::MAX);
    for frame in &frames {
        to.receive(frame, 0).unwrap();
    }
    frames
}

fn pair(a_config: Config, b_config: Config) -> (Stream, Stream) {
    let mut a = Stream::new(a_config).unwrap();
    let mut b = Stream::new(b_config).unwrap();
    a.open(b"request", 0).unwrap();
    pump(&mut a, &mut b);
    b.poll_events(8);
    b.accept(b"ok").unwrap();
    pump(&mut b, &mut a);
    a.poll_events(8);
    b.poll_events(8);
    (a, b)
}

fn data(offset: u64, bytes: &[u8]) -> Frame {
    Frame::Data {
        offset,
        bytes: bytes.into(),
    }
}

fn assert_abort(stream: &mut Stream, reason: CloseReason, outgoing: Option<Frame>) {
    let snapshot = stream.snapshot();
    assert_eq!(snapshot.state, State::Closed);
    assert_eq!(snapshot.pending_data_frames, 0);
    assert_eq!(snapshot.pending_send_bytes, 0);
    assert_eq!(snapshot.buffered_receive_bytes, 0);
    assert_eq!(snapshot.receive_capacity_bytes, 0);
    assert_eq!(snapshot.receive_unconsumed_bytes, 0);
    assert_eq!(snapshot.send_unacknowledged_bytes, 0);
    assert_eq!(
        stream.poll_frames(8),
        outgoing.into_iter().collect::<Vec<_>>()
    );
    assert_eq!(stream.poll_events(8), [Event::Closed { reason }]);
    assert!(stream.poll_frames(8).is_empty());
    assert!(stream.poll_events(8).is_empty());
}

#[test]
fn handshake_is_explicit_and_metadata_is_opaque() {
    let mut a = Stream::new(small()).unwrap();
    let mut b = Stream::new(small()).unwrap();
    let metadata = [0, 0xff, 0x80];
    a.open(&metadata, 0).unwrap();
    assert_eq!(a.state(), State::Opening(Direction::Outgoing));
    assert_eq!(a.send(b"early"), Err(Error::InvalidState));
    pump(&mut a, &mut b);
    assert_eq!(b.state(), State::Opening(Direction::Incoming));
    assert_eq!(b.send(b"early"), Err(Error::InvalidState));
    assert_eq!(
        b.poll_events(1),
        [Event::IncomingOpen {
            metadata: metadata.into(),
        }]
    );
    b.accept(&[0xfe]).unwrap();
    assert_eq!(b.send(b"data"), Ok(SendOutcome::Accepted(4)));
    let frames = pump(&mut b, &mut a);
    assert!(matches!(frames[0], Frame::Accept { .. }));
    assert!(matches!(frames[1], Frame::Data { offset: 0, .. }));
    assert_eq!(a.state(), State::Open);
    assert_eq!(
        a.poll_events(2),
        [
            Event::Opened {
                metadata: Box::new([0xfe]),
            },
            Event::Data {
                offset: 0,
                bytes: b"data".as_slice().into(),
            },
        ]
    );
}

#[test]
fn rejection_is_terminal_and_local_api_errors_are_atomic() {
    let mut a = Stream::new(small()).unwrap();
    let mut b = Stream::new(small()).unwrap();
    let before = a.snapshot();
    assert_eq!(a.open(&[0; 9], 0), Err(Error::MetadataTooLarge));
    assert_eq!(a.snapshot(), before);
    a.open(b"", 0).unwrap();
    pump(&mut a, &mut b);
    let before = b.snapshot();
    assert_eq!(b.accept(&[0; 9]), Err(Error::MetadataTooLarge));
    assert_eq!(b.snapshot(), before);
    b.reject(b"denied").unwrap();
    pump(&mut b, &mut a);
    assert_eq!(a.state(), State::Closed);
    assert_eq!(
        a.poll_events(8),
        [
            Event::Rejected {
                reason: b"denied".as_slice().into(),
            },
            Event::Closed {
                reason: CloseReason::Rejected,
            },
        ]
    );
    assert!(a.poll_events(8).is_empty());
    assert!(a.poll_frames(8).is_empty());
    assert_eq!(b.accept(b""), Err(Error::InvalidState));
}

#[test]
fn peer_receive_limits_control_partial_send() {
    let mut b_config = small();
    b_config.receive_window = 3;
    b_config.max_frame = 2;
    let (mut a, mut b) = pair(small(), b_config);
    assert_eq!(a.send(b"abcdef"), Ok(SendOutcome::Accepted(2)));
    assert_eq!(a.send(b"cdef"), Ok(SendOutcome::Accepted(1)));
    assert_eq!(a.send(b"def"), Ok(SendOutcome::WouldBlock));
    pump(&mut a, &mut b);
    assert_eq!(b.snapshot().receive_unconsumed_bytes, 3);
    assert_eq!(b.poll_events(8).len(), 2);
}

#[test]
fn binary_data_chunks_survive_bounded_ownership_transfer() {
    let (mut a, mut b) = pair(small(), small());
    for bytes in [[0, 0xff], [0x80, b'\n']] {
        assert_eq!(a.send(&bytes), Ok(SendOutcome::Accepted(2)));
    }
    pump(&mut a, &mut b);
    assert!(b.poll_events(0).is_empty());
    assert_eq!(
        b.poll_events(1),
        [Event::Data {
            offset: 0,
            bytes: Box::new([0, 0xff]),
        }]
    );
    assert_eq!(
        b.poll_events(1),
        [Event::Data {
            offset: 2,
            bytes: Box::new([0x80, b'\n'])
        }]
    );
    assert_eq!(b.snapshot().buffered_receive_bytes, 0);
    assert_eq!(b.snapshot().receive_capacity_bytes, 0);
    assert_eq!(b.snapshot().receive_unconsumed_bytes, 4);
}

#[test]
fn bad_data_and_fin_offsets_abort_without_partial_delivery() {
    let cases = [
        (data(1, b"x"), ProtocolError::IncorrectOffset),
        (data(0, b""), ProtocolError::InvalidDataSize),
        (data(0, b"12345"), ProtocolError::InvalidDataSize),
        (
            Frame::Fin { final_offset: 1 },
            ProtocolError::IncorrectOffset,
        ),
    ];
    for (frame, reason) in cases {
        let (_, mut b) = pair(small(), small());
        assert_eq!(b.receive(&frame, 0), Err(Error::Protocol(reason)));
        assert_abort(
            &mut b,
            CloseReason::ProtocolError,
            Some(Frame::Close {
                reason: CloseReason::ProtocolError,
            }),
        );
    }
    let (_, mut b) = pair(small(), small());
    b.receive(&data(0, b"x"), 0).unwrap();
    assert_eq!(
        b.receive(&data(0, b"x"), 0),
        Err(Error::Protocol(ProtocolError::IncorrectOffset))
    );
    assert_eq!(b.snapshot().buffered_receive_bytes, 0);
}

#[test]
fn data_after_fin_aborts_and_duplicate_fin_is_idempotent() {
    let (_, mut b) = pair(small(), small());
    b.receive(&Frame::Fin { final_offset: 0 }, 0).unwrap();
    b.receive(&Frame::Fin { final_offset: 0 }, 0).unwrap();
    assert_eq!(b.poll_events(8), [Event::RemoteFinished]);
    assert_eq!(
        b.receive(&data(0, b"x"), 0),
        Err(Error::Protocol(ProtocolError::DataAfterFin))
    );
}

#[test]
fn poll_does_not_release_credit_but_consumption_does() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"1234").unwrap();
    a.send(b"5678").unwrap();
    pump(&mut a, &mut b);
    assert_eq!(a.send(b"x"), Ok(SendOutcome::WouldBlock));
    let held = b.poll_events(8);
    assert_eq!(held.len(), 2);
    assert_eq!(b.snapshot().buffered_receive_bytes, 0);
    assert_eq!(b.snapshot().receive_unconsumed_bytes, 8);
    assert!(b.poll_frames(8).is_empty());
    assert_eq!(a.send(b"x"), Ok(SendOutcome::WouldBlock));
    assert_eq!(b.consume_through(9), Err(Error::InvalidConsumption));
    b.consume_through(2).unwrap();
    assert_eq!(pump(&mut b, &mut a), [Frame::WindowUpdate { consumed: 2 }]);
    assert_eq!(a.poll_events(8), [Event::Writable]);
    assert_eq!(a.send(b"xyz"), Ok(SendOutcome::Accepted(2)));
    assert_eq!(a.send(b"z"), Ok(SendOutcome::WouldBlock));
    drop(held);
}

#[test]
fn cumulative_credit_is_coalesced_and_duplicates_do_not_mint_credit() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"1234").unwrap();
    a.send(b"5678").unwrap();
    pump(&mut a, &mut b);
    b.poll_events(8);
    b.consume_through(1).unwrap();
    b.consume_through(4).unwrap();
    b.consume_through(2).unwrap();
    assert_eq!(pump(&mut b, &mut a), [Frame::WindowUpdate { consumed: 4 }]);
    a.receive(&Frame::WindowUpdate { consumed: 4 }, 0).unwrap();
    a.receive(&Frame::WindowUpdate { consumed: 1 }, 0).unwrap();
    assert_eq!(a.send(b"abcd"), Ok(SendOutcome::Accepted(4)));
    assert_eq!(a.send(b"e"), Ok(SendOutcome::WouldBlock));
    assert!(b.poll_frames(8).is_empty());
}

#[test]
fn credit_cannot_acknowledge_bytes_still_in_the_output_queue() {
    let (mut a, _) = pair(small(), small());
    a.send(b"1234").unwrap();
    assert_eq!(
        a.receive(&Frame::WindowUpdate { consumed: 1 }, 0),
        Err(Error::Protocol(ProtocolError::InvalidCredit))
    );
    assert_eq!(a.snapshot().pending_send_bytes, 0);
}

#[test]
fn consumption_cannot_acknowledge_undelivered_data() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"1234").unwrap();
    pump(&mut a, &mut b);
    let before = b.snapshot();
    assert_eq!(b.consume_through(1), Err(Error::InvalidConsumption));
    assert_eq!(b.snapshot(), before);
    assert!(b.poll_frames(8).is_empty());
}

#[test]
fn output_frame_limit_blocks_tiny_sends_and_wakes_after_drain() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"a").unwrap();
    a.send(b"b").unwrap();
    let before = a.snapshot();
    assert_eq!(a.send(b"c"), Ok(SendOutcome::WouldBlock));
    assert_eq!(a.snapshot(), before);
    assert_eq!(before.pending_data_frames, 2);
    assert_eq!(before.pending_send_bytes, 2);
    for frame in a.poll_frames(1) {
        b.receive(&frame, 0).unwrap();
    }
    assert_eq!(a.poll_events(8), [Event::Writable]);
    assert_eq!(a.send(b"c"), Ok(SendOutcome::Accepted(1)));
    assert_eq!(a.send(b"d"), Ok(SendOutcome::WouldBlock));
    assert_eq!(a.snapshot().pending_data_frames, 2);
}

#[test]
fn tiny_received_frames_obey_the_implicit_record_allowance() {
    let config = Config {
        receive_window: 1024,
        max_frame: 32,
        ..small()
    };
    let (_, mut b) = pair(config, config);
    let records = 1024 / 32 + 64;
    for offset in 0..records {
        b.receive(&data(offset, &[offset as u8]), 0).unwrap();
        let snapshot = b.snapshot();
        assert_eq!(snapshot.buffered_receive_bytes, offset as usize + 1);
        assert!(snapshot.receive_unconsumed_bytes <= 1024);
        assert!(snapshot.receive_capacity_bytes <= 1024);
        assert!(snapshot.receive_capacity_bytes >= snapshot.buffered_receive_bytes);
    }
    assert_eq!(b.poll_events(4).len(), 4);
    assert_eq!(b.snapshot().receive_unconsumed_bytes, records);
    assert_eq!(
        b.receive(&data(records, b"x"), 0),
        Err(Error::Protocol(ProtocolError::ReceiveWindowExceeded))
    );
}

#[test]
fn control_frames_progress_when_data_output_is_saturated() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"req").unwrap();
    pump(&mut a, &mut b);
    b.poll_events(8);
    b.send(b"a").unwrap();
    b.send(b"b").unwrap();
    assert_eq!(b.send(b"c"), Ok(SendOutcome::WouldBlock));
    b.consume_through(3).unwrap();
    b.finish().unwrap();
    assert_eq!(
        b.poll_frames(8),
        [
            Frame::WindowUpdate { consumed: 3 },
            data(0, b"a"),
            data(1, b"b"),
            Frame::Fin { final_offset: 2 },
        ]
    );
}

#[test]
fn event_batch_has_an_independent_upper_bound() {
    let config = Config {
        receive_window: 1024,
        max_frame: 1,
        ..small()
    };
    let (_, mut b) = pair(config, config);
    for offset in 0..1024 {
        b.receive(&data(offset, b"x"), 0).unwrap();
    }
    assert_eq!(b.poll_events(usize::MAX).len(), MAX_BATCH_EVENTS);
    assert_eq!(b.snapshot().buffered_receive_bytes, 1024 - MAX_BATCH_EVENTS);
}

#[test]
fn metadata_storage_is_bounded_even_before_events_are_polled() {
    let mut a = Stream::new(small()).unwrap();
    let mut b = Stream::new(small()).unwrap();
    a.open(b"12345678", 0).unwrap();
    pump(&mut a, &mut b);
    b.accept(b"12345678").unwrap();
    assert_eq!(
        b.snapshot().retained_metadata_bytes,
        3 * small().max_metadata
    );
    b.close(CloseReason::Cancelled).unwrap();
    assert_eq!(b.snapshot().retained_metadata_bytes, 0);
}

#[test]
fn half_close_preserves_response_and_fin_follows_data() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"req").unwrap();
    a.finish().unwrap();
    a.finish().unwrap();
    assert_eq!(a.send(b"more"), Err(Error::InvalidState));
    assert_eq!(
        pump(&mut a, &mut b),
        [data(0, b"req"), Frame::Fin { final_offset: 3 }]
    );
    assert_eq!(b.state(), State::HalfClosedRemote);
    assert_eq!(
        b.poll_events(8),
        [
            Event::Data {
                offset: 0,
                bytes: b"req".as_slice().into(),
            },
            Event::RemoteFinished,
        ]
    );
    b.consume_through(3).unwrap();
    b.send(b"res").unwrap();
    b.finish().unwrap();
    assert_eq!(b.state(), State::Draining);
    pump(&mut b, &mut a);
    assert_eq!(b.state(), State::Closed);
    assert_eq!(a.state(), State::Draining);
    a.poll_events(8);
    a.consume_through(3).unwrap();
    assert_eq!(a.state(), State::Closed);
    assert_eq!(
        a.poll_events(8),
        [Event::Closed {
            reason: CloseReason::Finished,
        }]
    );
    a.finish().unwrap();
    assert_eq!(a.snapshot().receive_capacity_bytes, 0);
    assert_eq!(b.snapshot().receive_capacity_bytes, 0);
}

#[test]
fn remote_finished_event_follows_all_data_even_across_batches() {
    let (_, mut b) = pair(small(), small());
    b.receive(&data(0, b"1234"), 0).unwrap();
    b.receive(&data(4, b"5678"), 0).unwrap();
    b.receive(&Frame::Fin { final_offset: 8 }, 0).unwrap();
    assert!(matches!(b.poll_events(1)[0], Event::Data { offset: 0, .. }));
    assert!(matches!(b.poll_events(1)[0], Event::Data { offset: 4, .. }));
    assert_eq!(b.poll_events(1), [Event::RemoteFinished]);
    b.finish().unwrap();
    b.poll_frames(8);
    assert_eq!(b.state(), State::Draining);
    b.consume_through(8).unwrap();
    assert_eq!(b.state(), State::Closed);
}

#[test]
fn cancel_discards_backlogs_and_emits_one_terminal_notification() {
    let (mut a, mut b) = pair(small(), small());
    a.send(b"1234").unwrap();
    a.send(b"5678").unwrap();
    b.send(b"held").unwrap();
    pump(&mut b, &mut a);
    a.close(CloseReason::Cancelled).unwrap();
    a.close(CloseReason::Cancelled).unwrap();
    assert_abort(
        &mut a,
        CloseReason::Cancelled,
        Some(Frame::Close {
            reason: CloseReason::Cancelled,
        }),
    );
    a.receive(&data(99, b"late"), 0).unwrap();
    assert!(a.poll_events(8).is_empty());
    assert!(a.poll_frames(8).is_empty());
    assert_eq!(a.send(b"x"), Err(Error::InvalidState));
    assert_eq!(a.consume_through(1), Err(Error::InvalidState));
}

#[test]
fn transport_loss_is_local_and_does_not_enqueue_unusable_frames() {
    let (mut a, _) = pair(small(), small());
    a.send(b"held").unwrap();
    a.transport_lost();
    a.transport_lost();
    assert_abort(&mut a, CloseReason::TransportLost, None);
}

#[test]
fn opening_timeout_is_exact_and_cannot_be_revived_by_late_accept() {
    let mut a = Stream::new(small()).unwrap();
    a.open(b"", 20).unwrap();
    a.poll_frames(8);
    a.tick(29).unwrap();
    assert_eq!(a.state(), State::Opening(Direction::Outgoing));
    a.receive(
        &Frame::Accept {
            receive_window: 8,
            max_frame: 4,
            metadata: Box::new([]),
        },
        30,
    )
    .unwrap();
    assert_abort(
        &mut a,
        CloseReason::OpenTimeout,
        Some(Frame::Close {
            reason: CloseReason::OpenTimeout,
        }),
    );
}

#[test]
fn incoming_open_is_also_subject_to_deadline() {
    let mut a = Stream::new(small()).unwrap();
    let mut b = Stream::new(small()).unwrap();
    a.open(b"", 0).unwrap();
    pump(&mut a, &mut b);
    b.tick(10).unwrap();
    assert_abort(
        &mut b,
        CloseReason::OpenTimeout,
        Some(Frame::Close {
            reason: CloseReason::OpenTimeout,
        }),
    );
}

#[test]
fn clock_errors_are_atomic_and_established_streams_have_no_idle_timeout() {
    let mut idle = Stream::new(small()).unwrap();
    let before = idle.snapshot();
    assert_eq!(idle.open(b"", u64::MAX - 1), Err(Error::InvalidTime));
    assert_eq!(idle.snapshot(), before);
    assert_eq!(
        idle.receive(
            &Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            u64::MAX - 1,
        ),
        Err(Error::InvalidTime)
    );
    assert_eq!(idle.snapshot(), before);
    idle.tick(5).unwrap();
    assert_eq!(idle.open(b"", 4), Err(Error::InvalidTime));
    assert_eq!(idle.tick(4), Err(Error::InvalidTime));
    let (mut a, _) = pair(small(), small());
    a.tick(u64::MAX).unwrap();
    assert_eq!(a.state(), State::Open);
}

#[test]
fn accept_cannot_arrive_before_open_was_dispatched() {
    let mut stream = Stream::new(small()).unwrap();
    stream.open(b"", 0).unwrap();
    assert_eq!(
        stream.receive(
            &Frame::Accept {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            0,
        ),
        Err(Error::Protocol(ProtocolError::UnexpectedFrame))
    );
}

#[test]
fn established_stream_rejects_invalid_control_frames() {
    let invalid = [
        (
            Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            ProtocolError::UnexpectedFrame,
        ),
        (
            Frame::Accept {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            ProtocolError::UnexpectedFrame,
        ),
        (
            Frame::Reject {
                reason: Box::new([]),
            },
            ProtocolError::UnexpectedFrame,
        ),
        (
            Frame::Close {
                reason: CloseReason::Finished,
            },
            ProtocolError::InvalidCloseReason,
        ),
    ];
    for (frame, reason) in invalid {
        let (_, mut stream) = pair(small(), small());
        assert_eq!(stream.receive(&frame, 0), Err(Error::Protocol(reason)));
        assert_abort(
            &mut stream,
            CloseReason::ProtocolError,
            Some(Frame::Close {
                reason: CloseReason::ProtocolError,
            }),
        );
    }
    let (mut a, mut b) = pair(small(), small());
    a.send(b"1234").unwrap();
    pump(&mut a, &mut b);
    assert_eq!(
        a.receive(&Frame::WindowUpdate { consumed: 5 }, 0),
        Err(Error::Protocol(ProtocolError::InvalidCredit))
    );
}

#[test]
fn peer_cancellation_releases_buffers_without_echoing_close() {
    let (_, mut b) = pair(small(), small());
    b.receive(&data(0, b"held"), 0).unwrap();
    b.send(b"held").unwrap();
    b.receive(
        &Frame::Close {
            reason: CloseReason::Cancelled,
        },
        0,
    )
    .unwrap();
    assert_abort(&mut b, CloseReason::Cancelled, None);
}

#[test]
fn unexpected_or_invalid_peer_frames_fail_closed() {
    let invalid = [
        (
            Frame::Open {
                receive_window: 0,
                max_frame: 1,
                metadata: Box::new([]),
            },
            ProtocolError::InvalidLimits,
        ),
        (
            Frame::Open {
                receive_window: 1,
                max_frame: 2,
                metadata: Box::new([]),
            },
            ProtocolError::InvalidLimits,
        ),
        (
            Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([0; 9]),
            },
            ProtocolError::MetadataTooLarge,
        ),
        (data(0, b"early"), ProtocolError::UnexpectedFrame),
        (
            Frame::Accept {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([]),
            },
            ProtocolError::UnexpectedFrame,
        ),
    ];
    for (frame, reason) in invalid {
        let mut stream = Stream::new(small()).unwrap();
        assert_eq!(stream.receive(&frame, 0), Err(Error::Protocol(reason)));
        assert_abort(
            &mut stream,
            CloseReason::ProtocolError,
            Some(Frame::Close {
                reason: CloseReason::ProtocolError,
            }),
        );
    }
}

#[test]
fn config_limits_are_validated_before_allocating_stream_buffers() {
    let invalid = [
        Config {
            receive_window: 0,
            ..small()
        },
        Config {
            receive_window: u32::MAX,
            ..small()
        },
        Config {
            max_frame: 0,
            ..small()
        },
        Config {
            max_frame: 9,
            ..small()
        },
        Config {
            max_pending_frames: 0,
            ..small()
        },
        Config {
            max_pending_frames: usize::MAX,
            ..small()
        },
        Config {
            max_metadata: usize::MAX,
            ..small()
        },
        Config {
            open_timeout_ms: 0,
            ..small()
        },
    ];
    for config in invalid {
        assert!(matches!(Stream::new(config), Err(Error::InvalidConfig(_))));
    }
}

#[test]
fn varied_duplex_interleavings_preserve_all_bytes_and_bounds() {
    let config = Config {
        receive_window: 97,
        max_frame: 19,
        max_pending_frames: 3,
        ..small()
    };
    for seed in 1..=8u64 {
        let (a, b) = pair(config, config);
        let mut streams = [a, b];
        let payloads: [Vec<u8>; 2] = [
            (0..2048).map(|i| (i * 37 + seed as usize) as u8).collect(),
            (0..1537).map(|i| (i * 19 + seed as usize) as u8).collect(),
        ];
        let mut sent = [0usize; 2];
        let mut received: [Vec<u8>; 2] = [Vec::new(), Vec::new()];
        let mut finished = [false; 2];
        let mut random = seed;
        let mut complete = false;
        for step in 0..10_000 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            for i in 0..2 {
                if !finished[i] {
                    if let SendOutcome::Accepted(count) =
                        streams[i].send(&payloads[i][sent[i]..]).unwrap()
                    {
                        sent[i] += count;
                    }
                    if sent[i] == payloads[i].len() {
                        streams[i].finish().unwrap();
                        finished[i] = true;
                    }
                }
                let batch = 1 + ((random >> (i * 8)) as usize % 3);
                for frame in streams[i].poll_frames(batch) {
                    streams[1 - i].receive(&frame, step).unwrap();
                }
                for event in streams[i].poll_events(batch) {
                    if let Event::Data { offset, bytes } = event {
                        assert_eq!(offset, received[i].len() as u64);
                        received[i].extend_from_slice(&bytes);
                    }
                }
                if step % 5 == i as u64 && streams[i].state() != State::Closed {
                    streams[i]
                        .consume_through(received[i].len() as u64)
                        .unwrap();
                }
                let snapshot = streams[i].snapshot();
                assert!(snapshot.pending_data_frames <= config.max_pending_frames);
                assert!(snapshot.pending_send_bytes <= 3 * 19);
                assert!(snapshot.buffered_receive_bytes <= config.receive_window as usize);
                assert!(snapshot.receive_unconsumed_bytes <= config.receive_window as u64);
                assert!(snapshot.send_unacknowledged_bytes <= config.receive_window as u64);
            }
            if streams.iter().all(|stream| stream.state() == State::Closed) {
                complete = true;
                break;
            }
        }
        assert!(complete, "duplex transfer stalled with seed {seed}");
        assert_eq!(received[0], payloads[1]);
        assert_eq!(received[1], payloads[0]);
    }
}
