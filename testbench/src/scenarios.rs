//! Shared real-broker scenarios for the command-line demo and integration tests.

use crate::{BenchConfig, BenchError, ConnectionConfig, Node, Role};
use skvoz_core::{CloseReason, Event, SendOutcome};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

pub async fn pair(case: &str, config: BenchConfig) -> Result<(Node, Node), BenchError> {
    let user = Node::connect(ConnectionConfig::from_env(Role::User)?, case, config).await?;
    let consumer = Node::connect(ConnectionConfig::from_env(Role::Consumer)?, case, config).await?;
    Ok((user, consumer))
}

pub async fn establish(user: &mut Node, consumer: &mut Node) -> Result<u64, BenchError> {
    let id = user.open(&[0, 255]).await?;
    consumer.turn(Duration::from_secs(1)).await?;
    let events = consumer.poll_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].stream_id, id);
    assert_eq!(
        events[0].event,
        Event::IncomingOpen {
            metadata: Box::new([0, 255])
        }
    );
    consumer.accept(id, &[128, 0]).await?;
    user.turn(Duration::from_secs(1)).await?;
    for node in [user, consumer] {
        let events = node.poll_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].stream_id, id);
        assert_eq!(
            events[0].event,
            Event::Opened {
                metadata: Box::new([128, 0])
            }
        );
    }
    Ok(id)
}

fn payload(id: u64, side: usize, length: usize) -> Vec<u8> {
    (0..length)
        .map(|i| (i as u64 * 37 + id * 13 + side as u64 * 127) as u8)
        .collect()
}

/// Both roles initiate streams and simultaneously send/consume binary payloads.
pub async fn exchange(case: &str, stream_count: usize, length: usize) -> Result<(), BenchError> {
    assert!((1..=16).contains(&stream_count));
    assert!(length <= 256 * 1024);
    let config = BenchConfig::default();
    let (mut user, mut consumer) = pair(case, config).await?;
    let mut ids = Vec::new();
    for i in 0..stream_count {
        let origin = if i % 2 == 0 { &mut user } else { &mut consumer };
        ids.push(origin.open(&[0, 255, (i % 2) as u8]).await?);
    }
    let start = Instant::now();
    let mut opened = [BTreeSet::new(), BTreeSet::new()];
    while opened.iter().any(|set| set.len() != stream_count) {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "real handshake timed out"
        );
        for (side, node) in [&mut user, &mut consumer].into_iter().enumerate() {
            node.turn(Duration::from_millis(1)).await?;
            for event in node.poll_events() {
                assert!(ids.contains(&event.stream_id));
                match event.event {
                    Event::IncomingOpen { metadata } => {
                        assert_eq!(&*metadata, &[0, 255, (event.stream_id & 1) as u8]);
                        node.accept(event.stream_id, &[128, 0]).await?;
                    }
                    Event::Opened { metadata } => {
                        assert_eq!(&*metadata, &[128, 0]);
                        assert!(opened[side].insert(event.stream_id));
                    }
                    other => panic!("unexpected handshake event: {other:?}"),
                }
            }
        }
    }
    let observed = std::process::Command::new("python3").args([
        "-c",
        "import json,os,sys,urllib.request; d=json.load(urllib.request.urlopen(os.environ['SKVOZ_NATS_MONITOR']+'/connz?limit=1024',timeout=2)); print(sum(c.get('name') in ['skvoz-'+sys.argv[1]+'-user','skvoz-'+sys.argv[1]+'-consumer'] for c in d['connections']))",
        case,
    ]).output()?;
    assert!(
        observed.status.success(),
        "real NATS connection observation failed"
    );
    assert_eq!(
        String::from_utf8(observed.stdout)?.trim(),
        "2",
        "streams must share exactly two real connections"
    );
    let expected: [BTreeMap<u64, Vec<u8>>; 2] = std::array::from_fn(|side| {
        ids.iter()
            .map(|&id| (id, payload(id, side, length)))
            .collect()
    });
    let mut sent: [BTreeMap<u64, usize>; 2] =
        std::array::from_fn(|_| ids.iter().map(|&id| (id, 0)).collect());
    let mut received: [BTreeMap<u64, Vec<u8>>; 2] =
        std::array::from_fn(|_| ids.iter().map(|&id| (id, Vec::new())).collect());
    let mut finished = [BTreeSet::new(), BTreeSet::new()];
    let mut closed = [BTreeSet::new(), BTreeSet::new()];
    while closed.iter().any(|set| set.len() != stream_count) {
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "real duplex transfer stalled"
        );
        for (side, node) in [&mut user, &mut consumer].into_iter().enumerate() {
            for &id in &ids {
                if !finished[side].contains(&id) {
                    let offset = sent[side][&id];
                    if let SendOutcome::Accepted(count) =
                        node.send(id, &expected[side][&id][offset..]).await?
                    {
                        *sent[side].get_mut(&id).unwrap() += count;
                    }
                    if sent[side][&id] == length {
                        node.finish(id).await?;
                        finished[side].insert(id);
                    }
                }
                let snapshot = node.snapshot(id).unwrap();
                assert!(snapshot.receive_unconsumed_bytes <= config.stream.receive_window as u64);
                assert!(snapshot.pending_data_frames <= config.stream.max_pending_frames);
                assert!(snapshot.send_unacknowledged_bytes <= config.stream.receive_window as u64);
            }
            node.turn(Duration::from_millis(1)).await?;
            for event in node.poll_events() {
                match event.event {
                    Event::Data { offset, bytes } => {
                        let buffer = received[side].get_mut(&event.stream_id).unwrap();
                        assert_eq!(offset, buffer.len() as u64);
                        buffer.extend_from_slice(&bytes);
                        let consumed = buffer.len() as u64;
                        drop(bytes);
                        node.consume_through(event.stream_id, consumed).await?;
                    }
                    Event::RemoteFinished => assert_eq!(
                        received[side][&event.stream_id],
                        expected[1 - side][&event.stream_id]
                    ),
                    Event::Closed { reason } => {
                        assert_eq!(reason, CloseReason::Finished);
                        assert!(closed[side].insert(event.stream_id));
                    }
                    Event::Writable => {}
                    other => panic!("unexpected transfer event: {other:?}"),
                }
            }
        }
    }
    for side in 0..2 {
        for &id in &ids {
            assert_eq!(received[side][&id], expected[1 - side][&id]);
        }
    }
    for node in [&mut user, &mut consumer] {
        assert_eq!(node.slot_count(), stream_count);
        for &id in &ids {
            let snapshot = node.snapshot(id).unwrap();
            assert_eq!(snapshot.buffered_receive_bytes, 0);
            assert_eq!(snapshot.pending_send_bytes, 0);
            assert_eq!(snapshot.receive_capacity_bytes, 0);
        }
    }
    user.shutdown().await?;
    consumer.shutdown().await?;
    println!(
        "PASS real NATS: {stream_count} streams, {length} bytes per direction per stream, both roles initiated, clean half-close"
    );
    Ok(())
}
