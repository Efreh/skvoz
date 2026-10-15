//! Real TLS/NATS contracts; run explicitly with the provisioned testbench fixture.
#![cfg(feature = "nats")]
use skvoz_core::{Config, Event, ManagerConfig, PeerId, SendOutcome, runtime::*};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

async fn node(id: u64, case: &str) -> NatsRuntime {
    let mut c = RuntimeConfig::new(
        if id == 0 {
            std::env::var("SKVOZ_NATS_SERVER_URL").or_else(|_| std::env::var("SKVOZ_NATS_URL"))
        } else {
            std::env::var("SKVOZ_NATS_URL")
        }
        .expect("provisioned NATS URL"),
        Trust::ManagedCa(std::env::var("SKVOZ_NATS_CA").unwrap().into()),
        Authentication {
            username: format!("p{id}"),
            password: std::env::var(format!("SKVOZ_NATS_P{id}_PASSWORD")).unwrap(),
        },
        format!(
            "skvoz.runtime.{}.{case}",
            std::env::var("SKVOZ_NATS_RUN_TOKEN").unwrap()
        ),
        PeerId(id),
        if id == 0 {
            Membership::BrokerAuthorized
        } else {
            Membership::Allowlist(BTreeSet::from([PeerId(0)]))
        },
    );
    c.initiate = if id == 0 { vec![] } else { vec![PeerId(0)] };
    c.io_timeout = Duration::from_secs(3);
    c.peer_timeout = Duration::from_secs(10);
    c.heartbeat_interval = Duration::from_secs(1);
    c.join_timeout = Duration::from_secs(15);
    c.terminal_drain_timeout = Duration::from_secs(3);
    let limits = ManagerConfig {
        stream: Config {
            receive_window: 4 << 20,
            max_frame: 16384,
            max_pending_frames: 64,
            max_metadata: 512,
            open_timeout_ms: 15000,
        },
        receive_budget: 128 << 20,
        receive_budget_per_peer: 4 << 20,
        send_budget: 64 << 20,
        send_budget_per_peer: 2 << 20,
        ..ManagerConfig::default()
    };
    NatsRuntime::connect(c, limits).await.unwrap()
}

async fn joined(a: &mut NatsRuntime, b: &mut NatsRuntime, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !a.peer_ready(PeerId(0)) || !b.peer_ready(PeerId(id)) {
        assert!(
            Instant::now() < deadline,
            "join: {:?} {:?}",
            a.status(),
            b.status()
        );
        a.turn(Duration::ZERO).await.unwrap();
        b.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert!(a.peer_status(PeerId(0)).unwrap().granted_bytes >= 4096);
}

async fn opened(a: &mut NatsRuntime, b: &mut NatsRuntime) -> (RuntimeKey, RuntimeKey) {
    let local = a.open(PeerId(0), b"opaque").unwrap();
    let mut remote = None;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            Instant::now() < deadline,
            "OPEN: {:?} {:?}",
            a.status(),
            b.status()
        );
        a.turn(Duration::ZERO).await.unwrap();
        b.turn(Duration::from_millis(1)).await.unwrap();
        for e in b.poll_events(64) {
            if let Event::IncomingOpen { .. } = e.event {
                b.accept(e.key, b"").unwrap();
                remote = Some(e.key);
            }
        }
        if a.poll_events(64)
            .iter()
            .any(|e| matches!(e.event, Event::Opened { .. }))
        {
            return (local, remote.unwrap());
        }
    }
}

async fn transfer(
    a: &mut NatsRuntime,
    b: &mut NatsRuntime,
    local: RuntimeKey,
    remote: RuntimeKey,
    start: usize,
    count: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let (mut sent, mut received) = (start, start);
    let data = [0x5a; 16384];
    while received < start + count {
        assert!(
            Instant::now() < deadline,
            "transfer sent={sent} received={received}; a={:?} b={:?}",
            a.status(),
            b.status()
        );
        while sent < start + count {
            match a
                .send(local, &data[..(start + count - sent).min(data.len())])
                .unwrap()
            {
                SendOutcome::Accepted(n) => sent += n,
                SendOutcome::WouldBlock => break,
            }
        }
        a.turn(Duration::ZERO).await.unwrap();
        b.turn(Duration::from_millis(1)).await.unwrap();
        a.poll_events(256);
        for event in b.poll_events(256) {
            match event.event {
                Event::Data { offset, bytes } => {
                    assert_eq!(event.key, remote);
                    assert_eq!(offset as usize, received);
                    assert!(bytes.iter().all(|b| *b == 0x5a));
                    received += bytes.len();
                    b.consume_through(remote, received as u64).unwrap();
                }
                Event::Closed { reason } => panic!(
                    "unexpected closure: {reason:?}; a={:?} peer={:?} b={:?} peer={:?}",
                    a.status(),
                    a.peer_status(PeerId(0)),
                    b.status(),
                    b.peer_status(PeerId(1))
                ),
                _ => {}
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires provisioned TLS/auth NATS fixture"]
async fn continuous_window_survives_empty_output_and_preserves_exact_bytes() {
    let (mut a, mut b) = (
        node(1, "continuous032").await,
        node(0, "continuous032").await,
    );
    joined(&mut a, &mut b, 1).await;
    let (local, remote) = opened(&mut a, &mut b).await;
    transfer(&mut a, &mut b, local, remote, 0, 4 << 20).await;
    for _ in 0..16 {
        a.turn(Duration::ZERO).await.unwrap();
        b.turn(Duration::ZERO).await.unwrap();
    }
    let before = a.peer_status(PeerId(0)).unwrap();
    assert_eq!(before.delivery_epoch, 0);
    assert!(before.granted_bytes > before.sent_bytes);
    tokio::time::sleep(Duration::from_millis(50)).await;
    transfer(&mut a, &mut b, local, remote, 4 << 20, 4 << 20).await;
    let after = a.peer_status(PeerId(0)).unwrap();
    assert_eq!(after.delivery_epoch, 0);
    assert!(after.granted_bytes >= before.granted_bytes);
    assert_eq!(a.status().counters.shard_overflows, 0);
    assert_eq!(b.status().counters.shard_overflows, 0);
    println!(
        "continuous exact bytes={} epoch={} control={} granted={} sent={} applied={}",
        8 << 20,
        after.delivery_epoch,
        after.sent_controls,
        after.granted_bytes,
        after.sent_bytes,
        after.applied_bytes
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires provisioned TLS/auth NATS fixture"]
async fn shared_send_flight_covers_recipients_on_different_lanes() {
    let (mut server, mut a, mut b) = (
        node(0, "domain032").await,
        node(1, "domain032").await,
        node(2, "domain032").await,
    );
    joined(&mut a, &mut server, 1).await;
    joined(&mut b, &mut server, 2).await;
    let (local_a, remote_a) = opened(&mut a, &mut server).await;
    let (local_b, remote_b) = opened(&mut b, &mut server).await;
    transfer(&mut server, &mut a, remote_a, local_a, 0, 2 << 20).await;
    transfer(&mut server, &mut b, remote_b, local_b, 0, 2 << 20).await;
    let total = 10 << 20;
    let (mut sent, mut received) = ([2 << 20; 2], [2 << 20; 2]);
    let data = [0x5a; 16384];
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut peak = 0;
    let mut turns = 0usize;
    while received != [total; 2] {
        assert!(
            Instant::now() < deadline,
            "shared flight deadline sent={sent:?} received={received:?}"
        );
        for (i, key) in [remote_a, remote_b].into_iter().enumerate() {
            while sent[i] < total {
                match server
                    .send(key, &data[..(total - sent[i]).min(data.len())])
                    .unwrap()
                {
                    SendOutcome::Accepted(n) => sent[i] += n,
                    SendOutcome::WouldBlock => break,
                }
            }
        }
        server.turn(Duration::ZERO).await.unwrap();
        let status = server.delivery_status().unwrap();
        assert!(status.send_flight_bytes <= status.limit_bytes);
        assert!(status.send_flight_records <= status.limit_records);
        assert!(status.receive_promised_bytes <= status.limit_bytes);
        assert!(status.receive_promised_records <= status.limit_records);
        peak = peak.max(status.send_flight_bytes);
        // Let DATA accumulate across two independent recipient sockets. The
        // global pool, rather than an individual lane grant, must bound it.
        if turns.is_multiple_of(16) {
            for (i, client, key) in [(0, &mut a, local_a), (1, &mut b, local_b)] {
                client.turn(Duration::ZERO).await.unwrap();
                for event in client.poll_events(256) {
                    match event.event {
                        Event::Data { offset, bytes } => {
                            assert_eq!(event.key, key);
                            assert_eq!(offset as usize, received[i]);
                            assert!(bytes.iter().all(|b| *b == 0x5a));
                            received[i] += bytes.len();
                            client.consume_through(key, received[i] as u64).unwrap();
                        }
                        Event::Closed { reason } => panic!("shared flight closed: {reason:?}"),
                        _ => {}
                    }
                }
            }
        }
        server.poll_events(256);
        tokio::task::yield_now().await;
        turns += 1;
    }
    assert!(
        peak > 256 << 10,
        "test must exercise an active shared flight: {peak}"
    );
    assert_eq!(server.status().counters.shard_overflows, 0);
    println!(
        "shared different-lane recipients=2 bytes_each={total} peak_flight={peak} limit={}",
        server.delivery_status().unwrap().limit_bytes
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires provisioned TLS/auth NATS fixture"]
async fn initial_hello_without_remote_interest_recovers_in_same_generation() {
    let mut client = node(1, "cold032").await;
    let generation = client.generation();
    // The destination has no subscription yet. Core NATS legitimately drops
    // this initial HELLO; the DATA path must never rely on broker persistence.
    client.turn(Duration::ZERO).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut server = node(0, "cold032").await;
    let start = Instant::now();
    while !client.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "initial lost HELLO was not recovered within the bootstrap bound"
        );
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(client.generation(), generation);
    assert_eq!(client.status().counters.joins, 1);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires provisioned TLS/auth NATS fixture"]
async fn simultaneous_peer_close_drains_both_prefixes_and_all_promises() {
    let (mut a, mut b) = (node(1, "close032").await, node(0, "close032").await);
    joined(&mut a, &mut b, 1).await;
    let (local, remote) = opened(&mut a, &mut b).await;
    assert!(matches!(
        a.send(local, &[1; 8192]).unwrap(),
        SendOutcome::Accepted(_)
    ));
    assert!(matches!(
        b.send(remote, &[2; 8192]).unwrap(),
        SendOutcome::Accepted(_)
    ));
    a.turn(Duration::ZERO).await.unwrap();
    b.turn(Duration::ZERO).await.unwrap();
    a.terminate_peer(PeerId(0));
    b.terminate_peer(PeerId(1));
    let until = Instant::now() + Duration::from_secs(3);
    while !a.peer_streams_drained(PeerId(0)) || !b.peer_streams_drained(PeerId(1)) {
        assert!(
            Instant::now() < until,
            "simultaneous close did not drain: {:?} {:?}",
            a.status(),
            b.status()
        );
        a.turn(Duration::ZERO).await.unwrap();
        b.turn(Duration::from_millis(1)).await.unwrap();
        a.poll_events(256);
        b.poll_events(256);
    }
    for node in [&a, &b] {
        let status = node.delivery_status().unwrap();
        assert_eq!(status.send_flight_bytes, 0);
        assert_eq!(status.receive_promised_bytes, 0);
        assert_eq!(status.quarantined_send_bytes, 0);
        assert_eq!(status.quarantined_receive_bytes, 0);
        assert_eq!(node.status().counters.shard_failures, 0);
    }
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires provisioned TLS/auth NATS fixture"]
async fn missing_terminal_receipt_keeps_unknown_debt_and_healthy_peer_progress() {
    let (mut a, mut b, mut healthy) = (
        node(1, "close-loss032").await,
        node(0, "close-loss032").await,
        node(2, "close-loss032").await,
    );
    joined(&mut a, &mut b, 1).await;
    joined(&mut healthy, &mut b, 2).await;
    let (_, remote) = opened(&mut a, &mut b).await;
    assert!(matches!(
        b.send(remote, &[1; 8192]).unwrap(),
        SendOutcome::Accepted(_)
    ));
    b.turn(Duration::ZERO).await.unwrap();
    let peer = b.peer_status(PeerId(1)).unwrap();
    let debt = peer.sent_bytes - peer.applied_bytes;
    assert!(debt > 0);
    a.shutdown().await.unwrap();
    b.terminate_peer(PeerId(1));
    let until = Instant::now() + Duration::from_millis(3200);
    while Instant::now() < until {
        b.turn(Duration::from_millis(1)).await.unwrap();
        healthy.turn(Duration::ZERO).await.unwrap();
        b.poll_events(256);
        healthy.poll_events(256);
    }
    assert!(
        !b.peer_streams_drained(PeerId(1)),
        "timeout fabricated a successful terminal receipt"
    );
    assert!(b.delivery_status().unwrap().quarantined_send_bytes >= debt);
    assert!(b.peer_ready(PeerId(2)));
    let (local, remote) = opened(&mut healthy, &mut b).await;
    transfer(&mut healthy, &mut b, local, remote, 0, 8192).await;
    assert_eq!(b.status().counters.shard_failures, 0);
    healthy.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
