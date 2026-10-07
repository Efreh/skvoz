use skvoz_core::{
    CloseReason, Event, ManagerConfig, PeerId,
    runtime::{Lifecycle, NatsRuntime, RuntimeError, Trust},
};
use skvoz_testbench::runtime_scenarios as r;
use std::time::{Duration, Instant};
#[tokio::test]
async fn grown_stream_window_survives_repeated_consumption_over_real_nats() {
    const WINDOW: usize = 256 << 10;
    const WARMUP: usize = 4 << 20;
    let profile = ManagerConfig {
        stream: skvoz_core::Config {
            receive_window: WINDOW as u32,
            max_frame: 16384,
            max_pending_frames: 32,
            ..ManagerConfig::default().stream
        },
        receive_budget: 2 << 20,
        receive_budget_per_peer: 2 << 20,
        max_peers: 1,
        ..ManagerConfig::default()
    };
    let mut server = NatsRuntime::connect(r::config(0, "persistent-window").unwrap(), profile)
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(r::config(1, "persistent-window").unwrap(), profile)
        .await
        .unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (local, remote) = r::handshake(&mut client, &mut server).await.unwrap();
    let payload = [0x73; 16384];
    let mut sent = 0;
    let mut received = 0;
    let start = Instant::now();
    for round in 0..13 {
        while server.snapshot(remote).unwrap().send_unacknowledged_bytes != 0 {
            assert!(start.elapsed() < Duration::from_secs(15));
            client.turn(Duration::ZERO).await.unwrap();
            server.turn(Duration::from_millis(1)).await.unwrap();
            server.poll_events(256);
        }
        let target = if round == 0 { WARMUP } else { sent + WINDOW };
        while received < target {
            assert!(start.elapsed() < Duration::from_secs(15));
            while sent < target {
                match server
                    .send(remote, &payload[..payload.len().min(target - sent)])
                    .unwrap()
                {
                    skvoz_core::SendOutcome::Accepted(count) => sent += count,
                    skvoz_core::SendOutcome::WouldBlock => break,
                }
            }
            if round != 0 {
                assert_eq!(
                    server.snapshot(remote).unwrap().send_unacknowledged_bytes,
                    WINDOW as u64,
                    "grown window lost after warmup in round {round}"
                );
            }
            server.turn(Duration::ZERO).await.unwrap();
            client.turn(Duration::from_millis(1)).await.unwrap();
            server.poll_events(256);
            for event in client.poll_events(256) {
                match event.event {
                    Event::Data { offset, bytes } => {
                        assert_eq!(event.key, local);
                        assert_eq!(offset, received as u64);
                        assert!(bytes.iter().all(|byte| *byte == 0x73));
                        received += bytes.len();
                        if round == 0 {
                            client.consume_through(local, received as u64).unwrap();
                        }
                    }
                    Event::Closed { reason } => panic!("unexpected terminal event: {reason:?}"),
                    _ => {}
                }
            }
        }
        assert_eq!(sent, target);
        client.consume_through(local, received as u64).unwrap();
    }
    assert_eq!(received, WARMUP + 12 * WINDOW);
    assert_eq!(server.peer_limits(remote).unwrap().receive_window, 65536);
    assert_eq!(server.status().counters.shard_overflows, 0);
    assert_eq!(client.status().counters.shard_overflows, 0);
    println!("persistent-window received={received} rounds=12 window={WINDOW}");
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn growing_duplex_credit_keeps_subscription_64_drained_during_output() {
    let profile = ManagerConfig {
        stream: skvoz_core::Config {
            receive_window: 32 << 20,
            max_frame: 16384,
            max_pending_frames: 32,
            ..ManagerConfig::default().stream
        },
        receive_budget: 32 << 20,
        receive_budget_per_peer: 32 << 20,
        max_peers: 1,
        ..ManagerConfig::default()
    };
    let mut server_config = r::config(0, "duplex-credit").unwrap();
    server_config.subscription_capacity = 64;
    server_config.max_incoming_per_turn = 32;
    let mut client_config = r::config(1, "duplex-credit").unwrap();
    client_config.subscription_capacity = 64;
    client_config.max_incoming_per_turn = 32;
    let mut server = NatsRuntime::connect(server_config, profile).await.unwrap();
    let mut client = NatsRuntime::connect(client_config, profile).await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (local, remote) = r::handshake(&mut client, &mut server).await.unwrap();
    async fn transfer(
        mut node: NatsRuntime,
        key: skvoz_core::runtime::RuntimeKey,
        byte: u8,
    ) -> NatsRuntime {
        const SIZE: usize = 4 << 20;
        let payload = vec![byte; SIZE];
        let mut sent = 0;
        let mut received = 0;
        let initial_window = node.peer_limits(key).unwrap().receive_window as u64;
        let mut largest_flight = 0;
        let started = Instant::now();
        while sent < SIZE || received < SIZE {
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "{:?}",
                node.status()
            );
            if sent < SIZE
                && let skvoz_core::SendOutcome::Accepted(count) =
                    node.send(key, &payload[sent..]).unwrap()
            {
                sent += count;
            }
            largest_flight =
                largest_flight.max(node.snapshot(key).unwrap().send_unacknowledged_bytes);
            node.turn(Duration::from_millis(1)).await.unwrap();
            for event in node.poll_events(256) {
                match event.event {
                    Event::Data { offset, bytes } => {
                        assert_eq!(offset, received as u64);
                        assert!(bytes.iter().all(|value| *value == (byte ^ 1)));
                        received += bytes.len();
                        node.consume_through(event.key, received as u64).unwrap();
                    }
                    Event::Closed { reason } => panic!("unexpected terminal event: {reason:?}"),
                    _ => {}
                }
            }
        }
        assert_eq!(received, SIZE);
        assert!(
            largest_flight > initial_window,
            "active stream flight did not grow"
        );
        assert_eq!(node.status().counters.shard_overflows, 0);
        assert!(node.peer_ready(key.stream.peer));
        println!(
            "duplex byte={byte:#x} sent={sent} received={received} initial_window={initial_window} largest_flight={largest_flight} elapsed={:?}",
            started.elapsed()
        );
        node
    }
    let (mut client, mut server) = tokio::join!(
        transfer(client, local, 0x40),
        transfer(server, remote, 0x41)
    );
    // Warm bidirectional credit also measures the peer RTT before a fresh flow
    // waits in its owner's queue. No acknowledgement precedes actual consumption.
    let (local, remote) = r::handshake(&mut client, &mut server).await.unwrap();
    let initial_window = client.peer_limits(local).unwrap().receive_window as usize;
    assert_eq!(initial_window, 65536);
    let payload = vec![0x72; 256 << 10];
    let mut sent = 0;
    let mut received = 0;
    let collect = |node: &mut NatsRuntime, received: &mut usize| {
        for event in node.poll_events(256) {
            match event.event {
                Event::Data { offset, bytes } => {
                    assert_eq!(event.key, remote);
                    assert_eq!(offset, *received as u64);
                    assert!(bytes.iter().all(|byte| *byte == 0x72));
                    *received += bytes.len();
                }
                Event::Closed { reason } => panic!("unexpected terminal event: {reason:?}"),
                _ => {}
            }
        }
    };
    let queued = Instant::now();
    while queued.elapsed() < Duration::from_millis(250) || received < initial_window {
        assert!(
            queued.elapsed() < Duration::from_secs(3),
            "initial flight did not arrive"
        );
        if let skvoz_core::SendOutcome::Accepted(count) =
            client.send(local, &payload[sent..]).unwrap()
        {
            sent += count;
        }
        client.turn(Duration::from_millis(1)).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(256);
        collect(&mut server, &mut received);
    }
    assert_eq!(received, initial_window);
    assert_eq!(
        server.snapshot(remote).unwrap().receive_unconsumed_bytes,
        received as u64
    );
    server
        .consume_through(remote, (initial_window / 2) as u64)
        .unwrap();
    let growth = Instant::now();
    let largest_flight = loop {
        assert!(
            growth.elapsed() < Duration::from_secs(2),
            "first consumption did not grow queued flow flight"
        );
        client.turn(Duration::from_millis(1)).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(256);
        collect(&mut server, &mut received);
        if let skvoz_core::SendOutcome::Accepted(count) =
            client.send(local, &payload[sent..]).unwrap()
        {
            sent += count;
        }
        let flight = client.snapshot(local).unwrap().send_unacknowledged_bytes;
        if flight > initial_window as u64 {
            break flight;
        }
    };
    println!(
        "queued wait_ms={} consumed={} initial_window={initial_window} largest_flight={largest_flight}",
        queued.elapsed().as_millis(),
        initial_window / 2
    );
    server.consume_through(remote, received as u64).unwrap();
    let drain = Instant::now();
    while received < sent {
        assert!(drain.elapsed() < Duration::from_secs(3));
        client.turn(Duration::from_millis(1)).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(256);
        collect(&mut server, &mut received);
        server.consume_through(remote, received as u64).unwrap();
    }
    assert_eq!(received, sent);
    assert_eq!(client.status().counters.shard_overflows, 0);
    assert_eq!(server.status().counters.shard_overflows, 0);
    assert!(client.peer_ready(PeerId(0)));
    assert!(server.peer_ready(PeerId(1)));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn expired_core_credit_freeze_retires_ready_session_without_late_data() {
    let mut server = r::node(0, "freeze-expiry").await.unwrap();
    let mut client = r::node(1, "freeze-expiry").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (_local, old) = r::handshake(&mut client, &mut server).await.unwrap();
    server.inject_expired_credit_freeze(PeerId(1));
    server.turn(Duration::ZERO).await.unwrap();
    assert!(!server.peer_ready(PeerId(1)));
    assert!(!server.peer_status(PeerId(1)).unwrap().ready);
    assert_eq!(server.status().active_peers, 0);
    assert_eq!(
        server.open(PeerId(1), b"too early"),
        Err(RuntimeError::PeerUnavailable)
    );
    let events = server.poll_events(64);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.event,
                Event::Closed {
                    reason: CloseReason::TransportLost
                }
            ))
            .count(),
        1
    );
    let start = Instant::now();
    while !client.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
        assert!(start.elapsed() < Duration::from_secs(8));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(64);
        server.poll_events(64);
    }
    let (local, fresh) = r::handshake(&mut client, &mut server).await.unwrap();
    assert_ne!(old.incarnation, fresh.incarnation);
    r::bytes(
        &mut client,
        &mut server,
        local,
        b"new flow after credit deadline",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn dynamic_join_restart_stale_connector_key_and_unrelated_peer() {
    let mut server = r::node(0, "restart").await.unwrap();
    let mut client = r::node(1, "restart").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (old, _) = r::handshake(&mut client, &mut server).await.unwrap();
    r::bytes(&mut client, &mut server, old, b"before restart")
        .await
        .unwrap();
    let mut healthy = r::node(2, "restart").await.unwrap();
    r::joined(&mut healthy, &mut server, 2).await.unwrap();
    let (healthy_key, _) = r::handshake(&mut healthy, &mut server).await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    let mut client = r::node(1, "restart").await.unwrap();
    let start = Instant::now();
    let mut closed = 0;
    while !client.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
        assert!(start.elapsed() < Duration::from_secs(6));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for e in server.poll_events(256) {
            if e.key.stream.peer == PeerId(1)
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                closed += 1;
            }
        }
        healthy.turn(Duration::ZERO).await.unwrap();
        healthy.poll_events(256);
    }
    assert_eq!(closed, 1);
    let (new, _) = r::handshake(&mut client, &mut server).await.unwrap();
    assert_eq!(old.stream.stream_id, new.stream.stream_id);
    assert_ne!(old.epoch, new.epoch);
    assert_eq!(client.consume_through(old, 1), Err(RuntimeError::StaleKey));
    assert_eq!(client.close(old), Err(RuntimeError::StaleKey));
    r::bytes(&mut client, &mut server, new, b"after restart")
        .await
        .unwrap();
    r::bytes(
        &mut healthy,
        &mut server,
        healthy_key,
        b"other peer survives",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    healthy.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn healthy_idle_slow_reader_and_silent_process_loss() {
    let mut server = r::node(0, "liveness").await.unwrap();
    let mut client = r::node(1, "liveness").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    client.send(key, b"slow reader").unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(2) {
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(2)).await.unwrap();
        client.poll_events(256);
    }
    assert!(client.peer_ready(PeerId(0)));
    assert!(server.peer_ready(PeerId(1)));
    assert_eq!(server.status().resources.streams, 1);
    drop(client);
    let start = Instant::now();
    let mut closed = 0;
    while closed == 0 {
        assert!(start.elapsed() < Duration::from_secs(2));
        server.turn(Duration::from_millis(2)).await.unwrap();
        for e in server.poll_events(256) {
            if matches!(
                e.event,
                Event::Closed {
                    reason: CloseReason::TransportLost
                }
            ) {
                closed += 1;
            }
        }
    }
    assert_eq!(closed, 1);
    assert_eq!(server.resources().streams, 0);
    assert_eq!(server.resources().reserved_receive_bytes, 0);
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn simultaneous_join_is_deterministic() {
    let mut config = r::config(0, "simultaneous").unwrap();
    config.initiate = vec![PeerId(1)];
    let mut server = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .unwrap();
    let mut client = r::node(1, "simultaneous").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    r::bytes(&mut client, &mut server, key, b"simultaneous")
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn trust_authentication_and_subject_acl_are_enforced() {
    let mut config = r::config(1, "security").unwrap();
    config.trust = Trust::ManagedCa(std::env::var("SKVOZ_NATS_WRONG_CA").unwrap().into());
    assert!(
        NatsRuntime::connect(config, ManagerConfig::default())
            .await
            .is_err()
    );
    let mut config = r::config(1, "security").unwrap();
    config.authentication.password = "wrong".into();
    let debug = format!("{config:?}");
    assert!(!debug.contains("wrong"));
    assert!(!debug.contains("127.0.0.1"));
    assert!(
        NatsRuntime::connect(config, ManagerConfig::default())
            .await
            .is_err()
    );
    let mut server = r::node(0, "security").await.unwrap();
    let mut client = r::node(1, "security").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let ns = r::config(1, "security").unwrap().namespace;
    client
        .inject_subject(format!("{ns}.join.0.2"), vec![0; 77])
        .await
        .unwrap();
    let start = Instant::now();
    while client.status().lifecycle == Lifecycle::Ready {
        assert!(start.elapsed() < Duration::from_secs(2));
        client.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(server.resources().streams, 0);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_broker_restart_recovers_new_streams_without_reviving_old() {
    let mut server = r::node(0, "broker_restart").await.unwrap();
    let mut client = r::node(1, "broker_restart").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (old, _) = r::handshake(&mut client, &mut server).await.unwrap();
    let generation = client.generation();
    let container = std::env::var("SKVOZ_NATS_CONTAINER").unwrap();
    let start = Instant::now();
    docker(&["restart", &container]);
    let mut closed = 0;
    while client.generation() == generation
        || !client.peer_ready(PeerId(0))
        || !server.peer_ready(PeerId(1))
    {
        assert!(start.elapsed() < Duration::from_secs(15));
        recovery_turn(&mut client).await;
        recovery_turn(&mut server).await;
        for e in client.poll_events(256) {
            if e.key == old
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                closed += 1;
            }
        }
        server.poll_events(256);
    }
    assert_eq!(closed, 1);
    assert_eq!(client.close(old), Err(RuntimeError::StaleKey));
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    r::bytes(
        &mut client,
        &mut server,
        key,
        b"new generation after real broker restart",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
async fn recovery_turn(node: &mut NatsRuntime) {
    if let Err(error) = node.turn(Duration::from_millis(2)).await {
        // A socket can fail before its asynchronous disconnect callback runs.
        assert!(
            matches!(error, RuntimeError::Transport | RuntimeError::Timeout),
            "unexpected recovery error: {error:?}; status: {:?}",
            node.status()
        );
    }
    assert!(
        matches!(
            node.status().lifecycle,
            Lifecycle::Ready | Lifecycle::Recovering
        ),
        "terminal recovery status: {:?}",
        node.status()
    );
}
#[tokio::test]
async fn injected_loss_of_final_data_without_callback_fails_watermark() {
    let mut server = r::node(0, "final_loss").await.unwrap();
    let mut client = r::node(1, "final_loss").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    client.inject_drop_next_data_after_extraction();
    client.send(key, b"intentionally lost final DATA").unwrap();
    let start = Instant::now();
    let mut closed = 0;
    while closed == 0 {
        assert!(start.elapsed() < Duration::from_secs(3));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for e in client.poll_events(256) {
            if e.key == key
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                closed += 1;
            }
        }
        server.poll_events(256);
    }
    assert_eq!(closed, 1);
    assert!(client.status().counters.peer_timeouts > 0);
    assert_eq!(client.resources().streams, 0);
    println!(
        "PASS deterministic final-DATA injection: no SlowConsumer callback, watermark prevents healthy acknowledgment"
    );
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_runtime_overflow_fails_affected_shard_and_preserves_other_shard() {
    let mut config = r::config(0, "overflow").unwrap();
    config.subscription_capacity = 2;
    let mut server = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .unwrap();
    let mut flood = r::node(1, "overflow").await.unwrap();
    let mut healthy = r::node(2, "overflow").await.unwrap();
    r::joined(&mut flood, &mut server, 1).await.unwrap();
    r::joined(&mut healthy, &mut server, 2).await.unwrap();
    let (flood_key, _) = r::handshake(&mut flood, &mut server).await.unwrap();
    let (healthy_key, _) = r::handshake(&mut healthy, &mut server).await.unwrap();
    let session = flood.peer_status(PeerId(0)).unwrap();
    let ns = r::config(1, "overflow").unwrap().namespace;
    let packet = skvoz_core::wire::encode(
        flood_key.stream.stream_id,
        &skvoz_core::Frame::WindowUpdate { consumed: 0 },
    )
    .unwrap();
    for sequence in session.sent_frames + 1..=session.sent_frames + 256 {
        let mut b = session.pair_token.to_be_bytes().to_vec();
        b.extend_from_slice(&sequence.to_be_bytes());
        b.extend_from_slice(&packet);
        flood
            .inject_subject(
                format!(
                    "{ns}.lane.0.{:032x}.1.data.1.{:032x}",
                    server.generation(),
                    flood.generation()
                ),
                b,
            )
            .await
            .unwrap();
    }
    // A queue burst is backpressured. An owner that remains stalled beyond
    // its existing I/O deadline terminates this shard without dropping DATA.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let start = Instant::now();
    let mut lost = 0;
    while lost == 0 {
        assert!(start.elapsed() < Duration::from_secs(2));
        server.turn(Duration::from_millis(1)).await.unwrap();
        healthy.turn(Duration::ZERO).await.unwrap();
        for e in server.poll_events(256) {
            if e.key.stream.peer == PeerId(1)
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                lost += 1;
            }
        }
    }
    assert_eq!(lost, 1);
    assert!(server.status().counters.shard_failures > 0);
    assert!(server.peer_ready(PeerId(2)));
    assert_eq!(server.resources().streams, 1);
    assert_eq!(server.resources().reserved_receive_bytes, 8192);
    r::bytes(
        &mut healthy,
        &mut server,
        healthy_key,
        b"healthy other shard exact bytes after actual overflow",
    )
    .await
    .unwrap();
    flood.shutdown().await.unwrap();
    healthy.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    server.poll_events(256);
    assert_eq!(server.resources().streams, 0);
    assert_eq!(server.resources().reserved_receive_bytes, 0);
}
#[tokio::test]
async fn tiny_runtime_subscription_pauses_then_resumes_exact_ordered_bytes() {
    let mut config = r::config(0, "tiny-pause").unwrap();
    config.subscription_capacity = 2;
    let mut server = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .unwrap();
    let mut client = r::node(1, "tiny-pause").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    let payload: Vec<u8> = (0..6144).map(|index| (index % 251) as u8).collect();
    let mut sent = 0;
    while sent < payload.len() {
        let skvoz_core::SendOutcome::Accepted(count) = client.send(key, &payload[sent..]).unwrap()
        else {
            panic!("initial peer credit must hold six records");
        };
        sent += count;
    }
    client.turn(Duration::ZERO).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut received = Vec::new();
    let started = Instant::now();
    while received.len() < payload.len() {
        assert!(started.elapsed() < Duration::from_secs(2));
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.turn(Duration::ZERO).await.unwrap();
        for event in server.poll_events(64) {
            match event.event {
                Event::Data { offset, bytes } => {
                    assert_eq!(offset, received.len() as u64);
                    received.extend_from_slice(&bytes);
                    server
                        .consume_through(event.key, received.len() as u64)
                        .unwrap();
                }
                Event::Closed { reason } => panic!("unexpected terminal event: {reason:?}"),
                _ => {}
            }
        }
    }
    assert_eq!(received, payload);
    assert_eq!(server.status().counters.shard_overflows, 0);
    assert!(server.peer_ready(PeerId(1)));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn admission_burst_does_not_interrupt_existing_peer() {
    let limits = ManagerConfig {
        max_peers: 1,
        ..ManagerConfig::default()
    };
    let mut server = NatsRuntime::connect(r::config(0, "peer_admission").unwrap(), limits)
        .await
        .unwrap();
    let mut client = r::node(1, "peer_admission").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    let mut excess = r::node(2, "peer_admission").await.unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(500) {
        excess.turn(Duration::ZERO).await.unwrap();
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert!(!excess.peer_ready(PeerId(0)));
    assert_eq!(server.status().resources.peers, 1);
    r::bytes(
        &mut client,
        &mut server,
        key,
        b"existing peer survives new-peer admission burst",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    excess.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn system_trust_in_isolated_child_and_embedded_url_rejected() {
    let exe = std::env::current_exe().unwrap();
    let output = std::process::Command::new(exe)
        .args(["system_trust_child", "--exact", "--nocapture"])
        .env("SKVOZ_SYSTEM_TRUST_CHILD", "1")
        .env("SSL_CERT_FILE", std::env::var("SKVOZ_NATS_CA").unwrap())
        .env(
            "SSL_CERT_DIR",
            std::env::var("SKVOZ_TRUST_EMPTY_DIR").unwrap(),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut config = r::config(1, "userinfo").unwrap();
    config.url = "tls://secret-user:secret-pass@127.0.0.1:4222".into();
    let result = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .err()
        .unwrap();
    assert_eq!(result, RuntimeError::Config);
    assert!(!result.to_string().contains("secret"));
}
#[tokio::test]
async fn system_trust_child() {
    if std::env::var("SKVOZ_SYSTEM_TRUST_CHILD").as_deref() != Ok("1") {
        return;
    }
    let mut config = r::config(1, "system_trust").unwrap();
    config.trust = Trust::System;
    let mut node = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}
fn docker(args: &[&str]) {
    assert!(
        std::process::Command::new("docker")
            .args(args)
            .output()
            .unwrap()
            .status
            .success()
    );
    let monitor = std::env::var("SKVOZ_NATS_MONITOR").unwrap();
    assert!(std::process::Command::new("python3").args(["-c", "import sys,time,urllib.request; deadline=time.monotonic()+5\nwhile True:\n try:\n  urllib.request.urlopen(sys.argv[1]+'/healthz',timeout=.2); break\n except OSError:\n  if time.monotonic()>deadline: raise\n  time.sleep(.02)", &monitor]).output().unwrap().status.success());
}
#[tokio::test]
async fn wrong_hostname_expired_and_malformed_ca_are_rejected() {
    let directory = std::path::PathBuf::from(std::env::var("SKVOZ_NATS_FIXTURE_DIR").unwrap());
    let container = std::env::var("SKVOZ_NATS_CONTAINER").unwrap();
    let original = std::fs::read(directory.join("server.pem")).unwrap();
    for invalid in ["wrong-name.pem", "expired.pem"] {
        std::fs::copy(directory.join(invalid), directory.join("server.pem")).unwrap();
        docker(&["restart", &container]);
        let result =
            NatsRuntime::connect(r::config(1, "bad_cert").unwrap(), ManagerConfig::default()).await;
        std::fs::write(directory.join("server.pem"), &original).unwrap();
        docker(&["restart", &container]);
        assert_eq!(
            result.err(),
            Some(RuntimeError::Tls),
            "certificate: {invalid}"
        );
    }
    let malformed = directory.join("malformed-ca.pem");
    std::fs::write(&malformed, b"invalid PEM").unwrap();
    let mut config = r::config(1, "bad_ca").unwrap();
    config.trust = Trust::ManagedCa(malformed);
    assert_eq!(
        NatsRuntime::connect(config, ManagerConfig::default())
            .await
            .err(),
        Some(RuntimeError::Tls)
    );
    let mut good = r::node(1, "restored_cert").await.unwrap();
    good.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_credential_revocation_closes_old_streams_and_reprovision_recovers() {
    let directory = std::path::PathBuf::from(std::env::var("SKVOZ_NATS_FIXTURE_DIR").unwrap());
    let config_path = directory.join("nats.conf");
    let original = std::fs::read_to_string(&config_path).unwrap();
    let password = std::env::var("SKVOZ_NATS_P1_PASSWORD").unwrap();
    let changed = original.replace(
        &format!("user: \"p1\", password: \"{password}\""),
        "user: \"p1\", password: \"revoked\"",
    );
    assert_ne!(changed, original);
    let container = std::env::var("SKVOZ_NATS_CONTAINER").unwrap();
    let mut server = r::node(0, "revocation").await.unwrap();
    let mut client = r::node(1, "revocation").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (old, _) = r::handshake(&mut client, &mut server).await.unwrap();
    std::fs::write(&config_path, changed).unwrap();
    docker(&["kill", "--signal=HUP", &container]);
    let start = Instant::now();
    let mut closed = 0;
    while closed == 0 && start.elapsed() < Duration::from_secs(5) {
        let _ = client.turn(Duration::from_millis(1)).await;
        let _ = server.turn(Duration::from_millis(1)).await;
        for e in client.poll_events(256) {
            if e.key == old
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                closed += 1;
            }
        }
        server.poll_events(256);
    }
    let denied = NatsRuntime::connect(
        r::config(1, "denied_revoked").unwrap(),
        ManagerConfig::default(),
    )
    .await
    .err();
    std::fs::write(&config_path, original).unwrap();
    docker(&["kill", "--signal=HUP", &container]);
    assert_eq!(closed, 1);
    assert!(matches!(
        denied,
        Some(RuntimeError::Authentication | RuntimeError::Authorization)
    ));
    client.shutdown().await.unwrap();
    let mut fresh = r::node(1, "revocation").await.unwrap();
    let start = Instant::now();
    while !fresh.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
        assert!(start.elapsed() < Duration::from_secs(8));
        fresh.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        fresh.poll_events(256);
        server.poll_events(256);
    }
    let (key, _) = r::handshake(&mut fresh, &mut server).await.unwrap();
    r::bytes(
        &mut fresh,
        &mut server,
        key,
        b"fresh stream with reprovisioned credentials",
    )
    .await
    .unwrap();
    fresh.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn finite_peer_drain_and_allowlist_revoke_are_explicit() {
    let mut config = r::config(0, "drain").unwrap();
    config.terminal_drain_timeout = Duration::from_millis(100);
    let mut server = NatsRuntime::connect(config, ManagerConfig::default())
        .await
        .unwrap();
    let mut client = r::node(1, "drain").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    r::handshake(&mut client, &mut server).await.unwrap();
    client.shutdown().await.unwrap();
    drop(client);
    let mut fresh = r::node(1, "drain").await.unwrap();
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(600) {
        fresh.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(
        server.status().last_error,
        Some(RuntimeError::TerminalDrainTimeout)
    );
    assert_eq!(server.status().resources.streams, 1);
    assert!(!server.peer_ready(PeerId(1)));
    assert!(server.status().membership_slots <= ManagerConfig::default().max_peers);
    server.poll_events(256);
    assert_eq!(server.resources().streams, 0);
    assert_eq!(server.revoke_peer(PeerId(1)), Err(RuntimeError::Config));
    fresh.revoke_peer(PeerId(0)).unwrap();
    assert_eq!(fresh.join_peer(PeerId(0)), Err(RuntimeError::Authorization));
    fresh.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn same_epoch_pair_rejoin_rejects_old_keys_data_and_control_replay() {
    let mut server = r::node(0, "pair_rejoin").await.unwrap();
    let mut client = r::node(1, "pair_rejoin").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (old, old_server) = r::handshake(&mut client, &mut server).await.unwrap();
    let old_session = client.peer_status(PeerId(0)).unwrap();
    let epoch = client.generation();
    client.join_peer(PeerId(0)).unwrap();
    let start = Instant::now();
    while client
        .peer_status(PeerId(0))
        .is_none_or(|p| p.incarnation == old.incarnation)
        || !client.peer_ready(PeerId(0))
        || !server.peer_ready(PeerId(1))
    {
        assert!(start.elapsed() < Duration::from_secs(4));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(256);
        server.poll_events(256);
    }
    let (new, _) = r::handshake(&mut client, &mut server).await.unwrap();
    assert_eq!(epoch, client.generation());
    assert_eq!(old.stream.stream_id, new.stream.stream_id);
    assert_ne!(old.incarnation, new.incarnation);
    assert_eq!(client.send(old, b"stale"), Err(RuntimeError::StaleKey));
    assert_eq!(client.consume_through(old, 1), Err(RuntimeError::StaleKey));
    assert_eq!(client.close(old), Err(RuntimeError::StaleKey));
    assert_eq!(server.close(old_server), Err(RuntimeError::StaleKey));
    let namespace = r::config(1, "pair_rejoin").unwrap().namespace;
    let packet = skvoz_core::wire::encode(
        new.stream.stream_id,
        &skvoz_core::Frame::Close {
            reason: CloseReason::Cancelled,
        },
    )
    .unwrap();
    let mut data = old_session.pair_token.to_be_bytes().to_vec();
    data.extend_from_slice(&1u64.to_be_bytes());
    data.extend_from_slice(&packet);
    client
        .inject_subject(
            format!(
                "{namespace}.lane.0.{:032x}.1.data.1.{epoch:032x}",
                server.generation()
            ),
            data,
        )
        .await
        .unwrap();
    for kind in [1u8, 3, 4, 6] {
        let mut c = b"SKC1".to_vec();
        c.push(kind);
        for n in [
            epoch,
            server.generation(),
            old_session.handshake_nonce,
            old_session.pair_token,
        ] {
            c.extend_from_slice(&n.to_be_bytes());
        }
        c.extend_from_slice(&0u64.to_be_bytes());
        let subject = if kind == 6 {
            format!(
                "{namespace}.lane.0.{:032x}.1.control.1.{epoch:032x}",
                server.generation()
            )
        } else {
            format!("{namespace}.join.0.1")
        };
        client.inject_subject(subject, c).await.unwrap();
    }
    // Replay READY in its original responder -> initiator direction, carrying
    // the actual retired handshake nonce/token rather than an invented nonce.
    let mut ready = b"SKC1".to_vec();
    ready.push(4);
    for n in [
        server.generation(),
        epoch,
        old_session.handshake_nonce,
        old_session.pair_token,
    ] {
        ready.extend_from_slice(&n.to_be_bytes());
    }
    ready.extend_from_slice(&0u64.to_be_bytes());
    server
        .inject_subject(format!("{namespace}.join.1.0"), ready)
        .await
        .unwrap();
    client.turn(Duration::from_millis(1)).await.unwrap();
    server.turn(Duration::from_millis(1)).await.unwrap();
    assert!(client.peer_ready(PeerId(0)));
    assert!(server.peer_ready(PeerId(1)));
    r::bytes(
        &mut client,
        &mut server,
        new,
        b"new pair unaffected by stale data control keys",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancelled_extracted_output_fails_shard_then_fresh_pair_opens() {
    use futures_util::FutureExt;
    let mut server = r::node(0, "cancel_output").await.unwrap();
    let mut client = r::node(1, "cancel_output").await.unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (old, _) = r::handshake(&mut client, &mut server).await.unwrap();
    let epoch = client.generation();
    client.send(old, b"cancel after extraction").unwrap();
    client.inject_pause_next_data_after_extraction();
    let mut pending = Box::pin(client.turn(Duration::ZERO));
    assert!(pending.as_mut().now_or_never().is_none());
    drop(pending);
    let events = client.poll_events(256);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.key == old
                && matches!(
                    e.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                ))
            .count(),
        1
    );
    assert_eq!(client.resources().streams, 0);
    let start = Instant::now();
    while !client.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
        assert!(start.elapsed() < Duration::from_secs(8));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        client.poll_events(256);
        server.poll_events(256);
    }
    assert_eq!(client.generation(), epoch);
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    assert_ne!(key.incarnation, old.incarnation);
    r::bytes(
        &mut client,
        &mut server,
        key,
        b"fresh pair after abandoned ordered output",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn injected_lost_lane_warmup_pong_retries_before_peer_ready() {
    let mut server = r::node(0, "warmup").await.unwrap();
    let mut client = r::node(1, "warmup").await.unwrap();
    client.inject_ignore_next_lane_pong();
    assert!(!client.peer_ready(PeerId(0)));
    assert_eq!(
        client.open(PeerId(0), b"too early"),
        Err(RuntimeError::PeerUnavailable)
    );
    r::joined(&mut client, &mut server, 1).await.unwrap();
    assert_eq!(client.status().counters.peer_timeouts, 0);
    let (key, _) = r::handshake(&mut client, &mut server).await.unwrap();
    r::bytes(
        &mut client,
        &mut server,
        key,
        b"first OPEN only after actual lane proof",
    )
    .await
    .unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_header_message_retires_only_its_transport_shard() {
    let mut server = r::node(0, "raw-headers").await.unwrap();
    let mut offender = r::node(1, "raw-headers").await.unwrap();
    let mut healthy = r::node(2, "raw-headers").await.unwrap();
    r::joined(&mut offender, &mut server, 1).await.unwrap();
    r::joined(&mut healthy, &mut server, 2).await.unwrap();
    let (offender_key, _) = r::handshake(&mut offender, &mut server).await.unwrap();
    let (healthy_key, _) = r::handshake(&mut healthy, &mut server).await.unwrap();
    let session = offender.peer_status(PeerId(0)).unwrap();
    let mut payload = session.pair_token.to_be_bytes().to_vec();
    payload.extend_from_slice(&(session.sent_frames + 1).to_be_bytes());
    payload.extend_from_slice(
        &skvoz_core::wire::encode(
            offender_key.stream.stream_id,
            &skvoz_core::Frame::WindowUpdate { consumed: 0 },
        )
        .unwrap(),
    );
    let namespace = r::config(1, "raw-headers").unwrap().namespace;
    offender
        .inject_subject_with_headers(
            format!(
                "{namespace}.lane.0.{:032x}.1.data.1.{:032x}",
                server.generation(),
                offender.generation()
            ),
            payload,
        )
        .await
        .unwrap();
    let started = Instant::now();
    let mut closed = false;
    while !closed {
        assert!(started.elapsed() < Duration::from_secs(2));
        server.turn(Duration::from_millis(1)).await.unwrap();
        healthy.turn(Duration::ZERO).await.unwrap();
        for event in server.poll_events(64) {
            if event.key.stream.peer == PeerId(1)
                && matches!(
                    event.event,
                    Event::Closed {
                        reason: CloseReason::TransportLost
                    }
                )
            {
                closed = true;
            }
        }
    }
    assert!(!server.peer_ready(PeerId(1)));
    assert!(server.peer_ready(PeerId(2)));
    r::bytes(
        &mut healthy,
        &mut server,
        healthy_key,
        b"plain payload survives another lane header error",
    )
    .await
    .unwrap();
    offender.shutdown().await.unwrap();
    healthy.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn duration_extreme_is_a_safe_configuration_error() {
    let mut config = r::config(1, "duration").unwrap();
    config.peer_timeout = Duration::MAX;
    assert_eq!(
        NatsRuntime::connect(config, ManagerConfig::default())
            .await
            .err(),
        Some(RuntimeError::Config)
    );
}
