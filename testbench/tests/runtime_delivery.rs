#![cfg(feature = "real-nats")]
//! Aggregate recipient flight and explicit broker processing barriers.
use skvoz_core::{
    Event, PeerId, SendOutcome,
    runtime::{NatsRuntime, RuntimeError},
};
use skvoz_network::EngineConfig;
use skvoz_testbench::runtime_scenarios as r;
use std::time::{Duration, Instant};

async fn node(id: u64, case: &str) -> NatsRuntime {
    let mut config = r::config(id, case).unwrap();
    config.io_timeout = Duration::from_secs(3);
    config.peer_timeout = Duration::from_secs(10);
    config.heartbeat_interval = Duration::from_secs(1);
    config.subscription_capacity = 64;
    NatsRuntime::connect(config, EngineConfig::default().core_limits(id == 0))
        .await
        .unwrap()
}

#[tokio::test]
async fn concurrent_sources_share_recipient_flight_and_a_fault_is_peer_scoped() {
    let mut recipient = node(0, "recipient_flight").await;
    // Both exact sources map to recipient shard 1, sharing one socket/queue.
    let mut first = node(1, "recipient_flight").await;
    let mut second = node(9, "recipient_flight").await;
    let until = Instant::now() + Duration::from_secs(12);
    while !first.peer_ready(PeerId(0))
        || !second.peer_ready(PeerId(0))
        || !recipient.peer_ready(PeerId(1))
        || !recipient.peer_ready(PeerId(9))
    {
        assert!(Instant::now() < until, "two-source readiness deadline");
        first.turn(Duration::ZERO).await.unwrap();
        second.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::from_millis(1)).await.unwrap();
    }
    let (a, remote_a) = r::handshake(&mut first, &mut recipient).await.unwrap();
    let (b, remote_b) = r::handshake(&mut second, &mut recipient).await.unwrap();
    // Exercise normal automatic window growth before slowing the receiver.
    r::bytes(&mut first, &mut recipient, a, &vec![1; 4 << 20])
        .await
        .unwrap();
    r::bytes(&mut second, &mut recipient, b, &vec![9; 4 << 20])
        .await
        .unwrap();
    let total = 12 << 20;
    let mut sent = [4 << 20; 2];
    let mut received = [4 << 20; 2];
    let mut finished = [false; 2];
    let mut remote_finished = [false; 2];
    let payloads = [vec![1; 128 << 10], vec![9; 128 << 10]];
    let started = Instant::now();
    let mut turns = 0usize;
    let mut maximum_flight = 0u64;
    let mut shared_grants = false;
    while !remote_finished.iter().all(|n| *n) {
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "aggregate delivery deadline: sent={sent:?} received={received:?}"
        );
        for (i, source, key) in [(0, &mut first, a), (1, &mut second, b)] {
            if sent[i] < total {
                let bytes = &payloads[i][..(total - sent[i]).min(payloads[i].len())];
                if let SendOutcome::Accepted(n) = source.send(key, bytes).unwrap() {
                    sent[i] += n;
                }
            }
            if sent[i] == total && !finished[i] {
                source.finish(key).unwrap();
                finished[i] = true;
            }
            source.turn(Duration::ZERO).await.unwrap();
            for event in source.poll_events(256) {
                assert!(
                    !matches!(event.event, Event::Closed { reason } if reason != skvoz_core::CloseReason::Finished)
                );
            }
        }
        // A finite slow ordinary reader leaves sender demand live without
        // changing queues, frames, windows or the broker's 4MiB pending limit.
        if turns.is_multiple_of(32) {
            recipient.turn(Duration::ZERO).await.unwrap();
            let (flight, limit, sources) = recipient.inspect_recipient_credit(1);
            assert!(flight <= limit);
            maximum_flight = maximum_flight.max(flight);
            shared_grants |= sources >= 2;
            for event in recipient.poll_events(256) {
                let i = if event.key == remote_a {
                    0
                } else {
                    assert_eq!(event.key, remote_b);
                    1
                };
                match event.event {
                    Event::Data { offset, bytes } => {
                        assert_eq!(offset as usize, received[i]);
                        assert!(bytes.iter().all(|n| *n == if i == 0 { 1 } else { 9 }));
                        received[i] += bytes.len();
                        recipient
                            .consume_through(event.key, received[i] as u64)
                            .unwrap();
                    }
                    Event::RemoteFinished => {
                        assert_eq!(received[i], total);
                        remote_finished[i] = true;
                    }
                    Event::Closed { reason } => {
                        assert_eq!(reason, skvoz_core::CloseReason::Finished)
                    }
                    _ => {}
                }
            }
            assert!(recipient.peer_ready(PeerId(1)) && recipient.peer_ready(PeerId(9)));
        }
        turns += 1;
        tokio::task::yield_now().await;
    }
    assert_eq!(received, [total; 2]);
    assert!(
        shared_grants,
        "concurrent sources never held shared lane grants"
    );
    assert!(
        maximum_flight >= 128 << 10,
        "slow reader never retained aggregate flight"
    );
    println!(
        "shared recipient lane=1 sources=2 maximum_outstanding={maximum_flight} limit={}",
        recipient.inspect_recipient_credit(1).1
    );
    assert_eq!(recipient.status().counters.shard_failures, 0);
    for key in [remote_a, remote_b] {
        recipient.finish(key).unwrap();
    }
    for _ in 0..64 {
        first.turn(Duration::ZERO).await.unwrap();
        second.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::ZERO).await.unwrap();
    }
    // One authenticated impossible demand is a peer failure, not a host error.
    let status = second.peer_status(PeerId(0)).unwrap();
    let config = r::config(9, "recipient_flight").unwrap();
    let mut message = b"SKC2".to_vec();
    message.push(7);
    for field in [
        second.epoch(),
        recipient.epoch(),
        u128::MAX,
        status.pair_token,
    ] {
        message.extend_from_slice(&field.to_be_bytes());
    }
    message.extend_from_slice(&(4u64 << 20).to_be_bytes());
    second
        .inject_subject(
            format!(
                "{}.lane.0.{:032x}.1.control.9.{:032x}",
                config.namespace,
                recipient.epoch(),
                second.epoch()
            ),
            message,
        )
        .await
        .unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while recipient.peer_ready(PeerId(9)) {
        assert!(Instant::now() < until);
        recipient.turn(Duration::from_millis(1)).await.unwrap();
        first.turn(Duration::ZERO).await.unwrap();
    }
    assert_eq!(recipient.status().last_error, Some(RuntimeError::Protocol));
    assert!(recipient.peer_ready(PeerId(1)));
    let (healthy, _) = r::handshake(&mut first, &mut recipient).await.unwrap();
    r::bytes(&mut first, &mut recipient, healthy, &[3; 65536])
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    recipient.shutdown().await.unwrap();
}

async fn broker(case: &str) -> async_nats::Client {
    let config = r::config(0, case).unwrap();
    async_nats::ConnectOptions::with_user_and_password(
        config.authentication.username,
        config.authentication.password,
    )
    .add_root_certificates(std::env::var("SKVOZ_NATS_CA").unwrap().into())
    .require_tls(true)
    .raw_message_limit(65588)
    .subscription_backpressure_timeout(Duration::from_secs(3))
    .connect(config.url)
    .await
    .unwrap()
}

#[tokio::test]
async fn broker_barrier_progress_cancel_and_disconnect_are_generation_bound() {
    use futures_util::FutureExt;
    let source = broker("barrier").await;
    let recipient = broker("barrier").await;
    let namespace = r::config(0, "barrier").unwrap().namespace;
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let mut retired = recipient
        .subscribe_into(format!("{namespace}.join.0.1"), tx.clone())
        .await
        .unwrap();
    let _retained = recipient
        .subscribe_into(format!("{namespace}.join.0.0"), tx)
        .await
        .unwrap();
    recipient.broker_barrier().await.unwrap();
    for _ in 0..16 {
        source
            .publish(format!("{namespace}.join.0.0"), "marker".into())
            .await
            .unwrap();
    }
    source.broker_barrier().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while rx.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    retired.unsubscribe().await.unwrap();
    let observer = recipient.clone();
    let canceled = tokio::spawn(async move { observer.broker_barrier().await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        !canceled.is_finished(),
        "barrier skipped the blocked retained source"
    );
    canceled.abort();
    assert!(canceled.await.unwrap_err().is_cancelled());
    assert!(
        recipient.broker_barrier().await.is_err(),
        "cancellation allocated a second observer"
    );
    for _ in 0..16 {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap()
                .payload
                .as_ref(),
            b"marker"
        );
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while recipient.broker_barrier().await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for _ in 0..16 {
        source
            .publish(format!("{namespace}.join.0.0"), "disconnect".into())
            .await
            .unwrap();
    }
    source.broker_barrier().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while rx.len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let observer = recipient.clone();
    let disconnected = tokio::spawn(async move { observer.broker_barrier().await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!disconnected.is_finished());
    recipient.force_reconnect().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), disconnected)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    while rx.recv().now_or_never().flatten().is_some() {}
    recipient.broker_barrier().await.unwrap();
}

#[tokio::test]
async fn queued_source_retirement_drains_before_same_id_epoch_reuse() {
    let mut recipient = node(0, "recipient_retire").await;
    let mut first = node(1, "recipient_retire").await;
    let mut old = node(9, "recipient_retire").await;
    let until = Instant::now() + Duration::from_secs(12);
    while !first.peer_ready(PeerId(0))
        || !old.peer_ready(PeerId(0))
        || !recipient.peer_ready(PeerId(1))
        || !recipient.peer_ready(PeerId(9))
    {
        assert!(Instant::now() < until);
        first.turn(Duration::ZERO).await.unwrap();
        old.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::from_millis(1)).await.unwrap();
    }
    let (a, remote_a) = r::handshake(&mut first, &mut recipient).await.unwrap();
    let (b, _) = r::handshake(&mut old, &mut recipient).await.unwrap();
    r::bytes(&mut first, &mut recipient, a, &vec![1; 4 << 20])
        .await
        .unwrap();
    r::bytes(&mut old, &mut recipient, b, &vec![9; 4 << 20])
        .await
        .unwrap();
    let old_epoch = old.epoch();
    let old_pair = old.peer_status(PeerId(0)).unwrap().pair_token;
    let mut sent = [4 << 20; 2];
    let until = Instant::now() + Duration::from_secs(5);
    let mut turns = 0usize;
    loop {
        assert!(Instant::now() < until, "shared DATA queue never filled");
        for (i, source, key) in [(0, &mut first, a), (1, &mut old, b)] {
            for _ in 0..8 {
                if sent[i] < 8 << 20
                    && let SendOutcome::Accepted(n) = source.send(key, &[7; 16384]).unwrap()
                {
                    sent[i] += n;
                }
            }
            source.turn(Duration::ZERO).await.unwrap();
        }
        tokio::task::yield_now().await;
        let (queued, outstanding, _) = recipient.inspect_source_retirement(1, PeerId(9));
        if queued == 64 && outstanding > 0 {
            break;
        }
        if turns.is_multiple_of(128) {
            recipient.turn(Duration::ZERO).await.unwrap();
            for event in recipient.poll_events(256) {
                if let Event::Data { offset, bytes } = event.event {
                    recipient
                        .consume_through(event.key, offset + bytes.len() as u64)
                        .unwrap();
                }
            }
        }
        turns += 1;
    }
    let before = recipient.inspect_source_retirement(1, PeerId(9));
    recipient.terminate_peer(PeerId(9));
    let quarantined = recipient.inspect_source_retirement(1, PeerId(9));
    assert_eq!(quarantined, (before.0, before.1, true));
    let started = Instant::now();
    recipient.turn(Duration::ZERO).await.unwrap();
    let cleanup_us = started.elapsed().as_micros();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(recipient.status().counters.shard_failures, 0);
    assert!(recipient.peer_ready(PeerId(1)));
    old.terminate_peer(PeerId(0));
    old.poll_events(256);
    old.shutdown().await.unwrap();
    let mut replacement = node(9, "recipient_retire").await;
    assert_ne!(replacement.epoch(), old_epoch);
    let until = Instant::now() + Duration::from_secs(12);
    while !replacement.peer_ready(PeerId(0)) || !recipient.peer_ready(PeerId(9)) {
        assert!(Instant::now() < until, "source retirement/reuse deadline");
        first.turn(Duration::ZERO).await.unwrap();
        replacement.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::from_millis(1)).await.unwrap();
        for event in recipient.poll_events(256) {
            if let Event::Data { offset, bytes } = event.event {
                recipient
                    .consume_through(event.key, offset + bytes.len() as u64)
                    .unwrap();
            }
        }
        first.poll_events(256);
    }
    assert!(!recipient.inspect_source_retirement(1, PeerId(9)).2);
    // Delayed old incarnation DATA cannot consume the new pair's credit or
    // affect the healthy source; both subject epoch and pair token are pinned.
    let config = r::config(9, "recipient_retire").unwrap();
    let mut stale = old_pair.to_be_bytes().to_vec();
    stale.extend_from_slice(&1u64.to_be_bytes());
    stale.extend_from_slice(
        &skvoz_core::wire::encode(
            b.stream.stream_id,
            &skvoz_core::Frame::WindowUpdate { consumed: 0 },
        )
        .unwrap(),
    );
    for epoch in [old_epoch, replacement.epoch()] {
        replacement
            .inject_subject(
                format!(
                    "{}.lane.0.{:032x}.1.data.9.{epoch:032x}",
                    config.namespace,
                    recipient.epoch()
                ),
                stale.clone(),
            )
            .await
            .unwrap();
    }
    for _ in 0..16 {
        first.turn(Duration::ZERO).await.unwrap();
        replacement.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::ZERO).await.unwrap();
        for event in recipient.poll_events(256) {
            if let Event::Data { offset, bytes } = event.event {
                recipient
                    .consume_through(event.key, offset + bytes.len() as u64)
                    .unwrap();
            }
        }
        first.poll_events(256);
    }
    assert!(recipient.peer_ready(PeerId(1)) && recipient.peer_ready(PeerId(9)));
    recipient.close(remote_a).unwrap();
    for _ in 0..16 {
        first.turn(Duration::ZERO).await.unwrap();
        recipient.turn(Duration::ZERO).await.unwrap();
        first.poll_events(256);
        recipient.poll_events(256);
    }
    let (healthy, _) = r::handshake(&mut first, &mut recipient).await.unwrap();
    r::bytes(&mut first, &mut recipient, healthy, &[3; 65536])
        .await
        .unwrap();
    let (fresh, _) = r::handshake(&mut replacement, &mut recipient)
        .await
        .unwrap();
    r::bytes(&mut replacement, &mut recipient, fresh, &[9; 65536])
        .await
        .unwrap();
    println!(
        "source retirement lane=1 queued={} retained_bytes={} cleanup_us={} fresh_epoch=true",
        before.0, before.1, cleanup_us
    );
    first.shutdown().await.unwrap();
    replacement.shutdown().await.unwrap();
    recipient.shutdown().await.unwrap();
}
