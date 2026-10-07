//! Real-broker scenarios are executed by testbench/run.py with real-nats enabled.

use futures_util::StreamExt;
use skvoz_core::{CloseReason, Event, Frame, SendOutcome, State, wire};
use skvoz_testbench::{BenchConfig, ConnectionConfig, FailureKind, Node, Role, scenarios};
use std::{
    process::Command,
    time::{Duration, Instant},
};

fn small() -> BenchConfig {
    let mut config = BenchConfig::default();
    config.stream.receive_window = 8;
    config.stream.max_frame = 4;
    config.stream.max_pending_frames = 2;
    config
}

async fn drive_until_closed(node: &mut Node, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while node.snapshot(id).unwrap().state != State::Closed {
        assert!(
            Instant::now() < deadline,
            "real terminal transition timed out"
        );
        node.turn(Duration::from_millis(10)).await.unwrap();
    }
}

fn assert_clean_terminal(node: &mut Node, id: u64, reason: CloseReason) {
    let snapshot = node.snapshot(id).unwrap();
    assert_eq!(snapshot.state, State::Closed);
    assert_eq!(snapshot.pending_send_bytes, 0);
    assert_eq!(snapshot.buffered_receive_bytes, 0);
    assert_eq!(snapshot.receive_capacity_bytes, 0);
    let events = node.poll_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].stream_id, id);
    assert_eq!(events[0].event, Event::Closed { reason });
    assert!(node.poll_events().is_empty());
}

#[tokio::test]
async fn large_binary_duplex_and_both_initiators() {
    scenarios::exchange("binary", 2, 65536).await.unwrap();
}

#[tokio::test]
async fn slow_consumer_poll_does_not_return_credit() {
    let (mut user, mut consumer) = scenarios::pair("backpressure", small()).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    assert_eq!(
        user.send(id, b"1234").await.unwrap(),
        SendOutcome::Accepted(4)
    );
    assert_eq!(
        user.send(id, b"5678").await.unwrap(),
        SendOutcome::Accepted(3)
    );
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    let held = consumer.poll_events();
    assert_eq!(held.len(), 2);
    let exact = held
        .iter()
        .flat_map(|event| match &event.event {
            Event::Data { bytes, .. } => bytes.to_vec(),
            _ => panic!("Expected held DATA"),
        })
        .collect::<Vec<_>>();
    assert_eq!(exact, b"1234567");
    assert_eq!(consumer.snapshot(id).unwrap().receive_unconsumed_bytes, 7);
    assert_eq!(user.send(id, b"x").await.unwrap(), SendOutcome::WouldBlock);
    // A real flush/turn with no WINDOW_UPDATE still cannot unblock the sender.
    user.turn(Duration::from_millis(20)).await.unwrap();
    assert_eq!(user.send(id, b"x").await.unwrap(), SendOutcome::WouldBlock);
    drop(held);
    consumer.consume_through(id, 4).await.unwrap();
    let started = Instant::now();
    let mut observed = Vec::new();
    let events = loop {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "Actual-consumption Writable timed out"
        );
        consumer.turn(Duration::ZERO).await.unwrap();
        user.turn(Duration::from_millis(10)).await.unwrap();
        observed.extend(user.poll_events());
        if !observed.is_empty() && user.snapshot(id).unwrap().send_unacknowledged_bytes == 3 {
            break observed;
        }
    };
    assert!(
        events
            .iter()
            .all(|event| event.stream_id == id && event.event == Event::Writable)
    );
    assert_eq!(
        user.send(id, b"abcdx").await.unwrap(),
        SendOutcome::Accepted(4)
    );
    assert_eq!(user.send(id, b"x").await.unwrap(), SendOutcome::WouldBlock);
    user.close(id).await.unwrap();
    drive_until_closed(&mut consumer, id).await;
    for node in [&mut user, &mut consumer] {
        let snapshot = node.snapshot(id).unwrap();
        assert_eq!(snapshot.receive_unconsumed_bytes, 0);
        assert_eq!(snapshot.pending_send_bytes, 0);
        assert_eq!(snapshot.buffered_receive_bytes, 0);
    }
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_is_possible_after_request_eof() {
    let (mut user, mut consumer) = scenarios::pair("halfclose", small()).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    user.send(id, &[0, 255, 128]).await.unwrap();
    user.finish(id).await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    let events = consumer.poll_events();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].event,
        Event::Data {
            offset: 0,
            bytes: Box::new([0, 255, 128])
        }
    );
    assert_eq!(events[1].event, Event::RemoteFinished);
    assert_eq!(
        consumer.snapshot(id).unwrap().state,
        State::HalfClosedRemote
    );
    drop(events);
    consumer.consume_through(id, 3).await.unwrap();
    consumer.send(id, b"reply").await.unwrap();
    consumer.send(id, b"y").await.unwrap();
    consumer.finish(id).await.unwrap();
    let mut response = Vec::new();
    let mut saw_fin = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while !saw_fin {
        assert!(Instant::now() < deadline);
        user.turn(Duration::from_millis(10)).await.unwrap();
        for event in user.poll_events() {
            match event.event {
                Event::Data { offset, bytes } => {
                    assert_eq!(offset, response.len() as u64);
                    response.extend_from_slice(&bytes);
                    drop(bytes);
                    user.consume_through(id, response.len() as u64)
                        .await
                        .unwrap();
                }
                Event::RemoteFinished => saw_fin = true,
                Event::Closed { reason } => assert_eq!(reason, CloseReason::Finished),
                Event::Writable => {}
                other => panic!("unexpected real event {other:?}"),
            }
        }
    }
    assert_eq!(response, b"reply");
    assert_eq!(user.snapshot(id).unwrap().state, State::Closed);
    assert_eq!(consumer.snapshot(id).unwrap().state, State::Closed);
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn reject_and_cancel_have_real_peer_observations() {
    let (mut user, mut consumer) = scenarios::pair("rejectcancel", small()).await.unwrap();
    let rejected = user.open(b"request").await.unwrap();
    let started = Instant::now();
    let mut progress = 0;
    let events = loop {
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "Reject IncomingOpen observation timed out"
        );
        user.turn(Duration::ZERO).await.unwrap();
        progress += consumer.turn(Duration::from_millis(10)).await.unwrap();
        let events = consumer.poll_events();
        if !events.is_empty() {
            break events;
        }
    };
    assert!(
        matches!(events.as_slice(), [event] if event.stream_id == rejected && matches!(&event.event, Event::IncomingOpen { metadata } if &**metadata == b"request"))
    );
    let elapsed = started.elapsed();
    let before_reject = consumer.snapshot(rejected);
    consumer.reject(rejected, b"denied").await.unwrap_or_else(|error| panic!(
        "Reject observation: error={error} elapsed={elapsed:?} progress={progress} events={events:?} user_failure={:?} consumer_failure={:?} user_snapshot={:?} consumer_snapshot_before={before_reject:?} consumer_snapshot_after={:?}",
        user.failure_kind(),consumer.failure_kind(),user.snapshot(rejected),consumer.snapshot(rejected)));
    drive_until_closed(&mut user, rejected).await;
    let events = user.poll_events();
    assert_eq!(
        events[0].event,
        Event::Rejected {
            reason: b"denied".as_slice().into()
        }
    );
    assert_eq!(
        events[1].event,
        Event::Closed {
            reason: CloseReason::Rejected
        }
    );
    consumer.poll_events();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    user.send(id, b"held").await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    consumer.send(id, b"held").await.unwrap();
    consumer.close(id).await.unwrap();
    drive_until_closed(&mut user, id).await;
    assert_clean_terminal(&mut user, id, CloseReason::Cancelled);
    assert_clean_terminal(&mut consumer, id, CloseReason::Cancelled);
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_owner_times_out_over_real_nats() {
    let mut config = small();
    config.stream.open_timeout_ms = 80;
    let mut user = Node::connect(
        ConnectionConfig::from_env(Role::User).unwrap(),
        "missingowner",
        config,
    )
    .await
    .unwrap();
    let id = user.open(b"no consumer").await.unwrap();
    drive_until_closed(&mut user, id).await;
    assert_clean_terminal(&mut user, id, CloseReason::OpenTimeout);
    user.shutdown().await.unwrap();
}

#[tokio::test]
async fn parallel_streams_share_two_connections() {
    scenarios::exchange("parallel", 16, 8192).await.unwrap();
}

#[tokio::test]
async fn namespace_and_capacity_isolation() {
    let (mut u1, mut c1) = scenarios::pair("isolation1", small()).await.unwrap();
    let (mut u2, mut c2) = scenarios::pair("isolation2", small()).await.unwrap();
    let id1 = scenarios::establish(&mut u1, &mut c1).await.unwrap();
    let id2 = scenarios::establish(&mut u2, &mut c2).await.unwrap();
    assert_eq!(id1, id2);
    u1.send(id1, b"one").await.unwrap();
    u2.send(id2, b"two").await.unwrap();
    c1.turn(Duration::from_secs(1)).await.unwrap();
    c2.turn(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        c1.poll_events()[0].event,
        Event::Data {
            offset: 0,
            bytes: b"one".as_slice().into()
        }
    );
    assert_eq!(
        c2.poll_events()[0].event,
        Event::Data {
            offset: 0,
            bytes: b"two".as_slice().into()
        }
    );
    for node in [u1, c1, u2, c2] {
        node.shutdown().await.unwrap();
    }
    let mut config = small();
    config.max_streams = 1;
    let mut user = Node::connect(
        ConnectionConfig::from_env(Role::User).unwrap(),
        "capacity",
        config,
    )
    .await
    .unwrap();
    config.max_streams = 2;
    let mut consumer = Node::connect(
        ConnectionConfig::from_env(Role::Consumer).unwrap(),
        "capacity",
        config,
    )
    .await
    .unwrap();
    scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    assert!(user.open(b"second").await.is_err());
    assert_eq!(user.slot_count(), 1);
    // Peer overload is also rejected instead of allocating an extra stream.
    let extra_id = consumer.open(b"peer second").await.unwrap();
    user.turn(Duration::from_secs(1)).await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    let events = consumer.poll_events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].stream_id, extra_id);
    assert!(matches!(events[0].event, Event::Rejected { .. }));
    assert_eq!(
        events[1].event,
        Event::Closed {
            reason: CloseReason::Rejected
        }
    );
    assert_eq!(user.slot_count(), 1);
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn real_subscription_overflow_fails_closed() {
    let mut config = small();
    config.subscription_capacity = 1;
    let (mut user, mut consumer) = scenarios::pair("overflow", config).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    for _ in 0..32 {
        user.inject_packet(wire::encode(id, &Frame::WindowUpdate { consumed: 0 }).unwrap())
            .await
            .unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while consumer.failure_kind().is_none() {
        assert!(
            Instant::now() < deadline,
            "actual SlowConsumer callback was not observed"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(consumer.failure_kind(), Some(FailureKind::SlowConsumer));
    drive_until_closed(&mut consumer, id).await;
    assert_clean_terminal(&mut consumer, id, CloseReason::TransportLost);
    assert!(consumer.send(id, b"x").await.is_err());
    // Dropping the failed node is safe; its client may have already closed.
    drop(consumer);
    user.shutdown().await.unwrap();
}

#[tokio::test]
async fn tls_trust_authentication_and_subject_permissions() {
    let mut wrong_password = ConnectionConfig::from_env(Role::User).unwrap();
    wrong_password.password = "intentionally-wrong".into();
    assert!(
        Node::connect(wrong_password, "badpassword", small())
            .await
            .is_err()
    );
    let mut wrong_ca = ConnectionConfig::from_env(Role::User).unwrap();
    wrong_ca.ca = std::env::var("SKVOZ_NATS_WRONG_CA").unwrap().into();
    assert!(Node::connect(wrong_ca, "badca", small()).await.is_err());
    let (mut user, mut consumer) = scenarios::pair("permissions", small()).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    let _ = user.attempt_forbidden_publish().await;
    let deadline = Instant::now() + Duration::from_secs(2);
    while user.failure_kind().is_none() {
        assert!(
            Instant::now() < deadline,
            "real subject permission denial not observed"
        );
        tokio::task::yield_now().await;
    }
    assert_eq!(user.failure_kind(), Some(FailureKind::ServerError));
    drive_until_closed(&mut user, id).await;
    assert_clean_terminal(&mut user, id, CloseReason::TransportLost);
    drop(user);
    consumer.shutdown().await.unwrap();
}

#[tokio::test]
async fn out_of_order_data_and_bad_version_travel_through_real_broker() {
    let (mut user, mut consumer) = scenarios::pair("invalid", small()).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    assert_eq!(user.snapshot(id).unwrap().send_unacknowledged_bytes, 0);
    let config = ConnectionConfig::from_env(Role::User).unwrap();
    let observer =
        async_nats::ConnectOptions::with_user_and_password("user".into(), config.password)
            .require_tls(true)
            .add_root_certificates(config.ca)
            .subscription_capacity(8)
            .connect(config.url)
            .await
            .unwrap();
    let mut observed = observer
        .subscribe(format!("skvoz.bench.{}.invalid.0.s.1.s", config.run_token))
        .await
        .unwrap();
    observer.flush().await.unwrap();
    user.inject_packet(
        wire::encode(
            id,
            &Frame::Data {
                offset: 1,
                bytes: Box::new([0]),
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(consumer.turn(Duration::from_secs(1)).await.is_err());
    assert_clean_terminal(&mut consumer, id, CloseReason::ProtocolError);
    let grant = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let message = observed.next().await.unwrap();
            if let Frame::PeerGrant {
                consumed_bytes,
                consumed_records,
                ..
            } = wire::decode(&message.payload).unwrap().frame
            {
                break (consumed_bytes, consumed_records);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(grant, (1, 1));
    let error = user.turn(Duration::from_secs(1)).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<skvoz_core::ManagerError>(),
        Some(&skvoz_core::ManagerError::Stream(
            skvoz_core::Error::Protocol(skvoz_core::ProtocolError::InvalidCredit)
        ))
    );
    drive_until_closed(&mut user, id).await;
    assert_clean_terminal(&mut user, id, CloseReason::ProtocolError);
    observed.unsubscribe().await.unwrap();
    observer.drain().await.unwrap();
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
    let (mut user, mut consumer) = scenarios::pair("invalid_version", small()).await.unwrap();
    let next = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    let mut bad = wire::encode(next, &Frame::WindowUpdate { consumed: 0 }).unwrap();
    bad[4] = 255;
    user.inject_packet(bad).await.unwrap();
    assert!(consumer.turn(Duration::from_secs(1)).await.is_err());
    assert_clean_terminal(&mut consumer, next, CloseReason::ProtocolError);
    user.shutdown().await.unwrap();
    consumer.shutdown().await.unwrap();
}

struct RestartBroker(String);
impl RestartBroker {
    fn restore(&self) -> std::io::Result<()> {
        let started = Command::new("docker").args(["start", &self.0]).output()?;
        if !started.status.success() {
            return Err(std::io::Error::other("test broker restart failed"));
        }
        // Restore the fixture before the next Cargo integration-test binary.
        // Docker start completes before the NATS listener is necessarily ready.
        let ready = Command::new("python3")
            .args([
                "-c",
                "import os,time,urllib.request\ndeadline=time.monotonic()+15\nwhile True:\n try:\n  urllib.request.urlopen(os.environ['SKVOZ_NATS_MONITOR']+'/healthz',timeout=.2).close(); break\n except OSError:\n  if time.monotonic()>=deadline: raise\n  time.sleep(.02)",
            ])
            .output()?;
        if !ready.status.success() {
            return Err(std::io::Error::other("test broker readiness deadline"));
        }
        Ok(())
    }
}
impl Drop for RestartBroker {
    fn drop(&mut self) {
        if let Err(error) = self.restore() {
            if std::thread::panicking() {
                eprintln!("Test broker restore failed during cleanup: {error}");
            } else {
                panic!("Test broker restore failed: {error}");
            }
        }
    }
}

#[tokio::test]
async fn z_actual_broker_shutdown_closes_existing_streams() {
    let container =
        std::env::var("SKVOZ_NATS_CONTAINER").expect("run the repository testbench runner");
    let token = std::env::var("SKVOZ_NATS_RUN_TOKEN").unwrap();
    let label = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{index .Config.Labels \"skvoz.testbench.run\"}}",
            &container,
        ])
        .output()
        .unwrap();
    assert!(label.status.success());
    assert_eq!(String::from_utf8(label.stdout).unwrap().trim(), token);
    let (mut user, mut consumer) = scenarios::pair("brokerloss", small()).await.unwrap();
    let id = scenarios::establish(&mut user, &mut consumer)
        .await
        .unwrap();
    user.send(id, b"held").await.unwrap();
    consumer.turn(Duration::from_secs(1)).await.unwrap();
    let _restart = RestartBroker(container.clone());
    let stopped = Command::new("docker")
        .args(["stop", "--time", "1", &container])
        .output()
        .unwrap();
    assert!(stopped.status.success());
    for node in [&mut user, &mut consumer] {
        drive_until_closed(node, id).await;
        assert_eq!(node.failure_kind(), Some(FailureKind::Disconnected));
        assert_clean_terminal(node, id, CloseReason::TransportLost);
        assert!(node.open(b"resume forbidden").await.is_err());
    }
}
