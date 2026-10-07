#![cfg(feature = "real-nats")]
//! Real transport regression for bounded, cancel-safe idle input readiness.
use skvoz_core::{Event, ManagerConfig, PeerId, SendOutcome, runtime::NatsRuntime};
use skvoz_network::{Accept, EngineConfig, EngineRole, Metadata, NetworkEngine, NetworkError};
use skvoz_testbench::runtime_scenarios as r;
use std::time::{Duration, Instant};

fn family_grant(families: &[u8]) -> skvoz_network::SessionConfig {
    serde_json::from_value(serde_json::json!({
        "session":"0123456789abcdef0123456789abcdef", "families":families,
        "source_grants": families.iter().map(|f| if *f==4 {"192.0.2.10/32"} else {"2001:db8::10/128"}).collect::<Vec<_>>(),
        "routes": families.iter().map(|f| if *f==4 {"0.0.0.0/0"} else {"::/0"}).collect::<Vec<_>>(),
        "dns_servers": families.iter().map(|f| if *f==4 {"192.0.2.53"} else {"2001:db8::53"}).collect::<Vec<_>>(),
        "mtu":1500,"channels":1,"packet_queue_bytes":262144,"packet_queue_records":256,
        "setup_timeout_ms":15000,"egress":{"ipv4":if families.contains(&4) {"nat44"} else {"none"},"ipv6":if families.contains(&6) {"routed"} else {"none"}}
    })).unwrap()
}

#[tokio::test]
async fn network_auto_selects_server_grants_and_strict_or_empty_intersection_rejects() {
    use skvoz_network::FamilyPolicy::{Auto, RequireAll};
    for (case, offered, supported, policy, accepted) in [
        ("auto4", vec![4, 6], vec![4], Auto, true),
        ("auto6", vec![4, 6], vec![6], Auto, true),
        ("auto46", vec![4, 6], vec![4, 6], Auto, true),
        ("required4", vec![4, 6], vec![4], RequireAll, false),
        ("required6", vec![4, 6], vec![6], RequireAll, false),
        ("empty_family", vec![6], vec![4], Auto, false),
    ] {
        let config = EngineConfig::default();
        let mut remote =
            NatsRuntime::connect(r::config(0, case).unwrap(), config.core_limits(true))
                .await
                .unwrap();
        let mut local =
            NatsRuntime::connect(r::config(1, case).unwrap(), config.core_limits(false))
                .await
                .unwrap();
        r::joined(&mut local, &mut remote, 1).await.unwrap();
        let mut server = NetworkEngine::new(
            remote,
            EngineRole::Server {
                grants: [(PeerId(1), family_grant(&supported))]
                    .into_iter()
                    .collect(),
            },
            config,
        )
        .unwrap();
        let mut client = NetworkEngine::new(local, EngineRole::Client, config).unwrap();
        client.open_ip(PeerId(0), offered, policy, 1500, 1).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "family negotiation deadline: {case}"
            );
            client.drive(Duration::ZERO).await.unwrap();
            server.drive(Duration::from_millis(1)).await.unwrap();
            if accepted {
                if let Some(selected) = client.sessions().first().and_then(|s| s.config.as_ref()) {
                    assert_eq!(selected.families, supported);
                    assert!(
                        selected
                            .source_grants
                            .iter()
                            .all(|g| supported.contains(&g.family()))
                    );
                    assert!(
                        selected
                            .routes
                            .iter()
                            .all(|g| supported.contains(&g.family()))
                    );
                    assert!(
                        selected
                            .dns_servers
                            .iter()
                            .all(|ip| supported.contains(&if ip.is_ipv4() { 4 } else { 6 }))
                    );
                    break;
                }
            } else if let Some(error) = client.last_error() {
                assert_eq!(error, NetworkError::UnsupportedFamily);
                assert!(client.sessions().is_empty());
                assert!(server.sessions().is_empty());
                assert_eq!(client.resources().queued_packet_bytes, 0);
                break;
            }
        }
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn network_initiator_rejects_unoffered_or_incomplete_required_config() {
    use skvoz_network::{Control, FamilyPolicy};
    for (case, offered, selected, policy) in [
        ("unoffered_family", vec![4], vec![6], FamilyPolicy::Auto),
        (
            "incomplete_required",
            vec![4, 6],
            vec![4],
            FamilyPolicy::RequireAll,
        ),
    ] {
        let config = EngineConfig::default();
        let mut server =
            NatsRuntime::connect(r::config(0, case).unwrap(), config.core_limits(true))
                .await
                .unwrap();
        let mut local =
            NatsRuntime::connect(r::config(1, case).unwrap(), config.core_limits(false))
                .await
                .unwrap();
        r::joined(&mut local, &mut server, 1).await.unwrap();
        let mut client = NetworkEngine::new(local, EngineRole::Client, config).unwrap();
        client.open_ip(PeerId(0), offered, policy, 1500, 1).unwrap();
        let grant = family_grant(&selected);
        let bytes = Control::Config(grant.clone()).encode().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                Instant::now() < deadline,
                "untrusted CONFIG deadline: {case}"
            );
            client.drive(Duration::ZERO).await.unwrap();
            server.turn(Duration::from_millis(1)).await.unwrap();
            for event in server.poll_events(256) {
                if matches!(event.event, Event::IncomingOpen { .. }) {
                    server
                        .accept(
                            event.key,
                            &Accept::IpSession {
                                v: 4,
                                session: grant.session.clone(),
                            }
                            .encode()
                            .unwrap(),
                        )
                        .unwrap();
                    assert_eq!(
                        server.send(event.key, &bytes).unwrap(),
                        SendOutcome::Accepted(bytes.len())
                    );
                }
            }
            if let Some(error) = client.last_error() {
                assert_eq!(error, NetworkError::InvalidConfiguration);
                assert!(client.sessions().is_empty());
                assert!(client.poll_packet().is_none());
                break;
            }
        }
        client.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}

async fn quiet(client: &mut NatsRuntime, server: &mut NatsRuntime) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(Instant::now() < deadline, "runtime quiescence deadline");
        let client_progress = client.turn(Duration::ZERO).await.unwrap();
        let server_progress = server.turn(Duration::ZERO).await.unwrap();
        client.poll_events(256);
        server.poll_events(256);
        if client_progress == 0 && server_progress == 0 {
            // Allow the flushed socket readers to deliver any pending controls.
            tokio::time::sleep(Duration::from_millis(10)).await;
            if client.turn(Duration::ZERO).await.unwrap() == 0
                && server.turn(Duration::ZERO).await.unwrap() == 0
            {
                client.poll_events(256);
                server.poll_events(256);
                return;
            }
        }
    }
}

#[tokio::test]
async fn network_initiator_preserves_incompatible_rejection_version() {
    let config = EngineConfig::default();
    let mut server = NatsRuntime::connect(
        r::config(0, "old_rejection").unwrap(),
        config.core_limits(true),
    )
    .await
    .unwrap();
    let mut local = NatsRuntime::connect(
        r::config(1, "old_rejection").unwrap(),
        config.core_limits(false),
    )
    .await
    .unwrap();
    r::joined(&mut local, &mut server, 1).await.unwrap();
    let mut client = NetworkEngine::new(local, EngineRole::Client, config).unwrap();
    client
        .open_ip(
            PeerId(0),
            vec![4, 6],
            skvoz_network::FamilyPolicy::Auto,
            1500,
            1,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "incompatible rejection deadline");
        client.drive(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for event in server.poll_events(256) {
            if matches!(event.event, Event::IncomingOpen { .. }) {
                server
                    .reject(
                        event.key,
                        br#"{"v":2,"type":"ip-session","error":"unsupported_version"}"#,
                    )
                    .unwrap();
            }
        }
        if let Some(error) = client.last_error() {
            assert_eq!(error, NetworkError::UnsupportedVersion);
            assert!(client.sessions().is_empty());
            break;
        }
    }
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn delayed_data_wakes_idle_turn_and_pending_wait_cancellation_loses_nothing() {
    let mut server_config = r::config(0, "idle_readiness").unwrap();
    let mut client_config = r::config(1, "idle_readiness").unwrap();
    for config in [&mut server_config, &mut client_config] {
        config.heartbeat_interval = Duration::from_secs(5);
        config.peer_timeout = Duration::from_secs(15);
        config.retry_initial = Duration::from_secs(1);
        config.retry_max = Duration::from_secs(1);
    }
    let mut server = NatsRuntime::connect(server_config, ManagerConfig::default())
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(client_config, ManagerConfig::default())
        .await
        .unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (local, remote) = r::handshake(&mut client, &mut server).await.unwrap();
    assert!(client.peer_limits(local).is_some());
    assert!(server.peer_limits(remote).is_some());
    let mut stale = local;
    stale.epoch = stale.epoch.wrapping_add(1);
    assert!(client.peer_limits(stale).is_none());
    stale = local;
    stale.incarnation = stale.incarnation.wrapping_add(1);
    assert!(client.peer_limits(stale).is_none());
    assert_eq!(client.limits().stream, ManagerConfig::default().stream);
    quiet(&mut client, &mut server).await;
    let started = Instant::now();
    let (progress, ()) = tokio::join!(server.turn(Duration::from_millis(400)), async {
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            client.send(local, b"delayed data").unwrap(),
            SendOutcome::Accepted(12)
        );
        client.turn(Duration::ZERO).await.unwrap();
    });
    assert!(
        progress.unwrap() > 0,
        "idle turn did not process the arriving frame"
    );
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "idle turn waited for its timer despite incoming DATA: {:?}",
        started.elapsed()
    );
    let events = server.poll_events(256);
    assert!(events.iter().any(|event| event.key == remote
        && matches!(&event.event,Event::Data {offset:0,bytes} if bytes.as_ref()==b"delayed data")));
    assert_eq!(
        server.snapshot(remote).unwrap().receive_unconsumed_bytes,
        12
    );
    server.consume_through(remote, 12).unwrap();
    quiet(&mut client, &mut server).await;

    // No work/output was extracted: cancelling only a pending idle wait is safe.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            server.turn(Duration::from_millis(400))
        )
        .await
        .is_err()
    );
    assert_eq!(
        client.send(local, b"after cancellation").unwrap(),
        SendOutcome::Accepted(18)
    );
    client.turn(Duration::ZERO).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        assert!(
            Instant::now() < deadline,
            "cancelled wait lost an incoming frame"
        );
        server.turn(Duration::from_millis(50)).await.unwrap();
        if server.poll_events(256).into_iter().any(|event|event.key==remote&&matches!(event.event,Event::Data{offset:12,bytes} if bytes.as_ref()==b"after cancellation")){break;}
    }
    assert_eq!(
        server.snapshot(remote).unwrap().receive_unconsumed_bytes,
        18
    );
    server.consume_through(remote, 30).unwrap();
    quiet(&mut client, &mut server).await;
    let started = Instant::now();
    assert_eq!(server.turn(Duration::from_millis(50)).await.unwrap(), 0);
    assert!(
        started.elapsed() >= Duration::from_millis(35),
        "idle wait spun instead of waiting"
    );
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "idle timeout exceeded its bound"
    );
    assert!(server.peer_ready(PeerId(1)));
    assert_eq!(server.status().counters.shard_overflows, 0);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_wake_ends_only_idle_wait_and_preserves_subsequent_nats_data() {
    let mut server_config = r::config(0, "host_readiness").unwrap();
    let mut client_config = r::config(1, "host_readiness").unwrap();
    for config in [&mut server_config, &mut client_config] {
        config.heartbeat_interval = Duration::from_secs(5);
        config.peer_timeout = Duration::from_secs(15);
        config.retry_initial = Duration::from_secs(1);
        config.retry_max = Duration::from_secs(1);
    }
    let mut server = NatsRuntime::connect(server_config, ManagerConfig::default())
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(client_config, ManagerConfig::default())
        .await
        .unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let (local, remote) = r::handshake(&mut client, &mut server).await.unwrap();
    quiet(&mut client, &mut server).await;

    let started = Instant::now();
    assert_eq!(
        server
            .turn_with_wake(Duration::from_millis(400), async {
                tokio::time::sleep(Duration::from_millis(40)).await;
            })
            .await
            .unwrap(),
        0,
        "host readiness must not count as transport progress"
    );
    assert!(started.elapsed() >= Duration::from_millis(30));
    assert!(
        started.elapsed() < Duration::from_millis(300),
        "host readiness did not end the idle wait"
    );
    assert!(server.poll_events(256).is_empty());
    assert_eq!(server.snapshot(remote).unwrap().receive_unconsumed_bytes, 0);

    // The host wake dropped only a pending subscriber poll. DATA arriving next
    // remains owned by that subscriber until the following complete turn.
    assert_eq!(
        client.send(local, b"after host wake").unwrap(),
        SendOutcome::Accepted(15)
    );
    // Even an immediately ready host future cannot interrupt active output.
    assert!(
        client
            .turn_with_wake(Duration::from_millis(400), async {
                panic!("host future polled while output was being handled");
            })
            .await
            .unwrap()
            > 0
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut received = 0;
    while received == 0 {
        assert!(Instant::now() < deadline, "host wake lost incoming DATA");
        server
            .turn_with_wake(Duration::from_millis(50), async {})
            .await
            .unwrap();
        for event in server.poll_events(256) {
            if event.key == remote
                && let Event::Data { offset, bytes } = event.event
            {
                assert_eq!(offset, 0);
                assert_eq!(bytes.as_ref(), b"after host wake");
                received += 1;
            }
        }
        // The fixture supplies a fresh immediate wake each iteration; yield so
        // the asynchronous socket reader can deliver the pending NATS frame.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(received, 1);
    assert_eq!(
        server.snapshot(remote).unwrap().receive_unconsumed_bytes,
        15
    );
    server.consume_through(remote, 15).unwrap();
    quiet(&mut client, &mut server).await;
    assert_eq!(server.snapshot(remote).unwrap().receive_unconsumed_bytes, 0);
    assert_eq!(server.status().counters.shard_overflows, 0);
    assert!(server.peer_ready(PeerId(1)));
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn network_admission_rejects_incompatible_remote_window_and_frame() {
    for (case, window, frame) in [
        ("network_window", 16384, 16384),
        ("network_frame", 65536, 1),
    ] {
        let config = EngineConfig::default();
        let mut server =
            NatsRuntime::connect(r::config(0, case).unwrap(), config.core_limits(true))
                .await
                .unwrap();
        let mut remote_limits = config.core_limits(false);
        remote_limits.stream.receive_window = window;
        remote_limits.stream.max_frame = frame;
        let mut client = NatsRuntime::connect(r::config(1, case).unwrap(), remote_limits)
            .await
            .unwrap();
        r::joined(&mut client, &mut server, 1).await.unwrap();
        let mut engine = NetworkEngine::new(
            server,
            EngineRole::Server {
                grants: Default::default(),
            },
            config,
        )
        .unwrap();
        let key = client
            .open(
                PeerId(0),
                &Metadata::IpSession {
                    v: 4,
                    families: vec![4],
                    family_policy: skvoz_network::FamilyPolicy::RequireAll,
                    max_mtu: 1500,
                    channels: 1,
                }
                .encode()
                .unwrap(),
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let rejection = loop {
            assert!(Instant::now() < deadline, "incompatible OPEN not rejected");
            client.turn(Duration::ZERO).await.unwrap();
            engine.drive(Duration::from_millis(1)).await.unwrap();
            if let Some(reason) = client.poll_events(256).into_iter().find_map(|event| {
                if event.key == key
                    && let Event::Rejected { reason } = event.event
                {
                    Some(reason)
                } else {
                    None
                }
            }) {
                break reason;
            }
        };
        let reason: serde_json::Value = serde_json::from_slice(&rejection).unwrap();
        assert_eq!(
            reason,
            serde_json::json!({"v":4,"type":"ip-session","error":"invalid_request"})
        );
        assert!(engine.sessions().is_empty());
        assert_eq!(engine.resources().streams, 0);
        assert_eq!(engine.resources().received_packet_bytes, 0);
        assert_eq!(engine.counters().rejected_opens, 1);
        client.shutdown().await.unwrap();
        engine.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn network_initiator_closes_incompatible_accept_before_readiness() {
    let config = EngineConfig::default();
    let mut remote_limits = config.core_limits(true);
    remote_limits.stream.max_frame = 1;
    let mut server = NatsRuntime::connect(r::config(0, "network_accept").unwrap(), remote_limits)
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(
        r::config(1, "network_accept").unwrap(),
        config.core_limits(false),
    )
    .await
    .unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let mut engine = NetworkEngine::new(client, EngineRole::Client, config).unwrap();
    engine
        .open_ip(
            PeerId(0),
            vec![4],
            skvoz_network::FamilyPolicy::RequireAll,
            1500,
            1,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut accepted = false;
    while !accepted || !engine.sessions().is_empty() {
        assert!(Instant::now() < deadline, "incompatible ACCEPT not retired");
        engine.drive(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for event in server.poll_events(256) {
            if matches!(event.event, Event::IncomingOpen { .. }) {
                server
                    .accept(
                        event.key,
                        &Accept::IpSession {
                            v: 4,
                            session: "0123456789abcdef0123456789abcdef"
                                .to_owned()
                                .try_into()
                                .unwrap(),
                        }
                        .encode()
                        .unwrap(),
                    )
                    .unwrap();
                accepted = true;
            }
        }
    }
    assert_eq!(
        engine.last_error(),
        Some(NetworkError::InvalidConfiguration)
    );
    assert_eq!(engine.resources().streams, 0);
    assert!(engine.poll_packet().is_none());
    server.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn reciprocal_lane_proof_pending_retains_setup_but_never_activates_early() {
    use skvoz_network::{Control, RecordParser, SessionConfig, SessionSignal, SessionState};
    let config = EngineConfig::default();
    let mut server_config = r::config(0, "network_asymmetric_proof").unwrap();
    let mut client_config = r::config(1, "network_asymmetric_proof").unwrap();
    for profile in [&mut server_config, &mut client_config] {
        profile.heartbeat_interval = Duration::from_secs(2);
        profile.peer_timeout = Duration::from_secs(10);
        profile.retry_initial = Duration::from_millis(250);
    }
    let mut server = NatsRuntime::connect(server_config, config.core_limits(true))
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(client_config, config.core_limits(false))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.peer_ready(PeerId(0)) {
        assert!(Instant::now() < deadline, "asymmetric proof setup deadline");
        client.turn(Duration::from_millis(1)).await.unwrap();
        // Keep only server's reciprocal proof pending; client may prove its lane.
        server.inject_ignore_next_lane_pong();
        server.turn(Duration::from_millis(1)).await.unwrap();
    }
    assert!(!server.peer_ready(PeerId(1)));
    let grant: SessionConfig = serde_json::from_value(serde_json::json!({
        "session":"0123456789abcdef0123456789abcdef", "families":[4],
        "source_grants":["192.0.2.10/32"], "routes":["0.0.0.0/0"],
        "dns_servers":["192.0.2.53"], "mtu":1500, "channels":1,
        "packet_queue_bytes":262144, "packet_queue_records":256,
        "setup_timeout_ms":15000, "egress":{"ipv4":"nat44","ipv6":"none"}
    }))
    .unwrap();
    let mut engine = NetworkEngine::new(
        server,
        EngineRole::Server {
            grants: [(PeerId(1), grant)].into_iter().collect(),
        },
        config,
    )
    .unwrap();
    let control = client
        .open(
            PeerId(0),
            &Metadata::IpSession {
                v: 4,
                families: vec![4],
                family_policy: skvoz_network::FamilyPolicy::RequireAll,
                max_mtu: 1500,
                channels: 1,
            }
            .encode()
            .unwrap(),
        )
        .unwrap();
    let mut parser = RecordParser::new(true, 16384, 65536).unwrap();
    let mut data = None;
    let mut data_opened = false;
    let mut ready_sent = false;
    let mut active = false;
    let mut retained_pending_proof = false;
    let deadline = Instant::now() + Duration::from_secs(4);
    while !active {
        assert!(Instant::now() < deadline, "reciprocal proof lost IP setup");
        client.turn(Duration::from_millis(1)).await.unwrap();
        engine.drive(Duration::from_millis(1)).await.unwrap();
        if !engine.peer_ready(PeerId(1)) && !engine.sessions().is_empty() {
            retained_pending_proof = true;
            assert_eq!(engine.sessions()[0].state, SessionState::Preparing);
            assert!(engine.poll_packet().is_none());
            assert_eq!(engine.counters().closed_sessions, 0);
        }
        for observed in client.poll_events(32) {
            match observed.event {
                Event::Opened { .. } if Some(observed.key) == data => data_opened = true,
                Event::Data { offset, bytes } if observed.key == control => {
                    parser.push(offset, &bytes).unwrap();
                    while let Some(record) = parser.next_record().unwrap() {
                        match Control::decode(&record).unwrap() {
                            Control::Config(config) => {
                                data = Some(
                                    client
                                        .open(
                                            PeerId(0),
                                            &Metadata::IpData {
                                                v: 4,
                                                session: config.session.clone(),
                                                channel: 0,
                                            }
                                            .encode()
                                            .unwrap(),
                                        )
                                        .unwrap(),
                                );
                            }
                            Control::Active(_) => active = true,
                            other => panic!("unexpected control {other:?}"),
                        }
                        client.consume_through(control, record.end_offset).unwrap();
                    }
                }
                Event::Closed { .. } | Event::Rejected { .. } => {
                    panic!("setup ended before reciprocal proof")
                }
                _ => {}
            }
        }
        if data_opened && !ready_sent {
            let id = engine.sessions()[0].session.clone();
            let ready = Control::Ready(SessionSignal { session: id })
                .encode()
                .unwrap();
            assert_eq!(
                client.send(control, &ready).unwrap(),
                SendOutcome::Accepted(ready.len())
            );
            ready_sent = true;
        }
        if !engine.peer_ready(PeerId(1)) {
            assert!(!active, "ACTIVE arrived before reciprocal proof");
        }
    }
    assert!(
        retained_pending_proof,
        "test did not exercise pending reciprocal proof"
    );
    assert!(engine.peer_ready(PeerId(1)));
    assert_eq!(engine.sessions()[0].state, SessionState::Active);
    assert_eq!(engine.counters().closed_sessions, 0);
    // Actual generation loss still retires the session and clears all ownership.
    client.transport_lost();
    client.shutdown().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while !engine.sessions().is_empty() {
        assert!(
            Instant::now() < deadline,
            "retired peer retained network ownership"
        );
        engine.drive(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(engine.resources().streams, 0);
    assert!(engine.poll_packet().is_none());
    engine.shutdown().await.unwrap();
}
