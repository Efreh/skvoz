#![cfg(feature = "real-nats")]

use skvoz_core::{PeerId, runtime::NatsRuntime};
use skvoz_network::{
    Egress, EngineConfig, EngineRole, FamilyPolicy, NetworkEngine, SessionConfig, SessionId,
    SessionState,
};
use skvoz_testbench::runtime_scenarios as r;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

fn packet(sequence: u32) -> Vec<u8> {
    let mut bytes = vec![0x73; 1500];
    bytes[..20].fill(0);
    bytes[0] = 0x45;
    bytes[2..4].copy_from_slice(&1500u16.to_be_bytes());
    bytes[8] = 64;
    bytes[9] = 17;
    bytes[12..16].copy_from_slice(&[198, 51, 100, 10]);
    bytes[16..20].copy_from_slice(&[192, 0, 2, 10]);
    let mut sum: u32 = bytes[..20]
        .chunks_exact(2)
        .map(|part| u16::from_be_bytes([part[0], part[1]]) as u32)
        .sum();
    while sum > 65535 {
        sum = (sum & 65535) + (sum >> 16);
    }
    bytes[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    bytes[20..24].copy_from_slice(&sequence.to_be_bytes());
    bytes
}

async fn pair(case: &str, records: usize) -> (NetworkEngine, NetworkEngine, SessionId) {
    let config = EngineConfig::default();
    let mut server = NatsRuntime::connect(r::config(0, case).unwrap(), config.core_limits(true))
        .await
        .unwrap();
    let mut client = NatsRuntime::connect(r::config(1, case).unwrap(), config.core_limits(false))
        .await
        .unwrap();
    r::joined(&mut client, &mut server, 1).await.unwrap();
    let grant = SessionConfig {
        session: serde_json::from_str(&format!("\"{}\"", "01".repeat(16))).unwrap(),
        families: vec![4],
        source_grants: vec!["192.0.2.10/32".parse().unwrap()],
        routes: vec!["0.0.0.0/0".parse().unwrap()],
        dns_servers: vec!["192.0.2.53".parse().unwrap()],
        mtu: 1500,
        channels: 1,
        packet_queue_bytes: 262144,
        packet_queue_records: records,
        setup_timeout_ms: 15000,
        egress: Egress {
            ipv4: "nat44".into(),
            ipv6: "none".into(),
        },
    };
    let mut server = NetworkEngine::new(
        server,
        EngineRole::Server {
            grants: BTreeMap::from([(PeerId(1), grant)]),
        },
        config,
    )
    .unwrap();
    let mut client = NetworkEngine::new(client, EngineRole::Client, config).unwrap();
    client
        .open_ip(PeerId(0), vec![4], FamilyPolicy::RequireAll, 1500, 1)
        .unwrap();
    let start = Instant::now();
    let mut local_ready = false;
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(8),
            "IP setup deadline"
        );
        client.drive(Duration::ZERO).await.unwrap();
        server.drive(Duration::from_millis(1)).await.unwrap();
        if let Some(session) = client.sessions().first() {
            if session.state == SessionState::Preparing && !local_ready {
                client.local_ready(&session.session).unwrap();
                local_ready = true;
            }
            if session.state == SessionState::Active
                && server.sessions()[0].state == SessionState::Active
            {
                return (client, server, session.session.clone());
            }
        }
    }
}

fn consume(client: &mut NetworkEngine, next: &mut u32) {
    while let Some(received) = client.poll_packet() {
        assert_eq!(
            received.packet,
            packet(*next),
            "IP packet lost or reordered"
        );
        *next += 1;
        client
            .complete_packet(received.key, received.end_offset)
            .unwrap();
    }
}

#[tokio::test]
async fn ip_sender_uses_grown_core_credit_beyond_sixteen_frames() {
    let (mut client, mut server, session) = pair("ip-grown-flight", 256).await;
    let mut sent = 0;
    let mut received = 0;
    let start = Instant::now();
    // Grow credit through real host consumption before pausing the consumer.
    while received < 4096 {
        assert!(start.elapsed() < Duration::from_secs(15));
        for _ in 0..16 {
            let resources = server.resources();
            if sent == 4096
                || resources.queued_packet_bytes + 1508 > 262144
                || resources.queued_packet_records == 256
            {
                break;
            }
            server.enqueue_packet(&session, &packet(sent)).unwrap();
            sent += 1;
        }
        server.drive(Duration::ZERO).await.unwrap();
        client.drive(Duration::from_millis(1)).await.unwrap();
        consume(&mut client, &mut received);
    }
    for _ in 0..8 {
        client.drive(Duration::ZERO).await.unwrap();
        server.drive(Duration::from_millis(1)).await.unwrap();
    }
    // 512 packets exceed the old maximum 16 * 16384 bytes in flight.
    let target = sent + 512;
    for _ in 0..256 {
        for _ in 0..16 {
            let resources = server.resources();
            if sent == target
                || resources.queued_packet_bytes + 1508 > 262144
                || resources.queued_packet_records == 256
            {
                break;
            }
            server.enqueue_packet(&session, &packet(sent)).unwrap();
            sent += 1;
        }
        server.drive(Duration::ZERO).await.unwrap();
        client.drive(Duration::from_millis(1)).await.unwrap();
        if sent == target && server.resources().queued_packet_records == 0 {
            break;
        }
    }
    assert_eq!(sent, target);
    assert_eq!(
        server.resources().queued_packet_records,
        0,
        "IP sender stopped despite available grown Core credit"
    );
    assert_eq!(client.counters().packet_drops, 0);
    while received < target {
        assert!(start.elapsed() < Duration::from_secs(15));
        consume(&mut client, &mut received);
        client.drive(Duration::ZERO).await.unwrap();
        server.drive(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(received, target);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    assert_eq!(client.resources().sessions, 0);
    assert_eq!(server.resources().sessions, 0);
}

#[tokio::test]
async fn full_ip_receive_queue_preserves_packets_until_consumed() {
    let (mut client, mut server, session) = pair("ip-queue-backpressure", 1).await;
    // One record per queue is intentional. Send successive records with complete
    // Core turns between admissions, while leaving the native consumer paused.
    for sequence in 0..8 {
        server.enqueue_packet(&session, &packet(sequence)).unwrap();
        server.drive(Duration::ZERO).await.unwrap();
        client.drive(Duration::from_millis(1)).await.unwrap();
    }
    for _ in 0..16 {
        server.drive(Duration::ZERO).await.unwrap();
        client.drive(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(client.resources().received_packet_records, 1);
    assert_eq!(
        client.counters().packet_drops,
        0,
        "valid IP records were dropped while the native queue was full"
    );
    let mut received = 0;
    let start = Instant::now();
    while received < 8 {
        assert!(start.elapsed() < Duration::from_secs(5));
        consume(&mut client, &mut received);
        client.drive(Duration::ZERO).await.unwrap();
        server.drive(Duration::from_millis(1)).await.unwrap();
    }
    assert_eq!(received, 8);
    assert_eq!(client.resources().received_packet_bytes, 0);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
