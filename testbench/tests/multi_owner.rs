#![cfg(feature = "real-nats")]
use skvoz_core::{
    CloseReason, Event, Frame, ManagerConfig, PeerId, StreamKey,
    nats::{FailureKind, NatsNode, PeerRoute},
    wire,
};
use skvoz_testbench::mesh;
use std::time::{Duration, Instant};
#[tokio::test]
async fn independent_clients_same_ids_mixed_load_and_idle_cleanup() {
    mesh::run(
        "fan_in",
        mesh::LoadOptions {
            clients: 8,
            streams_per_client: 6,
            active_per_client: 2,
            bytes: 32768,
        },
    )
    .await
    .unwrap();
}
async fn handshake(client: &mut NatsNode, server: &mut NatsNode) -> StreamKey {
    let key = client.open(PeerId(0), b"").unwrap();
    let start = Instant::now();
    let mut opened = false;
    while !opened {
        assert!(start.elapsed() < Duration::from_secs(3));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for e in server.poll_events(256) {
            if matches!(e.event, Event::IncomingOpen { .. }) {
                server.accept(e.key, b"").unwrap();
            }
        }
        for e in client.poll_events(256) {
            assert!(matches!(e.event, Event::Opened { .. }));
            opened = true;
        }
    }
    key
}
#[tokio::test]
async fn live_slots_reused_many_times_without_reusing_ids() {
    let o = mesh::LoadOptions {
        clients: 1,
        streams_per_client: 1,
        active_per_client: 0,
        bytes: 0,
    };
    let (mut server, mut clients) = mesh::nodes("churn", o).await.unwrap();
    let client = &mut clients[0];
    let initial_receive_backing = server.resources().reserved_receive_bytes;
    let mut previous = 0;
    for _ in 0..40 {
        let key = handshake(client, &mut server).await;
        assert!(key.stream_id > previous);
        previous = key.stream_id;
        client.close(key).unwrap();
        let start = Instant::now();
        while client.resources().streams + server.resources().streams > 0 {
            assert!(start.elapsed() < Duration::from_secs(3));
            client.turn(Duration::ZERO).await.unwrap();
            server.turn(Duration::from_millis(1)).await.unwrap();
            for n in [&mut *client, &mut server] {
                for e in n.poll_events(256) {
                    assert!(matches!(
                        e.event,
                        Event::Closed {
                            reason: CloseReason::Cancelled
                        } | Event::Opened { .. }
                    ));
                }
            }
        }
        assert_eq!(
            server.resources().reserved_receive_bytes,
            initial_receive_backing
        );
        assert_eq!(server.resources().receive_unconsumed_bytes, 0);
        assert_eq!(server.resources().buffered_receive_bytes, 0);
    }
    for n in clients {
        n.shutdown().await.unwrap();
    }
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn session_generations_and_authenticated_sender_subjects() {
    let mut server = NatsNode::connect(
        mesh::connection(
            0,
            "sessions",
            "g2",
            vec![PeerRoute {
                id: PeerId(1),
                session: "g1".into(),
            }],
        )
        .unwrap(),
        ManagerConfig::default(),
    )
    .await
    .unwrap();
    let mut client = NatsNode::connect(
        mesh::connection(
            1,
            "sessions",
            "g1",
            vec![PeerRoute {
                id: PeerId(0),
                session: "g2".into(),
            }],
        )
        .unwrap(),
        ManagerConfig::default(),
    )
    .await
    .unwrap();
    let namespace = format!(
        "skvoz.mesh.{}.sessions",
        std::env::var("SKVOZ_NATS_RUN_TOKEN").unwrap()
    );
    let packet = wire::encode(
        3,
        &Frame::Open {
            receive_window: 8,
            max_frame: 4,
            metadata: Box::new([]),
        },
    )
    .unwrap();
    client
        .inject_subject(format!("{namespace}.0.g1.1.g1"), packet.clone())
        .await
        .unwrap();
    let before = server.resources();
    server.turn(Duration::from_millis(20)).await.unwrap();
    assert_eq!(server.resources().streams, 0);
    assert_eq!(
        server.resources().reserved_receive_bytes,
        before.reserved_receive_bytes
    );
    assert_eq!(server.resources().receive_unconsumed_bytes, 0);
    client
        .inject_subject(format!("{namespace}.0.g2.1.old"), packet.clone())
        .await
        .unwrap();
    server.turn(Duration::from_millis(20)).await.unwrap();
    assert_eq!(server.resources().streams, 0);
    let key = handshake(&mut client, &mut server).await;
    assert_eq!(key.stream_id, 3);
    let _ = client
        .inject_subject(format!("{namespace}.0.g2.2.g1"), packet)
        .await;
    let start = Instant::now();
    while client.failure_kind().is_none() {
        assert!(start.elapsed() < Duration::from_secs(2));
        tokio::task::yield_now().await;
    }
    assert_eq!(client.failure_kind(), Some(FailureKind::ServerError));
    client.poll_events(256);
    assert_eq!(client.resources().streams, 0);
    drop(client);
    server.shutdown().await.unwrap();
}
#[tokio::test]
async fn remote_admission_rejects_with_zero_metadata_limit() {
    let mut small = ManagerConfig::default();
    small.stream.max_metadata = 0;
    small.max_streams = 1;
    small.max_streams_per_peer = 1;
    let mut server = NatsNode::connect(
        mesh::connection(
            0,
            "zero_metadata",
            "g1",
            vec![PeerRoute {
                id: PeerId(1),
                session: "g1".into(),
            }],
        )
        .unwrap(),
        small,
    )
    .await
    .unwrap();
    small.max_streams = 2;
    small.max_streams_per_peer = 2;
    let mut client = NatsNode::connect(
        mesh::connection(
            1,
            "zero_metadata",
            "g1",
            vec![PeerRoute {
                id: PeerId(0),
                session: "g1".into(),
            }],
        )
        .unwrap(),
        small,
    )
    .await
    .unwrap();
    handshake(&mut client, &mut server).await;
    let key = client.open(PeerId(0), b"").unwrap();
    let start = Instant::now();
    let mut rejected = false;
    while !rejected {
        assert!(start.elapsed() < Duration::from_secs(3));
        client.turn(Duration::ZERO).await.unwrap();
        server.turn(Duration::from_millis(1)).await.unwrap();
        for e in client.poll_events(256) {
            if e.key == key {
                match e.event {
                    Event::Rejected { reason } => {
                        assert!(reason.is_empty());
                        rejected = true;
                    }
                    Event::Closed { reason } => assert_eq!(reason, CloseReason::Rejected),
                    other => panic!("unexpected admission result: {other:?}"),
                }
            }
        }
        server.poll_events(256);
    }
    assert_eq!(server.resources().streams, 1);
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn real_tcp_embeds_universal_core_on_both_sides() {
    skvoz_testbench::tcp_demo::run().await.unwrap();
}

#[tokio::test]
async fn tcp_socket_error_cancels_both_actual_nodes() {
    skvoz_testbench::tcp_demo::failure(true).await.unwrap();
}
#[tokio::test]
async fn tcp_overall_deadline_cancels_both_actual_nodes() {
    skvoz_testbench::tcp_demo::failure(false).await.unwrap();
}
