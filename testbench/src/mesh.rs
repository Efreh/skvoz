//! Repeatable multi-owner workloads using the reusable embedded Core node.
use crate::BenchError;
use skvoz_core::{
    CloseReason, Event, ManagerConfig, PeerId, SendOutcome, StreamKey,
    nats::{NatsConfig, NatsNode, PeerRoute},
};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
#[derive(Clone, Copy, Debug)]
pub struct LoadOptions {
    pub clients: usize,
    pub streams_per_client: usize,
    pub active_per_client: usize,
    pub bytes: usize,
}
fn limits(peers: usize, streams: usize, per_peer: usize) -> ManagerConfig {
    ManagerConfig {
        max_peers: peers,
        max_streams: streams,
        max_streams_per_peer: per_peer,
        receive_budget: streams * 8192,
        receive_budget_per_peer: per_peer * 8192,
        send_budget: streams.min(1024) * 8192,
        send_budget_per_peer: per_peer * 8192,
        stream: skvoz_core::Config {
            open_timeout_ms: 30000,
            ..ManagerConfig::default().stream
        },
    }
}
pub fn connection(
    id: u64,
    case: &str,
    session: &str,
    peers: Vec<PeerRoute>,
) -> Result<NatsConfig, BenchError> {
    Ok(NatsConfig {
        url: std::env::var("SKVOZ_NATS_URL")?,
        ca: std::env::var("SKVOZ_NATS_CA")?.into(),
        username: format!("p{id}"),
        password: std::env::var(format!("SKVOZ_NATS_P{id}_PASSWORD"))?,
        namespace: format!(
            "skvoz.mesh.{}.{case}",
            std::env::var("SKVOZ_NATS_RUN_TOKEN")?
        ),
        id: PeerId(id),
        session: session.into(),
        peers,
        name: format!("skvoz-mesh-{case}-{id}"),
        subscription_capacity: 1024,
        client_capacity: 32,
        max_outgoing_per_turn: 32,
        max_incoming_per_turn: 128,
        io_timeout: Duration::from_secs(2),
    })
}
pub async fn nodes(case: &str, o: LoadOptions) -> Result<(NatsNode, Vec<NatsNode>), BenchError> {
    let peers = (1..=o.clients)
        .map(|id| PeerRoute {
            id: PeerId(id as u64),
            session: "g1".into(),
        })
        .collect();
    let server = NatsNode::connect(
        connection(0, case, "g1", peers)?,
        limits(
            o.clients,
            o.clients * o.streams_per_client,
            o.streams_per_client,
        ),
    )
    .await?;
    let mut clients = Vec::new();
    for id in 1..=o.clients {
        let mut c = connection(
            id as u64,
            case,
            "g1",
            vec![PeerRoute {
                id: PeerId(0),
                session: "g1".into(),
            }],
        )?;
        c.max_outgoing_per_turn = 4;
        c.max_incoming_per_turn = 32;
        c.subscription_capacity = 128;
        clients.push(
            NatsNode::connect(c, limits(1, o.streams_per_client, o.streams_per_client)).await?,
        );
    }
    Ok((server, clients))
}
fn check_deadline(start: Instant) -> Result<(), BenchError> {
    if start.elapsed() > Duration::from_secs(60) {
        Err(
            std::io::Error::other("multi-owner workload exceeded its 60-second phase deadline")
                .into(),
        )
    } else {
        Ok(())
    }
}
#[derive(Default)]
struct Transfer {
    sent: usize,
    received: usize,
    finished: bool,
    closed: bool,
    held: bool,
    completed_ms: Option<u128>,
}
fn byte(peer: u64, id: u64, side: u64, offset: usize) -> u8 {
    (peer * 17 + id * 31 + side * 127 + offset as u64 * 37) as u8
}
fn transfer(
    node: &mut NatsNode,
    states: &mut BTreeMap<StreamKey, Transfer>,
    side: u64,
    total: usize,
    hold: bool,
    started: Instant,
) -> Result<usize, BenchError> {
    let mut progressed = 0;
    for e in node.poll_events(256) {
        let t = states
            .get_mut(&e.key)
            .ok_or_else(|| std::io::Error::other("unexpected active stream event"))?;
        match e.event {
            Event::Data { offset, bytes } => {
                assert_eq!(offset, t.received as u64);
                // On clients the logical peer is server 0; their actual client ID is passed as side (>=1).
                let peer = if side == 0 { e.key.peer.0 } else { side };
                let remote_side = if side == 0 { 1 } else { 0 };
                for (i, &b) in bytes.iter().enumerate() {
                    assert_eq!(b, byte(peer, e.key.stream_id, remote_side, t.received + i));
                }
                t.received += bytes.len();
                drop(bytes);
                if hold {
                    t.held = true;
                } else {
                    node.consume_through(e.key, t.received as u64)?;
                }
                progressed += 1;
            }
            Event::RemoteFinished => {
                assert_eq!(t.received, total);
            }
            Event::Closed { reason } => {
                assert_eq!(reason, CloseReason::Finished);
                assert_eq!(t.received, total);
                t.closed = true;
                t.completed_ms = Some(started.elapsed().as_millis());
                progressed += 1;
            }
            Event::Writable => {}
            other => panic!("unexpected transfer event: {other:?}"),
        }
    }
    for (&key, t) in states.iter_mut() {
        if t.closed {
            continue;
        }
        if t.held && !hold {
            node.consume_through(key, t.received as u64)?;
            t.held = false;
        }
        if !t.finished {
            if t.sent < total {
                let count = (total - t.sent).min(1024);
                let peer = if side == 0 { key.peer.0 } else { side };
                let sender_side = if side == 0 { 0 } else { 1 };
                let payload: Vec<_> = (0..count)
                    .map(|i| byte(peer, key.stream_id, sender_side, t.sent + i))
                    .collect();
                if let SendOutcome::Accepted(n) = node.send(key, &payload)? {
                    t.sent += n;
                }
            }
            if t.sent == total {
                node.finish(key)?;
                t.finished = true;
            }
        }
    }
    Ok(progressed)
}
fn rss_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn memory_kib(field: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find(|l| l.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}
fn cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat[stat.rfind(')')? + 1..].split_whitespace().collect();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}
fn broker_snapshot(phase: &str) -> Result<(), BenchError> {
    let output=std::process::Command::new("python3").args(["-c", "import json,os,urllib.request; d=json.load(urllib.request.urlopen(os.environ['SKVOZ_NATS_MONITOR']+'/varz',timeout=2)); print(json.dumps({k:d.get(k) for k in ['version','connections','mem','cpu','slow_consumers','in_msgs','out_msgs']}))"]).output()?;
    if !output.status.success() {
        return Err(std::io::Error::other("broker resource observation failed").into());
    }
    println!(
        "LOAD broker phase={phase} {}",
        String::from_utf8(output.stdout)?.trim()
    );
    Ok(())
}
pub async fn run(case: &str, o: LoadOptions) -> Result<(), BenchError> {
    if o.clients == 0
        || o.clients > 512
        || o.streams_per_client == 0
        || o.streams_per_client > 512
        || o.active_per_client > o.streams_per_client
        || o.bytes > 2 * 1024 * 1024
    {
        return Err(std::io::Error::other("invalid load options").into());
    }
    println!(
        "LOAD workload clients={} streams_per_client={} active_per_client={} bytes_per_direction={} build_profile={}",
        o.clients,
        o.streams_per_client,
        o.active_per_client,
        o.bytes,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    );
    let start = Instant::now();
    let (mut server, mut clients) = nodes(case, o).await?;
    let initial_receive_backing = server.resources().reserved_receive_bytes;
    let connect_ms = start.elapsed().as_millis();
    let mut keys = Vec::new();
    for node in &mut clients {
        let mut k = Vec::new();
        for _ in 0..o.streams_per_client {
            k.push(node.open(PeerId(0), b"load")?);
        }
        keys.push(k);
    }
    let handshake = Instant::now();
    let mut opened = vec![0; o.clients];
    let mut server_opens = 0;
    while opened.iter().any(|n| *n != o.streams_per_client)
        || server_opens != o.clients * o.streams_per_client
    {
        check_deadline(handshake)?;
        for (i, node) in clients.iter_mut().enumerate() {
            node.turn(Duration::ZERO).await?;
            for e in node.poll_events(256) {
                assert!(matches!(e.event, Event::Opened { .. }));
                opened[i] += 1;
            }
            if i % 8 == 7 || i + 1 == o.clients {
                server.turn(Duration::ZERO).await?;
                for e in server.poll_events(256) {
                    match e.event {
                        Event::IncomingOpen { .. } => {
                            server.accept(e.key, b"ok")?;
                            server_opens += 1;
                        }
                        Event::Opened { .. } => {}
                        other => panic!("unexpected handshake event: {other:?}"),
                    }
                }
            }
        }
        tokio::task::yield_now().await;
    }
    let idle = server.resources();
    assert_eq!(idle.streams, o.clients * o.streams_per_client);
    assert_eq!(idle.receive_capacity_bytes, 0);
    assert_eq!(idle.reserved_receive_bytes, initial_receive_backing);
    // Every client has the same initial IDs: owner context, not numeric ID, routes data.
    for k in &keys {
        assert_eq!(k[0].stream_id, 3);
    }
    println!(
        "LOAD idle clients={} streams={} active={} connect_ms={} handshake_ms={} app_rss_kib={:?} reserved_receive_bytes={} receive_capacity_bytes={}",
        o.clients,
        idle.streams,
        o.clients * o.active_per_client,
        connect_ms,
        handshake.elapsed().as_millis(),
        rss_kib(),
        idle.reserved_receive_bytes,
        idle.receive_capacity_bytes
    );
    let mut client_states: Vec<BTreeMap<_, _>> = keys
        .iter()
        .map(|ks| {
            ks.iter()
                .take(o.active_per_client)
                .map(|&k| (k, Transfer::default()))
                .collect()
        })
        .collect();
    let mut server_states = BTreeMap::new();
    for (i, ks) in keys.iter().enumerate() {
        for k in ks.iter().take(o.active_per_client) {
            server_states.insert(
                StreamKey {
                    peer: PeerId(i as u64 + 1),
                    stream_id: k.stream_id,
                },
                Transfer::default(),
            );
        }
    }
    broker_snapshot("idle")?;
    let cpu_before = cpu_ticks();
    let traffic = Instant::now();
    let mut rounds = 0;
    let mut small_peer_progress = false;
    while server_states.values().any(|t| !t.closed)
        || client_states.iter().any(|s| s.values().any(|t| !t.closed))
    {
        check_deadline(traffic)?;
        rounds += 1;
        for (i, node) in clients.iter_mut().enumerate() {
            node.turn(Duration::ZERO).await?;
            transfer(
                node,
                &mut client_states[i],
                i as u64 + 1,
                o.bytes,
                i == 0 && rounds < 32,
                traffic,
            )?;
            if i % 8 == 7 || i + 1 == o.clients {
                server.turn(Duration::ZERO).await?;
                transfer(&mut server, &mut server_states, 0, o.bytes, false, traffic)?;
            }
        }
        if rounds < 32
            && client_states
                .iter()
                .skip(1)
                .any(|s| s.values().any(|t| t.received > 0))
        {
            small_peer_progress = true;
        }
        tokio::task::yield_now().await;
    }
    if o.clients > 1 && o.active_per_client > 0 && o.bytes > 0 {
        assert!(
            small_peer_progress,
            "slow reader prevented other client progress"
        );
    }
    let total_bytes = o.clients * o.active_per_client * o.bytes * 2;
    let traffic_duration = traffic.elapsed();
    let traffic_secs = traffic_duration.as_secs_f64();
    let mut latencies: Vec<_> = client_states
        .iter()
        .flat_map(|s| s.values().filter_map(|t| t.completed_ms))
        .collect();
    latencies.sort_unstable();
    let percentile = |q: usize| {
        latencies
            .get(latencies.len().saturating_sub(1) * q / 100)
            .copied()
    };
    // Cleanup idle entries through real cancellation, including repeated slot reuse.
    for (i, node) in clients.iter_mut().enumerate() {
        for k in keys[i].iter().skip(o.active_per_client) {
            node.close(*k)?;
        }
    }
    let cleanup = Instant::now();
    loop {
        let mut live = server.resources().streams;
        for node in &clients {
            live += node.resources().streams;
        }
        if live == 0 {
            break;
        }
        check_deadline(cleanup)?;
        for (i, node) in clients.iter_mut().enumerate() {
            node.turn(Duration::ZERO).await?;
            for e in node.poll_events(256) {
                assert!(
                    matches!(
                        e.event,
                        Event::Closed {
                            reason: CloseReason::Cancelled
                        }
                    ),
                    "unexpected client cleanup event: {e:?}; failure={:?}",
                    node.failure_kind()
                );
            }
            // Match handshake/traffic driving: a large CANCEL burst must not
            // outrun the server's finite subscription just because the workload
            // neglected to service it until every client finished a turn.
            if i % 8 == 7 || i + 1 == o.clients {
                server.turn(Duration::ZERO).await?;
                for e in server.poll_events(256) {
                    assert!(
                        matches!(
                            e.event,
                            Event::Closed {
                                reason: CloseReason::Cancelled
                            }
                        ),
                        "unexpected server cleanup event: {e:?}; failure={:?}",
                        server.failure_kind()
                    );
                }
            }
        }
        tokio::task::yield_now().await;
    }
    let remaining = server.resources();
    assert_eq!(remaining.streams, 0);
    assert_eq!(remaining.pending_send_bytes, 0);
    assert_eq!(remaining.buffered_receive_bytes, 0);
    assert_eq!(remaining.receive_unconsumed_bytes, 0);
    assert_eq!(remaining.receive_capacity_bytes, 0);
    assert!(
        remaining.reserved_receive_bytes
            <= limits(
                o.clients,
                o.clients * o.streams_per_client,
                o.streams_per_client
            )
            .receive_budget
    );
    println!(
        "LOAD complete payload_bytes={} traffic_ms={} bytes_per_second={:.0} rounds={} app_rss_kib={:?} remaining_streams=0 reserved_receive_bytes={} slow_reader_other_peer_progress={}",
        total_bytes,
        traffic_duration.as_millis(),
        total_bytes as f64 / traffic_secs,
        rounds,
        rss_kib(),
        remaining.reserved_receive_bytes,
        small_peer_progress
    );
    println!(
        "LOAD measurements active_completion_ms_p50={:?} p95={:?} p99={:?} app_cpu_ticks_before={:?} after={:?} app_peak_rss_kib={:?} app_contains_all_clients_and_server=true",
        percentile(50),
        percentile(95),
        percentile(99),
        cpu_before,
        cpu_ticks(),
        memory_kib("VmHWM:")
    );
    broker_snapshot("complete")?;
    for node in clients {
        node.shutdown().await?;
    }
    server.shutdown().await?;
    Ok(())
}
