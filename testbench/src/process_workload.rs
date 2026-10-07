//! Independent-process release workload. Host buffers are explicitly bounded.
use crate::{BenchError, runtime_scenarios};
use skvoz_core::{
    Event, ManagerConfig, PeerId, SendOutcome,
    runtime::{NatsRuntime, RuntimeKey},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    time::{Duration, Instant},
};
struct Echo {
    pending: VecDeque<(u64, Vec<u8>, usize)>,
    eof: bool,
    fin: bool,
}
struct ClientStream {
    key: RuntimeKey,
    opened: bool,
    sent: usize,
    received: usize,
    fin: bool,
    closed: bool,
}
pub async fn worker(args: &[String]) -> Result<(), BenchError> {
    let id: u64 = args[0].parse()?;
    let clients: usize = args[1].parse()?;
    let streams: usize = args[2].parse()?;
    let active: usize = args[3].parse()?;
    let bytes: usize = args[4].parse()?;
    let duration: u64 = args[5].parse()?;
    let directory = PathBuf::from(std::env::var("SKVOZ_QUALIFY_DIR")?);
    let mut config = runtime_scenarios::config(id, "process")?;
    if id != 0
        && let Ok(url) = std::env::var("SKVOZ_NATS_CLIENT_URL")
    {
        config.url = url;
    }
    config.heartbeat_interval = Duration::from_millis(500);
    config.peer_timeout = Duration::from_secs(8);
    config.join_timeout = Duration::from_secs(20);
    config.io_timeout = Duration::from_secs(3);
    config.terminal_drain_timeout = Duration::from_secs(10);
    // THIS workload sends 8192-byte windows as 8 x 1024-byte frames.
    // Four extra slots cover its lifecycle/coalesced-credit bursts. This is
    // not a bound for arbitrary finer DATA fragmentation by another producer.
    let lane_peers = if id == 0 { clients.div_ceil(8) } else { 1 };
    let required = lane_peers
        .checked_mul(streams)
        .and_then(|n| n.checked_mul(12))
        .and_then(|n| n.checked_next_power_of_two())
        .ok_or_else(|| std::io::Error::other("qualification queue profile overflow"))?
        .max(256);
    if required > 65536 {
        return Err(std::io::Error::other(
            "qualification queue profile exceeds finite capacity limit",
        )
        .into());
    }
    config.subscription_capacity = required;
    config.join_capacity = 512;
    config.max_incoming_per_turn = 256;
    config.max_outgoing_per_turn = 128;
    let total = clients
        .checked_mul(streams)
        .ok_or_else(|| std::io::Error::other("workload product overflow"))?;
    let limits = ManagerConfig {
        max_peers: if id == 0 { clients } else { 1 },
        max_streams: if id == 0 { total } else { streams },
        max_streams_per_peer: streams,
        receive_budget: if id == 0 { total } else { streams } * 8192,
        receive_budget_per_peer: streams * 8192,
        send_budget: total.min(4096) * 8192,
        send_budget_per_peer: streams * 8192,
        stream: skvoz_core::Config {
            max_metadata: 512,
            open_timeout_ms: 20000,
            ..ManagerConfig::default().stream
        },
    };
    let mut node = NatsRuntime::connect(config, limits).await?;
    let start = Instant::now();
    let mut echo: BTreeMap<RuntimeKey, Echo> = BTreeMap::new();
    let mut local = Vec::new();
    let mut go = None;
    let mut announced = false;
    let mut completion = Vec::new();
    let mut completed = 0;
    let mut peak_streams = 0;
    let mut peak_host_buffers = 0;
    let mut host_buffers = 0usize;
    let mut ready = VecDeque::new();
    let mut ready_set = BTreeSet::new();
    let slow = Duration::from_millis(
        std::env::var("SKVOZ_SLOW_READER_DELAY_MS")
            .unwrap_or_else(|_| "0".into())
            .parse()?,
    );
    let deadline = Duration::from_secs(
        std::env::var("SKVOZ_WORKER_DEADLINE_SECONDS")
            .unwrap_or_else(|_| "90".into())
            .parse()?,
    );
    let mut consumption: BTreeMap<RuntimeKey, (Instant, u64)> = BTreeMap::new();
    if id == 0 {
        std::fs::write(directory.join("server.ready"), b"ready")?;
    }
    loop {
        if start.elapsed() > deadline {
            return Err(std::io::Error::other("independent-process workload deadline").into());
        }
        node.turn(Duration::from_millis(1)).await?;
        peak_streams = peak_streams.max(node.status().resources.streams);
        if id != 0 && local.is_empty() && node.peer_ready(PeerId(0)) {
            for _ in 0..streams {
                local.push(ClientStream {
                    key: node.open(PeerId(0), b"echo workload")?,
                    opened: false,
                    sent: 0,
                    received: 0,
                    fin: false,
                    closed: false,
                });
            }
        }
        for e in node.poll_events(256) {
            if id == 0 {
                match e.event {
                    Event::IncomingOpen { .. } => {
                        node.accept(e.key, b"")?;
                        echo.insert(
                            e.key,
                            Echo {
                                pending: VecDeque::new(),
                                eof: false,
                                fin: false,
                            },
                        );
                    }
                    Event::Data { offset, bytes } => {
                        let queue = &mut echo
                            .get_mut(&e.key)
                            .ok_or_else(|| std::io::Error::other("unknown echo stream"))?
                            .pending;
                        host_buffers += bytes.len();
                        queue.push_back((offset, bytes.into_vec(), 0));
                        if ready_set.insert(e.key) {
                            ready.push_back(e.key);
                        }
                    }
                    Event::RemoteFinished => {
                        if let Some(s) = echo.get_mut(&e.key) {
                            s.eof = true;
                            if ready_set.insert(e.key) {
                                ready.push_back(e.key);
                            }
                        }
                    }
                    Event::Closed { reason } => {
                        if reason != skvoz_core::CloseReason::Finished
                            && reason != skvoz_core::CloseReason::Cancelled
                        {
                            return Err(std::io::Error::other(format!(
                                "server stream failed: {reason:?}, phase={}, key={:?}, status={:?}, peer={:?}", if directory.join("go").exists(){"traffic_or_hold"}else{"join"}, e.key, node.status(),node.peer_status(e.key.stream.peer)
                            ))
                            .into());
                        }
                        echo.remove(&e.key);
                        completed += 1;
                    }
                    _ => {}
                }
            } else {
                let n = local
                    .iter()
                    .position(|s| s.key == e.key)
                    .ok_or_else(|| std::io::Error::other("unknown client stream"))?;
                let s = &mut local[n];
                match e.event {
                    Event::Opened { .. } => s.opened = true,
                    Event::Data {
                        offset,
                        bytes: data,
                    } => {
                        if offset != s.received as u64
                            || data
                                .iter()
                                .enumerate()
                                .any(|(j, b)| *b != (id as u8).wrapping_add((s.received + j) as u8))
                        {
                            return Err(
                                std::io::Error::other("independent-process echo mismatch").into()
                            );
                        }
                        s.received += data.len();
                        if slow.is_zero() {
                            node.consume_through(e.key, s.received as u64)?;
                        } else {
                            consumption
                                .entry(e.key)
                                .and_modify(|(_, offset)| *offset = s.received as u64)
                                .or_insert((Instant::now() + slow, s.received as u64));
                        }
                    }
                    Event::Closed { reason } => {
                        if n < active && reason != skvoz_core::CloseReason::Finished {
                            return Err(std::io::Error::other(format!(
                                "client active stream failed: {reason:?}, status={:?}",
                                node.status()
                            ))
                            .into());
                        }
                        s.closed = true;
                        if let Some(go) = go
                            && n < active
                        {
                            completion.push(Instant::now().duration_since(go).as_micros() as u64);
                        }
                    }
                    _ => {}
                }
            }
        }
        let consumed: Vec<_> = consumption
            .iter()
            .filter(|(_, (due, _))| *due <= Instant::now())
            .map(|(key, (_, offset))| (*key, *offset))
            .collect();
        for (key, offset) in consumed {
            node.consume_through(key, offset)?;
            consumption.remove(&key);
        }
        if id == 0 {
            for _ in 0..ready.len().min(256) {
                let key = ready.pop_front().unwrap();
                ready_set.remove(&key);
                let Some(s) = echo.get_mut(&key) else {
                    continue;
                };
                for _ in 0..8 {
                    let Some((offset, data, position)) = s.pending.front_mut() else {
                        break;
                    };
                    if let SendOutcome::Accepted(n) = node.send(key, &data[*position..])? {
                        *position += n;
                    } else {
                        break;
                    }
                    if *position == data.len() {
                        node.consume_through(key, *offset + data.len() as u64)?;
                        host_buffers -= data.len();
                        s.pending.pop_front();
                    } else {
                        break;
                    }
                }
                if s.eof && s.pending.is_empty() && !s.fin {
                    node.finish(key)?;
                    s.fin = true;
                }
                if !s.pending.is_empty() && ready_set.insert(key) {
                    ready.push_back(key);
                }
            }
            if host_buffers > total * 8192 {
                return Err(std::io::Error::other("echo host receive bound exceeded").into());
            }
            peak_host_buffers = peak_host_buffers.max(host_buffers);
            if directory.join("stop").exists() && node.status().resources.streams == 0 {
                break;
            }
        } else if !local.is_empty() && local.iter().all(|s| s.opened || s.closed) {
            if !announced {
                std::fs::write(directory.join(format!("ready.{id}")), b"ready")?;
                announced = true;
            }
            if go.is_none() && directory.join("go").exists() {
                go = Some(Instant::now());
            }
            if let Some(go) = go {
                for s in local.iter_mut().take(active) {
                    if s.sent < bytes {
                        let count = (bytes - s.sent).min(8192);
                        let data: Vec<_> = (0..count)
                            .map(|j| (id as u8).wrapping_add((s.sent + j) as u8))
                            .collect();
                        if let SendOutcome::Accepted(n) = node.send(s.key, &data)? {
                            s.sent += n;
                        }
                    }
                    if s.sent == bytes && !s.fin {
                        node.finish(s.key)?;
                        s.fin = true;
                    }
                }
                if local.iter().take(active).all(|s| s.closed)
                    && go.elapsed() >= Duration::from_secs(duration)
                {
                    for s in local.iter_mut().skip(active) {
                        if !s.fin && !s.closed {
                            node.close(s.key)?;
                            s.fin = true;
                        }
                    }
                    if node.status().resources.streams == 0 {
                        break;
                    }
                }
            }
        }
    }
    if id != 0 && local.iter().take(active).any(|s| s.received != bytes) {
        return Err(std::io::Error::other("client incomplete echo").into());
    }
    let status = node.status();
    let resources = node.resources();
    let json = serde_json::json!({"id":id,"elapsed_ms":start.elapsed().as_millis(),"payload_bytes_per_direction":if id==0{0}else{active*bytes},"remaining_streams":status.resources.streams,"reserved_receive_bytes":status.resources.reserved_receive_bytes,"receive_budget":node.limits().receive_budget,"pending_send_bytes":status.resources.pending_send_bytes,"buffered_receive_bytes":resources.buffered_receive_bytes,"receive_unconsumed_bytes":resources.receive_unconsumed_bytes,"receive_capacity_bytes":resources.receive_capacity_bytes,"peak_streams":peak_streams,"peak_host_echo_bytes":peak_host_buffers,"completion_us":completion,"completed_streams":completed,"shard_failures":status.counters.shard_failures,"peer_timeouts":status.counters.peer_timeouts,"build_profile":if cfg!(debug_assertions){"debug"}else{"release"},"subscription_capacity":required});
    std::fs::write(
        directory.join(format!("result.{id}.json")),
        json.to_string(),
    )?;
    node.shutdown().await?;
    Ok(())
}
