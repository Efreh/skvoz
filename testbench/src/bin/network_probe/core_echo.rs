//! Test-only Core capacity diagnostic with no IP packet or native device path.
use super::{ProbeResult, field};
use skvoz_core::{
    Config, Event, ManagerConfig, PeerId, SendOutcome,
    runtime::{Authentication, Membership, NatsRuntime, RuntimeConfig, RuntimeKey, Trust},
};
use std::{
    collections::{BTreeSet, VecDeque},
    path::Path,
    time::{Duration, Instant},
};

const FRAME: usize = 16384;
const WINDOW: usize = 65536;
fn fill(offset: u64, bytes: &mut [u8]) {
    bytes.fill(0xa5);
    let mut cursor = 0;
    while cursor < bytes.len() {
        let absolute = offset + cursor as u64;
        let within = (absolute % FRAME as u64) as usize;
        let size = (FRAME - within).min(bytes.len() - cursor);
        if within < 8 {
            let header = (absolute / FRAME as u64).to_be_bytes();
            let header_size = (8 - within).min(size);
            bytes[cursor..cursor + header_size]
                .copy_from_slice(&header[within..within + header_size]);
        }
        cursor += size;
    }
}

fn verify(offset: u64, bytes: &[u8]) -> ProbeResult<()> {
    let mut expected = [0; FRAME];
    for (index, chunk) in bytes.chunks(FRAME).enumerate() {
        fill(
            offset + (index * FRAME) as u64,
            &mut expected[..chunk.len()],
        );
        if chunk != &expected[..chunk.len()] {
            return Err(std::io::Error::other("Core echo plaintext pattern mismatch").into());
        }
    }
    Ok(())
}

struct Pending {
    bytes: Box<[u8]>,
    cursor: usize,
    end: u64,
}

pub async fn run(value: &serde_json::Value) -> ProbeResult<()> {
    let server = field(value, "mode")? == "core-echo-server";
    let peer = value["peer"].as_u64().ok_or("Missing peer")?;
    if peer != if server { 0 } else { 3 } {
        return Err("Core echo requires server p0 and client p3".into());
    }
    let seconds = value["seconds"].as_u64().unwrap_or(10);
    let warmup = value["warmup_seconds"].as_u64().unwrap_or(0);
    if !(1..=30).contains(&seconds) || warmup > 5 {
        return Err("Core echo duration exceeds its finite diagnostic bound".into());
    }
    let remote = PeerId(if server { 3 } else { 0 });
    let mut config = RuntimeConfig::new(
        field(value, "url")?,
        Trust::ManagedCa(field(value, "ca_file")?.into()),
        Authentication {
            username: format!("p{peer}"),
            password: field(value, "password")?.to_owned(),
        },
        field(value, "namespace")?,
        PeerId(peer),
        Membership::Allowlist(BTreeSet::from([remote])),
    );
    config.tls_server_name = Some(field(value, "tls_server_name")?.to_owned());
    config.initiate = if server { vec![] } else { vec![remote] };
    config.subscription_capacity = 32;
    config.join_capacity = 32;
    config.client_capacity = 16;
    config.max_incoming_per_turn = 32;
    config.max_outgoing_per_turn = 32;
    config.io_timeout = Duration::from_secs(3);
    config.heartbeat_interval = Duration::from_secs(2);
    config.peer_timeout = Duration::from_secs(10);
    config.retry_initial = Duration::from_millis(250);
    config.max_retries = 24;
    config.terminal_drain_timeout = Duration::from_secs(3);
    let limits = ManagerConfig {
        stream: Config {
            receive_window: WINDOW as u32,
            max_frame: FRAME as u32,
            max_pending_frames: 128,
            max_metadata: 512,
            open_timeout_ms: 15000,
        },
        max_peers: if server { 128 } else { 1 },
        max_streams: if server { 512 } else { 32 },
        max_streams_per_peer: 32,
        receive_budget: if server { 67108864 } else { 2097152 },
        receive_budget_per_peer: 2097152,
        send_budget: if server { 67108864 } else { 2097152 },
        send_budget_per_peer: 2097152,
    };
    let mut runtime = NatsRuntime::connect(config, limits).await?;
    println!("CORE ECHO READY");
    let result = transfer(&mut runtime, value, server, remote, seconds, warmup).await;
    let shutdown = runtime.shutdown().await;
    result?;
    shutdown?;
    println!("CORE ECHO STOPPED");
    Ok(())
}

async fn transfer(
    runtime: &mut NatsRuntime,
    value: &serde_json::Value,
    server: bool,
    remote: PeerId,
    seconds: u64,
    warmup: u64,
) -> ProbeResult<()> {
    let mut key: Option<RuntimeKey> = None;
    let mut opened = false;
    let mut sent = 0_u64;
    let mut received = 0_u64;
    let mut pending: VecDeque<Pending> = VecDeque::new();
    let mut queued = 0;
    let mut finish = false;
    let mut remote_finished = false;
    let mut warmup_end = None;
    let mut measuring: Option<(Instant, u64)> = None;
    let mut sending_end = None;
    let mut deadline = Instant::now() + Duration::from_secs(30);
    let mut buffer = [0; FRAME];
    let stop_file = value["stop_file"].as_str().map(Path::new);
    let mut next_stop_check = Instant::now();
    loop {
        if Instant::now() >= deadline {
            return Err("Core echo startup/transfer/drain deadline".into());
        }
        if Instant::now() >= next_stop_check {
            next_stop_check = Instant::now() + Duration::from_millis(100);
            if stop_file.is_some_and(Path::exists) {
                return Err("Core echo stopped before verified completion".into());
            }
        }
        if !server && key.is_none() && runtime.peer_ready(remote) {
            key = Some(runtime.open(remote, b"core-echo1")?);
        }
        if let Some(key) = key {
            if server {
                while let Some(front) = pending.front_mut() {
                    match runtime.send(key, &front.bytes[front.cursor..])? {
                        SendOutcome::Accepted(size) => {
                            front.cursor += size;
                            sent += size as u64;
                            queued -= size;
                            if front.cursor == front.bytes.len() {
                                // Original receive credit follows complete echo
                                // ownership transfer into Core, never queue copy.
                                runtime.consume_through(key, front.end)?;
                                pending.pop_front();
                            }
                        }
                        SendOutcome::WouldBlock => break,
                    }
                }
                if remote_finished && pending.is_empty() && !finish {
                    runtime.finish(key)?;
                    finish = true;
                }
            } else if opened {
                let now = Instant::now();
                if measuring.is_none() && now >= warmup_end.unwrap() && received == sent {
                    measuring = Some((now, received));
                    sending_end = Some(now + Duration::from_secs(seconds));
                }
                let generating = if let Some(end) = sending_end {
                    now < end
                } else {
                    now < warmup_end.unwrap()
                };
                if generating {
                    // Exact64KiB stream credit bounds this to four full frames;
                    // no extra application flight queue or byte-window increase.
                    for _ in 0..4 {
                        if runtime
                            .snapshot(key)
                            .ok_or("Core echo stream disappeared")?
                            .send_unacknowledged_bytes
                            == WINDOW as u64
                        {
                            break;
                        }
                        fill(sent, &mut buffer);
                        match runtime.send(key, &buffer)? {
                            SendOutcome::Accepted(size) => sent += size as u64,
                            SendOutcome::WouldBlock => break,
                        }
                    }
                } else if measuring.is_some() && !finish {
                    runtime.finish(key)?;
                    finish = true;
                    deadline = now + Duration::from_secs(10);
                }
            }
        }
        // Fully awaited, with the same bounded NATS readiness as the IP driver.
        runtime.turn(Duration::from_millis(1)).await?;
        for event in runtime.poll_events(32) {
            match event.event {
                Event::IncomingOpen { metadata } if server && key.is_none() => {
                    if event.key.stream.peer != remote || metadata.as_ref() != b"core-echo1" {
                        runtime.reject(event.key, b"invalid echo request")?;
                        continue;
                    }
                    runtime.accept(event.key, b"core-echo1")?;
                    key = Some(event.key);
                    deadline = Instant::now() + Duration::from_secs(warmup + seconds + 15);
                }
                Event::Opened { metadata } if key == Some(event.key) => {
                    if metadata.as_ref() != b"core-echo1" {
                        return Err("Invalid echo ACCEPT".into());
                    }
                    opened = true;
                    warmup_end = Some(Instant::now() + Duration::from_secs(warmup));
                    if !server {
                        deadline = Instant::now() + Duration::from_secs(warmup + seconds + 15);
                    }
                }
                Event::Data { offset, bytes } if key == Some(event.key) => {
                    if offset != received {
                        return Err("Core echo offset mismatch".into());
                    }
                    verify(offset, &bytes)?;
                    received += bytes.len() as u64;
                    if server {
                        if queued + bytes.len() > WINDOW {
                            return Err("Core echo receive queue bound".into());
                        }
                        queued += bytes.len();
                        pending.push_back(Pending {
                            bytes,
                            cursor: 0,
                            end: received,
                        });
                    } else {
                        runtime.consume_through(event.key, received)?;
                    }
                }
                Event::RemoteFinished if key == Some(event.key) => {
                    remote_finished = true;
                }
                Event::Closed { reason } if key == Some(event.key) => {
                    if reason != skvoz_core::CloseReason::Finished
                        || !finish
                        || !remote_finished
                        || sent != received
                        || !pending.is_empty()
                    {
                        return Err(
                            "Core echo stream ended without exact verified completion".into()
                        );
                    }
                    if !server {
                        let (started, initial) =
                            measuring.ok_or("Core echo missing measurement")?;
                        let elapsed = started.elapsed().as_secs_f64();
                        let bytes = received - initial;
                        if bytes == 0 {
                            return Err("Core echo made no measurement progress".into());
                        }
                        println!(
                            "CORE ECHO {}",
                            serde_json::json!({
                                "bytes":bytes, "seconds":elapsed, "mbit_s": bytes as f64*8.0/elapsed/1_000_000.0,
                                "sent_bytes":sent, "received_bytes":received,
                                "verified_pattern":true, "verified_offsets":true, "verified_every_byte":true,
                            "integrity":"sequence-tagged16KiB pattern, slice comparison of all bytes",
                                "scope":"one-direction verified echoed plaintext, no IP/native path",
                                "counters":format!("{:?}",runtime.status().counters)
                            })
                        );
                    }
                    return Ok(());
                }
                Event::Rejected { .. } if key == Some(event.key) => {
                    return Err("Core echo OPEN rejected".into());
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod pattern_tests {
    use super::*;
    #[test]
    fn fragmented_pattern_preserves_full_sequence_and_detects_corruption() {
        let mut bytes = vec![0; 65536];
        fill(0, &mut bytes);
        verify(0, &bytes).unwrap();
        let mut offset = 0;
        for part in bytes.chunks(1507) {
            verify(offset as u64, part).unwrap();
            offset += part.len();
        }
        assert!(verify(FRAME as u64, &bytes).is_err());
        bytes[FRAME + 777] ^= 1;
        assert!(verify(0, &bytes).is_err());
    }
}
