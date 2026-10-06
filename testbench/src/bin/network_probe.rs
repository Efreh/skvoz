//! Bounded E1 fixture: real kernel TUN packets through the shared network engine.
use skvoz_core::{
    PeerId,
    runtime::{Authentication, Membership, NatsRuntime, RuntimeConfig, Trust},
};
use skvoz_network::{
    EngineConfig, EngineRole, NetworkEngine, NetworkError, ReceivedPacket, SessionConfig,
    SessionState, validate_packet,
};
use skvoz_network_native::TunDevice;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    io::ErrorKind,
    path::Path,
    time::{Duration, Instant},
};
use tokio::io::{Interest, unix::AsyncFd};
#[path = "network_probe/adversary.rs"]
mod adversary;
#[path = "network_probe/core_echo.rs"]
mod core_echo;

type ProbeResult<T> = Result<T, Box<dyn Error>>;

fn field<'a>(value: &'a serde_json::Value, name: &str) -> ProbeResult<&'a str> {
    value[name]
        .as_str()
        .ok_or_else(|| std::io::Error::other(format!("Missing configuration field {name}")).into())
}

#[tokio::main]
async fn main() -> ProbeResult<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("Expected fixture configuration path"))?;
    let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    if matches!(
        value["mode"].as_str(),
        Some("core-echo-server" | "core-echo-client")
    ) {
        return core_echo::run(&value).await;
    }
    let peer = value["peer"].as_u64().ok_or("Missing peer")?;
    let mtu = value["mtu"].as_u64().ok_or("Missing MTU")? as u16;
    let malicious = value["mode"].as_str() == Some("adversary");
    let tun = if malicious {
        None
    } else {
        let device = AsyncFd::new(TunDevice::create(
            field(&value, "interface")?,
            usize::from(mtu),
        )?)?;
        println!("TUN READY {}", device.get_ref().name());
        Some(device)
    };
    let grants: BTreeMap<PeerId, SessionConfig> = value["grants"]
        .as_object()
        .ok_or("Missing grants")?
        .iter()
        .map(|(id, config)| Ok((PeerId(id.parse()?), serde_json::from_value(config.clone())?)))
        .collect::<ProbeResult<_>>()?;
    let engine_config = EngineConfig::default();
    let mut config = RuntimeConfig::new(
        field(&value, "url")?,
        Trust::ManagedCa(field(&value, "ca_file")?.into()),
        Authentication {
            username: format!("p{peer}"),
            password: field(&value, "password")?.to_owned(),
        },
        field(&value, "namespace")?,
        PeerId(peer),
        Membership::Allowlist(if peer == 0 {
            grants.keys().copied().collect()
        } else {
            BTreeSet::from([PeerId(0)])
        }),
    );
    config.tls_server_name = Some(field(&value, "tls_server_name")?.to_owned());
    config.initiate = if peer == 0 { vec![] } else { vec![PeerId(0)] };
    config.max_incoming_per_turn = 32;
    config.max_outgoing_per_turn = 32;
    config.subscription_capacity = 32;
    config.join_capacity = 32;
    config.client_capacity = 16;
    config.io_timeout = Duration::from_secs(3);
    config.heartbeat_interval = Duration::from_secs(2);
    config.peer_timeout = Duration::from_secs(10);
    config.retry_initial = Duration::from_millis(250);
    config.max_retries = 24;
    config.terminal_drain_timeout = Duration::from_secs(3);
    let runtime = NatsRuntime::connect(config, engine_config.core_limits(peer == 0)).await?;
    if malicious {
        return adversary::run(runtime, &value).await;
    }
    let tun = tun.ok_or("Missing native TUN")?;
    let role = if peer == 0 {
        EngineRole::Server { grants }
    } else {
        EngineRole::Client
    };
    let mut engine = NetworkEngine::new(runtime, role, engine_config)?;
    let ready_file = Path::new(field(&value, "ready_file")?);
    let stop_file = ready_file.with_extension("stop");
    let mut buffer = vec![0; usize::from(mtu) + 1];
    let mut pending_write: Option<(ReceivedPacket, Instant)> = None;
    let mut opening = false;
    let mut local_ready = false;
    let mut announced = BTreeSet::new();
    let mut last_status = Instant::now();
    let started = Instant::now();
    let mut inbound = 0_u64;
    let mut outbound = 0_u64;
    let mut forbidden = 0_u64;
    let mut turns = 0_u64;
    loop {
        if stop_file.exists() {
            break;
        }
        let sessions = engine.sessions();
        for _ in 0..16 {
            let count = match tun.try_io(Interest::READABLE, |device| {
                device.try_read_packet(&mut buffer)
            }) {
                Ok(count) => count,
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => return Err(error.into()),
            };
            let info = validate_packet(&buffer[..count], mtu, &[4, 6])?;
            let session = sessions.iter().find(|session| {
                session.state == SessionState::Active
                    && (peer != 0
                        || session.config.as_ref().is_some_and(|config| {
                            config
                                .source_grants
                                .iter()
                                .any(|g| g.contains(info.destination))
                        }))
            });
            if let Some(session) = session {
                match engine.enqueue_packet(&session.session, &buffer[..count]) {
                    Ok(()) => inbound += 1,
                    Err(NetworkError::Forbidden) => forbidden += 1,
                    Err(NetworkError::Overloaded) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        // Wake only Core's idle wait. Matching try_io calls clear stale readiness
        // after an actual WouldBlock; writable interest exists only for a packet
        // awaiting its atomic native write. Never cancel an active Core turn.
        let interest = if pending_write.is_some() {
            Interest::READABLE | Interest::WRITABLE
        } else {
            Interest::READABLE
        };
        let mut wake_error = None;
        let wake = async {
            if let Err(error) = tun.ready(interest).await {
                wake_error = Some(error);
            }
        };
        if let Err(error) = engine.drive_with_wake(Duration::from_millis(1), wake).await {
            eprintln!(
                "RUNTIME FAILURE {error}; resources={:?}",
                engine.resources()
            );
            break;
        }
        if let Some(error) = wake_error {
            return Err(error.into());
        }
        turns += 1;
        if peer != 0 && !opening && engine.peer_ready(PeerId(0)) {
            let families = value["families"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|n| n.as_u64().map(|n| n as u8))
                        .collect()
                })
                .unwrap_or_else(|| vec![4, 6]);
            engine.open_ip(
                PeerId(0),
                families,
                skvoz_network::FamilyPolicy::RequireAll,
                mtu,
                1,
            )?;
            opening = true;
        }
        let sessions = engine.sessions();
        for session in &sessions {
            if peer != 0
                && !local_ready
                && session.state == SessionState::Preparing
                && session.config.is_some()
                && ready_file.exists()
            {
                engine.local_ready(&session.session)?;
                local_ready = true;
            }
            if session.state == SessionState::Active
                && announced.insert(session.session.as_str().to_owned())
            {
                println!(
                    "SESSION ACTIVE peer={} id={}",
                    session.peer.0,
                    session.session.as_str()
                );
            }
        }
        // One native pending write at most; completion credit follows atomic write.
        for _ in 0..16 {
            if pending_write.is_none() {
                pending_write = engine.poll_packet().map(|packet| (packet, Instant::now()));
            }
            let Some((packet, queued)) = pending_write.as_ref() else {
                break;
            };
            if !sessions.iter().any(|s| s.session == packet.session) {
                pending_write = None;
                continue;
            }
            match tun.try_io(Interest::WRITABLE, |device| {
                device.try_write_packet(&packet.packet)
            }) {
                Ok(()) => {
                    engine.complete_packet(packet.key, packet.end_offset)?;
                    outbound += 1;
                    pending_write = None;
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if queued.elapsed() > Duration::from_secs(1) {
                        engine.close_session(&packet.session)?;
                        pending_write = None;
                    }
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        }
        if last_status.elapsed() >= Duration::from_secs(1) {
            println!(
                "STATUS {}",
                serde_json::json!({"seconds":started.elapsed().as_secs(),"turns":turns,"sessions":sessions.len(),
                    "inbound":inbound,"outbound":outbound,"forbidden":forbidden,
                    "resources":format!("{:?}",engine.resources()),"counters":format!("{:?}",engine.counters()),
                    "last_error":format!("{:?}",engine.last_error()),
                    "core":format!("{:?}",engine.core_status())})
            );
            last_status = Instant::now();
        }
    }
    engine.shutdown().await?;
    println!("STOPPED resources={:?}", engine.resources());
    Ok(())
}
