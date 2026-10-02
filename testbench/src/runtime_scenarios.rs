//! Dynamic universal Core qualification helpers; credentials are runner-provisioned.
use crate::BenchError;
use skvoz_core::{
    Event, ManagerConfig, PeerId, SendOutcome,
    runtime::{Authentication, Membership, NatsRuntime, RuntimeConfig, RuntimeKey, Trust},
};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
pub fn config(id: u64, case: &str) -> Result<RuntimeConfig, BenchError> {
    Ok(RuntimeConfig {
        url: std::env::var("SKVOZ_NATS_URL")?,
        trust: Trust::ManagedCa(std::env::var("SKVOZ_NATS_CA")?.into()),
        authentication: Authentication {
            username: format!("p{id}"),
            password: std::env::var(format!("SKVOZ_NATS_P{id}_PASSWORD"))?,
        },
        namespace: format!(
            "skvoz.runtime.{}.{case}",
            std::env::var("SKVOZ_NATS_RUN_TOKEN")?
        ),
        id: PeerId(id),
        membership: if id == 0 {
            Membership::BrokerAuthorized
        } else {
            Membership::Allowlist(BTreeSet::from([PeerId(0)]))
        },
        initiate: if id == 0 { vec![] } else { vec![PeerId(0)] },
        shards: 8,
        subscription_capacity: 128,
        join_capacity: 128,
        client_capacity: 16,
        max_incoming_per_turn: 64,
        max_outgoing_per_turn: 32,
        io_timeout: Duration::from_millis(700),
        heartbeat_interval: Duration::from_millis(100),
        peer_timeout: Duration::from_millis(800),
        join_timeout: Duration::from_secs(5),
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_secs(1),
        max_retries: 8,
        terminal_drain_timeout: Duration::from_secs(5),
    })
}
pub async fn node(id: u64, case: &str) -> Result<NatsRuntime, BenchError> {
    Ok(NatsRuntime::connect(config(id, case)?, ManagerConfig::default()).await?)
}
pub async fn joined(
    client: &mut NatsRuntime,
    server: &mut NatsRuntime,
    id: u64,
) -> Result<(), BenchError> {
    let start = Instant::now();
    while !client.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(id)) {
        if start.elapsed() > Duration::from_secs(8) {
            return Err(std::io::Error::other(format!(
                "join deadline client={:?} server={:?}",
                client.status(),
                server.status()
            ))
            .into());
        }
        client.turn(Duration::ZERO).await?;
        server.turn(Duration::from_millis(1)).await?;
    }
    Ok(())
}
pub async fn handshake(
    client: &mut NatsRuntime,
    server: &mut NatsRuntime,
) -> Result<(RuntimeKey, RuntimeKey), BenchError> {
    let key = client.open(PeerId(0), b"opaque destination")?;
    let mut remote = None;
    let mut opened = false;
    let start = Instant::now();
    while !opened {
        if start.elapsed() > Duration::from_secs(4) {
            return Err(std::io::Error::other("stream handshake deadline").into());
        }
        client.turn(Duration::ZERO).await?;
        server.turn(Duration::from_millis(1)).await?;
        for e in server.poll_events(256) {
            if let Event::IncomingOpen { .. } = e.event {
                server.accept(e.key, b"")?;
                remote = Some(e.key);
            }
        }
        for e in client.poll_events(256) {
            if e.key == key && matches!(e.event, Event::Opened { .. }) {
                opened = true;
            }
        }
    }
    Ok((key, remote.unwrap()))
}
pub async fn bytes(
    client: &mut NatsRuntime,
    server: &mut NatsRuntime,
    key: RuntimeKey,
    data: &[u8],
) -> Result<(), BenchError> {
    let mut sent = 0;
    let mut received = Vec::new();
    let start = Instant::now();
    while received.len() < data.len() {
        if start.elapsed() > Duration::from_secs(5) {
            return Err(std::io::Error::other("runtime byte deadline").into());
        }
        if sent < data.len()
            && let SendOutcome::Accepted(n) = client.send(key, &data[sent..])?
        {
            sent += n;
        }
        client.turn(Duration::ZERO).await?;
        server.turn(Duration::from_millis(1)).await?;
        for e in server.poll_events(256) {
            if let Event::Data { offset, bytes } = e.event {
                received.extend_from_slice(&bytes);
                server.consume_through(e.key, offset + bytes.len() as u64)?;
            }
        }
        client.poll_events(256);
    }
    if received != data {
        return Err(std::io::Error::other("runtime bytes differ").into());
    }
    Ok(())
}
