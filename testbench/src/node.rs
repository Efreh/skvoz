//! Compatibility adapter for fixed-pair regression fixtures. Reusable node
//! ownership/routing/NATS live in the reusable Core library.
pub use skvoz_core::nats::{FailureKind, NatsError as BenchError};
use skvoz_core::{
    Config, Event, ManagerConfig, PeerId, SendOutcome, Snapshot, State, StreamKey,
    nats::{NatsConfig, NatsNode, PeerRoute},
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    User,
    Consumer,
}
impl Role {
    pub fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Consumer => "consumer",
        }
    }
    pub fn id(self) -> PeerId {
        PeerId(match self {
            Self::User => 0,
            Self::Consumer => 1,
        })
    }
    fn peer(self) -> Self {
        match self {
            Self::User => Self::Consumer,
            Self::Consumer => Self::User,
        }
    }
}
pub struct ConnectionConfig {
    pub url: String,
    pub ca: PathBuf,
    pub password: String,
    pub run_token: String,
    pub role: Role,
}
impl ConnectionConfig {
    pub fn from_env(role: Role) -> Result<Self, BenchError> {
        Ok(Self {
            url: std::env::var("SKVOZ_NATS_URL")?,
            ca: std::env::var("SKVOZ_NATS_CA")?.into(),
            password: std::env::var(match role {
                Role::User => "SKVOZ_NATS_USER_PASSWORD",
                Role::Consumer => "SKVOZ_NATS_CONSUMER_PASSWORD",
            })?,
            run_token: std::env::var("SKVOZ_NATS_RUN_TOKEN")?,
            role,
        })
    }
}
#[derive(Clone, Copy, Debug)]
pub struct BenchConfig {
    pub stream: Config,
    pub max_streams: usize,
    pub subscription_capacity: usize,
}
impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            stream: Config {
                receive_window: 8192,
                max_frame: 1024,
                max_pending_frames: 8,
                max_metadata: 256,
                open_timeout_ms: 1000,
            },
            max_streams: 32,
            subscription_capacity: 256,
        }
    }
}
#[derive(Debug)]
pub struct NodeEvent {
    pub stream_id: u64,
    pub event: Event,
}
pub struct Node {
    node: NatsNode,
    peer: PeerId,
    known: BTreeMap<u64, Snapshot>,
}
impl Node {
    pub async fn connect(
        c: ConnectionConfig,
        case: &str,
        config: BenchConfig,
    ) -> Result<Self, BenchError> {
        let peer = c.role.peer().id();
        let limits = ManagerConfig {
            stream: config.stream,
            max_peers: 1,
            max_streams: config.max_streams,
            max_streams_per_peer: config.max_streams,
            receive_budget: config.max_streams * config.stream.receive_window as usize,
            receive_budget_per_peer: config.max_streams * config.stream.receive_window as usize,
            send_budget: config.max_streams * config.stream.receive_window as usize,
            send_budget_per_peer: config.max_streams * config.stream.receive_window as usize,
        };
        let node = NatsNode::connect(
            NatsConfig {
                url: c.url,
                ca: c.ca,
                username: c.role.label().into(),
                password: c.password,
                namespace: format!("skvoz.bench.{}.{}", c.run_token, case),
                id: c.role.id(),
                session: "s".into(),
                peers: vec![PeerRoute {
                    id: peer,
                    session: "s".into(),
                }],
                name: format!("skvoz-{case}-{}", c.role.label()),
                subscription_capacity: config.subscription_capacity,
                client_capacity: 32,
                max_outgoing_per_turn: 256,
                max_incoming_per_turn: 1,
                io_timeout: Duration::from_secs(2),
            },
            limits,
        )
        .await?;
        Ok(Self {
            node,
            peer,
            known: BTreeMap::new(),
        })
    }
    fn key(&self, id: u64) -> StreamKey {
        StreamKey {
            peer: self.peer,
            stream_id: id,
        }
    }
    fn remember(&mut self) {
        for (&id, s) in &mut self.known {
            if let Some(new) = self.node.snapshot(StreamKey {
                peer: self.peer,
                stream_id: id,
            }) {
                *s = new;
            }
        }
    }
    async fn drive(&mut self) -> Result<(), BenchError> {
        self.node.flush_pending().await?;
        self.remember();
        Ok(())
    }
    pub fn failure_kind(&self) -> Option<FailureKind> {
        self.node.failure_kind()
    }
    /// Historical fixture snapshots remain only in this regression adapter.
    pub fn slot_count(&self) -> usize {
        self.known.len()
    }
    pub fn snapshot(&self, id: u64) -> Option<Snapshot> {
        self.node
            .snapshot(self.key(id))
            .or_else(|| self.known.get(&id).copied())
    }
    pub async fn open(&mut self, m: &[u8]) -> Result<u64, BenchError> {
        let k = self.node.open(self.peer, m)?;
        self.known
            .insert(k.stream_id, self.node.snapshot(k).unwrap());
        self.drive().await?;
        Ok(k.stream_id)
    }
    pub async fn accept(&mut self, id: u64, m: &[u8]) -> Result<(), BenchError> {
        self.node.accept(self.key(id), m)?;
        self.drive().await
    }
    pub async fn reject(&mut self, id: u64, m: &[u8]) -> Result<(), BenchError> {
        self.node.reject(self.key(id), m)?;
        self.drive().await
    }
    pub async fn send(&mut self, id: u64, b: &[u8]) -> Result<SendOutcome, BenchError> {
        let r = self.node.send(self.key(id), b)?;
        self.drive().await?;
        Ok(r)
    }
    pub async fn finish(&mut self, id: u64) -> Result<(), BenchError> {
        self.node.finish(self.key(id))?;
        self.drive().await
    }
    pub async fn consume_through(&mut self, id: u64, n: u64) -> Result<(), BenchError> {
        self.node.consume_through(self.key(id), n)?;
        self.drive().await
    }
    pub async fn close(&mut self, id: u64) -> Result<(), BenchError> {
        self.node.close(self.key(id))?;
        self.drive().await
    }
    pub fn poll_events(&mut self) -> Vec<NodeEvent> {
        self.remember();
        let events = self.node.poll_events(256);
        events
            .into_iter()
            .map(|e| {
                if let Some(s) = self.node.snapshot(e.key) {
                    self.known.insert(e.key.stream_id, s);
                } else if let Event::Closed { .. } = e.event
                    && let Some(s) = self.known.get_mut(&e.key.stream_id)
                {
                    s.state = State::Closed;
                    s.pending_data_frames = 0;
                    s.pending_send_bytes = 0;
                    s.buffered_receive_bytes = 0;
                    s.receive_capacity_bytes = 0;
                    s.receive_unconsumed_bytes = 0;
                    s.send_unacknowledged_bytes = 0;
                    s.retained_metadata_bytes = 0;
                }
                NodeEvent {
                    stream_id: e.key.stream_id,
                    event: e.event,
                }
            })
            .collect()
    }
    pub async fn turn(&mut self, wait: Duration) -> Result<usize, BenchError> {
        let r = self.node.turn(wait).await;
        self.remember();
        r
    }
    pub async fn inject_packet(&mut self, b: Vec<u8>) -> Result<(), BenchError> {
        self.node.inject_packet(self.peer, b).await
    }
    pub async fn attempt_forbidden_publish(&mut self) -> Result<(), BenchError> {
        self.node
            .inject_subject("skvoz.forbidden".into(), b"forbidden".to_vec())
            .await
    }
    pub async fn shutdown(self) -> Result<(), BenchError> {
        self.node.shutdown().await
    }
}
