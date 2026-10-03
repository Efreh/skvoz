//! Public embedding profiles and runtime observations, independent of transport driving.
use super::TRANSPORT_PACKET_BYTES;
use crate::{Aggregate, Event, ManagerConfig, ManagerError, PeerId, StreamKey};
use std::{collections::BTreeSet, fmt, path::PathBuf, time::Duration};

#[derive(Clone, Debug)]
pub enum Trust {
    System,
    ManagedCa(PathBuf),
}
#[derive(Clone)]
pub struct Authentication {
    pub username: String,
    pub password: String,
}
impl fmt::Debug for Authentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Authentication([redacted])")
    }
}
#[derive(Clone, Debug)]
pub enum Membership {
    Allowlist(BTreeSet<PeerId>),
    BrokerAuthorized,
}
#[derive(Clone)]
pub struct RuntimeConfig {
    pub url: String,
    /// Optional certificate identity override; dialing and SNI still follow `url`.
    pub tls_server_name: Option<String>,
    pub trust: Trust,
    pub authentication: Authentication,
    pub namespace: String,
    pub id: PeerId,
    pub membership: Membership,
    pub initiate: Vec<PeerId>,
    pub shards: usize,
    pub subscription_capacity: usize,
    pub join_capacity: usize,
    pub client_capacity: usize,
    pub max_incoming_per_turn: usize,
    pub max_outgoing_per_turn: usize,
    pub io_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub peer_timeout: Duration,
    pub join_timeout: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub max_retries: usize,
    pub terminal_drain_timeout: Duration,
}
impl fmt::Debug for RuntimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("endpoint", &"[redacted]")
            .field("authentication", &self.authentication)
            .field("id", &self.id)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Config,
    Random,
    Tls,
    Authentication,
    Authorization,
    Timeout,
    Transport,
    Protocol,
    Admission,
    PeerUnavailable,
    StaleKey,
    IdentityExhausted,
    TerminalDrainTimeout,
    RetryExhausted,
    Manager(ManagerError),
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core runtime error: {self:?}")
    }
}
impl std::error::Error for RuntimeError {}
impl From<ManagerError> for RuntimeError {
    fn from(e: ManagerError) -> Self {
        if e == ManagerError::Admission {
            Self::Admission
        } else {
            Self::Manager(e)
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Connecting,
    Ready,
    Recovering,
    Failed,
    ShuttingDown,
    Closed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeKey {
    pub epoch: u128,
    pub incarnation: u64,
    pub stream: StreamKey,
}
#[derive(Debug)]
pub struct RuntimeEvent {
    pub key: RuntimeKey,
    pub event: Event,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct Counters {
    pub joins: u64,
    pub replacements: u64,
    pub peer_timeouts: u64,
    pub shard_failures: u64,
    pub shard_overflows: u64,
    pub invalid_input: u64,
    pub retries: u64,
    pub join_overflows: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct PeerStatus {
    pub generation: u128,
    pub pair_token: u128,
    pub incarnation: u64,
    pub sent_frames: u64,
    pub received_frames: u64,
    pub handshake_nonce: u128,
    pub ready: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct Status {
    pub lifecycle: Lifecycle,
    pub resources: Aggregate,
    pub active_peers: usize,
    pub membership_slots: usize,
    pub connections: usize,
    pub attempt: usize,
    pub last_error: Option<RuntimeError>,
    pub counters: Counters,
    pub configured_transport_payload_bound: usize,
}
fn valid_token(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
impl RuntimeConfig {
    /// Bounded embedding profile. All peers in the namespace share shard count.
    pub fn new(
        url: impl Into<String>,
        trust: Trust,
        authentication: Authentication,
        namespace: impl Into<String>,
        id: PeerId,
        membership: Membership,
    ) -> Self {
        Self {
            url: url.into(),
            tls_server_name: None,
            trust,
            authentication,
            namespace: namespace.into(),
            id,
            membership,
            initiate: Vec::new(),
            shards: 8,
            subscription_capacity: 128,
            join_capacity: 128,
            client_capacity: 16,
            max_incoming_per_turn: 64,
            max_outgoing_per_turn: 32,
            io_timeout: Duration::from_secs(2),
            heartbeat_interval: Duration::from_secs(1),
            peer_timeout: Duration::from_secs(5),
            join_timeout: Duration::from_secs(10),
            retry_initial: Duration::from_millis(200),
            retry_max: Duration::from_secs(5),
            max_retries: 8,
            terminal_drain_timeout: Duration::from_secs(10),
        }
    }
    /// Validate an embedding profile without I/O; returns its transport payload bound.
    pub fn validate_profile(&self, limits: ManagerConfig) -> Result<usize, RuntimeError> {
        let address: async_nats::ServerAddr = self.url.parse().map_err(|_| RuntimeError::Config)?;
        if self
            .tls_server_name
            .as_deref()
            .is_some_and(|name| crate::runtime_tls::identity(name).is_err())
            || address.username().is_some()
            || address.password().is_some()
            || self.namespace.len() > 256
            || self.namespace.split('.').any(|t| !valid_token(t))
            || self.authentication.username.is_empty()
            || self.authentication.password.is_empty()
            || !(1..=32).contains(&self.shards)
            || !(1..=65536).contains(&self.subscription_capacity)
            || !(1..=65536).contains(&self.join_capacity)
            || !(1..=65536).contains(&self.client_capacity)
            || !(1..=256).contains(&self.max_incoming_per_turn)
            || !(1..=256).contains(&self.max_outgoing_per_turn)
            || self.io_timeout.is_zero()
            || self.heartbeat_interval.is_zero()
            || self.peer_timeout <= self.heartbeat_interval
            || self.join_timeout <= self.io_timeout
            || self.retry_initial.is_zero()
            || self.retry_max < self.retry_initial
            || self.max_retries == 0
            || self.terminal_drain_timeout.is_zero()
            || [
                self.io_timeout,
                self.heartbeat_interval,
                self.peer_timeout,
                self.join_timeout,
                self.retry_initial,
                self.retry_max,
                self.terminal_drain_timeout,
            ]
            .iter()
            .any(|d| *d > Duration::from_secs(86400))
            || self.max_retries > 1024
            || self.initiate.len() > limits.max_peers
            || self.initiate.contains(&self.id)
        {
            return Err(RuntimeError::Config);
        }
        if let Membership::Allowlist(p) = &self.membership
            && (p.len() > limits.max_peers
                || p.contains(&self.id)
                || self.initiate.iter().any(|id| !p.contains(id)))
        {
            return Err(RuntimeError::Config);
        }
        self.subscription_capacity
            .checked_mul(2)
            .and_then(|n| n.checked_mul(self.shards))
            .and_then(|n| n.checked_add(self.join_capacity))
            .and_then(|n| n.checked_add(self.client_capacity.checked_mul(self.shards + 1)?))
            .and_then(|n| n.checked_mul(TRANSPORT_PACKET_BYTES))
            .ok_or(RuntimeError::Config)
    }
}
