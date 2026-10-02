//! Authenticated dynamic NATS sessions with bounded transport shards.
//!
//! Broker permissions MUST bind sender, recipient and sender shard. A subject is
//! not an identity proof without that provisioning contract. See the runtime guide.
use crate::{
    Aggregate, CloseReason, Event, Manager, ManagerConfig, ManagerError, PeerId, Resources,
    SendOutcome, Snapshot, StreamKey, wire,
};
use futures_util::{FutureExt, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

pub const TRANSPORT_PACKET_BYTES: usize = wire::MAX_PACKET_BYTES + 24;
const CONTROL_BYTES: usize = 77;
fn shard(id: PeerId, count: usize) -> usize {
    (id.0 % count as u64) as usize
}
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
#[derive(Default)]
struct Latch(AtomicU8);
impl Latch {
    fn mark(&self, k: u8) {
        self.0.fetch_or(k, Ordering::SeqCst);
    }
    fn get(&self) -> u8 {
        self.0.load(Ordering::SeqCst)
    }
}
struct BatchGuard {
    latches: Vec<Arc<Latch>>,
    complete: bool,
}
impl Drop for BatchGuard {
    fn drop(&mut self) {
        if !self.complete {
            for latch in &self.latches {
                latch.mark(4);
            }
        }
    }
}
struct OutputGuard {
    latch: Arc<Latch>,
    complete: bool,
}
impl Drop for OutputGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.latch.mark(4);
        }
    }
}
struct Connection {
    client: async_nats::Client,
    data: async_nats::Subscriber,
    control: Option<async_nats::Subscriber>,
    latch: Arc<Latch>,
    dirty: bool,
}
#[derive(Clone, Copy)]
struct Control {
    kind: u8,
    sender: u128,
    recipient: u128,
    hello: u128,
    token: u128,
    watermark: u64,
}
impl Control {
    fn encode(self) -> Vec<u8> {
        let mut b = Vec::with_capacity(CONTROL_BYTES);
        b.extend_from_slice(b"SKC1");
        b.push(self.kind);
        for x in [self.sender, self.recipient, self.hello, self.token] {
            b.extend_from_slice(&x.to_be_bytes());
        }
        b.extend_from_slice(&self.watermark.to_be_bytes());
        b
    }
    fn decode(b: &[u8]) -> Option<Self> {
        if b.len() != CONTROL_BYTES || &b[..4] != b"SKC1" || !(1..=6).contains(&b[4]) {
            return None;
        }
        let n = |i| u128::from_be_bytes(b[i..i + 16].try_into().unwrap());
        Some(Self {
            kind: b[4],
            sender: n(5),
            recipient: n(21),
            hello: n(37),
            token: n(53),
            watermark: u64::from_be_bytes(b[69..77].try_into().unwrap()),
        })
    }
}
#[derive(Clone, Copy)]
struct Flight {
    message: Control,
    due: Instant,
    expires: Instant,
}
#[derive(Clone, Copy)]
struct Candidate {
    generation: u128,
    token: u128,
    hello: u128,
    responder: bool,
    expires: Instant,
}
struct Session {
    generation: u128,
    token: u128,
    hello: u128,
    incarnation: u64,
    tx: u64,
    rx: u64,
    next_ping: Instant,
    ping: Option<(u128, u64, Instant)>,
    ping_due: Instant,
    pong: Option<(u128, u64)>,
    retiring: bool,
    proven: bool,
    retired_at: Option<Instant>,
}
struct Peer {
    session: Option<Session>,
    incoming: Option<Flight>,
    outgoing: Option<Flight>,
    candidate: Option<Candidate>,
}
impl Peer {
    fn new() -> Self {
        Self {
            session: None,
            incoming: None,
            outgoing: None,
            candidate: None,
        }
    }
}
/// One exclusive owner must drive turns and drain events. No stream is resumed.
pub struct NatsRuntime {
    config: RuntimeConfig,
    limits: ManagerConfig,
    manager: Manager,
    epoch: u128,
    incarnation: u64,
    started: Instant,
    join: Option<Connection>,
    lanes: Vec<Option<Connection>>,
    peers: BTreeMap<PeerId, Peer>,
    cursor: Option<PeerId>,
    lane_cursor: usize,
    lifecycle: Lifecycle,
    last_error: Option<RuntimeError>,
    counters: Counters,
    active: usize,
    attempt: usize,
    retry_at: Instant,
    drain_started: Option<Instant>,
    transport_bound: usize,
    #[cfg(feature = "fault-injection")]
    drop_next_data: bool,
    #[cfg(feature = "fault-injection")]
    pause_next_data: bool,
    #[cfg(feature = "fault-injection")]
    ignore_next_pong: bool,
}
fn random() -> Result<u128, RuntimeError> {
    let mut b = [0; 16];
    getrandom::fill(&mut b).map_err(|_| RuntimeError::Random)?;
    let v = u128::from_be_bytes(b);
    if v == 0 {
        Err(RuntimeError::Random)
    } else {
        Ok(v)
    }
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
        if address.username().is_some()
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
impl NatsRuntime {
    pub async fn connect(
        config: RuntimeConfig,
        limits: ManagerConfig,
    ) -> Result<Self, RuntimeError> {
        let transport_bound = config.validate_profile(limits)?;
        let now = Instant::now();
        let mut node = Self {
            manager: Manager::new(limits)?,
            epoch: random()?,
            incarnation: 0,
            started: now,
            join: None,
            lanes: (0..config.shards).map(|_| None).collect(),
            peers: BTreeMap::new(),
            cursor: None,
            lane_cursor: 0,
            lifecycle: Lifecycle::Connecting,
            last_error: None,
            counters: Counters::default(),
            active: 0,
            attempt: 0,
            retry_at: now,
            drain_started: None,
            transport_bound,
            #[cfg(feature = "fault-injection")]
            drop_next_data: false,
            #[cfg(feature = "fault-injection")]
            pause_next_data: false,
            #[cfg(feature = "fault-injection")]
            ignore_next_pong: false,
            config,
            limits,
        };
        node.join = Some(node.connection(None).await?);
        node.lifecycle = Lifecycle::Ready;
        for id in node.config.initiate.clone() {
            node.join_peer(id)?;
        }
        Ok(node)
    }
    async fn connection(&self, lane: Option<usize>) -> Result<Connection, RuntimeError> {
        let latch = Arc::new(Latch::default());
        let cb = latch.clone();
        let mut options = async_nats::ConnectOptions::with_user_and_password(
            self.config.authentication.username.clone(),
            self.config.authentication.password.clone(),
        )
        .require_tls(true)
        .tls_first()
        .connection_timeout(self.config.io_timeout)
        .max_reconnects(1)
        .ignore_discovered_servers()
        .client_capacity(self.config.client_capacity)
        .subscription_capacity(if lane.is_some() {
            self.config.subscription_capacity
        } else {
            self.config.join_capacity
        })
        .event_callback(move |event| {
            let cb = cb.clone();
            async move {
                match event {
                    async_nats::Event::Disconnected | async_nats::Event::Closed => cb.mark(1),
                    async_nats::Event::SlowConsumer(_) => cb.mark(2),
                    async_nats::Event::ServerError(_) => cb.mark(8),
                    async_nats::Event::ClientError(_) => cb.mark(4),
                    _ => {}
                }
            }
        });
        if let Trust::ManagedCa(ca) = &self.config.trust {
            options = options.add_root_certificates(ca.clone());
        }
        let client = tokio::time::timeout(
            self.config.io_timeout,
            options.connect(self.config.url.clone()),
        )
        .await
        .map_err(|_| RuntimeError::Timeout)?
        .map_err(|e| {
            let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&e);
            while let Some(s) = source {
                if s.is::<async_nats::rustls::Error>()
                    || s.downcast_ref::<std::io::Error>().is_some_and(|io| {
                        io.get_ref()
                            .is_some_and(|inner| inner.is::<async_nats::rustls::Error>())
                    })
                {
                    return RuntimeError::Tls;
                }
                source = s.source();
            }
            match e.kind() {
                async_nats::ConnectErrorKind::Tls => RuntimeError::Tls,
                async_nats::ConnectErrorKind::Authentication => RuntimeError::Authentication,
                async_nats::ConnectErrorKind::AuthorizationViolation => RuntimeError::Authorization,
                async_nats::ConnectErrorKind::TimedOut => RuntimeError::Timeout,
                _ => RuntimeError::Transport,
            }
        })?;
        if client.max_payload() != TRANSPORT_PACKET_BYTES {
            return Err(RuntimeError::Config);
        }
        let base = if let Some(s) = lane {
            format!(
                "{}.lane.{}.{:032x}.{s}",
                self.config.namespace, self.config.id.0, self.epoch
            )
        } else {
            format!("{}.join.{}", self.config.namespace, self.config.id.0)
        };
        let subject = if lane.is_some() {
            format!("{base}.data.*.*")
        } else {
            format!("{base}.*")
        };
        let data = tokio::time::timeout(self.config.io_timeout, client.subscribe(subject))
            .await
            .map_err(|_| RuntimeError::Timeout)?
            .map_err(|_| RuntimeError::Transport)?;
        let control = if lane.is_some() {
            Some(
                tokio::time::timeout(
                    self.config.io_timeout,
                    client.subscribe(format!("{base}.control.*.*")),
                )
                .await
                .map_err(|_| RuntimeError::Timeout)?
                .map_err(|_| RuntimeError::Transport)?,
            )
        } else {
            None
        };
        tokio::time::timeout(self.config.io_timeout, client.flush())
            .await
            .map_err(|_| RuntimeError::Timeout)?
            .map_err(|_| RuntimeError::Transport)?;
        if latch.get() != 0 {
            return Err(RuntimeError::Authorization);
        }
        Ok(Connection {
            client,
            data,
            control,
            latch,
            dirty: false,
        })
    }
    fn allowed(&self, id: PeerId) -> bool {
        id != self.config.id
            && match &self.config.membership {
                Membership::Allowlist(ids) => ids.contains(&id),
                Membership::BrokerAuthorized => true,
            }
    }
    fn ensure_peer(&mut self, id: PeerId) -> Result<(), RuntimeError> {
        if !self.allowed(id) {
            return Err(RuntimeError::Authorization);
        }
        if !self.peers.contains_key(&id) {
            if self.peers.len() >= self.limits.max_peers {
                return Err(RuntimeError::Admission);
            }
            self.peers.insert(id, Peer::new());
        }
        Ok(())
    }
    pub fn join_peer(&mut self, id: PeerId) -> Result<(), RuntimeError> {
        self.ensure_peer(id)?;
        if self.peers[&id].outgoing.is_some() {
            return Ok(());
        }
        let now = Instant::now();
        self.peers.get_mut(&id).unwrap().outgoing = Some(Flight {
            message: Control {
                kind: 1,
                sender: self.epoch,
                recipient: 0,
                hello: random()?,
                token: 0,
                watermark: 0,
            },
            due: now,
            expires: now + self.config.join_timeout,
        });
        Ok(())
    }
    /// Remove allowlisted authorization. BrokerAuthorized revocation belongs to
    /// the broker provisioner; use terminate_peer only for local termination.
    pub fn revoke_peer(&mut self, id: PeerId) -> Result<(), RuntimeError> {
        let Membership::Allowlist(ids) = &mut self.config.membership else {
            return Err(RuntimeError::Config);
        };
        ids.remove(&id);
        self.terminate_peer(id);
        Ok(())
    }
    pub fn terminate_peer(&mut self, id: PeerId) {
        self.retire(id);
        if let Some(p) = self.peers.get_mut(&id) {
            p.incoming = None;
            p.outgoing = None;
            p.candidate = None;
        }
        self.config.initiate.retain(|p| *p != id);
    }
    fn retire(&mut self, id: PeerId) {
        if let Some(p) = self.peers.get_mut(&id)
            && let Some(s) = &mut p.session
            && !s.retiring
        {
            s.retiring = true;
            s.retired_at = Some(Instant::now());
            if s.proven {
                self.active -= 1;
            }
            self.manager.peer_lost(id);
        }
    }
    fn now(&self) -> Result<u64, RuntimeError> {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .map_err(|_| RuntimeError::IdentityExhausted)
    }
    pub fn status(&self) -> Status {
        Status {
            lifecycle: self.lifecycle,
            resources: self.manager.aggregate(),
            active_peers: self.active,
            membership_slots: self.peers.len(),
            connections: usize::from(self.join.is_some())
                + self.lanes.iter().filter(|c| c.is_some()).count(),
            attempt: self.attempt,
            last_error: self.last_error,
            counters: self.counters,
            configured_transport_payload_bound: self.transport_bound,
        }
    }
    pub fn resources(&self) -> Resources {
        self.manager.resources()
    }
    /// Explicit per-peer protocol diagnostics; tokens are not authentication secrets.
    pub fn peer_status(&self, id: PeerId) -> Option<PeerStatus> {
        let s = self.peers.get(&id)?.session.as_ref()?;
        Some(PeerStatus {
            generation: s.generation,
            pair_token: s.token,
            incarnation: s.incarnation,
            sent_frames: s.tx,
            received_frames: s.rx,
            handshake_nonce: s.hello,
            ready: !s.retiring && s.proven,
        })
    }
    pub fn peer_ready(&self, id: PeerId) -> bool {
        self.lifecycle == Lifecycle::Ready
            && self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_some_and(|s| !s.retiring && s.proven)
    }
    fn check_key(&self, key: RuntimeKey) -> Result<(), RuntimeError> {
        if key.epoch != self.epoch
            || !self
                .peers
                .get(&key.stream.peer)
                .and_then(|p| p.session.as_ref())
                .is_some_and(|s| s.incarnation == key.incarnation && !s.retiring)
        {
            Err(RuntimeError::StaleKey)
        } else {
            Ok(())
        }
    }
    fn prepare(&mut self) -> Result<(), RuntimeError> {
        self.apply_failures();
        if self.lifecycle != Lifecycle::Ready {
            return Err(RuntimeError::Transport);
        }
        self.manager.tick(self.now()?)?;
        Ok(())
    }
    pub fn open(&mut self, peer: PeerId, metadata: &[u8]) -> Result<RuntimeKey, RuntimeError> {
        self.prepare()?;
        if !self.peer_ready(peer) {
            return Err(RuntimeError::PeerUnavailable);
        }
        let key = self.manager.open(peer, metadata, self.now()?)?;
        Ok(RuntimeKey {
            epoch: self.epoch,
            incarnation: self.peers[&peer].session.as_ref().unwrap().incarnation,
            stream: key,
        })
    }
    pub fn accept(&mut self, key: RuntimeKey, metadata: &[u8]) -> Result<(), RuntimeError> {
        self.prepare()?;
        self.check_key(key)?;
        Ok(self.manager.accept(key.stream, metadata)?)
    }
    pub fn reject(&mut self, key: RuntimeKey, reason: &[u8]) -> Result<(), RuntimeError> {
        self.prepare()?;
        self.check_key(key)?;
        Ok(self.manager.reject(key.stream, reason)?)
    }
    pub fn send(&mut self, key: RuntimeKey, bytes: &[u8]) -> Result<SendOutcome, RuntimeError> {
        self.prepare()?;
        self.check_key(key)?;
        Ok(self.manager.send(key.stream, bytes)?)
    }
    pub fn finish(&mut self, key: RuntimeKey) -> Result<(), RuntimeError> {
        self.prepare()?;
        self.check_key(key)?;
        Ok(self.manager.finish(key.stream)?)
    }
    pub fn consume_through(&mut self, key: RuntimeKey, offset: u64) -> Result<(), RuntimeError> {
        self.prepare()?;
        self.check_key(key)?;
        Ok(self.manager.consume_through(key.stream, offset)?)
    }
    pub fn close(&mut self, key: RuntimeKey) -> Result<(), RuntimeError> {
        self.apply_failures();
        self.check_key(key)?;
        Ok(self.manager.close(key.stream, CloseReason::Cancelled)?)
    }
    pub fn snapshot(&self, key: RuntimeKey) -> Option<Snapshot> {
        self.check_key(key).ok()?;
        self.manager.snapshot(key.stream)
    }
    pub fn poll_events(&mut self, max: usize) -> Vec<RuntimeEvent> {
        self.apply_failures();
        self.manager
            .poll_events(max)
            .into_iter()
            .map(|e| RuntimeEvent {
                key: RuntimeKey {
                    epoch: self.epoch,
                    incarnation: self.peers[&e.key.peer]
                        .session
                        .as_ref()
                        .unwrap()
                        .incarnation,
                    stream: e.key,
                },
                event: e.event,
            })
            .collect()
    }
    fn apply_failures(&mut self) {
        if self.lifecycle != Lifecycle::Ready {
            return;
        }
        let join_failure = self.join.as_ref().map_or(1, |c| c.latch.get());
        if join_failure == 2 {
            self.counters.join_overflows += 1;
            let _ = self.join.as_ref().unwrap().latch.0.compare_exchange(
                2,
                0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        } else if join_failure != 0 {
            self.manager.transport_lost();
            for p in self.peers.values_mut() {
                if let Some(s) = &mut p.session {
                    s.retiring = true;
                }
            }
            self.active = 0;
            self.join = None;
            for lane in &mut self.lanes {
                *lane = None;
            }
            self.lifecycle = if join_failure & 8 != 0 {
                Lifecycle::Failed
            } else {
                Lifecycle::Recovering
            };
            self.last_error = Some(if join_failure & 8 != 0 {
                RuntimeError::Authorization
            } else {
                RuntimeError::Transport
            });
            self.drain_started = Some(Instant::now());
            self.retry_at = Instant::now();
            return;
        }
        for lane in 0..self.lanes.len() {
            if self.lanes[lane]
                .as_ref()
                .is_some_and(|c| c.latch.get() != 0)
            {
                if self.lanes[lane]
                    .as_ref()
                    .is_some_and(|c| c.latch.get() & 2 != 0)
                {
                    self.counters.shard_overflows += 1;
                }
                self.lanes[lane] = None;
                self.counters.shard_failures += 1;
                let ids: Vec<_> = self
                    .peers
                    .keys()
                    .filter(|p| shard(**p, self.config.shards) == lane)
                    .copied()
                    .collect();
                for id in ids {
                    self.retire(id);
                    let p = self.peers.get_mut(&id).unwrap();
                    p.incoming = None;
                    p.outgoing = None;
                    p.candidate = None;
                }
            }
        }
    }
    async fn send_control(
        &mut self,
        id: PeerId,
        message: Control,
        established: bool,
    ) -> Result<(), RuntimeError> {
        let (client, latch, subject) = if established {
            let s = self.peers[&id]
                .session
                .as_ref()
                .ok_or(RuntimeError::PeerUnavailable)?;
            let lane = shard(self.config.id, self.config.shards);
            let c = self.lanes[shard(id, self.config.shards)]
                .as_ref()
                .ok_or(RuntimeError::Transport)?;
            (
                c.client.clone(),
                c.latch.clone(),
                format!(
                    "{}.lane.{}.{:032x}.{lane}.control.{}.{:032x}",
                    self.config.namespace, id.0, s.generation, self.config.id.0, self.epoch
                ),
            )
        } else {
            let c = self.join.as_ref().ok_or(RuntimeError::Transport)?;
            (
                c.client.clone(),
                c.latch.clone(),
                format!(
                    "{}.join.{}.{}",
                    self.config.namespace, id.0, self.config.id.0
                ),
            )
        };
        let mut guard = OutputGuard {
            latch,
            complete: false,
        };
        tokio::time::timeout(
            self.config.io_timeout,
            client.publish(subject, message.encode().into()),
        )
        .await
        .map_err(|_| RuntimeError::Timeout)?
        .map_err(|_| RuntimeError::Transport)?;
        // Flush READY promptly; this is only a local socket flush. Matching
        // lane PONG proves readiness across connections. Other control batches
        // are flushed once per touched connection/turn.
        if message.kind == 4 {
            tokio::time::timeout(self.config.io_timeout, client.flush())
                .await
                .map_err(|_| RuntimeError::Timeout)?
                .map_err(|_| RuntimeError::Transport)?;
        }
        if established {
            self.lanes[shard(id, self.config.shards)]
                .as_mut()
                .unwrap()
                .dirty = true;
        } else {
            self.join.as_mut().unwrap().dirty = true;
        }
        guard.complete = true;
        Ok(())
    }
    fn handle_join(&mut self, id: PeerId, c: Control) -> Result<(), RuntimeError> {
        if c.sender == 0 || c.hello == 0 || !self.allowed(id) {
            return Ok(());
        }
        if let Err(e) = self.ensure_peer(id) {
            if matches!(e, RuntimeError::Admission | RuntimeError::Authorization) {
                self.counters.invalid_input += 1;
                return Ok(());
            }
            return Err(e);
        }
        let now = Instant::now();
        if self.peers[&id].session.as_ref().is_some_and(|s| {
            s.retired_at
                .is_some_and(|t| t.elapsed() > self.config.terminal_drain_timeout)
        }) {
            return Ok(());
        }
        match c.kind {
            1 => {
                // If both initiate concurrently, the lower PeerId wins.
                if self.peers[&id].outgoing.is_some() {
                    if self.config.id < id {
                        return Ok(());
                    }
                    self.peers.get_mut(&id).unwrap().outgoing = None;
                }
                if let Some(s) = self.peers[&id].session.as_ref()
                    && s.generation == c.sender
                    && s.hello == c.hello
                    && !s.retiring
                {
                    self.peers.get_mut(&id).unwrap().incoming = Some(Flight {
                        message: Control {
                            kind: 4,
                            sender: self.epoch,
                            recipient: c.sender,
                            hello: c.hello,
                            token: s.token,
                            watermark: 0,
                        },
                        due: now,
                        expires: now + self.config.join_timeout,
                    });
                    return Ok(());
                }
                let old = self.peers[&id].incoming;
                if old
                    .is_some_and(|f| f.message.recipient == c.sender && f.message.hello == c.hello)
                {
                    self.peers
                        .get_mut(&id)
                        .unwrap()
                        .incoming
                        .as_mut()
                        .unwrap()
                        .due = now;
                    return Ok(());
                }
                self.peers.get_mut(&id).unwrap().incoming = Some(Flight {
                    message: Control {
                        kind: 2,
                        sender: self.epoch,
                        recipient: c.sender,
                        hello: c.hello,
                        token: random()?,
                        watermark: 0,
                    },
                    due: now,
                    expires: now + self.config.join_timeout,
                });
            }
            2 if c.recipient == self.epoch && c.token != 0 => {
                if let Some(f) = self.peers.get_mut(&id).unwrap().outgoing.as_mut()
                    && f.message.hello == c.hello
                {
                    f.message = Control {
                        kind: 3,
                        sender: self.epoch,
                        recipient: c.sender,
                        hello: c.hello,
                        token: c.token,
                        watermark: 0,
                    };
                    f.due = now;
                }
            }
            3 if c.recipient == self.epoch => {
                let valid = self.peers[&id].incoming.is_some_and(|f| {
                    f.message.kind == 2
                        && f.message.recipient == c.sender
                        && f.message.hello == c.hello
                        && f.message.token == c.token
                        && f.expires > now
                });
                let active = self.peers[&id].session.as_ref().is_some_and(|s| {
                    s.generation == c.sender
                        && s.token == c.token
                        && s.hello == c.hello
                        && !s.retiring
                });
                if valid {
                    self.peers.get_mut(&id).unwrap().candidate = Some(Candidate {
                        generation: c.sender,
                        token: c.token,
                        hello: c.hello,
                        responder: true,
                        expires: now + self.config.join_timeout,
                    });
                } else if active {
                    self.peers.get_mut(&id).unwrap().incoming = Some(Flight {
                        message: Control {
                            kind: 4,
                            sender: self.epoch,
                            recipient: c.sender,
                            hello: c.hello,
                            token: c.token,
                            watermark: 0,
                        },
                        due: now,
                        expires: now + self.config.join_timeout,
                    });
                }
            }
            4 if c.recipient == self.epoch => {
                if self.peers[&id].outgoing.is_some_and(|f| {
                    f.message.kind == 3
                        && f.message.recipient == c.sender
                        && f.message.hello == c.hello
                        && f.message.token == c.token
                        && f.expires > now
                }) {
                    self.peers.get_mut(&id).unwrap().candidate = Some(Candidate {
                        generation: c.sender,
                        token: c.token,
                        hello: c.hello,
                        responder: false,
                        expires: now + self.config.join_timeout,
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn promote(&mut self, id: PeerId) -> Result<(), RuntimeError> {
        let Some(candidate) = self.peers[&id].candidate else {
            return Ok(());
        };
        if candidate.expires <= Instant::now() {
            self.peers.get_mut(&id).unwrap().candidate = None;
            return Ok(());
        }
        let already = self.peers[&id]
            .session
            .as_ref()
            .is_some_and(|s| s.token == candidate.token && !s.retiring);
        if !already {
            self.retire(id);
            if self.manager.peer_streams(id).is_some_and(|n| n != 0) {
                return Ok(());
            }
            if self.manager.peer_streams(id).is_some() {
                self.manager.remove_peer(id)?;
                self.counters.replacements += 1;
            }
            let lane = shard(id, self.config.shards);
            if self.lanes[lane].is_none() {
                self.lanes[lane] = Some(self.connection(Some(lane)).await?);
            }
            self.incarnation = self
                .incarnation
                .checked_add(1)
                .ok_or(RuntimeError::IdentityExhausted)?;
            self.manager.register_peer(id, self.config.id > id)?;
            self.peers.get_mut(&id).unwrap().session = Some(Session {
                generation: candidate.generation,
                token: candidate.token,
                hello: candidate.hello,
                incarnation: self.incarnation,
                tx: 0,
                rx: 0,
                next_ping: Instant::now(),
                ping: None,
                ping_due: Instant::now(),
                pong: None,
                retiring: false,
                proven: false,
                retired_at: None,
            });
            self.counters.joins += 1;
            self.attempt = 0;
        }
        let p = self.peers.get_mut(&id).unwrap();
        p.candidate = None;
        p.outgoing = None;
        p.incoming = None;
        if candidate.responder {
            self.send_control(
                id,
                Control {
                    kind: 4,
                    sender: self.epoch,
                    recipient: candidate.generation,
                    hello: candidate.hello,
                    token: candidate.token,
                    watermark: 0,
                },
                false,
            )
            .await?;
        }
        Ok(())
    }
    fn sender(&self, subject: &str, lane: Option<usize>, control: bool) -> Option<PeerId> {
        let prefix = if let Some(lane) = lane {
            format!(
                "{}.lane.{}.{:032x}.{lane}.{}.",
                self.config.namespace,
                self.config.id.0,
                self.epoch,
                if control { "control" } else { "data" }
            )
        } else {
            format!("{}.join.{}.", self.config.namespace, self.config.id.0)
        };
        let tail = subject.strip_prefix(&prefix)?;
        let (id, generation) = if lane.is_some() {
            tail.split_once('.')?
        } else {
            (tail, "")
        };
        let id = PeerId(id.parse().ok()?);
        if !self.allowed(id) {
            return None;
        }
        if let Some(lane) = lane {
            if shard(id, self.config.shards) != lane {
                return None;
            }
            let s = self.peers.get(&id)?.session.as_ref()?;
            if s.retiring || generation != format!("{:032x}", s.generation) {
                return None;
            }
        }
        Some(id)
    }
    fn handle_message(
        &mut self,
        message: async_nats::Message,
        lane: Option<usize>,
        control: bool,
    ) -> Result<(), RuntimeError> {
        let Some(id) = self.sender(message.subject.as_str(), lane, control) else {
            self.counters.invalid_input += 1;
            return Ok(());
        };
        if lane.is_none() {
            if let Some(c) = Control::decode(&message.payload) {
                return self.handle_join(id, c);
            }
            self.counters.invalid_input += 1;
            return Ok(());
        }
        if control {
            let Some(c) = Control::decode(&message.payload) else {
                self.counters.invalid_input += 1;
                return Ok(());
            };
            let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
            if c.sender != s.generation || c.recipient != self.epoch || c.token != s.token {
                return Ok(());
            }
            match c.kind {
                5 if c.hello != 0 => s.pong = Some((c.hello, c.watermark)),
                6 => {
                    #[cfg(feature = "fault-injection")]
                    if self.ignore_next_pong {
                        self.ignore_next_pong = false;
                        return Ok(());
                    }
                    if s.ping.is_some_and(|(nonce, watermark, _)| {
                        nonce == c.hello && watermark == c.watermark
                    }) {
                        s.ping = None;
                        if !s.proven {
                            s.proven = true;
                            self.active += 1;
                        }
                        s.next_ping = Instant::now() + self.config.heartbeat_interval;
                    }
                }
                _ => {}
            }
            return Ok(());
        }
        if message.payload.len() < 24 {
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        let token = u128::from_be_bytes(message.payload[..16].try_into().unwrap());
        let seq = u64::from_be_bytes(message.payload[16..24].try_into().unwrap());
        let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
        if token != s.token {
            return Ok(());
        }
        if s.rx.checked_add(1) != Some(seq) {
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        let packet = match wire::decode(&message.payload[24..]) {
            Ok(p) => p,
            Err(_) => {
                self.retire(id);
                self.last_error = Some(RuntimeError::Protocol);
                return Ok(());
            }
        };
        if self
            .manager
            .receive(
                StreamKey {
                    peer: id,
                    stream_id: packet.stream_id,
                },
                &packet.frame,
                self.now()?,
            )
            .is_err()
        {
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        self.peers
            .get_mut(&id)
            .unwrap()
            .session
            .as_mut()
            .unwrap()
            .rx = seq;
        Ok(())
    }
    async fn output(&mut self) -> Result<usize, RuntimeError> {
        let mut count = 0;
        let mut guard = BatchGuard {
            latches: Vec::new(),
            complete: false,
        };
        for _ in 0..self.config.max_outgoing_per_turn {
            let Some(frame) = self.manager.poll_frames(1).pop() else {
                break;
            };
            let id = frame.key.peer;
            let s = self
                .peers
                .get_mut(&id)
                .unwrap()
                .session
                .as_mut()
                .ok_or(RuntimeError::PeerUnavailable)?;
            if s.retiring {
                continue;
            }
            let lane = shard(id, self.config.shards);
            let c = self.lanes[lane].as_mut().ok_or(RuntimeError::Transport)?;
            if !guard.latches.iter().any(|l| Arc::ptr_eq(l, &c.latch)) {
                guard.latches.push(c.latch.clone());
            }
            s.tx = s.tx.checked_add(1).ok_or(RuntimeError::IdentityExhausted)?;
            #[cfg(feature = "fault-injection")]
            if self.pause_next_data && matches!(frame.frame, crate::Frame::Data { .. }) {
                self.pause_next_data = false;
                std::future::pending::<()>().await;
            }
            #[cfg(feature = "fault-injection")]
            if self.drop_next_data && matches!(frame.frame, crate::Frame::Data { .. }) {
                self.drop_next_data = false;
                count += 1;
                continue;
            }
            let mut bytes = Vec::with_capacity(TRANSPORT_PACKET_BYTES);
            bytes.extend_from_slice(&s.token.to_be_bytes());
            bytes.extend_from_slice(&s.tx.to_be_bytes());
            bytes.extend_from_slice(
                &wire::encode(frame.key.stream_id, &frame.frame)
                    .map_err(|_| RuntimeError::Protocol)?,
            );
            let subject = format!(
                "{}.lane.{}.{:032x}.{}.data.{}.{:032x}",
                self.config.namespace,
                id.0,
                s.generation,
                shard(self.config.id, self.config.shards),
                self.config.id.0,
                self.epoch
            );
            tokio::time::timeout(
                self.config.io_timeout,
                c.client.publish(subject, bytes.into()),
            )
            .await
            .map_err(|_| RuntimeError::Timeout)?
            .map_err(|_| RuntimeError::Transport)?;
            c.dirty = true;
            count += 1;
        }
        // Every extracted frame stays guarded until all touched socket flushes
        // complete; cancellation conservatively fails every touched shard.
        for c in self
            .lanes
            .iter()
            .chain(std::iter::once(&self.join))
            .flatten()
        {
            if c.dirty && !guard.latches.iter().any(|l| Arc::ptr_eq(l, &c.latch)) {
                guard.latches.push(c.latch.clone());
            }
        }
        for c in self
            .lanes
            .iter_mut()
            .chain(std::iter::once(&mut self.join))
            .flatten()
        {
            if c.dirty {
                tokio::time::timeout(self.config.io_timeout, c.client.flush())
                    .await
                    .map_err(|_| RuntimeError::Timeout)?
                    .map_err(|_| RuntimeError::Transport)?;
                c.dirty = false;
            }
        }
        guard.complete = true;
        Ok(count)
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_drop_next_data_after_extraction(&mut self) {
        self.drop_next_data = true;
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_pause_next_data_after_extraction(&mut self) {
        self.pause_next_data = true;
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_ignore_next_lane_pong(&mut self) {
        self.ignore_next_pong = true;
    }
    fn rotating_peers(&mut self) -> Vec<PeerId> {
        let max = self.config.max_outgoing_per_turn;
        let mut ids: Vec<_> = self
            .cursor
            .map(|id| {
                self.peers
                    .range((std::ops::Bound::Excluded(id), std::ops::Bound::Unbounded))
                    .take(max)
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default();
        if ids.len() < max {
            ids.extend(self.peers.keys().take(max - ids.len()).copied());
        }
        ids.sort();
        ids.dedup();
        self.cursor = ids.last().copied();
        ids
    }
    async fn control_work(&mut self) -> Result<usize, RuntimeError> {
        let now = Instant::now();
        let mut count = 0;
        let ids = self.rotating_peers();
        for id in ids {
            if self.peers[&id].session.as_ref().is_some_and(|s| {
                s.retired_at
                    .is_some_and(|t| t.elapsed() > self.config.terminal_drain_timeout)
            }) {
                let p = self.peers.get_mut(&id).unwrap();
                p.incoming = None;
                p.outgoing = None;
                p.candidate = None;
                self.last_error = Some(RuntimeError::TerminalDrainTimeout);
            }
            self.promote(id).await?;
            for incoming in [false, true] {
                let flight = if incoming {
                    self.peers[&id].incoming
                } else {
                    self.peers[&id].outgoing
                };
                if let Some(f) = flight {
                    if f.expires <= now {
                        let p = self.peers.get_mut(&id).unwrap();
                        if incoming {
                            p.incoming = None;
                        } else {
                            p.outgoing = None;
                        }
                    } else if f.due <= now {
                        if !incoming && f.message.kind == 3 {
                            let lane = shard(id, self.config.shards);
                            if self.lanes[lane].is_none() {
                                self.lanes[lane] = Some(self.connection(Some(lane)).await?);
                            }
                        }
                        self.send_control(id, f.message, false).await?;
                        let p = self.peers.get_mut(&id).unwrap();
                        let f = if incoming {
                            p.incoming.as_mut()
                        } else {
                            p.outgoing.as_mut()
                        };
                        if let Some(f) = f {
                            f.due = now + self.config.retry_initial;
                        }
                        count += 1;
                    }
                }
            }
            let timeout = self.peers[&id].session.as_ref().is_some_and(|s| {
                !s.retiring && s.ping.is_some_and(|(_, _, deadline)| deadline <= now)
            });
            if timeout {
                self.retire(id);
                self.counters.peer_timeouts += 1;
            }
            let pong = self.peers[&id].session.as_ref().and_then(|s| {
                if !s.retiring {
                    s.pong.filter(|(_, watermark)| s.rx >= *watermark)
                } else {
                    None
                }
            });
            if let Some((nonce, watermark)) = pong {
                let s = self.peers[&id].session.as_ref().unwrap();
                self.send_control(
                    id,
                    Control {
                        kind: 6,
                        sender: self.epoch,
                        recipient: s.generation,
                        hello: nonce,
                        token: s.token,
                        watermark,
                    },
                    true,
                )
                .await?;
                self.peers
                    .get_mut(&id)
                    .unwrap()
                    .session
                    .as_mut()
                    .unwrap()
                    .pong = None;
                count += 1;
            }
            let repeat = self.peers[&id].session.as_ref().and_then(|s| {
                if !s.retiring && s.ping_due <= now {
                    s.ping
                } else {
                    None
                }
            });
            if let Some((nonce, watermark, _)) = repeat {
                let s = self.peers[&id].session.as_ref().unwrap();
                self.send_control(
                    id,
                    Control {
                        kind: 5,
                        sender: self.epoch,
                        recipient: s.generation,
                        hello: nonce,
                        token: s.token,
                        watermark,
                    },
                    true,
                )
                .await?;
                self.peers
                    .get_mut(&id)
                    .unwrap()
                    .session
                    .as_mut()
                    .unwrap()
                    .ping_due = now + self.config.retry_initial;
                count += 1;
            }
            let ping = self.peers[&id]
                .session
                .as_ref()
                .is_some_and(|s| !s.retiring && s.ping.is_none() && s.next_ping <= now);
            if ping {
                let nonce = random()?;
                let s = self.peers[&id].session.as_ref().unwrap();
                let watermark = s.tx;
                self.send_control(
                    id,
                    Control {
                        kind: 5,
                        sender: self.epoch,
                        recipient: s.generation,
                        hello: nonce,
                        token: s.token,
                        watermark,
                    },
                    true,
                )
                .await?;
                self.peers
                    .get_mut(&id)
                    .unwrap()
                    .session
                    .as_mut()
                    .unwrap()
                    .ping = Some((nonce, watermark, now + self.config.peer_timeout));
                self.peers
                    .get_mut(&id)
                    .unwrap()
                    .session
                    .as_mut()
                    .unwrap()
                    .ping_due = now + self.config.retry_initial;
                count += 1;
            }
            let remove = self.peers[&id].session.as_ref().is_none_or(|s| s.retiring)
                && self.manager.peer_streams(id).is_none_or(|n| n == 0)
                && self.peers[&id].incoming.is_none()
                && self.peers[&id].outgoing.is_none()
                && self.peers[&id].candidate.is_none();
            if remove {
                if self.manager.peer_streams(id).is_some() {
                    self.manager.remove_peer(id)?;
                }
                self.peers.remove(&id);
                if self.config.initiate.contains(&id) {
                    self.join_peer(id)?;
                }
            }
        }
        Ok(count)
    }
    async fn recover(&mut self) -> Result<(), RuntimeError> {
        if self.manager.aggregate().streams != 0 {
            if self
                .drain_started
                .is_some_and(|n| n.elapsed() > self.config.terminal_drain_timeout)
            {
                self.lifecycle = Lifecycle::Failed;
                self.last_error = Some(RuntimeError::TerminalDrainTimeout);
            }
            return Ok(());
        }
        if Instant::now() < self.retry_at {
            return Ok(());
        }
        if self.attempt >= self.config.max_retries {
            self.lifecycle = Lifecycle::Failed;
            self.last_error = Some(RuntimeError::RetryExhausted);
            return Ok(());
        }
        self.attempt += 1;
        self.counters.retries += 1;
        self.epoch = random()?;
        self.peers.clear();
        self.manager = Manager::new(self.limits)?;
        self.started = Instant::now();
        match self.connection(None).await {
            Ok(c) => {
                self.join = Some(c);
                self.lifecycle = Lifecycle::Ready;
                self.drain_started = None;
                for id in self.config.initiate.clone() {
                    self.join_peer(id)?;
                }
            }
            Err(e) => {
                self.last_error = Some(e);
                if matches!(
                    e,
                    RuntimeError::Authentication
                        | RuntimeError::Authorization
                        | RuntimeError::Tls
                        | RuntimeError::Config
                ) {
                    self.lifecycle = Lifecycle::Failed;
                    return Ok(());
                }
                let base = self
                    .config
                    .retry_initial
                    .saturating_mul(
                        1u32.checked_shl(self.attempt.min(20) as u32)
                            .unwrap_or(u32::MAX),
                    )
                    .min(self.config.retry_max);
                let jitter = Duration::from_millis((random()? % (base.as_millis() / 4 + 1)) as u64);
                self.retry_at = Instant::now() + base + jitter;
            }
        }
        Ok(())
    }
    /// Bounded message work. Host polling cadence is part of the liveness profile.
    pub async fn turn(&mut self, wait: Duration) -> Result<usize, RuntimeError> {
        self.apply_failures();
        if self.lifecycle == Lifecycle::Recovering {
            self.recover().await?;
            return Ok(0);
        }
        if self.lifecycle != Lifecycle::Ready {
            return Ok(0);
        }
        self.manager.tick(self.now()?)?;
        let mut progress = self.control_work().await?;
        // Join traffic has a separate budget; established control cannot be
        // starved by a membership flood. Shards rotate after every message.
        for _ in 0..(self.config.max_incoming_per_turn / 4).max(1) {
            let next = self
                .join
                .as_mut()
                .and_then(|c| c.data.next().now_or_never().flatten());
            let Some(m) = next else {
                break;
            };
            let id = self.sender(m.subject.as_str(), None, true);
            self.handle_message(m, None, true)?;
            if let Some(id) = id
                && self.peers.contains_key(&id)
            {
                self.promote(id).await?;
            }
            progress += 1;
        }
        for control in [true, false] {
            for _ in 0..self.config.max_incoming_per_turn {
                let mut next = None;
                for _ in 0..self.lanes.len() {
                    let lane = self.lane_cursor;
                    self.lane_cursor = (self.lane_cursor + 1) % self.lanes.len();
                    if let Some(m) = self.lanes[lane].as_mut().and_then(|c| {
                        if control {
                            c.control.as_mut()?.next().now_or_never().flatten()
                        } else {
                            c.data.next().now_or_never().flatten()
                        }
                    }) {
                        next = Some((m, lane));
                        break;
                    }
                }
                let Some((m, lane)) = next else {
                    break;
                };
                self.handle_message(m, Some(lane), control)?;
                progress += 1;
            }
        }
        progress += self.output().await?;
        self.apply_failures();
        if progress == 0 && !wait.is_zero() {
            tokio::time::sleep(
                wait.min(self.config.heartbeat_interval)
                    .min(self.config.retry_initial),
            )
            .await;
        }
        Ok(progress)
    }
    /// Explicit local loss injection also used by hosts on their transport boundary.
    pub fn transport_lost(&mut self) {
        if let Some(c) = &self.join {
            c.latch.mark(1);
        }
        self.apply_failures();
    }
    /// Bounded diagnostic publication for real broker/ACL qualification.
    pub async fn inject_subject(
        &self,
        subject: String,
        payload: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        if payload.len() > TRANSPORT_PACKET_BYTES {
            return Err(RuntimeError::Config);
        }
        let c = self.join.as_ref().ok_or(RuntimeError::Transport)?;
        tokio::time::timeout(self.config.io_timeout, async {
            c.client
                .publish(subject, payload.into())
                .await
                .map_err(|_| RuntimeError::Transport)?;
            c.client.flush().await.map_err(|_| RuntimeError::Transport)
        })
        .await
        .map_err(|_| RuntimeError::Timeout)?
    }
    pub fn generation(&self) -> u128 {
        self.epoch
    }
    pub async fn shutdown(&mut self) -> Result<(), RuntimeError> {
        self.lifecycle = Lifecycle::ShuttingDown;
        self.manager.transport_lost();
        self.active = 0;
        for p in self.peers.values_mut() {
            if let Some(s) = &mut p.session {
                s.retiring = true;
                s.retired_at = Some(Instant::now());
            }
        }
        for c in self.lanes.iter_mut().chain(std::iter::once(&mut self.join)) {
            if let Some(c) = c.take() {
                let _ = tokio::time::timeout(self.config.io_timeout, c.client.drain()).await;
            }
        }
        self.lifecycle = Lifecycle::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_is_exact_bounded_and_versioned() {
        // Reduce before usize conversion: non-power-of-two profiles must route
        // high PeerIds identically on 32-bit and 64-bit hosts.
        assert_eq!(shard(PeerId(u64::MAX), 7), 1);
        assert_eq!(shard(PeerId(u64::MAX), 32), 31);
        let c = Control {
            kind: 1,
            sender: 2,
            recipient: 3,
            hello: 4,
            token: 5,
            watermark: 6,
        };
        let mut b = c.encode();
        assert_eq!(b.len(), CONTROL_BYTES);
        assert_eq!(Control::decode(&b).unwrap().watermark, 6);
        b.push(0);
        assert!(Control::decode(&b).is_none());
        b.pop();
        b[4] = 7;
        assert!(Control::decode(&b).is_none());
        b[4] = 1;
        b[0] = 0;
        assert!(Control::decode(&b).is_none());
    }
    #[test]
    fn cancelled_batch_fails_every_touched_shard_and_overflow_cannot_hide_loss() {
        let a = Arc::new(Latch::default());
        let b = Arc::new(Latch::default());
        drop(BatchGuard {
            latches: vec![a.clone(), b.clone()],
            complete: false,
        });
        assert_eq!(a.get(), 4);
        assert_eq!(b.get(), 4);
        let latch = Latch::default();
        latch.mark(2);
        latch.mark(1);
        assert_eq!(latch.get(), 3);
        assert!(
            latch
                .0
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        );
        assert_eq!(latch.get(), 3);
    }
}
