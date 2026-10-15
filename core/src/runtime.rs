//! Authenticated dynamic NATS sessions with bounded transport shards.
//!
//! Broker permissions MUST bind sender, recipient and sender shard. A subject is
//! not an identity proof without that provisioning contract. See the runtime guide.
mod api;
mod closing;
mod continuous;
mod delivery;
mod diagnostics;
mod protocol;
mod termination;
pub use api::{
    Authentication, Counters, DeliveryStatus, Lifecycle, Membership, PeerStatus, RuntimeConfig,
    RuntimeError, RuntimeEvent, RuntimeKey, Status, StreamDiagnostics, Trust, verified_tls_config,
};
pub use diagnostics::TurnDiagnostics;

use crate::credit::Amount;
use crate::{
    CloseReason, Manager, ManagerConfig, PeerId, PeerLimits, Resources, SendOutcome, Snapshot,
    StreamKey, wire,
};
use futures_util::{FutureExt, Stream, StreamExt};
use protocol::{CONTROL_BYTES, Control, DATA_HEADER, DataHeader};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

pub const RUNTIME_FRAME_BYTES: usize = 16 * 1024;
pub const TRANSPORT_PACKET_BYTES: usize = DATA_HEADER + 16 + 12 + RUNTIME_FRAME_BYTES;
// Broker payload also serves bounded provisioning/control outside Core.
pub const BROKER_PAYLOAD_BYTES: usize = 65588;
fn elapsed_us(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX)
}
fn shard(id: PeerId, count: usize) -> usize {
    (id.0 % count as u64) as usize
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
struct Incoming {
    message: async_nats::Message,
    lane: Option<usize>,
    control: bool,
}
enum MessageReceiver {
    Subscription(async_nats::Subscriber),
    Shared(mpsc::Receiver<async_nats::Message>),
}
impl Stream for MessageReceiver {
    type Item = async_nats::Message;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::Subscription(subscriber) => Pin::new(subscriber).poll_next(cx),
            Self::Shared(receiver) => receiver.poll_recv(cx),
        }
    }
}
struct Connection {
    client: async_nats::Client,
    data: MessageReceiver,
    control: Option<MessageReceiver>,
    data_sender: Option<mpsc::Sender<async_nats::Message>>,
    control_sender: Option<mpsc::Sender<async_nats::Message>>,
    sources: BTreeMap<PeerId, [async_nats::SubscriptionHandle; 2]>,
    removing: Vec<[async_nats::SubscriptionHandle; 2]>,
    retired: BTreeMap<PeerId, RetiredSource>,
    processed_data: u64,
    latch: Arc<Latch>,
    dirty: bool,
}
struct RetiredSource {
    outstanding: Amount,
    barrier: Option<u64>,
}
impl MessageReceiver {
    fn queued(&self) -> usize {
        match self {
            Self::Shared(receiver) => receiver.len(),
            Self::Subscription(_) => 0,
        }
    }
}
fn retire_source_handles<T>(sources: &mut BTreeMap<PeerId, T>, removing: &mut Vec<T>, id: PeerId) {
    if let Some(handles) = sources.remove(&id) {
        removing.push(handles);
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
const BROKER_PENDING_BYTES: u64 = 4 * 1024 * 1024;
const DATA_DELIVERY_BYTES: u64 = 3 * 1024 * 1024;
const MSG_FRAMING_BYTES: usize = 1024;
const SOURCE_CONTROL_BYTES: u64 = 12 * (CONTROL_BYTES + MSG_FRAMING_BYTES) as u64;
const ENVELOPE_WEIGHT: u64 = (TRANSPORT_PACKET_BYTES + MSG_FRAMING_BYTES) as u64;

fn lane_credit_limit(sources: usize) -> Result<u64, RuntimeError> {
    BROKER_PENDING_BYTES
        .checked_sub(
            (sources as u64)
                .checked_mul(SOURCE_CONTROL_BYTES)
                .ok_or(RuntimeError::Config)?,
        )
        .and_then(|n| n.checked_sub(ENVELOPE_WEIGHT))
        .map(|n| n.min(DATA_DELIVERY_BYTES))
        .ok_or(RuntimeError::Admission)
}

#[derive(Default)]
struct DeliveryFlight {
    sent: u64,
    acknowledged: u64,
    sequence: u64,
}
impl DeliveryFlight {
    fn acknowledge(&mut self, sequence: u64, bytes: u64) -> Result<(), RuntimeError> {
        if sequence < self.sequence || bytes < self.acknowledged || bytes > self.sent {
            return Err(RuntimeError::Protocol);
        }
        self.sequence = sequence;
        self.acknowledged = bytes;
        Ok(())
    }
}
struct Session {
    generation: u128,
    token: u128,
    hello: u128,
    incarnation: u64,
    tx: u64,
    rx: u64,
    next_ping: Instant,
    can_ping: bool,
    proof_deadline: Instant,
    ping: Option<(u128, u64, Instant, u64)>,
    delivery: DeliveryFlight,
    sender_credit: delivery::Sender,
    recipient_credit: delivery::Recipient,
    control: delivery::ControlWindow,
    pending_control: Option<Control>,
    closing: Option<closing::Closing>,
    advertised: delivery::Limit,
    target: Amount,
    demand: u64,
    announced_demand: u64,
    remote_demand: u64,
    growth_applied: u64,
    growth_records: u64,
    last_data: Instant,
    fence_sent: bool,
    frozen_sent: bool,
    fence_deadline: Option<Instant>,
    pong: Option<(u128, u64)>,
    retiring: bool,
    proven: bool,
    retired_at: Option<Instant>,
}
impl Session {
    fn acknowledge_receipt(
        &mut self,
        control: &Control,
        epoch: u128,
    ) -> Result<bool, RuntimeError> {
        if control.kind != 6
            || control.sender != self.generation
            || control.recipient != epoch
            || control.token != self.token
        {
            return Ok(false);
        }
        if self.ping.is_some_and(|(nonce, watermark, _, _)| {
            nonce == control.hello && watermark == control.watermark
        }) {
            let (_, watermark, _, bytes) = self.ping.take().unwrap();
            self.delivery.acknowledge(watermark, bytes)?;
            return Ok(true);
        }
        Ok(false)
    }
}
struct Peer {
    hello_attempts: usize,
    join_failures: usize,
    session: Option<Session>,
    incoming: Option<Flight>,
    outgoing: Option<Flight>,
    candidate: Option<Candidate>,
}
impl Peer {
    fn new() -> Self {
        Self {
            hello_attempts: 0,
            join_failures: 0,
            session: None,
            incoming: None,
            outgoing: None,
            candidate: None,
        }
    }
}
fn peer_batch(
    peers: &BTreeMap<PeerId, Peer>,
    cursor: &mut Option<PeerId>,
    maximum: usize,
) -> Vec<PeerId> {
    let mut ids = Vec::with_capacity(maximum.min(peers.len()));
    if let Some(last) = *cursor {
        ids.extend(
            peers
                .range((std::ops::Bound::Excluded(last), std::ops::Bound::Unbounded))
                .take(maximum)
                .map(|(id, _)| *id),
        );
        ids.extend(
            peers
                .range(..=last)
                .take(maximum - ids.len())
                .map(|(id, _)| *id),
        );
    } else {
        ids.extend(peers.keys().take(maximum).copied());
    }
    *cursor = ids.last().copied();
    ids
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
    expected_peers: BTreeMap<PeerId, u128>,
    cursor: Option<PeerId>,
    credit_cursor: Option<PeerId>,
    credit_visit_cursor: Option<PeerId>,
    lane_cursor: usize,
    lifecycle: Lifecycle,
    last_error: Option<RuntimeError>,
    counters: Counters,
    diagnostics: TurnDiagnostics,
    active: usize,
    attempt: usize,
    retry_at: Instant,
    drain_started: Option<Instant>,
    transport_bound: usize,
    retired_send: BTreeMap<PeerId, Amount>,
    #[cfg(feature = "fault-injection")]
    drop_next_data: bool,
    #[cfg(feature = "fault-injection")]
    pause_next_data: Option<tokio::sync::oneshot::Sender<()>>,
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
impl NatsRuntime {
    /// Disabled by default. A new collection resets owner-local counters.
    pub fn set_diagnostics_enabled(&mut self, enabled: bool) {
        self.diagnostics.set_enabled(enabled);
    }
    pub fn diagnostics(&self) -> TurnDiagnostics {
        self.diagnostics
    }

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
            expected_peers: BTreeMap::new(),
            cursor: None,
            credit_cursor: None,
            credit_visit_cursor: None,
            lane_cursor: 0,
            lifecycle: Lifecycle::Connecting,
            last_error: None,
            counters: Counters::default(),
            diagnostics: TurnDiagnostics::default(),
            active: 0,
            attempt: 0,
            retry_at: now,
            drain_started: None,
            transport_bound,
            retired_send: BTreeMap::new(),
            #[cfg(feature = "fault-injection")]
            drop_next_data: false,
            #[cfg(feature = "fault-injection")]
            pause_next_data: None,
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
        .connection_timeout(self.config.io_timeout)
        .max_reconnects(1)
        .ignore_discovered_servers()
        .client_capacity(self.config.client_capacity)
        .subscription_backpressure_timeout(self.config.io_timeout)
        .raw_message_limit(TRANSPORT_PACKET_BYTES)
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
        if let Some(name) = self.config.tls_server_name.as_deref() {
            options =
                options.tls_client_config(crate::runtime_tls::config(&self.config.trust, name)?);
        } else if let Trust::ManagedCa(ca) = &self.config.trust {
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
        if client.max_payload() != BROKER_PAYLOAD_BYTES {
            return Err(RuntimeError::Config);
        }
        let (data, control, data_sender, control_sender) = if lane.is_some() {
            let (data_sender, data) = mpsc::channel(self.config.subscription_capacity);
            let (control_sender, control) = mpsc::channel(self.config.subscription_capacity);
            (
                MessageReceiver::Shared(data),
                Some(MessageReceiver::Shared(control)),
                Some(data_sender),
                Some(control_sender),
            )
        } else {
            let subject = format!("{}.join.{}.*", self.config.namespace, self.config.id.0);
            let data = tokio::time::timeout(self.config.io_timeout, client.subscribe(subject))
                .await
                .map_err(|_| RuntimeError::Timeout)?
                .map_err(|_| RuntimeError::Transport)?;
            (MessageReceiver::Subscription(data), None, None, None)
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
            data_sender,
            control_sender,
            sources: BTreeMap::new(),
            removing: Vec::new(),
            retired: BTreeMap::new(),
            processed_data: 0,
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
    async fn subscribe_source(&mut self, lane: usize, id: PeerId) -> Result<(), RuntimeError> {
        // Complete retired UNSUB commands before registering the same source
        // again or charging capacity for a newly admitted peer.
        self.unsubscribe_sources().await?;
        let source_count = self
            .lanes
            .iter()
            .flatten()
            .map(|connection| connection.sources.len() + connection.retired.len())
            .sum::<usize>();
        let outstanding = self.domain_obligation()?.bytes;
        let send_flight = self.send_flight()?.bytes;
        let connection = self.lanes[lane].as_mut().ok_or(RuntimeError::Transport)?;
        if connection.sources.contains_key(&id) {
            return Ok(());
        }
        if connection.retired.contains_key(&id)
            || source_count >= self.limits.max_peers
            || outstanding > lane_credit_limit(source_count + 1)?
            || send_flight > lane_credit_limit(source_count + 1)?
        {
            return Err(RuntimeError::Admission);
        }
        let mut guard = OutputGuard {
            latch: connection.latch.clone(),
            complete: false,
        };
        let base = format!(
            "{}.lane.{}.{:032x}.{lane}",
            self.config.namespace, self.config.id.0, self.epoch
        );
        let client = connection.client.clone();
        let data_sender = connection.data_sender.as_ref().unwrap().clone();
        let control_sender = connection.control_sender.as_ref().unwrap().clone();
        let data = self
            .with_lane_progress(
                client.subscribe_into(format!("{base}.data.{}.*", id.0), data_sender),
            )
            .await?;
        let control = self
            .with_lane_progress(
                client.subscribe_into(format!("{base}.control.{}.*", id.0), control_sender),
            )
            .await?;
        self.with_lane_progress(client.broker_barrier()).await?;
        let connection = self.lanes[lane].as_mut().ok_or(RuntimeError::Transport)?;
        if connection.latch.get() != 0 {
            return Err(if connection.latch.get() & 8 != 0 {
                RuntimeError::Authorization
            } else {
                RuntimeError::Transport
            });
        }
        let handles = [data, control];
        connection.sources.insert(id, handles);
        guard.complete = true;
        Ok(())
    }
    async fn unsubscribe_sources(&mut self) -> Result<(), RuntimeError> {
        for lane in 0..self.lanes.len() {
            let Some(connection) = self.lanes[lane].as_ref() else {
                continue;
            };
            if connection.removing.is_empty() {
                continue;
            }
            let mut guard = OutputGuard {
                latch: connection.latch.clone(),
                complete: false,
            };
            let client = connection.client.clone();
            while let Some(handles) = self.lanes[lane].as_mut().unwrap().removing.pop() {
                for mut handle in handles {
                    self.with_lane_progress(async move { handle.unsubscribe().await })
                        .await?;
                }
            }
            self.with_lane_progress(client.broker_barrier()).await?;
            let connection = self.lanes[lane].as_mut().ok_or(RuntimeError::Transport)?;
            if connection.latch.get() != 0 {
                return Err(RuntimeError::Transport);
            }
            let barrier = connection
                .processed_data
                .checked_add(connection.data.queued() as u64)
                .ok_or(RuntimeError::IdentityExhausted)?;
            for source in connection.retired.values_mut() {
                if source.barrier.is_none() {
                    source.barrier = Some(barrier);
                }
            }
            connection
                .retired
                .retain(|_, source| source.barrier.is_none_or(|n| n > connection.processed_data));
            guard.complete = true;
        }
        Ok(())
    }
    #[cfg(feature = "fault-injection")]
    fn lane_outstanding(&self, lane: usize) -> Result<u64, RuntimeError> {
        Ok(self.lane_obligation(lane)?.bytes)
    }
    fn ensure_peer(&mut self, id: PeerId) -> Result<(), RuntimeError> {
        if !self.allowed(id) {
            return Err(RuntimeError::Authorization);
        }
        if !self.peers.contains_key(&id) {
            let retired: std::collections::BTreeSet<_> = self
                .lanes
                .iter()
                .flatten()
                .flat_map(|c| c.retired.keys())
                .chain(self.retired_send.keys())
                .filter(|key| !self.peers.contains_key(key) && **key != id)
                .copied()
                .collect();
            if self.peers.len() + retired.len() >= self.limits.max_peers {
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
        self.peers.get_mut(&id).unwrap().hello_attempts = 0;
        self.peers.get_mut(&id).unwrap().outgoing = Some(Flight {
            message: Control {
                kind: 1,
                sender: self.epoch,
                recipient: 0,
                hello: random()?,
                token: 0,
                watermark: 0,
                ..Control::default()
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
        self.expected_peers.remove(&id);
        self.terminate_peer(id);
        Ok(())
    }
    /// This runtime's incarnation, for a host's current admission lease.
    pub fn epoch(&self) -> u128 {
        self.epoch
    }
    /// Admit one peer, optionally requiring its exact current incarnation.
    /// Changing an active incarnation requires explicit revocation first.
    pub fn authorize_peer(
        &mut self,
        id: PeerId,
        expected_epoch: Option<u128>,
    ) -> Result<(), RuntimeError> {
        if id == self.config.id || expected_epoch == Some(0) {
            return Err(RuntimeError::Config);
        }
        if self
            .peers
            .get(&id)
            .and_then(|p| p.session.as_ref())
            .is_some_and(|s| {
                !s.retiring && expected_epoch.is_some_and(|epoch| epoch != s.generation)
            })
        {
            return Err(RuntimeError::Authorization);
        }
        let Membership::Allowlist(ids) = &mut self.config.membership else {
            return Err(RuntimeError::Config);
        };
        if !ids.contains(&id) && ids.len() >= self.limits.max_peers {
            return Err(RuntimeError::Admission);
        }
        ids.insert(id);
        if let Some(epoch) = expected_epoch {
            self.expected_peers.insert(id, epoch);
        } else {
            self.expected_peers.remove(&id);
        }
        Ok(())
    }
    fn retire_source(&mut self, id: PeerId) {
        let outstanding = self
            .peers
            .get_mut(&id)
            .and_then(|p| p.session.as_mut())
            .map_or(Amount::default(), |s| {
                let outstanding = s.recipient_credit.obligation();
                let flight = s.sender_credit.outstanding();
                if flight != Amount::default() {
                    let old = self.retired_send.entry(id).or_default();
                    *old = old.add(flight).expect("bounded send quarantine");
                }
                s.sender_credit = delivery::Sender::default();
                s.recipient_credit = delivery::Recipient::default();
                outstanding
            });
        for connection in self.lanes.iter_mut().flatten() {
            if connection.sources.contains_key(&id) {
                connection.retired.insert(
                    id,
                    RetiredSource {
                        outstanding,
                        barrier: None,
                    },
                );
            }
            retire_source_handles(&mut connection.sources, &mut connection.removing, id);
        }
    }
    pub fn terminate_peer(&mut self, id: PeerId) {
        self.start_close(id);
        if let Some(p) = self.peers.get_mut(&id) {
            p.incoming = None;
            p.outgoing = None;
            p.candidate = None;
        }
        self.config.initiate.retain(|p| *p != id);
        if self
            .peers
            .get(&id)
            .and_then(|p| p.session.as_ref())
            .is_none_or(|s| {
                s.closing.is_none() && s.sender_credit.outstanding() == Amount::default()
            })
        {
            self.retire_source(id);
        }
    }
    fn retire(&mut self, id: PeerId) {
        if let Some(p) = self.peers.get_mut(&id)
            && let Some(s) = &mut p.session
            && !s.retiring
        {
            s.retiring = true;
            s.retired_at = Some(Instant::now());
            s.ping = None;
            s.next_ping = Instant::now();
            if s.proven {
                self.active -= 1;
            }
            self.manager.peer_lost(id);
        }
    }
    fn sync_manager_failures(&mut self) {
        while let Some(peer) = self.manager.poll_failed_peer() {
            self.retire(peer);
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
            active_peers: self.active.saturating_sub(
                self.peers
                    .iter()
                    .filter(|(peer, state)| {
                        state
                            .session
                            .as_ref()
                            .is_some_and(|session| !session.retiring && session.proven)
                            && self.manager.peer_failed(**peer)
                    })
                    .count(),
            ),
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
        let needed = self
            .manager
            .next_transport_weight(id, DATA_HEADER + MSG_FRAMING_BYTES);
        Some(PeerStatus {
            generation: s.generation,
            pair_token: s.token,
            incarnation: s.incarnation,
            sent_frames: s.tx,
            received_frames: s.rx,
            handshake_nonce: s.hello,
            delivery_epoch: s.sender_credit.epoch,
            granted_bytes: s.sender_credit.allowed.bytes,
            sent_bytes: s.sender_credit.sent.bytes,
            applied_bytes: s.sender_credit.applied.bytes,
            available_records: s.sender_credit.room().records,
            sent_controls: s.control.sent,
            received_controls: s.control.received,
            waiting_delivery: needed
                .is_some_and(|bytes| !s.sender_credit.room().contains(Amount::data(bytes))),
            waiting_control: s.control.deadline.is_some(),
            ready: !s.retiring && s.proven && !self.manager.peer_failed(id),
        })
    }
    pub fn peer_ready(&self, id: PeerId) -> bool {
        self.lifecycle == Lifecycle::Ready
            && !self.manager.peer_failed(id)
            && self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_some_and(|s| !s.retiring && s.proven)
    }
    /// True after this peer's stream ownership and terminal records are released.
    /// Hosts must also await a complete successful turn before revoking transport.
    pub fn peer_streams_drained(&self, id: PeerId) -> bool {
        self.manager.peer_streams(id).is_none_or(|count| count == 0)
            && self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_none_or(|s| !s.retiring || s.sender_credit.outstanding() == Amount::default())
            && self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_none_or(|s| s.closing.is_none_or(|c| c.complete()))
            && !self.retired_send.contains_key(&id)
            && !self.lanes.iter().flatten().any(|c| {
                c.retired.contains_key(&id)
                    || (c.sources.contains_key(&id)
                        && self
                            .peers
                            .get(&id)
                            .and_then(|p| p.session.as_ref())
                            .is_some_and(|s| s.retiring))
            })
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
        self.sync_manager_failures();
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
    /// On-demand layered wait observation. Socket/batch wall time is reported
    /// separately by Counters and opt-in TurnDiagnostics, without a stream scan.
    pub fn stream_diagnostics(&self, key: RuntimeKey) -> Option<StreamDiagnostics> {
        self.check_key(key).ok()?;
        let application = self
            .manager
            .flow_observation(key.stream, self.now().ok()?)?;
        let s = self.peers.get(&key.stream.peer)?.session.as_ref()?;
        let needed = self
            .manager
            .stream_transport_weight(key.stream, DATA_HEADER + MSG_FRAMING_BYTES);
        let waiting_delivery =
            needed.is_some_and(|bytes| !s.sender_credit.room().contains(Amount::data(bytes)));
        let free = self
            .domain_limit()
            .ok()?
            .sub(self.send_flight().ok()?)
            .ok()?;
        let waiting_shared_flight = needed.is_some_and(|bytes| !free.contains(Amount::data(bytes)));
        Some(StreamDiagnostics {
            scheduler_eligible: application.queued_for_output
                && needed.is_some()
                && s.proven
                && !s.retiring
                && !waiting_delivery
                && !waiting_shared_flight,
            application,
            waiting_delivery,
            waiting_shared_flight,
            waiting_control_ms: s.control.deadline.map(|deadline| {
                u64::try_from(
                    Instant::now()
                        .saturating_duration_since(deadline - self.config.peer_timeout)
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            }),
        })
    }
    /// Local configured limits; this does not change runtime or stream state.
    pub fn limits(&self) -> ManagerConfig {
        self.limits
    }
    /// Validated remote stream limits, available only for this live generation.
    pub fn peer_limits(&self, key: RuntimeKey) -> Option<PeerLimits> {
        self.check_key(key).ok()?;
        self.manager.peer_limits(key.stream)
    }
    pub fn poll_events(&mut self, max: usize) -> Vec<RuntimeEvent> {
        self.poll_events_with_data_budget(max, |_, _| true)
    }
    /// Preserve Core ownership of DATA denied by the current generation's host.
    pub fn poll_events_with_data_budget(
        &mut self,
        max: usize,
        mut admit: impl FnMut(RuntimeKey, usize) -> bool,
    ) -> Vec<RuntimeEvent> {
        self.apply_failures();
        let epoch = self.epoch;
        let peers = &self.peers;
        self.manager
            .poll_events_with_data_budget(max, |stream, bytes| {
                peers
                    .get(&stream.peer)
                    .and_then(|peer| peer.session.as_ref())
                    .is_some_and(|session| {
                        admit(
                            RuntimeKey {
                                epoch,
                                incarnation: session.incarnation,
                                stream,
                            },
                            bytes,
                        )
                    })
            })
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
                eprintln!(
                    "Transport shard failure: lane={} reason={}",
                    lane,
                    self.lanes[lane].as_ref().unwrap().latch.get()
                );
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
        let mut message = message;
        if established {
            let session = self
                .peers
                .get_mut(&id)
                .and_then(|p| p.session.as_mut())
                .ok_or(RuntimeError::PeerUnavailable)?;
            message.ack = session.control.received;
            if matches!(message.kind, 5 | 6) && !session.proven {
                // Readiness carries a backed bootstrap. Opening a short stream
                // after readiness never needs an extra delivery-credit RTT.
                message.body_len = 16;
                let promise = session.recipient_credit.obligation();
                message.body[..8].copy_from_slice(&promise.bytes.min(4096).to_be_bytes());
                message.body[8..16].copy_from_slice(&promise.records.min(2).to_be_bytes());
            }
            session.control.mark_receipt_sent();
        }
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
        if c.sender == 0
            || c.hello == 0
            || !self.allowed(id)
            || self
                .expected_peers
                .get(&id)
                .is_some_and(|epoch| *epoch != c.sender)
        {
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
                            ..Control::default()
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
                        ..Control::default()
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
                        ..Control::default()
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
                            ..Control::default()
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
        if !self.allowed(id)
            || self
                .expected_peers
                .get(&id)
                .is_some_and(|epoch| *epoch != candidate.generation)
        {
            self.peers.get_mut(&id).unwrap().candidate = None;
            return Ok(());
        }
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
            if self.peers[&id].session.is_some() {
                self.retire_source(id);
            }
            let lane = shard(id, self.config.shards);
            if self.lanes[lane].is_none() {
                self.lanes[lane] = Some(self.connection(Some(lane)).await?);
            }
            match self.subscribe_source(lane, id).await {
                Ok(()) => {}
                Err(RuntimeError::Admission) => return Ok(()),
                Err(error) => return Err(error),
            }
            self.incarnation = self
                .incarnation
                .checked_add(1)
                .ok_or(RuntimeError::IdentityExhausted)?;
            self.manager.register_peer(id, self.config.id > id)?;
            self.manager.set_peer_output_enabled(id, false)?;
            self.peers.get_mut(&id).unwrap().session = Some(Session {
                generation: candidate.generation,
                token: candidate.token,
                hello: candidate.hello,
                incarnation: self.incarnation,
                tx: 0,
                rx: 0,
                next_ping: Instant::now(),
                can_ping: !candidate.responder,
                proof_deadline: Instant::now() + self.config.peer_timeout,
                ping: None,
                delivery: DeliveryFlight::default(),
                sender_credit: delivery::Sender::default(),
                recipient_credit: delivery::Recipient::default(),
                control: delivery::ControlWindow::default(),
                pending_control: None,
                closing: None,
                advertised: delivery::Limit::default(),
                target: Amount {
                    bytes: 4096,
                    records: 2,
                },
                demand: 0,
                announced_demand: 0,
                remote_demand: 0,
                growth_applied: 0,
                growth_records: 0,
                last_data: Instant::now(),
                fence_sent: false,
                frozen_sent: false,
                fence_deadline: None,
                pong: None,
                retiring: false,
                proven: false,
                retired_at: None,
            });
            let free = self.domain_limit()?.sub(self.domain_obligation()?)?;
            let bootstrap = Amount {
                bytes: free.bytes.min(4096),
                records: free.records.min(2),
            };
            self.peers
                .get_mut(&id)
                .unwrap()
                .session
                .as_mut()
                .unwrap()
                .recipient_credit
                .extend(bootstrap)?;
            self.counters.joins += 1;
            self.attempt = 0;
        }
        let p = self.peers.get_mut(&id).unwrap();
        p.candidate = None;
        p.outgoing = None;
        p.incoming = None;
        p.join_failures = 0;
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
                    ..Control::default()
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
        let closing_source = lane.is_some()
            && self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_some_and(|s| s.closing.is_some());
        if !(self.allowed(id) || closing_source) {
            return None;
        }
        if let Some(lane) = lane {
            if shard(id, self.config.shards) != lane {
                return None;
            }
            let s = self.peers.get(&id)?.session.as_ref()?;
            if (s.retiring && !control && s.closing.is_none())
                || generation != format!("{:032x}", s.generation)
            {
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
        if let Some(lane) = lane
            && !control
        {
            let connection = self.lanes[lane].as_mut().ok_or(RuntimeError::Transport)?;
            connection.processed_data = connection
                .processed_data
                .checked_add(1)
                .ok_or(RuntimeError::IdentityExhausted)?;
            connection
                .retired
                .retain(|_, source| source.barrier.is_none_or(|n| n > connection.processed_data));
        }
        let Some(id) = self.sender(message.subject.as_str(), lane, control) else {
            self.counters.invalid_input += 1;
            return Ok(());
        };
        if lane.is_none() {
            if let Ok(c) = Control::decode(&message.payload) {
                return self.handle_join(id, c);
            }
            if message.payload.starts_with(b"SKC") && message.payload.get(..4) != Some(b"SKC3") {
                self.last_error = Some(RuntimeError::UnsupportedVersion);
                if self.config.initiate.contains(&id) {
                    self.manager.transport_lost();
                    self.active = 0;
                    self.lifecycle = Lifecycle::Failed;
                }
            }
            self.counters.invalid_input += 1;
            return Ok(());
        }
        if control {
            let Ok(c) = Control::decode(&message.payload) else {
                self.counters.invalid_input += 1;
                return Ok(());
            };
            let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
            if c.sender != s.generation || c.recipient != self.epoch || c.token != s.token {
                return Ok(());
            }
            if let Err(error) = s.control.acknowledge(c.ack) {
                self.retire(id);
                self.last_error = Some(error);
                return Ok(());
            }
            if (12..=14).contains(&c.kind) {
                let _ = s;
                if let Err(error) = self.receive_close(id, c) {
                    self.retire(id);
                    self.last_error = Some(error);
                    self.retired_send.entry(id).or_default();
                }
                return Ok(());
            }
            if s.retiring && c.kind != 6 {
                return Ok(());
            }
            if matches!(c.kind, 5 | 6)
                && c.body_len == 16
                && s.sender_credit.allowed == Amount::default()
                && s.sender_credit.epoch == 0
            {
                let allowed = Amount {
                    bytes: u64::from_be_bytes(c.body[..8].try_into().unwrap()),
                    records: u64::from_be_bytes(c.body[8..16].try_into().unwrap()),
                };
                if allowed.bytes > 4096 || allowed.records > 2 {
                    return Err(RuntimeError::Protocol);
                }
                s.sender_credit.grant(delivery::Limit {
                    allowed,
                    ..delivery::Limit::default()
                })?;
            }
            match c.kind {
                5 if c.hello != 0 => {
                    s.pong = Some((c.hello, c.watermark));
                    s.can_ping = true;
                }
                6 => {
                    #[cfg(feature = "fault-injection")]
                    if self.ignore_next_pong {
                        self.ignore_next_pong = false;
                        return Ok(());
                    }
                    if s.acknowledge_receipt(&c, self.epoch)? {
                        s.sender_credit.apply_receipt(Amount {
                            bytes: s.delivery.acknowledged,
                            records: s.delivery.sequence,
                        })?;
                        if !s.proven && !s.retiring {
                            s.proven = true;
                            self.active += 1;
                            self.manager.set_peer_output_enabled(id, true)?;
                            self.retired_send.remove(&id);
                        }
                        s.next_ping = if s.delivery.sent - s.delivery.acknowledged
                            >= DATA_DELIVERY_BYTES / 2
                        {
                            Instant::now()
                        } else {
                            Instant::now() + self.config.heartbeat_interval
                        };
                    }
                }
                7..=12 => {
                    // Control can overtake DATA. Apply prefix-dependent commands
                    // only after the matching ordered publication prefix.
                    let _ = s;
                    if let Err(error) = self.apply_control(id, c) {
                        self.retire(id);
                        self.last_error = Some(error);
                    }
                }
                _ => {}
            }
            return Ok(());
        }
        let (header, body) = match DataHeader::decode(&message.payload) {
            Ok(value) => value,
            Err(error) => {
                self.retire(id);
                self.last_error = Some(error);
                return Ok(());
            }
        };
        let seq = header.sequence;
        let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
        if header.token != s.token {
            return Ok(());
        }
        let weight = Amount::data(message.payload.len() + MSG_FRAMING_BYTES);
        if header.epoch != s.recipient_credit.epoch
            || !s.recipient_credit.obligation().contains(weight)
            || s.control.acknowledge(header.control_ack).is_err()
        {
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        if s.rx.checked_add(1) != Some(seq) {
            eprintln!(
                "Peer envelope sequence failure: time_ms={} id={} peer={} received={} expected={:?}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
                self.config.id.0,
                id.0,
                seq,
                s.rx.checked_add(1)
            );
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        let packet = match wire::decode(body) {
            Ok(p) => p,
            Err(_) => {
                self.retire(id);
                self.last_error = Some(RuntimeError::Protocol);
                return Ok(());
            }
        };
        let closing = self.peers[&id].session.as_ref().unwrap().closing.is_some();
        if !closing
            && let Err(error) = self.manager.receive(
                StreamKey {
                    peer: id,
                    stream_id: packet.stream_id,
                },
                &packet.frame,
                self.now()?,
            )
        {
            eprintln!(
                "Peer stream receive failure: peer={} stream={} sequence={} error={error:?}",
                id.0, packet.stream_id, seq
            );
            self.retire(id);
            self.last_error = Some(RuntimeError::Protocol);
            return Ok(());
        }
        let session = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
        session.recipient_credit.apply(weight)?;
        if matches!(
            packet.frame,
            crate::Frame::Open { .. } | crate::Frame::Accept { .. } | crate::Frame::Data { .. }
        ) {
            session.remote_demand = session.remote_demand.max(256 * 1024);
            session.target.bytes = session.target.bytes.max(256 * 1024);
            session.target.records = session.target.records.max(32);
        }
        session.rx = seq;
        session.last_data = Instant::now();
        if !closing
            && let Some(command) = session.pending_control.take()
            && let Err(error) = self.apply_control(id, command)
        {
            self.retire(id);
            self.last_error = Some(error);
        }
        self.sync_manager_failures();
        Ok(())
    }
    async fn output(&mut self) -> Result<usize, RuntimeError> {
        let started = Instant::now();
        let mut count = 0;
        let mut guard = BatchGuard {
            latches: Vec::new(),
            complete: false,
        };
        for (&id, peer) in &mut self.peers {
            if let Some(session) = &mut peer.session
                && session.proven
                && !session.retiring
            {
                let demand = self.manager.transport_demand(
                    id,
                    DATA_HEADER + MSG_FRAMING_BYTES,
                    DATA_DELIVERY_BYTES as usize,
                );
                if demand != 0 {
                    session.demand = session.demand.max(demand as u64).max(256 * 1024);
                }
            }
        }
        let mut output_bytes = 0usize;
        for _ in 0..self.config.max_outgoing_per_turn.min(32) {
            if output_bytes >= 256 * 1024
                || (count != 0 && started.elapsed() >= Duration::from_millis(1))
            {
                break;
            }
            let free = self.domain_limit()?.sub(self.send_flight()?)?;
            let peers = &mut self.peers;
            let frame = self
                .manager
                .poll_frames_with_budget(1, |key, size, _| {
                    peers
                        .get(&key.peer)
                        .and_then(|p| p.session.as_ref())
                        .is_some_and(|s| {
                            let weight = Amount::data(size + DATA_HEADER + MSG_FRAMING_BYTES);
                            !s.retiring
                                && free.contains(weight)
                                && s.sender_credit.room().contains(weight)
                        })
                })
                .pop();
            let Some(frame) = frame else {
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
            if matches!(frame.frame, crate::Frame::Data { .. })
                && let Some(extracted) = self.pause_next_data.take()
            {
                let _ = extracted.send(());
                std::future::pending::<()>().await;
            }
            #[cfg(feature = "fault-injection")]
            if self.drop_next_data && matches!(frame.frame, crate::Frame::Data { .. }) {
                self.drop_next_data = false;
                count += 1;
                continue;
            }
            let size = wire::encoded_size(frame.key.stream_id, &frame.frame)
                .map_err(|_| RuntimeError::Protocol)?;
            output_bytes += size;
            let weight = (size + DATA_HEADER + MSG_FRAMING_BYTES) as u64;
            let amount = Amount {
                bytes: weight,
                records: 1,
            };
            s.sender_credit.reserve(amount)?;
            s.sender_credit.commit(amount)?;
            s.delivery.sent = s
                .delivery
                .sent
                .checked_add(weight)
                .ok_or(RuntimeError::IdentityExhausted)?;
            if s.delivery.sent - s.delivery.acknowledged >= DATA_DELIVERY_BYTES / 2 {
                s.next_ping = Instant::now();
            }
            let mut bytes = Vec::with_capacity(DATA_HEADER + size);
            DataHeader {
                token: s.token,
                sequence: s.tx,
                control_ack: s.control.received,
                epoch: s.sender_credit.epoch,
            }
            .encode(size, &mut bytes)?;
            s.control.mark_receipt_sent();
            wire::encode_into(&mut bytes, frame.key.stream_id, &frame.frame)
                .map_err(|_| RuntimeError::Protocol)?;
            let subject = format!(
                "{}.lane.{}.{:032x}.{}.data.{}.{:032x}",
                self.config.namespace,
                id.0,
                s.generation,
                shard(self.config.id, self.config.shards),
                self.config.id.0,
                self.epoch
            );
            let client = c.client.clone();
            self.with_lane_progress(client.publish(subject, bytes.into()))
                .await?;
            self.counters.published(&frame.frame);
            self.lanes[lane].as_mut().unwrap().dirty = true;
            count += 1;
        }
        // Every extracted frame stays guarded until all touched socket flushes
        // complete; cancellation conservatively fails every touched shard.
        count += self.credit_work().await?;
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
        for lane in 0..=self.lanes.len() {
            let connection = if lane == self.lanes.len() {
                &self.join
            } else {
                &self.lanes[lane]
            };
            if let Some(c) = connection.as_ref().filter(|c| c.dirty) {
                let client = c.client.clone();
                self.with_lane_progress(client.flush()).await?;
                let connection = if lane == self.lanes.len() {
                    &mut self.join
                } else {
                    &mut self.lanes[lane]
                };
                connection.as_mut().unwrap().dirty = false;
                self.counters.socket_flushes_completed =
                    self.counters.socket_flushes_completed.saturating_add(1);
            }
        }
        guard.complete = true;
        self.counters.output_elapsed_ns = self
            .counters
            .output_elapsed_ns
            .saturating_add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        Ok(count)
    }
    // Socket output can await channel capacity or a flush while the broker is
    // delivering the opposite direction. Drain directly into the authenticated
    // Manager; no additional raw-message queue or detached output task is used.
    // The absolute I/O timeout remains live even under continuous incoming work.
    async fn with_lane_progress<F, E, T>(&mut self, operation: F) -> Result<T, RuntimeError>
    where
        F: Future<Output = Result<T, E>>,
    {
        // The ordinary turn already drains a bounded incoming batch. Poll the
        // operation first; only backpressured publication/flush needs extra
        // receive work to unblock the shared NATS connection.
        let pending = tokio::time::timeout(self.config.io_timeout, operation);
        tokio::pin!(pending);
        loop {
            tokio::select! {
                biased;
                result = &mut pending => return result
                    .map_err(|_| RuntimeError::Timeout)?
                    .map_err(|_| RuntimeError::Transport),
                next = std::future::poll_fn(|cx| self.poll_lane_incoming(cx)) => {
                    if let Some(next) = next {
                        self.handle_message(next.message, next.lane, next.control)?;
                    } else {
                        return Err(RuntimeError::Transport);
                    }
                }
            }
        }
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inspect_recipient_credit(&self, lane: usize) -> (u64, u64, usize) {
        let sources = self
            .peers
            .iter()
            .filter(|(id, peer)| {
                shard(**id, self.config.shards) == lane
                    && peer
                        .session
                        .as_ref()
                        .is_some_and(|s| s.recipient_credit.obligation().bytes > 0)
            })
            .count();
        (
            self.lane_outstanding(lane).unwrap(),
            self.domain_limit().unwrap().bytes,
            sources,
        )
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inspect_source_retirement(&self, lane: usize, id: PeerId) -> (usize, u64, bool) {
        let connection = self.lanes[lane].as_ref().unwrap();
        let retired = connection.retired.get(&id);
        let outstanding = retired.map_or_else(
            || {
                self.peers
                    .get(&id)
                    .and_then(|p| p.session.as_ref())
                    .map_or(0, |s| s.recipient_credit.obligation().bytes)
            },
            |s| s.outstanding.bytes,
        );
        (connection.data.queued(), outstanding, retired.is_some())
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_drop_next_data_after_extraction(&mut self) {
        self.drop_next_data = true;
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_pause_next_data_after_extraction(
        &mut self,
    ) -> tokio::sync::oneshot::Receiver<()> {
        let (extracted, observed) = tokio::sync::oneshot::channel();
        self.pause_next_data = Some(extracted);
        observed
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_ignore_next_lane_pong(&mut self) {
        self.ignore_next_pong = true;
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inspect_pending_lane_proof(&self, id: PeerId) -> Option<(u128, u64)> {
        let session = self.peers.get(&id)?.session.as_ref()?;
        if session.proven || session.retiring {
            return None;
        }
        session
            .ping
            .map(|(nonce, watermark, _, _)| (nonce, watermark))
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inspect_lane_pong_injection_pending(&self) -> bool {
        self.ignore_next_pong
    }
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub fn inject_expired_credit_freeze(&mut self, peer: PeerId) {
        self.manager.expire_credit_freeze(peer);
    }
    fn rotating_peers(&mut self) -> Vec<PeerId> {
        peer_batch(
            &self.peers,
            &mut self.cursor,
            self.config.max_outgoing_per_turn,
        )
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
            count += self.close_work(id).await?;
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
                            p.join_failures = p.join_failures.saturating_add(1);
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
                        let due = if !incoming && f.message.kind == 1 {
                            p.hello_attempts += 1;
                            if p.hello_attempts < 3 {
                                now + self.config.retry_initial.max(Duration::from_millis(250))
                            } else {
                                f.expires
                            }
                        } else {
                            f.expires
                        };
                        let pending = if incoming {
                            p.incoming.as_mut()
                        } else {
                            p.outgoing.as_mut()
                        };
                        if let Some(pending) = pending {
                            pending.due = due;
                        }
                        count += 1;
                    }
                }
            }
            let timeout = self.peers[&id].session.as_ref().is_some_and(|s| {
                !s.retiring
                    && ((!s.proven && now >= s.proof_deadline)
                        || s.ping.is_some_and(|(_, _, deadline, _)| deadline <= now))
            });
            if timeout {
                let session = self.peers[&id].session.as_ref().unwrap();
                eprintln!(
                    "Peer watermark timeout: peer={} tx={} rx={} ping={:?} pong={:?}",
                    id.0,
                    session.tx,
                    session.rx,
                    session.ping.map(|(_, watermark, _, _)| watermark),
                    session.pong.map(|(_, watermark)| watermark)
                );
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
                        ..Control::default()
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
            let ping = self.peers[&id].session.as_ref().is_some_and(|s| {
                // A failed lane cannot prove the old prefix. Keep its debt
                // quarantined and let retirement reach a fresh pair instead.
                self.lanes[shard(id, self.config.shards)].is_some()
                    && s.closing.is_none()
                    && s.can_ping
                    && s.ping.is_none()
                    && s.next_ping <= now
                    && (!s.retiring
                        || (s.proven && s.sender_credit.outstanding() != Amount::default()))
            });
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
                        ..Control::default()
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
                    .ping = Some((
                    nonce,
                    watermark,
                    now + self.config.peer_timeout,
                    self.peers[&id].session.as_ref().unwrap().delivery.sent,
                ));
                count += 1;
            }
            let remove = self.peers[&id].session.as_ref().is_none_or(|s| {
                s.retiring
                    && s.closing.is_none_or(|c| c.complete())
                    && (s.sender_credit.outstanding() == Amount::default()
                        || s.retired_at
                            .is_some_and(|t| t.elapsed() >= self.config.terminal_drain_timeout))
            }) && self.manager.peer_streams(id).is_none_or(|n| n == 0)
                && self.peers[&id].incoming.is_none()
                && self.peers[&id].outgoing.is_none()
                && self.peers[&id].candidate.is_none();
            if remove {
                if self.manager.peer_streams(id).is_some() {
                    self.manager.remove_peer(id)?;
                }
                self.retire_source(id);
                let failures = self.peers[&id].join_failures;
                self.peers.remove(&id);
                if self.config.initiate.contains(&id) {
                    if failures < self.config.max_retries {
                        self.join_peer(id)?;
                        self.peers.get_mut(&id).unwrap().join_failures = failures;
                    } else {
                        self.last_error = Some(RuntimeError::RetryExhausted);
                        if self.active == 0 && self.config.initiate.len() == 1 {
                            self.lifecycle = Lifecycle::Failed;
                        }
                    }
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
        let ids: Vec<_> = self.peers.keys().copied().collect();
        for id in ids {
            self.retire_source(id);
        }
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
    // Pending polls register the owner waker without extracting a message. A
    // Ready message is handed immediately to the normal authenticated decoder.
    fn poll_incoming(&mut self, cx: &mut Context<'_>) -> Poll<Option<Incoming>> {
        if let Some(join) = &mut self.join {
            match Pin::new(&mut join.data).poll_next(cx) {
                Poll::Ready(Some(message)) => {
                    return Poll::Ready(Some(Incoming {
                        message,
                        lane: None,
                        control: true,
                    }));
                }
                Poll::Ready(None) => {
                    join.latch.mark(1);
                    return Poll::Ready(None);
                }
                Poll::Pending => {}
            }
        }
        self.poll_lane_incoming(cx)
    }
    fn poll_lane_incoming(&mut self, cx: &mut Context<'_>) -> Poll<Option<Incoming>> {
        for control in [true, false] {
            for _ in 0..self.lanes.len() {
                let lane = self.lane_cursor;
                self.lane_cursor = (self.lane_cursor + 1) % self.lanes.len();
                let Some(connection) = &mut self.lanes[lane] else {
                    continue;
                };
                let subscriber = if control {
                    let Some(subscriber) = &mut connection.control else {
                        continue;
                    };
                    subscriber
                } else {
                    &mut connection.data
                };
                match Pin::new(subscriber).poll_next(cx) {
                    Poll::Ready(Some(message)) => {
                        return Poll::Ready(Some(Incoming {
                            message,
                            lane: Some(lane),
                            control,
                        }));
                    }
                    Poll::Ready(None) => {
                        connection.latch.mark(1);
                        return Poll::Ready(None);
                    }
                    Poll::Pending => {}
                }
            }
        }
        Poll::Pending
    }
    /// Bounded message work. Host polling cadence is part of the liveness profile.
    pub async fn turn(&mut self, wait: Duration) -> Result<usize, RuntimeError> {
        self.turn_with_wake(wait, std::future::pending()).await
    }
    /// Run a complete bounded turn, allowing host readiness to end idle waiting.
    /// The wake future is polled only after output guards have completed and no
    /// work was done. An incoming NATS message takes priority over a host wake.
    /// A host wake does not count as transport work or consume any message.
    /// Hosts must clear their readiness after WouldBlock to avoid repeated wakes,
    /// and must still await the complete turn rather than cancel active output.
    pub async fn turn_with_wake<F>(
        &mut self,
        wait: Duration,
        wake: F,
    ) -> Result<usize, RuntimeError>
    where
        F: Future<Output = ()>,
    {
        // One absolute work bound includes registration, publication and flush.
        // A host deadline must wake idle waiting, never cancel an extracted
        // output batch. If the complete turn expires, fence the whole transport
        // before exposing its terminal events or returning to the host.
        let profile_turn = self.diagnostics.enabled.then(Instant::now);
        let mut progress =
            match tokio::time::timeout(self.config.io_timeout, self.active_turn()).await {
                Ok(result) => result?,
                Err(_) => {
                    self.transport_lost();
                    self.last_error = Some(RuntimeError::Timeout);
                    return Err(RuntimeError::Timeout);
                }
            };
        let mut wake = std::pin::pin!(wake);
        let measured = self.diagnostics.enabled;
        if progress == 0 && !wait.is_zero() && self.lifecycle == Lifecycle::Ready {
            let profile_idle = measured.then(Instant::now);
            if measured {
                self.diagnostics.idle_count = self.diagnostics.idle_count.saturating_add(1);
            }
            let control_wait = if self.peers.values().any(|p| {
                p.session
                    .as_ref()
                    .is_some_and(|s| s.control.receipt_dirty())
            }) {
                Duration::from_millis(2)
            } else {
                wait
            };
            let timeout = control_wait
                .min(self.config.heartbeat_interval)
                .min(self.config.retry_initial);
            if let Ok(incoming) = tokio::time::timeout(
                timeout,
                std::future::poll_fn(|cx| match self.poll_incoming(cx) {
                    Poll::Ready(incoming) => Poll::Ready(incoming),
                    Poll::Pending => wake.as_mut().poll(cx).map(|()| None),
                }),
            )
            .await
            {
                if let Some(incoming) = incoming {
                    let id = if incoming.lane.is_none() {
                        self.sender(incoming.message.subject.as_str(), None, true)
                    } else {
                        None
                    };
                    self.handle_message(incoming.message, incoming.lane, incoming.control)?;
                    progress += 1;
                    if let Some(id) = id
                        && self.peers.contains_key(&id)
                    {
                        match tokio::time::timeout(self.config.io_timeout, self.promote(id)).await {
                            Ok(result) => result?,
                            Err(_) => {
                                self.transport_lost();
                                self.last_error = Some(RuntimeError::Timeout);
                                return Err(RuntimeError::Timeout);
                            }
                        }
                    }
                }
                self.apply_failures();
            }
            if let Some(start) = profile_idle {
                self.diagnostics.idle_us =
                    self.diagnostics.idle_us.saturating_add(elapsed_us(start));
            }
        }
        if let Some(start) = profile_turn {
            self.diagnostics.turn_us = self.diagnostics.turn_us.saturating_add(elapsed_us(start));
        }
        Ok(progress)
    }
    async fn active_turn(&mut self) -> Result<usize, RuntimeError> {
        self.apply_failures();
        if self.lifecycle == Lifecycle::Recovering {
            self.recover().await?;
            return Ok(0);
        }
        if self.lifecycle != Lifecycle::Ready {
            return Ok(0);
        }
        self.unsubscribe_sources().await?;
        let measured = self.diagnostics.enabled;
        if measured {
            self.diagnostics.turns = self.diagnostics.turns.saturating_add(1);
        }
        self.manager.tick(self.now()?)?;
        self.sync_manager_failures();
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
        let profile_output = measured.then(Instant::now);
        progress += self.output().await?;
        if let Some(start) = profile_output {
            self.diagnostics.output_us =
                self.diagnostics.output_us.saturating_add(elapsed_us(start));
        }
        self.apply_failures();
        if measured {
            self.diagnostics.progress = self.diagnostics.progress.saturating_add(progress as u64);
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
    #[cfg(feature = "fault-injection")]
    #[doc(hidden)]
    pub async fn inject_subject_with_headers(
        &self,
        subject: String,
        payload: Vec<u8>,
    ) -> Result<(), RuntimeError> {
        let c = self.join.as_ref().ok_or(RuntimeError::Transport)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Test", "unsupported");
        tokio::time::timeout(self.config.io_timeout, async {
            c.client
                .publish_with_headers(subject, headers, payload.into())
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
    fn terminal_source_retirement_preserves_bound_and_allows_reuse_after_cleanup() {
        let mut sources = BTreeMap::from([(PeerId(1), [10, 11]), (PeerId(2), [20, 21])]);
        let mut removing = Vec::new();
        retire_source_handles(&mut sources, &mut removing, PeerId(1));
        retire_source_handles(&mut sources, &mut removing, PeerId(1));
        assert_eq!(sources.len() + removing.len(), 2);
        assert_eq!(sources.get(&PeerId(2)), Some(&[20, 21]));
        assert!(!sources.contains_key(&PeerId(1)));
        assert_eq!(removing.pop(), Some([10, 11]));
        assert!(removing.is_empty());
        // Completed unsubscribe frees one row for a distinct new peer. A
        // same-ID incarnation replacement retains its existing source row.
        sources.insert(PeerId(3), [30, 31]);
        assert_eq!(sources.len() + removing.len(), 2);
        assert_eq!(sources.get(&PeerId(2)), Some(&[20, 21]));
        retire_source_handles(&mut sources, &mut removing, PeerId(3));
        assert_eq!(removing.pop(), Some([30, 31]));
        sources.insert(PeerId(3), [32, 33]);
        assert_eq!(sources.get(&PeerId(3)), Some(&[32, 33]));
        assert_eq!(sources.len() + removing.len(), 2);
    }
    #[test]
    fn delivery_flight_acknowledges_only_authenticated_snapshot_prefix() {
        let now = Instant::now();
        let mut session = Session {
            generation: 10,
            token: 20,
            hello: 30,
            incarnation: 1,
            tx: 9,
            rx: 0,
            next_ping: now,
            can_ping: true,
            proof_deadline: now,
            ping: Some((40, 7, now, 100)),
            delivery: DeliveryFlight {
                sent: 400,
                acknowledged: 0,
                sequence: 0,
            },
            sender_credit: delivery::Sender::default(),
            recipient_credit: delivery::Recipient::default(),
            control: delivery::ControlWindow::default(),
            pending_control: None,
            closing: None,
            advertised: delivery::Limit::default(),
            target: Amount {
                bytes: 4096,
                records: 2,
            },
            demand: 0,
            announced_demand: 0,
            remote_demand: 0,
            growth_applied: 0,
            growth_records: 0,
            last_data: now,
            fence_sent: false,
            frozen_sent: false,
            fence_deadline: None,
            pong: None,
            retiring: false,
            proven: true,
            retired_at: None,
        };
        let receipt = Control {
            kind: 6,
            sender: 10,
            recipient: 50,
            hello: 40,
            token: 20,
            watermark: 7,
            ..Control::default()
        };
        for invalid in [
            Control {
                sender: 11,
                ..receipt
            },
            Control {
                recipient: 51,
                ..receipt
            },
            Control {
                token: 21,
                ..receipt
            },
            Control {
                hello: 41,
                ..receipt
            },
            Control {
                watermark: 8,
                ..receipt
            },
        ] {
            assert!(!session.acknowledge_receipt(&invalid, 50).unwrap());
            assert_eq!(session.delivery.acknowledged, 0);
            assert!(session.ping.is_some());
        }
        assert!(session.acknowledge_receipt(&receipt, 50).unwrap());
        assert_eq!(session.delivery.sent - session.delivery.acknowledged, 300);
        assert_eq!(session.delivery.sequence, 7);
        assert!(!session.acknowledge_receipt(&receipt, 50).unwrap());
        assert_eq!(session.delivery.acknowledged, 100);
        session.ping = Some((60, 9, now, 400));
        assert!(
            session
                .acknowledge_receipt(
                    &Control {
                        hello: 60,
                        watermark: 9,
                        ..receipt
                    },
                    50
                )
                .unwrap()
        );
        assert_eq!(session.delivery.sent - session.delivery.acknowledged, 0);
        assert!(matches!(
            session.delivery.acknowledge(8, 400),
            Err(RuntimeError::Protocol)
        ));
    }
    #[test]
    fn recipient_flight_counts_all_sources_and_preserves_the_current_profile() {
        let limit = lane_credit_limit(128).unwrap();
        assert!(limit < DATA_DELIVERY_BYTES);
        assert!(limit + 128 * SOURCE_CONTROL_BYTES + ENVELOPE_WEIGHT <= BROKER_PENDING_BYTES);
        let mut first = delivery::Recipient::default();
        let mut second = delivery::Recipient::default();
        first
            .extend(Amount {
                bytes: limit - 1024,
                records: 100,
            })
            .unwrap();
        second
            .extend(Amount {
                bytes: 1024,
                records: 1,
            })
            .unwrap();
        assert_eq!(first.obligation().bytes + second.obligation().bytes, limit);
        first.freeze().unwrap();
        first
            .observe_frozen(
                1,
                Amount {
                    bytes: 512,
                    records: 1,
                },
            )
            .unwrap();
        assert!(first.reclaim().unwrap().is_none());
        first
            .apply(Amount {
                bytes: 512,
                records: 1,
            })
            .unwrap();
        first.reclaim().unwrap().unwrap();
        assert_eq!(second.obligation().bytes, 1024);
        assert!(lane_credit_limit(usize::MAX).is_err());
        assert!(std::mem::size_of::<Peer>() + std::mem::size_of::<RetiredSource>() <= 2048);
    }
    #[test]
    fn control_and_credit_visitation_reach_every_peer_with_independent_cursors() {
        let peers: BTreeMap<_, _> = (0..128).map(|id| (PeerId(id), Peer::new())).collect();
        let (mut control, mut credit) = (Some(PeerId(7)), Some(PeerId(71)));
        let (mut controls, mut credits) = (BTreeMap::new(), BTreeMap::new());
        for _ in 0..8 {
            for id in peer_batch(&peers, &mut control, 32) {
                *controls.entry(id).or_insert(0) += 1;
            }
            for id in peer_batch(&peers, &mut credit, 32) {
                *credits.entry(id).or_insert(0) += 1;
            }
        }
        assert_eq!(controls.len(), 128);
        assert_eq!(credits.len(), 128);
        assert!(controls.values().all(|n| *n == 2));
        assert!(credits.values().all(|n| *n == 2));
    }
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
            ..Control::default()
        };
        let mut b = c.encode();
        assert_eq!(b.len(), CONTROL_BYTES);
        assert_eq!(Control::decode(&b).unwrap().watermark, 6);
        b.push(0);
        assert!(Control::decode(&b).is_err());
        b.pop();
        b[4] = 15;
        assert!(Control::decode(&b).is_err());
        b[4] = 1;
        b[..4].copy_from_slice(b"SKC1");
        assert!(Control::decode(&b).is_err());
        b[..4].copy_from_slice(b"SKC2");
        b[0] = 0;
        assert!(Control::decode(&b).is_err());
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
