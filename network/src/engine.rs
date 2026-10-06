use crate::*;
use skvoz_core::runtime::{NatsRuntime, RuntimeKey, Status};
use skvoz_core::{Event, ManagerConfig, PeerId, PeerLimits, SendOutcome};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
pub enum EngineRole {
    Client,
    Server {
        grants: BTreeMap<PeerId, SessionConfig>,
    },
}
#[derive(Clone, Copy, Debug)]
pub struct EngineConfig {
    pub ip_sessions: usize,
    pub packet_queue_bytes: usize,
    pub packet_queue_records: usize,
    pub control_queue_bytes: usize,
    pub control_queue_records: usize,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            ip_sessions: 128,
            packet_queue_bytes: 262144,
            packet_queue_records: 256,
            control_queue_bytes: 32768,
            control_queue_records: 8,
        }
    }
}
impl EngineConfig {
    fn validate(self) -> Result<(), NetworkError> {
        if !(1..=128).contains(&self.ip_sessions)
            || !(1508..=262144).contains(&self.packet_queue_bytes)
            || !(1..=256).contains(&self.packet_queue_records)
            || !(16392..=32768).contains(&self.control_queue_bytes)
            || !(1..=8).contains(&self.control_queue_records)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
    /// Canonical network receive/send profile; the same Core serves all profiles.
    pub fn core_limits(self, server: bool) -> ManagerConfig {
        crate::config::Limits::canonical(if server {
            crate::config::Role::Server
        } else {
            crate::config::Role::Client
        })
        .manager(if server {
            crate::config::Role::Server
        } else {
            crate::config::Role::Client
        })
    }
}

fn validate_core_profile(limits: ManagerConfig, server: bool) -> Result<(), NetworkError> {
    let canonical = EngineConfig::default().core_limits(server);
    // Frame/window are a progress contract, not merely resource upper bounds.
    // An IP session needs at least its control stream and one packet stream.
    let session_receive = 2 * canonical.stream.receive_window as usize;
    if limits.stream != canonical.stream
        || !(1..=canonical.max_peers).contains(&limits.max_peers)
        || !(2..=canonical.max_streams).contains(&limits.max_streams)
        || !(2..=canonical.max_streams_per_peer).contains(&limits.max_streams_per_peer)
        || !(session_receive..=canonical.receive_budget).contains(&limits.receive_budget)
        || !(session_receive..=canonical.receive_budget_per_peer)
            .contains(&limits.receive_budget_per_peer)
        // Smaller shared SEND pools can split records into tiny accepted
        // prefixes under contention and exhaust the envelope flight bound.
        || limits.send_budget != canonical.send_budget
        || limits.send_budget_per_peer != canonical.send_budget_per_peer
    {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(())
}

fn compatible_peer_profile(limits: Option<PeerLimits>) -> bool {
    limits.is_some_and(|limits| limits.receive_window == 65536 && limits.max_frame == 16384)
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn fixed_bookkeeping_backs_backend_and_pending_cancellation_keys() {
        // Both arrays have finite capacities, without tree nodes or a copy.
        let bytes = 512 * (std::mem::size_of::<BackendEvent>() + 512)
            + 2048 * std::mem::size_of::<RuntimeKey>();
        assert!(bytes <= 524288, "bookkeeping requires {bytes} bytes");
    }

    #[test]
    fn canonical_send_pool_and_stream_profile_with_finite_role_bounds() {
        let canonical = EngineConfig::default().core_limits(false);
        assert!(validate_core_profile(canonical, false).is_ok());
        let mut smaller = canonical;
        smaller.max_streams = 2;
        smaller.max_streams_per_peer = 2;
        smaller.receive_budget = 131072;
        smaller.receive_budget_per_peer = 131072;
        assert!(validate_core_profile(smaller, false).is_ok());
        for (case, mut invalid) in [canonical; 9].into_iter().enumerate() {
            match case {
                0 => invalid.stream.receive_window = 16384,
                1 => invalid.stream.max_frame = 1,
                2 => invalid.max_peers = 2,
                3 => invalid.max_streams = canonical.max_streams + 1,
                4 => invalid.send_budget_per_peer = CONTROL_MAX + 7,
                5 => invalid.receive_budget_per_peer = 65536,
                6 => invalid.stream.max_pending_frames = 129,
                7 => invalid.stream.max_metadata = 513,
                8 => invalid.send_budget = CONTROL_MAX + 8,
                _ => unreachable!(),
            }
            assert_eq!(
                validate_core_profile(invalid, false),
                Err(NetworkError::InvalidConfiguration)
            );
        }
        assert!(!compatible_peer_profile(None));
        assert!(!compatible_peer_profile(Some(PeerLimits {
            receive_window: 65536,
            max_frame: 1,
        })));
        assert!(!compatible_peer_profile(Some(PeerLimits {
            receive_window: 16384,
            max_frame: 16384,
        })));
        assert!(compatible_peer_profile(Some(PeerLimits {
            receive_window: 65536,
            max_frame: 16384,
        })));
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Negotiating,
    Preparing,
    AwaitingActive,
    Active,
}
#[derive(Clone, Debug)]
pub struct SessionStatus {
    pub session: SessionId,
    pub state: SessionState,
    pub peer: PeerId,
    pub config: Option<SessionConfig>,
}
#[derive(Debug)]
pub struct ReceivedPacket {
    pub session: SessionId,
    pub key: RuntimeKey,
    pub end_offset: u64,
    pub packet: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct EngineCounters {
    pub uploaded: u64,
    pub downloaded: u64,
    pub packet_drops: u64,
    pub invalid_packets: u64,
    pub rejected_opens: u64,
    pub closed_sessions: u64,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct EngineResources {
    pub sessions: usize,
    pub streams: usize,
    pub queued_packet_bytes: usize,
    pub queued_packet_records: usize,
    pub received_packet_bytes: usize,
    pub received_packet_records: usize,
    pub control_bytes: usize,
    pub parser_bytes: usize,
}
#[derive(Debug)]
pub enum BackendEvent {
    TcpOpen {
        key: RuntimeKey,
        host: String,
        port: u16,
    },
    Tcp {
        key: RuntimeKey,
        event: Event,
    },
    Reserve {
        key: RuntimeKey,
        session: SessionId,
        families: Vec<u8>,
        mtu: u16,
        channels: u8,
    },
    Activate {
        key: RuntimeKey,
        session: SessionId,
    },
    Retire {
        key: RuntimeKey,
        session: SessionId,
    },
}
struct PendingReservation {
    session: SessionId,
    families: Vec<u8>,
    mtu: u16,
    channels: u8,
    deadline: Instant,
}
struct Session {
    id: SessionId,
    control: RuntimeKey,
    config: Option<SessionConfig>,
    requested: Option<(Vec<u8>, FamilyPolicy, u16, u8)>,
    state: SessionState,
    channels: BTreeMap<u8, (RuntimeKey, bool)>,
    local_ready: bool,
    deadline: Instant,
    seed: u64,
    send_bytes: usize,
    send_records: usize,
    receive_bytes: usize,
    receive_records: usize,
}
struct Pending {
    bytes: Vec<u8>,
    cursor: usize,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReceiptState {
    Queued,
    Delivered,
    Done,
}
struct Receipt {
    _reservation: Option<crate::budget::Reservation>,
    end: u64,
    size: usize,
    state: ReceiptState,
}
struct Channel {
    session: SessionId,
    control: bool,
    parser: RecordParser,
    output: VecDeque<Pending>,
    receipts: VecDeque<Receipt>,
    sent: u64,
    flight: VecDeque<u64>,
}
/// One owner exclusively drives one Core runtime and its incoming dispatcher.
pub struct NetworkEngine {
    pub(crate) runtime: NatsRuntime,
    role: EngineRole,
    config: EngineConfig,
    sessions: BTreeMap<PeerId, Session>,
    streams: BTreeMap<RuntimeKey, Channel>,
    packets: PacketDispatch,
    schedule: VecDeque<RuntimeKey>,
    counters: EngineCounters,
    last_error: Option<NetworkError>,
    consumed: BTreeMap<RuntimeKey, u64>,
    budget: Option<crate::budget::Budget>,
    native_send: BTreeMap<PeerId, (usize, usize)>,
    native_receive: BTreeMap<PeerId, (usize, usize)>,
    native_terminal: BTreeMap<PeerId, (usize, Instant)>,
    ip_turn: BTreeMap<PeerId, (usize, usize)>,
    ip_reservations: BTreeMap<PeerId, crate::budget::Reservation>,
    tcp_enabled: bool,
    tcp: BTreeSet<RuntimeKey>,
    tcp_closing: Vec<RuntimeKey>,
    backend: VecDeque<BackendEvent>,
    dynamic: Option<(Vec<u8>, u16, u8)>,
    reservations: BTreeMap<RuntimeKey, PendingReservation>,
    reserved_config: Option<(RuntimeKey, SessionConfig, Instant)>,
}
impl NetworkEngine {
    pub fn new(
        runtime: NatsRuntime,
        role: EngineRole,
        config: EngineConfig,
    ) -> Result<Self, NetworkError> {
        config.validate()?;
        validate_core_profile(runtime.limits(), matches!(role, EngineRole::Server { .. }))?;
        if let EngineRole::Server { grants } = &role {
            if grants.len() > 4096 {
                return Err(NetworkError::InvalidConfiguration);
            }
            for value in grants.values() {
                value.validate()?;
            }
            let all: Vec<_> = grants
                .iter()
                .flat_map(|(peer, c)| c.source_grants.iter().map(move |g| (peer, g)))
                .collect();
            for (i, (owner, p)) in all.iter().enumerate() {
                for (other, q) in all.iter().skip(i + 1) {
                    if owner != other && (p.contains(q.address) || q.contains(p.address)) {
                        return Err(NetworkError::InvalidConfiguration);
                    }
                }
            }
        }
        Ok(Self {
            runtime,
            role,
            config,
            sessions: BTreeMap::new(),
            streams: BTreeMap::new(),
            packets: PacketDispatch::default(),
            schedule: VecDeque::new(),
            counters: EngineCounters::default(),
            last_error: None,
            consumed: BTreeMap::new(),
            budget: None,
            native_send: BTreeMap::new(),
            native_receive: BTreeMap::new(),
            native_terminal: BTreeMap::new(),
            ip_turn: BTreeMap::new(),
            ip_reservations: BTreeMap::new(),
            tcp_enabled: false,
            tcp: BTreeSet::new(),
            tcp_closing: Vec::with_capacity(2048),
            backend: VecDeque::new(),
            dynamic: None,
            reservations: BTreeMap::new(),
            reserved_config: None,
        })
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn tcp_allowance(&self, peer: PeerId) -> (usize, usize, usize) {
        let (send, receive, records) = self.native_allowance(peer);
        if self.sessions.contains_key(&peer) {
            (
                send.saturating_sub(16384),
                receive.saturating_sub(16384),
                records.saturating_sub(8),
            )
        } else {
            (send, receive, records)
        }
    }
    pub(crate) fn native_allowance(&self, peer: PeerId) -> (usize, usize, usize) {
        let send = self.native_send.get(&peer).copied().unwrap_or((0, 0));
        let receive = self.native_receive.get(&peer).copied().unwrap_or((0, 0));
        let ip = self.ip_turn.get(&peer).copied().unwrap_or((0, 0));
        let bytes = 32768usize.saturating_sub(send.0 + receive.0 + ip.0);
        (
            bytes,
            bytes,
            16usize.saturating_sub(send.1.max(receive.1) + ip.1),
        )
    }
    pub(crate) fn account_native(
        &mut self,
        peer: PeerId,
        send: usize,
        receive: usize,
        records: usize,
    ) {
        let s = self.native_send.entry(peer).or_default();
        s.0 += send;
        s.1 += records;
        let r = self.native_receive.entry(peer).or_default();
        r.0 += receive;
        r.1 += records;
    }
    pub fn set_budget(&mut self, budget: crate::budget::Budget) {
        self.budget = Some(budget);
    }
    fn reserve_ip(&mut self, peer: PeerId, channels: u8) -> Result<(), NetworkError> {
        if self.ip_reservations.contains_key(&peer) {
            return Ok(());
        }
        if let Some(budget) = &self.budget {
            // Reserve all simultaneously live packet/parser/receipt/encoded copies.
            let bytes = 2 * self.config.packet_queue_bytes
                + usize::from(channels) * (65536 + 32768)
                + 2 * self.config.control_queue_bytes
                + 65536;
            let records = 2 * self.config.packet_queue_records + usize::from(channels) * 128 + 32;
            self.ip_reservations
                .insert(peer, budget.reserve(bytes, records)?);
        }
        Ok(())
    }
    fn backend_push(&mut self, event: BackendEvent) -> Result<(), NetworkError> {
        if self.backend.len() >= 512 {
            return Err(NetworkError::Overloaded);
        }
        self.backend.push_back(event);
        Ok(())
    }
    pub fn enable_tcp(&mut self) {
        self.tcp_enabled = true;
    }
    pub fn enable_dynamic_server(
        &mut self,
        families: Vec<u8>,
        mtu: u16,
        channels: u8,
    ) -> Result<(), NetworkError> {
        if !matches!(self.role, EngineRole::Server { .. }) || !self.sessions.is_empty() {
            return Err(NetworkError::InvalidState);
        }
        if !families.is_empty() {
            validate_families(&families)?;
        }
        self.dynamic = Some((families, mtu, channels));
        Ok(())
    }
    pub fn poll_backend(&mut self) -> Option<BackendEvent> {
        self.backend.pop_front()
    }
    pub fn open_tcp(
        &mut self,
        peer: PeerId,
        host: String,
        port: u16,
    ) -> Result<RuntimeKey, NetworkError> {
        if !self.tcp_enabled || !matches!(self.role, EngineRole::Client) {
            return Err(NetworkError::InvalidState);
        }
        let key = self.runtime.open(
            peer,
            &Metadata::Tcp {
                v: crate::NETWORK_VERSION,
                host,
                port,
            }
            .encode()?,
        )?;
        self.tcp.insert(key);
        Ok(key)
    }
    pub fn accept_tcp(&mut self, key: RuntimeKey) -> Result<(), NetworkError> {
        if !self.tcp.contains(&key) || !compatible_peer_profile(self.runtime.peer_limits(key)) {
            return Err(NetworkError::InvalidState);
        }
        self.runtime.accept(
            key,
            &Accept::Tcp {
                v: crate::NETWORK_VERSION,
                status: "connected".into(),
            }
            .encode()?,
        )?;
        Ok(())
    }
    pub fn reject_tcp(&mut self, key: RuntimeKey, error: &str) -> Result<(), NetworkError> {
        self.tcp.remove(&key);
        self.reject(key, "tcp", error)
    }
    pub fn close_tcp(&mut self, key: RuntimeKey) {
        self.tcp.remove(&key);
        // Core retains the finite slot and its metadata until paced cancellation.
        self.tcp_closing.retain(|pending| {
            self.runtime
                .snapshot(*pending)
                .is_some_and(|s| s.state != skvoz_core::State::Closed)
        });
        if self
            .runtime
            .snapshot(key)
            .is_some_and(|s| s.state != skvoz_core::State::Closed)
            && let Err(index) = self.tcp_closing.binary_search(&key)
        {
            self.tcp_closing.insert(index, key);
        }
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn finish_native_tcp(&mut self, key: RuntimeKey) -> Result<bool, NetworkError> {
        if self
            .native_terminal
            .get(&key.stream.peer)
            .map(|entry| entry.0)
            .unwrap_or(0)
            >= 4
        {
            return Ok(false);
        }
        self.runtime.finish(key)?;
        self.native_terminal
            .entry(key.stream.peer)
            .or_insert_with(|| (0, Instant::now()))
            .0 += 1;
        Ok(true)
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn close_native_tcp(&mut self, key: RuntimeKey) -> bool {
        if self
            .runtime
            .snapshot(key)
            .is_some_and(|s| s.state != skvoz_core::State::Closed)
        {
            if self
                .native_terminal
                .get(&key.stream.peer)
                .map(|entry| entry.0)
                .unwrap_or(0)
                >= 4
                || self.native_allowance(key.stream.peer).2 == 0
            {
                return false;
            }
            self.native_terminal
                .entry(key.stream.peer)
                .or_insert_with(|| (0, Instant::now()))
                .0 += 1;
            self.account_native(key.stream.peer, 0, 0, 1);
        }
        self.tcp.remove(&key);
        if let Ok(index) = self.tcp_closing.binary_search(&key) {
            self.tcp_closing.remove(index);
        }
        let _ = self.runtime.close(key);
        true
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn tcp_cancellations_pending(&self) -> bool {
        !self.tcp_closing.is_empty()
    }
    pub fn complete_reservation(
        &mut self,
        key: RuntimeKey,
        mut config: SessionConfig,
    ) -> Result<bool, NetworkError> {
        let Some(p) = self.reservations.remove(&key) else {
            return Ok(false);
        };
        if Instant::now() >= p.deadline || self.runtime.snapshot(key).is_none() {
            self.ip_reservations.remove(&key.stream.peer);
            return Ok(false);
        }
        if config.session != p.session {
            return Err(NetworkError::InvalidConfiguration);
        }
        config.families = p.families.clone();
        config.mtu = p.mtu;
        config.channels = p.channels;
        config.validate()?;
        let metadata = Metadata::IpSession {
            v: crate::NETWORK_VERSION,
            families: p.families,
            family_policy: FamilyPolicy::RequireAll,
            max_mtu: p.mtu,
            channels: p.channels,
        }
        .encode()?;
        self.reserved_config = Some((key, config, p.deadline));
        self.incoming(key, &metadata)?;
        Ok(true)
    }
    pub fn fail_reservation(&mut self, key: RuntimeKey, error: &str) {
        if self.reservations.remove(&key).is_some() {
            self.ip_reservations.remove(&key.stream.peer);
            let _ = self.reject(key, "ip-session", error);
        }
    }
    pub fn complete_activation(
        &mut self,
        key: RuntimeKey,
        id: &SessionId,
    ) -> Result<bool, NetworkError> {
        self.reject_expired_setup(key.stream.peer)?;
        let Some(s) = self.sessions.get_mut(&key.stream.peer) else {
            return Ok(false);
        };
        if s.control != key
            || s.id != *id
            || s.state != SessionState::AwaitingActive
            || !self.runtime.peer_ready(key.stream.peer)
        {
            return Ok(false);
        }
        s.state = SessionState::Active;
        self.queue_control(
            key,
            Control::Active(SessionSignal {
                session: id.clone(),
            }),
        )?;
        Ok(true)
    }
    pub fn core_status(&self) -> Status {
        self.runtime.status()
    }
    pub fn peer_ready(&self, peer: PeerId) -> bool {
        self.runtime.peer_ready(peer)
    }
    pub fn last_error(&self) -> Option<NetworkError> {
        self.last_error.clone()
    }
    pub fn counters(&self) -> EngineCounters {
        self.counters
    }
    pub fn resources(&self) -> EngineResources {
        EngineResources {
            sessions: self.sessions.len(),
            streams: self.streams.len(),
            queued_packet_bytes: self.sessions.values().map(|s| s.send_bytes).sum(),
            queued_packet_records: self.sessions.values().map(|s| s.send_records).sum(),
            received_packet_bytes: self.sessions.values().map(|s| s.receive_bytes).sum(),
            received_packet_records: self.sessions.values().map(|s| s.receive_records).sum(),
            control_bytes: self
                .streams
                .values()
                .filter(|c| c.control)
                .flat_map(|c| &c.output)
                .map(|p| p.bytes.len())
                .sum(),
            parser_bytes: self
                .streams
                .values()
                .map(|c| c.parser.buffered_bytes())
                .sum(),
        }
    }
    pub fn sessions(&self) -> Vec<SessionStatus> {
        self.sessions
            .iter()
            .map(|(peer, s)| SessionStatus {
                session: s.id.clone(),
                state: s.state,
                peer: *peer,
                config: s.config.clone(),
            })
            .collect()
    }
    fn add_stream(
        &mut self,
        key: RuntimeKey,
        session: SessionId,
        control: bool,
        mtu: u16,
    ) -> Result<(), NetworkError> {
        let parser = RecordParser::new(
            control,
            if control {
                CONTROL_MAX
            } else {
                usize::from(mtu)
            },
            65536,
        )?;
        self.streams.insert(
            key,
            Channel {
                session,
                control,
                parser,
                output: VecDeque::new(),
                receipts: VecDeque::new(),
                sent: 0,
                flight: VecDeque::new(),
            },
        );
        self.schedule.push_back(key);
        Ok(())
    }
    pub fn open_ip(
        &mut self,
        peer: PeerId,
        families: Vec<u8>,
        family_policy: FamilyPolicy,
        max_mtu: u16,
        channels: u8,
    ) -> Result<RuntimeKey, NetworkError> {
        if !matches!(self.role, EngineRole::Client)
            || self.sessions.contains_key(&peer)
            || self.sessions.len() >= self.config.ip_sessions
            || !self.sessions.is_empty()
        {
            return Err(NetworkError::InvalidState);
        }
        let m = Metadata::IpSession {
            v: crate::NETWORK_VERSION,
            families: families.clone(),
            family_policy,
            max_mtu,
            channels,
        };
        let encoded = m.encode()?;
        self.reserve_ip(peer, channels)?;
        let key = match self.runtime.open(peer, &encoded) {
            Ok(k) => k,
            Err(e) => {
                self.ip_reservations.remove(&peer);
                return Err(e.into());
            }
        };
        let id = SessionId::random()?;
        let seed = getrandom::u64().map_err(|_| NetworkError::InvalidState)?;
        self.add_stream(key, id.clone(), true, max_mtu)?;
        self.sessions.insert(
            peer,
            Session {
                id,
                control: key,
                config: None,
                requested: Some((families, family_policy, max_mtu, channels)),
                state: SessionState::Negotiating,
                channels: BTreeMap::new(),
                local_ready: false,
                deadline: Instant::now() + Duration::from_secs(15),
                seed,
                send_bytes: 0,
                send_records: 0,
                receive_bytes: 0,
                receive_records: 0,
            },
        );
        self.last_error = None;
        Ok(key)
    }
    fn reject(&mut self, key: RuntimeKey, kind: &str, error: &str) -> Result<(), NetworkError> {
        self.counters.rejected_opens = self.counters.rejected_opens.saturating_add(1);
        let bytes = serde_json::to_vec(&serde_json::json!({"v":3,"type":kind,"error":error}))
            .map_err(|_| NetworkError::InvalidMetadata)?;
        self.runtime.reject(key, &bytes)?;
        Ok(())
    }
    fn incoming(&mut self, key: RuntimeKey, metadata: &[u8]) -> Result<(), NetworkError> {
        let m = match Metadata::decode(metadata) {
            Ok(m) => m,
            Err(e) => {
                return self.reject(
                    key,
                    Metadata::request_type(metadata),
                    match e {
                        NetworkError::UnsupportedVersion => "unsupported_version",
                        NetworkError::UnsupportedType => "unsupported_type",
                        _ => "invalid_request",
                    },
                );
            }
        };
        if (matches!(&m, Metadata::IpSession { .. } | Metadata::IpData { .. }) || self.tcp_enabled)
            && !compatible_peer_profile(self.runtime.peer_limits(key))
        {
            return self.reject(key, Metadata::request_type(metadata), "invalid_request");
        }
        match m {
            Metadata::IpSession {
                families,
                family_policy,
                max_mtu,
                channels,
                ..
            } => {
                if let Some((supported, mtu, k)) = &self.dynamic
                    && self.reserved_config.is_none()
                {
                    let families = match family_policy.negotiate(&families, supported) {
                        Ok(selected) => selected,
                        Err(_) => return self.reject(key, "ip-session", "unsupported_family"),
                    };
                    if self.sessions.contains_key(&key.stream.peer)
                        || self
                            .reservations
                            .keys()
                            .any(|k| k.stream.peer == key.stream.peer)
                        || self.sessions.len() + self.reservations.len() >= self.config.ip_sessions
                    {
                        return self.reject(key, "ip-session", "overloaded");
                    }
                    let session = SessionId::random()?;
                    let mtu = (*mtu).min(max_mtu);
                    let channels = (*k).min(channels);
                    self.reserve_ip(key.stream.peer, channels)?;
                    self.reservations.insert(
                        key,
                        PendingReservation {
                            session: session.clone(),
                            families: families.clone(),
                            mtu,
                            channels,
                            deadline: Instant::now() + Duration::from_secs(15),
                        },
                    );
                    self.backend_push(BackendEvent::Reserve {
                        key,
                        session,
                        families,
                        mtu,
                        channels,
                    })?;
                    return Ok(());
                }
                let EngineRole::Server { grants } = &self.role else {
                    return self.reject(key, "ip-session", "forbidden");
                };
                let reserved = self.reserved_config.take();
                let fixed_deadline = reserved.as_ref().map(|(_, _, deadline)| *deadline);
                let Some(mut config) = reserved
                    .map(|(_, config, _)| config)
                    .or_else(|| grants.get(&key.stream.peer).cloned())
                else {
                    return self.reject(key, "ip-session", "forbidden");
                };
                if self.sessions.contains_key(&key.stream.peer)
                    || self.sessions.len() >= self.config.ip_sessions
                {
                    return self.reject(key, "ip-session", "overloaded");
                }
                config.families = match family_policy.negotiate(&families, &config.families) {
                    Ok(selected) => selected,
                    Err(_) => return self.reject(key, "ip-session", "unsupported_family"),
                };
                config.mtu = config.mtu.min(max_mtu);
                config.channels = config.channels.min(channels);
                config
                    .source_grants
                    .retain(|g| config.families.contains(&g.family()));
                config
                    .routes
                    .retain(|g| config.families.contains(&g.family()));
                config
                    .dns_servers
                    .retain(|g| config.families.contains(&(if g.is_ipv4() { 4 } else { 6 })));
                if !config.families.contains(&4) {
                    config.egress.ipv4 = "none".into();
                }
                if !config.families.contains(&6) {
                    config.egress.ipv6 = "none".into();
                }
                if self.dynamic.is_none() {
                    config.session = SessionId::random()?;
                }
                config.packet_queue_bytes = config
                    .packet_queue_bytes
                    .min(self.config.packet_queue_bytes);
                config.packet_queue_records = config
                    .packet_queue_records
                    .min(self.config.packet_queue_records);
                if config.validate().is_err() {
                    return self.reject(key, "ip-session", "network_unavailable");
                }
                let id = config.session.clone();
                let seed = getrandom::u64().map_err(|_| NetworkError::InvalidState)?;
                self.runtime.accept(
                    key,
                    &Accept::IpSession {
                        v: crate::NETWORK_VERSION,
                        session: id.clone(),
                    }
                    .encode()?,
                )?;
                self.add_stream(key, id.clone(), true, config.mtu)?;
                self.sessions.insert(
                    key.stream.peer,
                    Session {
                        id,
                        control: key,
                        config: Some(config.clone()),
                        requested: None,
                        state: SessionState::Preparing,
                        channels: BTreeMap::new(),
                        local_ready: false,
                        deadline: fixed_deadline.unwrap_or_else(|| {
                            Instant::now() + Duration::from_millis(config.setup_timeout_ms)
                        }),
                        seed,
                        send_bytes: 0,
                        send_records: 0,
                        receive_bytes: 0,
                        receive_records: 0,
                    },
                );
                self.queue_control(key, Control::Config(config))?;
            }
            Metadata::IpData {
                session, channel, ..
            } => {
                if !matches!(self.role, EngineRole::Server { .. }) {
                    return self.reject(key, "ip-data", "forbidden");
                }
                let Some(s) = self.sessions.get(&key.stream.peer) else {
                    return self.reject(key, "ip-data", "forbidden");
                };
                let c = s.config.as_ref().ok_or(NetworkError::InvalidState)?;
                if !matches!(self.role, EngineRole::Server { .. })
                    || s.id != session
                    || s.control.epoch != key.epoch
                    || s.control.incarnation != key.incarnation
                    || s.state != SessionState::Preparing
                    || channel >= c.channels
                    || s.channels.contains_key(&channel)
                {
                    return self.reject(key, "ip-data", "forbidden");
                }
                let mtu = c.mtu;
                self.runtime.accept(
                    key,
                    &Accept::IpData {
                        v: crate::NETWORK_VERSION,
                        session: session.clone(),
                        channel,
                    }
                    .encode()?,
                )?;
                self.add_stream(key, session, false, mtu)?;
                self.sessions
                    .get_mut(&key.stream.peer)
                    .unwrap()
                    .channels
                    .insert(channel, (key, true));
            }
            Metadata::Tcp { host, port, .. } => {
                if self.tcp_enabled && matches!(self.role, EngineRole::Server { .. }) {
                    self.tcp.insert(key);
                    self.backend_push(BackendEvent::TcpOpen { key, host, port })?;
                } else {
                    self.reject(key, "tcp", "network_unavailable")?;
                }
            }
        }
        Ok(())
    }
    fn queue_control(&mut self, key: RuntimeKey, c: Control) -> Result<(), NetworkError> {
        let bytes = c.encode()?;
        let stream = self
            .streams
            .get_mut(&key)
            .ok_or(NetworkError::InvalidState)?;
        if stream.output.len() >= self.config.control_queue_records
            || stream.output.iter().map(|p| p.bytes.len()).sum::<usize>() + bytes.len()
                > self.config.control_queue_bytes
        {
            return Err(NetworkError::Overloaded);
        }
        stream.output.push_back(Pending { bytes, cursor: 0 });
        Ok(())
    }
    fn opened(&mut self, key: RuntimeKey, metadata: &[u8]) -> Result<(), NetworkError> {
        self.reject_expired_setup(key.stream.peer)?;
        if !compatible_peer_profile(self.runtime.peer_limits(key)) {
            return Err(NetworkError::InvalidConfiguration);
        }
        let accepted = Accept::decode(metadata)?;
        let s = self
            .sessions
            .get_mut(&key.stream.peer)
            .ok_or(NetworkError::InvalidState)?;
        if matches!(self.role, EngineRole::Server { .. }) {
            return if server_accept_matches(key, s.control, &s.id, &s.channels, &accepted) {
                Ok(())
            } else {
                Err(NetworkError::InvalidState)
            };
        }
        match accepted {
            Accept::IpSession { session, .. }
                if key == s.control && s.state == SessionState::Negotiating =>
            {
                s.id = session.clone();
                s.state = SessionState::Preparing;
                self.streams.get_mut(&key).unwrap().session = session;
            }
            Accept::IpData {
                session, channel, ..
            } if session == s.id => {
                let Some((data, ready)) = s.channels.get_mut(&channel) else {
                    return Err(NetworkError::InvalidState);
                };
                if *data != key || *ready {
                    return Err(NetworkError::InvalidState);
                }
                *ready = true;
            }
            _ => return Err(NetworkError::InvalidState),
        }
        self.maybe_ready(key.stream.peer)
    }
    pub fn local_ready(&mut self, session: &SessionId) -> Result<(), NetworkError> {
        let peer = self.session_peer(session)?;
        self.reject_expired_setup(peer)?;
        let s = self.sessions.get_mut(&peer).unwrap();
        if !matches!(self.role, EngineRole::Client)
            || s.state != SessionState::Preparing
            || s.config.is_none()
            || s.local_ready
        {
            return Err(NetworkError::InvalidState);
        }
        s.local_ready = true;
        self.maybe_ready(peer)
    }
    fn maybe_ready(&mut self, peer: PeerId) -> Result<(), NetworkError> {
        self.reject_expired_setup(peer)?;
        let s = self
            .sessions
            .get_mut(&peer)
            .ok_or(NetworkError::InvalidState)?;
        if matches!(self.role, EngineRole::Client)
            && s.state == SessionState::Preparing
            && s.local_ready
            && s.config
                .as_ref()
                .is_some_and(|c| s.channels.len() == usize::from(c.channels))
            && s.channels.values().all(|(_, ready)| *ready)
        {
            s.state = SessionState::AwaitingActive;
            let key = s.control;
            let session = s.id.clone();
            self.queue_control(key, Control::Ready(SessionSignal { session }))?;
        }
        Ok(())
    }
    fn control(&mut self, key: RuntimeKey, record: Record) -> Result<(), NetworkError> {
        self.reject_expired_setup(key.stream.peer)?;
        let control = Control::decode(&record)?;
        let s = self
            .sessions
            .get(&key.stream.peer)
            .ok_or(NetworkError::InvalidState)?;
        if key != s.control {
            return Err(NetworkError::InvalidState);
        }
        match control {
            Control::Config(c)
                if matches!(self.role, EngineRole::Client)
                    && s.state == SessionState::Preparing
                    && s.config.is_none() =>
            {
                let (mut families, family_policy, mtu, k) =
                    s.requested.clone().ok_or(NetworkError::InvalidState)?;
                families.sort_unstable();
                if c.session != s.id
                    || !family_policy.accepts(&families, &c.families)
                    || c.mtu > mtu
                    || c.channels > k
                    || c.packet_queue_bytes > self.config.packet_queue_bytes
                    || c.packet_queue_records > self.config.packet_queue_records
                {
                    return Err(NetworkError::InvalidConfiguration);
                }
                let peer = key.stream.peer;
                let state = self.sessions.get_mut(&peer).unwrap();
                state.config = Some(c.clone());
                state.deadline = state
                    .deadline
                    .min(Instant::now() + Duration::from_millis(c.setup_timeout_ms));
                for channel in 0..c.channels {
                    let m = Metadata::IpData {
                        v: crate::NETWORK_VERSION,
                        session: c.session.clone(),
                        channel,
                    }
                    .encode()?;
                    let data = self.runtime.open(peer, &m)?;
                    self.add_stream(data, c.session.clone(), false, c.mtu)?;
                    self.sessions
                        .get_mut(&peer)
                        .unwrap()
                        .channels
                        .insert(channel, (data, false));
                }
            }
            Control::Ready(signal)
                if matches!(self.role, EngineRole::Server { .. })
                    && signal.session == s.id
                    && s.state == SessionState::Preparing
                    && s.config
                        .as_ref()
                        .is_some_and(|c| s.channels.len() == usize::from(c.channels)) =>
            {
                if self.dynamic.is_some() {
                    self.sessions.get_mut(&key.stream.peer).unwrap().state =
                        SessionState::AwaitingActive;
                    self.backend_push(BackendEvent::Activate {
                        key,
                        session: signal.session,
                    })?;
                } else {
                    self.sessions.get_mut(&key.stream.peer).unwrap().state = SessionState::Active;
                    self.queue_control(key, Control::Active(signal))?;
                }
            }
            Control::Active(signal)
                if matches!(self.role, EngineRole::Client)
                    && signal.session == s.id
                    && s.state == SessionState::AwaitingActive =>
            {
                self.sessions.get_mut(&key.stream.peer).unwrap().state = SessionState::Active;
            }
            Control::Close(signal) | Control::Error(signal) if signal.session == s.id => {
                self.close_peer(key.stream.peer);
                return Ok(());
            }
            _ => return Err(NetworkError::InvalidState),
        }
        self.runtime.consume_through(key, record.end_offset)?;
        Ok(())
    }
    fn data(&mut self, key: RuntimeKey, offset: u64, bytes: &[u8]) -> Result<(), NetworkError> {
        self.streams
            .get_mut(&key)
            .ok_or(NetworkError::InvalidState)?
            .parser
            .push(offset, bytes)
    }
    fn drain_records(
        &mut self,
        key: RuntimeKey,
        budget: &mut usize,
        records: &mut usize,
        max_records: usize,
    ) -> Result<(), NetworkError> {
        loop {
            let Some(size) = self.streams[&key].parser.next_record_bytes()? else {
                break;
            };
            if size > *budget || *records >= max_records {
                break;
            }
            let record = self
                .streams
                .get_mut(&key)
                .unwrap()
                .parser
                .next_record()?
                .unwrap();
            *budget -= size;
            *records += 1;
            if self.streams[&key].control {
                self.control(key, record)?;
                if !self.streams.contains_key(&key) {
                    break;
                }
                continue;
            }
            let s = self
                .sessions
                .get_mut(&key.stream.peer)
                .ok_or(NetworkError::InvalidState)?;
            if s.state != SessionState::Active {
                return Err(NetworkError::InvalidState);
            }
            let c = s.config.as_ref().ok_or(NetworkError::InvalidState)?;
            let info = validate_packet(&record.payload, c.mtu, &c.families);
            if info.is_err() {
                self.counters.invalid_packets = self.counters.invalid_packets.saturating_add(1);
            }
            let size = record.payload.len() + 8;
            let permitted = info.is_ok_and(|info| match self.role {
                EngineRole::Server { .. } => {
                    c.source_grants.iter().any(|g| g.contains(info.source))
                }
                EngineRole::Client => c.source_grants.iter().any(|g| g.contains(info.destination)),
            });
            let admit = permitted
                && s.receive_bytes + size <= c.packet_queue_bytes
                && s.receive_records < c.packet_queue_records;
            let stream = self.streams.get_mut(&key).unwrap();
            if stream.receipts.len() >= 4096 {
                return Err(NetworkError::Overloaded);
            }
            let reservation = self
                .budget
                .as_ref()
                .map(|b| b.reserve(std::mem::size_of::<Receipt>(), 1))
                .transpose()?;
            stream.receipts.push_back(Receipt {
                _reservation: reservation,
                end: record.end_offset,
                size: if admit { size } else { 0 },
                state: if admit {
                    ReceiptState::Queued
                } else {
                    ReceiptState::Done
                },
            });
            if admit {
                s.receive_bytes += size;
                s.receive_records += 1;
                self.packets.push(ReceivedPacket {
                    session: s.id.clone(),
                    key,
                    end_offset: record.end_offset,
                    packet: record.payload,
                });
            } else {
                self.counters.packet_drops = self.counters.packet_drops.saturating_add(1);
                self.drain_consumed(key)?;
            }
        }
        Ok(())
    }
    pub fn poll_packet(&mut self) -> Option<ReceivedPacket> {
        let packet = self.packets.pop()?;
        let stream = self.streams.get_mut(&packet.key)?;
        let receipt = stream
            .receipts
            .iter_mut()
            .find(|r| r.end == packet.end_offset)?;
        receipt.state = ReceiptState::Delivered;
        Some(packet)
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn note_native_packet_write(&mut self, size: usize) {
        self.counters.downloaded = self.counters.downloaded.saturating_add(size as u64);
    }
    pub fn complete_packet(
        &mut self,
        key: RuntimeKey,
        end_offset: u64,
    ) -> Result<(), NetworkError> {
        let stream = self
            .streams
            .get_mut(&key)
            .ok_or(NetworkError::InvalidState)?;
        let size = complete_receipt(&mut stream.receipts, end_offset)?;
        let s = self
            .sessions
            .get_mut(&key.stream.peer)
            .ok_or(NetworkError::InvalidState)?;

        s.receive_bytes -= size;
        s.receive_records -= 1;
        self.drain_consumed(key)
    }
    fn drain_consumed(&mut self, key: RuntimeKey) -> Result<(), NetworkError> {
        let stream = self
            .streams
            .get_mut(&key)
            .ok_or(NetworkError::InvalidState)?;
        if let Some(end) = completed_prefix(&mut stream.receipts) {
            self.consumed.insert(key, end);
        }
        Ok(())
    }
    fn session_peer(&self, id: &SessionId) -> Result<PeerId, NetworkError> {
        self.sessions
            .iter()
            .find_map(|(p, s)| (&s.id == id).then_some(*p))
            .ok_or(NetworkError::InvalidState)
    }
    pub fn enqueue_packet(&mut self, id: &SessionId, packet: &[u8]) -> Result<(), NetworkError> {
        let peer = self.session_peer(id)?;
        let s = self.sessions.get_mut(&peer).unwrap();
        if s.state != SessionState::Active {
            return Err(NetworkError::InvalidState);
        }
        let c = s.config.as_ref().ok_or(NetworkError::InvalidState)?;
        let info = validate_packet(packet, c.mtu, &c.families)?;
        let permitted = match self.role {
            EngineRole::Client => c.source_grants.iter().any(|g| g.contains(info.source)),
            EngineRole::Server { .. } => {
                c.source_grants.iter().any(|g| g.contains(info.destination))
            }
        };
        if !permitted {
            return Err(NetworkError::Forbidden);
        }
        let size = packet.len() + 8;
        if s.send_bytes + size > c.packet_queue_bytes || s.send_records >= c.packet_queue_records {
            self.counters.packet_drops = self.counters.packet_drops.saturating_add(1);
            return Err(NetworkError::Overloaded);
        }
        let channel = packet_channel(info, s.seed, c.channels)?;
        let (key, ready) = s.channels.get(&channel).ok_or(NetworkError::InvalidState)?;
        if !ready {
            return Err(NetworkError::InvalidState);
        }
        let bytes = encode_record(16, packet)?;
        self.streams
            .get_mut(key)
            .ok_or(NetworkError::InvalidState)?
            .output
            .push_back(Pending { bytes, cursor: 0 });
        s.send_bytes += size;
        s.send_records += 1;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), NetworkError> {
        // Byte credit alone permits many tiny frames. Keep a finite envelope
        // flight bound using the same Core consumption prefix, without a new wire.
        for (key, channel) in &mut self.streams {
            if let Some(snapshot) = self.runtime.snapshot(*key) {
                let consumed = channel
                    .sent
                    .saturating_sub(snapshot.send_unacknowledged_bytes);
                retire_flight(&mut channel.flight, consumed);
            }
        }
        let mut flights: BTreeMap<PeerId, usize> = BTreeMap::new();
        for (key, channel) in &self.streams {
            if !channel.control {
                *flights.entry(key.stream.peer).or_default() += channel.flight.len();
            }
        }
        let count = self.schedule.len();
        let mut quanta: BTreeMap<PeerId, (usize, usize)> = self
            .native_send
            .keys()
            .chain(self.native_receive.keys())
            .map(|p| {
                let s = self.native_send.get(p).copied().unwrap_or((0, 0));
                let r = self.native_receive.get(p).copied().unwrap_or((0, 0));
                (*p, (32768usize.saturating_sub(s.0 + r.0), s.1.max(r.1)))
            })
            .collect();
        for _ in 0..count {
            let Some(key) = self.schedule.pop_front() else {
                break;
            };
            if !self.streams.contains_key(&key) {
                continue;
            }
            self.schedule.push_back(key);
            let control = self.streams[&key].control;
            let (mut budget, mut records) = if control {
                (32768, 0)
            } else {
                *quanta.entry(key.stream.peer).or_insert((32768, 0))
            };
            loop {
                let stream = self.streams.get_mut(&key).unwrap();
                if stream.output.is_empty()
                    || budget == 0
                    || records >= 16
                    || (!control && flights.get(&key.stream.peer).copied().unwrap_or(0) >= 16)
                {
                    break;
                }
                let batch = gather_output(&stream.output, budget, 16 - records);
                match self.runtime.send(key, &batch) {
                    Ok(SendOutcome::Accepted(n)) => {
                        if !control {
                            let mut remaining = n;
                            for pending in &stream.output {
                                let accepted = remaining.min(pending.bytes.len() - pending.cursor);
                                let header = 8usize.saturating_sub(pending.cursor);
                                self.counters.uploaded = self
                                    .counters
                                    .uploaded
                                    .saturating_add(accepted.saturating_sub(header) as u64);
                                remaining -= accepted;
                                if remaining == 0 {
                                    break;
                                }
                            }
                        }
                        stream.sent = stream
                            .sent
                            .checked_add(n as u64)
                            .expect("Core accepted offset cannot overflow");
                        if !control {
                            stream.flight.push_back(stream.sent);
                            *flights.entry(key.stream.peer).or_default() += 1;
                        }
                        let (completed_bytes, completed_records) =
                            advance_output(&mut stream.output, n);
                        budget -= n;
                        records += completed_records;
                        if !control {
                            let s = self.sessions.get_mut(&key.stream.peer).unwrap();
                            s.send_bytes -= completed_bytes;
                            s.send_records -= completed_records;
                        }
                    }
                    Ok(SendOutcome::WouldBlock) => break,
                    Err(error) => {
                        self.last_error = Some(error.into());
                        self.close_peer(key.stream.peer);
                        break;
                    }
                }
            }
            if !control {
                quanta.insert(key.stream.peer, (budget, records));
            }
        }
        if !self.schedule.is_empty() {
            self.schedule.rotate_left(1);
        }
        self.ip_turn = quanta
            .into_iter()
            .map(|(p, (bytes, records))| {
                let s = self.native_send.get(&p).copied().unwrap_or((0, 0));
                let r = self.native_receive.get(&p).copied().unwrap_or((0, 0));
                (
                    p,
                    (
                        32768usize.saturating_sub(bytes + s.0 + r.0),
                        records.saturating_sub(s.1.max(r.1)),
                    ),
                )
            })
            .collect();
        Ok(())
    }
    fn close_peer(&mut self, peer: PeerId) {
        let Some(s) = self.sessions.remove(&peer) else {
            return;
        };
        if self.dynamic.is_some() {
            let retire_result = self.backend_push(BackendEvent::Retire {
                key: s.control,
                session: s.id,
            });
            if retire_result.is_err() {
                self.last_error = Some(NetworkError::Overloaded);
                self.runtime.transport_lost();
            }
        }
        let keys: Vec<_> = self
            .streams
            .keys()
            .filter(|k| k.stream.peer == peer)
            .copied()
            .collect();
        for key in keys {
            let _ = self.runtime.close(key);
            self.streams.remove(&key);
            self.consumed.remove(&key);
        }
        self.packets.remove(peer);
        self.schedule.retain(|k| k.stream.peer != peer);
        self.ip_reservations.remove(&peer);
        self.counters.closed_sessions = self.counters.closed_sessions.saturating_add(1);
    }
    fn reject_expired_setup(&mut self, peer: PeerId) -> Result<(), NetworkError> {
        if self
            .sessions
            .get(&peer)
            .is_some_and(|s| setup_expired(s.state, s.deadline, Instant::now()))
        {
            self.last_error = Some(NetworkError::Timeout);
            self.close_peer(peer);
            return Err(NetworkError::Timeout);
        }
        Ok(())
    }
    fn retire_expired_setups(&mut self) {
        let expired: Vec<_> = self
            .reservations
            .iter()
            .filter(|(key, p)| {
                Instant::now() >= p.deadline || self.runtime.snapshot(**key).is_none()
            })
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            if let Some(p) = self.reservations.remove(&key) {
                let retire_result = self.backend_push(BackendEvent::Retire {
                    key,
                    session: p.session,
                });
                if retire_result.is_err() {
                    self.last_error = Some(NetworkError::Overloaded);
                    self.runtime.transport_lost();
                }
                self.ip_reservations.remove(&key.stream.peer);
                let _ = self.runtime.close(key);
            }
        }
        let peers: Vec<_> = self
            .sessions
            .iter()
            .filter(|(_, s)| setup_expired(s.state, s.deadline, Instant::now()))
            .map(|(peer, _)| *peer)
            .collect();
        for peer in peers {
            let _ = self.reject_expired_setup(peer);
        }
    }
    #[cfg(feature = "linux-runtime")]
    pub(crate) fn session_streams(&self, id: &SessionId) -> Result<Vec<RuntimeKey>, NetworkError> {
        let peer = self.session_peer(id)?;
        Ok(self
            .streams
            .keys()
            .filter(|k| k.stream.peer == peer)
            .copied()
            .collect())
    }
    pub fn close_session(&mut self, id: &SessionId) -> Result<(), NetworkError> {
        let peer = self.session_peer(id)?;
        self.close_peer(peer);
        Ok(())
    }
    /// Never cancel this future after extraction of Core output frames.
    pub async fn drive(&mut self, wait: Duration) -> Result<(), NetworkError> {
        self.drive_with_wake(wait, std::future::pending()).await
    }
    /// Host readiness may end only Core's idle wait; this drive remains complete.
    /// Hosts must clear native readiness on WouldBlock and await the whole drive.
    pub async fn drive_with_wake<F>(&mut self, wait: Duration, wake: F) -> Result<(), NetworkError>
    where
        F: Future<Output = ()>,
    {
        self.retire_expired_setups();
        // Native completions are batched after ownership release, never on poll.
        let consumed = std::mem::take(&mut self.consumed);
        for (key, end) in consumed {
            if self.streams.contains_key(&key)
                && let Err(error) = self.runtime.consume_through(key, end)
            {
                self.last_error = Some(error.into());
                self.close_peer(key.stream.peer);
            }
        }
        // Direct setup/error cancellations share the native terminal quantum.
        // The set is bounded by live Core slots; each successful close releases it.
        let mut index = 0;
        while index < self.tcp_closing.len() {
            let key = self.tcp_closing[index];
            if self
                .runtime
                .snapshot(key)
                .is_none_or(|s| s.state == skvoz_core::State::Closed)
            {
                self.tcp_closing.remove(index);
                continue;
            }
            if self
                .native_terminal
                .get(&key.stream.peer)
                .map(|entry| entry.0)
                .unwrap_or(0)
                >= 4
                || self.native_allowance(key.stream.peer).2 == 0
            {
                index += 1;
                continue;
            }
            self.tcp_closing.remove(index);
            self.native_terminal
                .entry(key.stream.peer)
                .or_insert_with(|| (0, Instant::now()))
                .0 += 1;
            self.account_native(key.stream.peer, 0, 0, 1);
            let _ = self.runtime.close(key);
        }
        // Feed already-owned network output before Core decides whether it is idle.
        // This is the single SEND quantum for this drive, not a second batch.
        self.flush()?;
        if let Err(error) = self.runtime.turn_with_wake(wait, wake).await {
            self.last_error = Some(error.into());
            let peers: Vec<_> = self.sessions.keys().copied().collect();
            for peer in peers {
                self.close_peer(peer);
            }
            return Err(error.into());
        }
        self.retire_expired_setups();
        // Never extract an event that cannot transfer into the bounded backend.
        // Core retains terminal ownership until the actor drains existing work.
        for observed in self
            .runtime
            .poll_events((512 - self.backend.len()).min(256))
        {
            let key = observed.key;
            if self.tcp.contains(&key) {
                if let Event::Opened { metadata } = &observed.event
                    && (!compatible_peer_profile(self.runtime.peer_limits(key))
                        || !matches!(Accept::decode(metadata), Ok(Accept::Tcp { .. })))
                {
                    self.close_tcp(key);
                    self.backend_push(BackendEvent::Tcp {
                        key,
                        event: Event::Closed {
                            reason: skvoz_core::CloseReason::Cancelled,
                        },
                    })?;
                    continue;
                }
                if matches!(
                    observed.event,
                    Event::Closed { .. } | Event::Rejected { .. }
                ) {
                    self.tcp.remove(&key);
                }
                self.backend_push(BackendEvent::Tcp {
                    key,
                    event: observed.event,
                })?;
                continue;
            }
            let result = match observed.event {
                Event::IncomingOpen { metadata } => self.incoming(key, &metadata),
                Event::Opened { metadata } => self.opened(key, &metadata),
                Event::Data { offset, bytes } => self.data(key, offset, &bytes),
                Event::Rejected { reason } => {
                    self.last_error = Some(
                        Rejection::decode(&reason)
                            .map(|r| r.network_error())
                            .unwrap_or_else(|error| error),
                    );
                    if self.streams.contains_key(&key) {
                        self.close_peer(key.stream.peer);
                    }
                    Ok(())
                }
                Event::RemoteFinished | Event::Closed { .. } => {
                    if self.streams.contains_key(&key) {
                        self.close_peer(key.stream.peer);
                    }
                    Ok(())
                }
                Event::Writable => Ok(()),
            };
            if let Err(error) = result {
                self.last_error = Some(error);
                if self.streams.contains_key(&key) {
                    self.close_peer(key.stream.peer);
                }
            }
        }
        // Process control first without assuming ordering across data channels.
        let controls: Vec<_> = self
            .streams
            .iter()
            .filter(|(_, c)| c.control)
            .map(|(k, _)| *k)
            .collect();
        for key in controls {
            if !self.streams.contains_key(&key) {
                continue;
            }
            // The other peer's lane proof can complete before our reciprocal
            // proof. Keep bounded setup bytes until our own proof is ready.
            if !self.runtime.peer_ready(key.stream.peer) {
                continue;
            }
            if let Err(error) = self.drain_records(key, &mut 32768, &mut 0, 8) {
                self.last_error = Some(error);
                self.close_peer(key.stream.peer);
            }
        }
        let mut receive_quanta: BTreeMap<PeerId, (usize, usize)> = self
            .sessions
            .keys()
            .map(|p| {
                let s = self.native_send.get(p).copied().unwrap_or((0, 0));
                let r = self.native_receive.get(p).copied().unwrap_or((0, 0));
                let ip = self.ip_turn.get(p).copied().unwrap_or((0, 0));
                (
                    *p,
                    (
                        32768usize.saturating_sub(s.0 + r.0 + ip.0),
                        s.1.max(r.1) + ip.1,
                    ),
                )
            })
            .collect();
        let channels: Vec<_> = self.schedule.iter().copied().collect();
        for key in channels {
            if self.streams.get(&key).is_none_or(|c| c.control)
                || !self.runtime.peer_ready(key.stream.peer)
                || !self
                    .sessions
                    .get(&key.stream.peer)
                    .is_some_and(|s| s.state == SessionState::Active)
            {
                continue;
            }
            let (mut budget, mut records) =
                *receive_quanta.entry(key.stream.peer).or_insert((32768, 0));
            if let Err(error) = self.drain_records(key, &mut budget, &mut records, 16) {
                self.last_error = Some(error);
                self.close_peer(key.stream.peer);
            }
            receive_quanta.insert(key.stream.peer, (budget, records));
        }
        let expired: Vec<_> = self
            .sessions
            .iter()
            .filter(|(peer, s)| {
                (s.state != SessionState::Active && Instant::now() >= s.deadline)
                    || self.runtime.snapshot(s.control).is_none()
                    || self
                        .runtime
                        .peer_status(**peer)
                        .is_none_or(|p| p.incarnation != s.control.incarnation)
            })
            .map(|(p, _)| *p)
            .collect();
        for peer in expired {
            self.last_error = Some(
                if self.sessions.get(&peer).is_some_and(|s| {
                    s.state != SessionState::Active && Instant::now() >= s.deadline
                }) {
                    NetworkError::Timeout
                } else {
                    NetworkError::Runtime(skvoz_core::runtime::RuntimeError::StaleKey)
                },
            );
            self.close_peer(peer);
        }
        self.native_send.clear();
        self.native_receive.clear();
        // Replenish only after a complete Core turn and five elapsed ms.
        // No saved/catch-up tokens: four terminal envelopes per peer at most.
        self.native_terminal
            .retain(|_, (_, started)| started.elapsed() < Duration::from_millis(5));
        self.ip_turn.clear();
        Ok(())
    }
    pub async fn shutdown(&mut self) -> Result<(), NetworkError> {
        let peers: Vec<_> = self.sessions.keys().copied().collect();
        for peer in peers {
            self.close_peer(peer);
        }
        self.runtime.shutdown().await?;
        Ok(())
    }
}

fn complete_receipt(receipts: &mut VecDeque<Receipt>, end: u64) -> Result<usize, NetworkError> {
    let receipt = receipts
        .iter_mut()
        .find(|r| r.end == end && r.state == ReceiptState::Delivered)
        .ok_or(NetworkError::InvalidState)?;
    receipt.state = ReceiptState::Done;
    let size = receipt.size;
    receipt.size = 0;
    Ok(size)
}
// Completed drops behind an outstanding OS write cannot advance Core credit.
fn completed_prefix(receipts: &mut VecDeque<Receipt>) -> Option<u64> {
    let mut end = None;
    while receipts
        .front()
        .is_some_and(|r| r.state == ReceiptState::Done)
    {
        end = receipts.pop_front().map(|r| r.end);
    }
    end
}
#[cfg(test)]
mod ownership_tests {
    use super::*;
    #[test]
    fn completion_prefix_waits_for_older_packet_before_later_drop() {
        let mut receipts = VecDeque::from([
            Receipt {
                _reservation: None,
                end: 28,
                size: 28,
                state: ReceiptState::Delivered,
            },
            Receipt {
                _reservation: None,
                end: 56,
                size: 0,
                state: ReceiptState::Done,
            },
            Receipt {
                _reservation: None,
                end: 84,
                size: 28,
                state: ReceiptState::Delivered,
            },
        ]);
        assert_eq!(completed_prefix(&mut receipts), None);
        complete_receipt(&mut receipts, 84).unwrap();
        assert_eq!(completed_prefix(&mut receipts), None);
        complete_receipt(&mut receipts, 28).unwrap();
        assert_eq!(completed_prefix(&mut receipts), Some(84));
        assert!(receipts.is_empty());
    }
    #[test]
    fn completion_requires_delivered_exact_token_and_is_single_use() {
        let mut receipts = VecDeque::from([Receipt {
            _reservation: None,
            end: 28,
            size: 28,
            state: ReceiptState::Queued,
        }]);
        assert_eq!(
            complete_receipt(&mut receipts, 28),
            Err(NetworkError::InvalidState)
        );
        receipts[0].state = ReceiptState::Delivered;
        assert_eq!(
            complete_receipt(&mut receipts, 27),
            Err(NetworkError::InvalidState)
        );
        assert_eq!(complete_receipt(&mut receipts, 28), Ok(28));
        assert_eq!(
            complete_receipt(&mut receipts, 28),
            Err(NetworkError::InvalidState)
        );
        assert_eq!(completed_prefix(&mut receipts), Some(28));
    }
    #[test]
    fn queued_packet_is_not_consumed_by_polling_or_copying() {
        let mut receipts = VecDeque::from([Receipt {
            _reservation: None,
            end: 28,
            size: 28,
            state: ReceiptState::Queued,
        }]);
        assert_eq!(completed_prefix(&mut receipts), None);
        receipts[0].state = ReceiptState::Delivered;
        assert_eq!(completed_prefix(&mut receipts), None);
    }
}

#[derive(Default)]
struct PacketDispatch {
    queues: BTreeMap<PeerId, VecDeque<ReceivedPacket>>,
    schedule: VecDeque<PeerId>,
}
impl PacketDispatch {
    fn push(&mut self, packet: ReceivedPacket) {
        let peer = packet.key.stream.peer;
        let queue = self.queues.entry(peer).or_default();
        if queue.is_empty() {
            self.schedule.push_back(peer);
        }
        queue.push_back(packet);
    }
    fn pop(&mut self) -> Option<ReceivedPacket> {
        let peer = self.schedule.pop_front()?;
        let queue = self.queues.get_mut(&peer)?;
        let packet = queue.pop_front()?;
        if queue.is_empty() {
            self.queues.remove(&peer);
        } else {
            self.schedule.push_back(peer);
        }
        Some(packet)
    }
    fn remove(&mut self, peer: PeerId) {
        self.queues.remove(&peer);
        self.schedule.retain(|p| *p != peer);
    }
}

fn server_accept_matches(
    key: RuntimeKey,
    control: RuntimeKey,
    id: &SessionId,
    channels: &BTreeMap<u8, (RuntimeKey, bool)>,
    accepted: &Accept,
) -> bool {
    match accepted {
        Accept::IpSession { session, .. } => key == control && session == id,
        Accept::IpData {
            session, channel, ..
        } => {
            session == id
                && channels
                    .get(channel)
                    .is_some_and(|(data, ready)| *data == key && *ready)
        }
        _ => false,
    }
}
#[cfg(test)]
mod dispatcher_tests {
    use super::*;
    use skvoz_core::StreamKey;
    fn key(peer: u64, stream_id: u64) -> RuntimeKey {
        RuntimeKey {
            epoch: 77,
            incarnation: 2,
            stream: StreamKey {
                peer: PeerId(peer),
                stream_id,
            },
        }
    }
    fn id() -> SessionId {
        "0123456789abcdef0123456789abcdef"
            .to_owned()
            .try_into()
            .unwrap()
    }
    #[test]
    fn busy_peer_does_not_delay_single_packet_from_healthy_peer() {
        let mut dispatcher = PacketDispatch::default();
        for end in 1..=256 {
            dispatcher.push(ReceivedPacket {
                session: id(),
                key: key(1, 4),
                end_offset: end,
                packet: vec![0; 20],
            });
        }
        dispatcher.push(ReceivedPacket {
            session: id(),
            key: key(2, 4),
            end_offset: 28,
            packet: vec![0; 20],
        });
        assert_eq!(dispatcher.pop().unwrap().key.stream.peer, PeerId(1));
        assert_eq!(dispatcher.pop().unwrap().key.stream.peer, PeerId(2));
        dispatcher.remove(PeerId(1));
        assert!(dispatcher.pop().is_none());
        assert!(dispatcher.queues.is_empty());
    }
    #[test]
    fn server_accept_local_opened_validates_without_client_transition() {
        let control = key(1, 0);
        let data = key(1, 2);
        let channels = BTreeMap::from([(0, (data, true))]);
        assert!(server_accept_matches(
            control,
            control,
            &id(),
            &channels,
            &Accept::IpSession {
                v: crate::NETWORK_VERSION,
                session: id()
            }
        ));
        assert!(server_accept_matches(
            data,
            control,
            &id(),
            &channels,
            &Accept::IpData {
                v: crate::NETWORK_VERSION,
                session: id(),
                channel: 0
            }
        ));
        assert!(!server_accept_matches(
            key(2, 2),
            control,
            &id(),
            &channels,
            &Accept::IpData {
                v: crate::NETWORK_VERSION,
                session: id(),
                channel: 0
            }
        ));
        assert!(!server_accept_matches(
            data,
            control,
            &id(),
            &channels,
            &Accept::IpData {
                v: crate::NETWORK_VERSION,
                session: id(),
                channel: 1
            }
        ));
    }
}

fn gather_output(queue: &VecDeque<Pending>, budget: usize, records: usize) -> Vec<u8> {
    let budget = budget.min(16384);
    let mut batch = Vec::with_capacity(budget);
    for pending in queue.iter().take(records) {
        let suffix = &pending.bytes[pending.cursor..];
        let size = suffix.len().min(budget - batch.len());
        batch.extend_from_slice(&suffix[..size]);
        if batch.len() == budget {
            break;
        }
    }
    batch
}
fn advance_output(queue: &mut VecDeque<Pending>, mut accepted: usize) -> (usize, usize) {
    let mut bytes = 0;
    let mut records = 0;
    while accepted > 0 {
        let pending = queue
            .front_mut()
            .expect("accepted prefix belongs to pending records");
        let size = accepted.min(pending.bytes.len() - pending.cursor);
        pending.cursor += size;
        accepted -= size;
        if pending.cursor == pending.bytes.len() {
            bytes += pending.bytes.len();
            records += 1;
            queue.pop_front();
        }
    }
    (bytes, records)
}
#[cfg(test)]
mod batch_tests {
    use super::*;
    #[test]
    fn coalesced_send_keeps_every_partial_record_suffix() {
        for cut in 1..=30 {
            let mut queue = VecDeque::from([
                Pending {
                    bytes: (0..10).collect(),
                    cursor: 0,
                },
                Pending {
                    bytes: (10..20).collect(),
                    cursor: 0,
                },
                Pending {
                    bytes: (20..30).collect(),
                    cursor: 0,
                },
            ]);
            let batch = gather_output(&queue, 32768, 16);
            assert_eq!(batch, (0..30).collect::<Vec<_>>());
            let (_, records) = advance_output(&mut queue, cut);
            assert_eq!(records, cut / 10);
            assert_eq!(
                gather_output(&queue, 32768, 16),
                (cut as u8..30).collect::<Vec<_>>()
            );
        }
    }
    #[test]
    fn gather_respects_record_and_byte_quantum() {
        let queue = VecDeque::from([
            Pending {
                bytes: vec![1; 1508],
                cursor: 100,
            },
            Pending {
                bytes: vec![2; 1508],
                cursor: 0,
            },
            Pending {
                bytes: vec![3; 1508],
                cursor: 0,
            },
        ]);
        assert_eq!(gather_output(&queue, 32768, 1).len(), 1408);
        assert_eq!(gather_output(&queue, 2000, 16).len(), 2000);
        assert_eq!(gather_output(&queue, 32768, 2).len(), 2916);
    }
    #[test]
    fn frame_sized_gather_avoids_recopying_unacceptable_suffix() {
        let mut queue = VecDeque::from([
            Pending {
                bytes: vec![1; 14000],
                cursor: 0,
            },
            Pending {
                bytes: vec![2; 10000],
                cursor: 0,
            },
        ]);
        let first = gather_output(&queue, 32768, 16);
        assert_eq!(first.len(), 16384);
        assert_eq!(advance_output(&mut queue, first.len()), (14000, 1));
        let second = gather_output(&queue, 32768 - first.len(), 15);
        assert_eq!(second, vec![2; 7616]);
        assert_eq!(advance_output(&mut queue, second.len()), (10000, 1));
        assert!(queue.is_empty());
    }
}

fn setup_expired(state: SessionState, deadline: Instant, now: Instant) -> bool {
    state != SessionState::Active && now >= deadline
}
#[cfg(test)]
mod deadline_tests {
    use super::*;
    #[test]
    fn expired_setup_cannot_be_promoted_by_fully_ready_buffered_control() {
        let now = Instant::now();
        for state in [
            SessionState::Negotiating,
            SessionState::Preparing,
            SessionState::AwaitingActive,
        ] {
            assert!(setup_expired(state, now, now));
            assert!(setup_expired(state, now, now + Duration::from_millis(1)));
            assert!(!setup_expired(state, now + Duration::from_millis(1), now));
        }
        assert!(!setup_expired(
            SessionState::Active,
            now,
            now + Duration::from_secs(3600)
        ));
    }
}

fn retire_flight(flight: &mut VecDeque<u64>, consumed: u64) {
    while flight.front().is_some_and(|end| *end <= consumed) {
        flight.pop_front();
    }
}
#[cfg(test)]
mod flight_tests {
    use super::*;
    #[test]
    fn envelope_flight_releases_only_fully_consumed_accepted_frames() {
        let mut flight = VecDeque::from([60, 120, 180]);
        retire_flight(&mut flight, 59);
        assert_eq!(flight.len(), 3);
        retire_flight(&mut flight, 60);
        assert_eq!(flight, VecDeque::from([120, 180]));
        retire_flight(&mut flight, 179);
        assert_eq!(flight, VecDeque::from([180]));
        retire_flight(&mut flight, 180);
        assert!(flight.is_empty());
    }
}
