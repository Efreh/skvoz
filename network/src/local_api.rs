//! API1 envelopes shared by embedding, native control and the privileged helper.
use crate::{NetworkError, SessionConfig, SessionId, config::*};
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};

pub const BODY_MAX: usize = 32768;
pub fn parse_strict_json(bytes: &[u8]) -> Result<Value, NetworkError> {
    parse_strict_json_bounded(bytes, BODY_MAX)
}
pub fn parse_strict_json_bounded(bytes: &[u8], limit: usize) -> Result<Value, NetworkError> {
    if bytes.is_empty() || bytes.len() > limit {
        return Err(NetworkError::InvalidMetadata);
    }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value =
        StrictValue::deserialize(&mut decoder).map_err(|_| NetworkError::InvalidMetadata)?;
    decoder.end().map_err(|_| NetworkError::InvalidMetadata)?;
    Ok(value.0)
}
struct StrictValue(Value);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("strict JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                serde_json::Number::from_f64(v)
                    .map(|n| StrictValue(n.into()))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element::<StrictValue>()? {
                    out.push(v.0)
                }
                Ok(StrictValue(out.into()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
                let mut out = serde_json::Map::new();
                while let Some(k) = a.next_key::<String>()? {
                    if out.contains_key(&k) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    out.insert(k, a.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(out.into()))
            }
        }
        d.deserialize_any(StrictVisitor)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Operation {
    Hello,
    Status,
    StartProxy,
    StopProxy,
    OpenTcp,
    StartIp,
    AttachIp,
    LocalReady,
    StopIp,
    PrepareShutdown,
    UpdateRegistry,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub v: u8,
    pub id: u32,
    pub op: Operation,
    pub args: Value,
    pub fd_count: u8,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiError {
    UnsupportedVersion,
    UnsupportedFamily,
    InvalidRequest,
    InvalidState,
    UnknownHandle,
    Forbidden,
    Overloaded,
    LocalSetupFailed,
    NetworkUnavailable,
    Timeout,
    Closed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub v: u8,
    pub id: u32,
    #[serde(deserialize_with = "required_option")]
    pub result: Option<Value>,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<ApiError>,
    pub fd_count: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub v: u8,
    pub seq: u64,
    pub event: String,
    pub data: Value,
    pub fd_count: u8,
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    pub uploaded: u64,
    pub downloaded: u64,
    pub tcp_open: u64,
    pub ip_sessions: u64,
    pub packet_in: u64,
    pub packet_out: u64,
    pub packet_dropped: u64,
    pub queue_bytes: u64,
    pub queue_records: u64,
    pub buffer_bytes: u64,
    pub buffer_records: u64,
    pub errors: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingStatus {
    pub control_ready: bool,
    pub eligible_exits: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusSession {
    pub handle: SessionId,
    pub state: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStatus {
    pub lifecycle: String,
    pub mode: String,
    #[serde(deserialize_with = "required_option")]
    pub session: Option<StatusSession>,
    pub counters: Counters,
    pub routing: RoutingStatus,
}
impl RuntimeStatus {
    pub fn validate(&self) -> Result<(), NetworkError> {
        if !matches!(
            self.lifecycle.as_str(),
            "starting" | "ready" | "closing" | "closed"
        ) || !matches!(self.mode.as_str(), "idle" | "proxy" | "ip" | "server")
            || self.routing.eligible_exits > 8
            || self.session.as_ref().is_some_and(|session| {
                !matches!(
                    session.state.as_str(),
                    "negotiating"
                        | "preparing"
                        | "awaiting_active"
                        | "active"
                        | "closing"
                        | "closed"
                )
            })
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    UserStop,
    ModeChange,
    Shutdown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloArgs {
    pub api: u8,
    pub network: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmptyArgs {}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyArgs {
    #[serde(deserialize_with = "required_option")]
    pub http_bind: Option<String>,
    #[serde(deserialize_with = "required_option")]
    pub socks_bind: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TcpArgs {
    pub host: String,
    pub port: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IpArgs {
    pub families: Vec<u8>,
    pub family_policy: crate::FamilyPolicy,
    pub max_mtu: u16,
    pub channels: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandleArgs {
    pub handle: SessionId,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttachArgs {
    pub handle: SessionId,
    pub interface: String,
    pub mtu: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopArgs {
    pub handle: SessionId,
    pub reason: StopReason,
}
pub fn arguments<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T, NetworkError> {
    serde_json::from_value(value.clone()).map_err(|_| NetworkError::InvalidMetadata)
}
pub fn bind_address(value: &str) -> Result<SocketAddr, NetworkError> {
    let a: SocketAddr = value.parse().map_err(|_| NetworkError::InvalidMetadata)?;
    if a.port() == 0
        || a.ip().is_unspecified()
        || a.to_string() != value
        || a.ip().to_canonical() != a.ip()
    {
        return Err(NetworkError::InvalidMetadata);
    }
    // The initial product profile exposes only loopback/private headless listeners.
    let private = match a.ip() {
        IpAddr::V4(v) => v.is_private() || v.is_loopback(),
        IpAddr::V6(v) => v.is_loopback() || v.is_unique_local(),
    };
    if !private {
        return Err(NetworkError::Forbidden);
    }
    Ok(a)
}
impl Request {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let r: Self = serde_json::from_value(parse_strict_json(bytes)?)
            .map_err(|_| NetworkError::InvalidMetadata)?;
        r.validate()?;
        Ok(r)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 {
            return Err(NetworkError::UnsupportedVersion);
        }
        if self.id == 0
            || self.id > 2147483647
            || self.fd_count != u8::from(self.op == Operation::AttachIp)
        {
            return Err(NetworkError::InvalidMetadata);
        }
        match self.op {
            Operation::Hello => {
                let a: HelloArgs = arguments(&self.args)?;
                if a.api != 1 || a.network != crate::NETWORK_VERSION {
                    return Err(NetworkError::UnsupportedVersion);
                }
            }
            Operation::Status | Operation::StopProxy | Operation::PrepareShutdown => {
                let _: EmptyArgs = arguments(&self.args)?;
            }
            Operation::UpdateRegistry => {
                let registry: crate::routing::Registry = arguments(&self.args)?;
                registry.validate()?;
                if serde_json::to_vec(&registry)
                    .map_err(|_| NetworkError::InvalidRecord)?
                    .len()
                    > 16384
                {
                    return Err(NetworkError::InvalidRecord);
                }
            }
            Operation::StartProxy => {
                let a: ProxyArgs = arguments(&self.args)?;
                for b in [a.http_bind, a.socks_bind].into_iter().flatten() {
                    bind_address(&b)?;
                }
            }
            Operation::OpenTcp => {
                let a: TcpArgs = arguments(&self.args)?;
                crate::Metadata::Tcp {
                    v: crate::NETWORK_VERSION,
                    host: a.host,
                    port: a.port,
                }
                .encode()?;
            }
            Operation::StartIp => {
                let a: IpArgs = arguments(&self.args)?;
                crate::Metadata::IpSession {
                    v: crate::NETWORK_VERSION,
                    families: a.families,
                    family_policy: a.family_policy,
                    max_mtu: a.max_mtu,
                    channels: a.channels,
                }
                .encode()?;
            }
            Operation::AttachIp => {
                let a: AttachArgs = arguments(&self.args)?;
                validate_ifname(&a.interface)?;
                if !(576..=1500).contains(&a.mtu) {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            Operation::LocalReady => {
                let _: HandleArgs = arguments(&self.args)?;
            }
            Operation::StopIp => {
                let _: StopArgs = arguments(&self.args)?;
            }
        }
        Ok(())
    }
}
impl Response {
    pub fn success(id: u32, result: Value, fd: bool) -> Self {
        Self {
            v: 1,
            id,
            result: Some(result),
            error: None,
            fd_count: u8::from(fd),
        }
    }
    pub fn failure(id: u32, error: ApiError) -> Self {
        Self {
            v: 1,
            id,
            result: None,
            error: Some(error),
            fd_count: 0,
        }
    }
}
impl From<NetworkError> for ApiError {
    fn from(e: NetworkError) -> Self {
        match e {
            NetworkError::UnsupportedVersion => Self::UnsupportedVersion,
            NetworkError::UnsupportedFamily => Self::UnsupportedFamily,
            NetworkError::Forbidden => Self::Forbidden,
            NetworkError::Overloaded => Self::Overloaded,
            NetworkError::Timeout => Self::Timeout,
            NetworkError::InvalidState => Self::InvalidState,
            NetworkError::Runtime(_) => Self::NetworkUnavailable,
            _ => Self::InvalidRequest,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HelperOperation {
    Hello,
    PrepareClient,
    ActivateClient,
    PrepareServer,
    ReservePeer,
    ActivatePeer,
    RetirePeer,
    AbortClient,
    RestoreClient,
    Recover,
    StopServer,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperRequest {
    pub v: u8,
    pub id: u32,
    pub op: HelperOperation,
    pub args: Value,
    pub fd_count: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperResponse {
    pub v: u8,
    pub id: u32,
    #[serde(deserialize_with = "required_option")]
    pub result: Option<Value>,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<String>,
    pub fd_count: u8,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportEndpoint {
    #[serde(deserialize_with = "canonical_ip")]
    pub ip: IpAddr,
    pub port: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareClientArgs {
    pub handle: SessionId,
    pub config: SessionConfig,
    pub transport_endpoints: Vec<TransportEndpoint>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareServerArgs {
    pub network: NetworkConfig,
    pub server: ServerConfig,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservePeerArgs {
    pub peer: String,
    pub session: SessionId,
    pub families: Vec<u8>,
    pub mtu: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSessionArgs {
    pub peer: String,
    pub session: SessionId,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPrepared {
    pub handle: SessionId,
    pub interface: String,
    pub mtu: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerPrepared {
    pub interface: String,
    pub mtu: u16,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerReserved {
    pub session: SessionId,
    pub source_grants: Vec<crate::IpPrefix>,
    pub egress: crate::Egress,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionResult {
    pub session: SessionId,
}
impl HelperRequest {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let r: Self = serde_json::from_value(parse_strict_json(bytes)?)
            .map_err(|_| NetworkError::InvalidMetadata)?;
        r.validate()?;
        Ok(r)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 {
            return Err(NetworkError::UnsupportedVersion);
        }
        if self.id == 0 || self.id > 2147483647 || self.fd_count != 0 {
            return Err(NetworkError::InvalidMetadata);
        }
        match self.op {
            HelperOperation::Hello => {
                let a: HelloArgs = arguments(&self.args)?;
                if a.api != 1 || a.network != crate::NETWORK_VERSION {
                    return Err(NetworkError::UnsupportedVersion);
                }
            }
            HelperOperation::Recover | HelperOperation::StopServer => {
                let _: EmptyArgs = arguments(&self.args)?;
            }
            HelperOperation::PrepareServer => {
                let a: PrepareServerArgs = arguments(&self.args)?;
                a.server.validate(&a.network)?;
            }
            HelperOperation::PrepareClient => {
                let a: PrepareClientArgs = arguments(&self.args)?;
                a.config.validate()?;
                if a.transport_endpoints.is_empty()
                    || a.transport_endpoints.len() > 32
                    || a.transport_endpoints.iter().any(|e| {
                        e.port == 0
                            || e.ip.to_canonical() != e.ip
                            || e.ip.is_unspecified()
                            || e.ip.is_multicast()
                    })
                {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            HelperOperation::ReservePeer => {
                let a: ReservePeerArgs = arguments(&self.args)?;
                parse_peer(&a.peer)?;
                crate::validate_families(&a.families)?;
                if !(576..=1500).contains(&a.mtu) || a.families.contains(&6) && a.mtu < 1280 {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            HelperOperation::ActivatePeer | HelperOperation::RetirePeer => {
                let a: PeerSessionArgs = arguments(&self.args)?;
                parse_peer(&a.peer)?;
            }
            HelperOperation::ActivateClient | HelperOperation::AbortClient => {
                let _: HandleArgs = arguments(&self.args)?;
            }
            HelperOperation::RestoreClient => {
                let _: StopArgs = arguments(&self.args)?;
            }
        }
        Ok(())
    }
    pub fn new(id: u32, op: HelperOperation, args: Value) -> Result<Self, NetworkError> {
        let r = Self {
            v: 1,
            id,
            op,
            args,
            fd_count: 0,
        };
        r.validate()?;
        Ok(r)
    }
}
impl HelperResponse {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let r: Self = serde_json::from_value(parse_strict_json(bytes)?)
            .map_err(|_| NetworkError::InvalidMetadata)?;
        if r.v != 1
            || r.id == 0
            || r.id > 2147483647
            || r.fd_count > 1
            || r.result.is_some() == r.error.is_some()
            || r.error.as_ref().is_some_and(|e| {
                e.is_empty()
                    || e.len() > 64
                    || !e.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
            })
            || r.error.is_some() && r.fd_count != 0
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(r)
    }
    pub fn success(id: u32, result: Value, fd: bool) -> Self {
        Self {
            v: 1,
            id,
            result: Some(result),
            error: None,
            fd_count: u8::from(fd),
        }
    }
    pub fn failure(id: u32, error: &str) -> Self {
        Self {
            v: 1,
            id,
            result: None,
            error: Some(error.into()),
            fd_count: 0,
        }
    }
}
pub fn handle_result(id: &SessionId) -> Value {
    json!({"handle":id})
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperState {
    pub state: String,
    #[serde(deserialize_with = "required_option")]
    pub handle: Option<SessionId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredEvent {
    pub handle: SessionId,
    pub config: SessionConfig,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClosedEvent {
    pub handle: SessionId,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<ApiError>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStateEvent {
    pub state: String,
    #[serde(deserialize_with = "required_option")]
    pub error: Option<ApiError>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatsEvent {
    pub counters: Counters,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestEvent {
    pub id: u64,
    pub protocol: String,
    pub host: String,
    pub port: u16,
    pub result: String,
    pub uploaded: u64,
    pub downloaded: u64,
}
impl Event {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let event: Self = serde_json::from_value(parse_strict_json(bytes)?)
            .map_err(|_| NetworkError::InvalidMetadata)?;
        event.validate()?;
        Ok(event)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 {
            return Err(NetworkError::UnsupportedVersion);
        }
        if self.seq == 0 || self.seq > i64::MAX as u64 || self.fd_count != 0 {
            return Err(NetworkError::InvalidMetadata);
        }
        match self.event.as_str() {
            "CONFIGURED" => {
                let data: ConfiguredEvent = arguments(&self.data)?;
                data.config.validate()?;
            }
            "ACTIVE" => {
                let _: HandleArgs = arguments(&self.data)?;
            }
            "CLOSED" => {
                let _: ClosedEvent = arguments(&self.data)?;
            }
            "RUNTIME_STATE" => {
                let data: RuntimeStateEvent = arguments(&self.data)?;
                if !matches!(
                    data.state.as_str(),
                    "starting" | "ready" | "closing" | "closed"
                ) {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            "STATS" => {
                let _: StatsEvent = arguments(&self.data)?;
            }
            "REQUEST" => {
                let data: RequestEvent = arguments(&self.data)?;
                if data.id == 0
                    || !matches!(
                        data.protocol.as_str(),
                        "HTTP" | "CONNECT" | "SOCKS5" | "TCP"
                    )
                    || !matches!(
                        data.result.as_str(),
                        "opening"
                            | "active"
                            | "finished"
                            | "cancelled"
                            | "forbidden"
                            | "overloaded"
                            | "network_unavailable"
                            | "timeout"
                            | "local_setup_failed"
                            | "invalid_request"
                            | "unsupported_family"
                    )
                {
                    return Err(NetworkError::InvalidMetadata);
                }
                crate::Metadata::Tcp {
                    v: crate::NETWORK_VERSION,
                    host: data.host,
                    port: data.port,
                }
                .encode()?;
            }
            _ => return Err(NetworkError::InvalidMetadata),
        }
        Ok(())
    }
}
impl Response {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let response: Self = serde_json::from_value(parse_strict_json(bytes)?)
            .map_err(|_| NetworkError::InvalidMetadata)?;
        response.validate()?;
        Ok(response)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 {
            return Err(NetworkError::UnsupportedVersion);
        }
        if self.id == 0
            || self.id > i32::MAX as u32
            || self.fd_count > 1
            || self.result.is_some() == self.error.is_some()
            || self.error.is_some() && self.fd_count != 0
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(())
    }
}
impl HelperState {
    pub fn validate(&self, role: Role) -> Result<(), NetworkError> {
        if !matches!(
            (&*self.state, &self.handle),
            ("idle", None) | ("guarded", Some(_))
        ) || role == Role::Server && self.handle.is_some()
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(())
    }
}
