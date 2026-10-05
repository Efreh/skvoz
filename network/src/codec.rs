use crate::{IpPrefix, NetworkError};
use serde::{Deserialize, Serialize};
use std::{collections::VecDeque, net::IpAddr};

pub const NETWORK_VERSION: u8 = 2;
pub const METADATA_MAX: usize = 512;
pub const CONTROL_MAX: usize = 16384;
pub const RECORD_HEADER: usize = 8;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionId(String);
impl SessionId {
    pub fn random() -> Result<Self, NetworkError> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|_| NetworkError::InvalidState)?;
        Ok(Self(bytes.iter().map(|b| format!("{b:02x}")).collect()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for SessionId {
    type Error = NetworkError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(Self(value))
    }
}
impl From<SessionId> for String {
    fn from(value: SessionId) -> Self {
        value.0
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Metadata {
    #[serde(rename = "tcp")]
    Tcp { v: u8, host: String, port: u16 },
    #[serde(rename = "ip-session")]
    IpSession {
        v: u8,
        families: Vec<u8>,
        max_mtu: u16,
        channels: u8,
    },
    #[serde(rename = "ip-data")]
    IpData {
        v: u8,
        session: SessionId,
        channel: u8,
    },
}
#[derive(Deserialize)]
struct MetadataHeader {
    v: u8,
    #[serde(rename = "type")]
    kind: String,
}
impl Metadata {
    pub(crate) fn request_type(bytes: &[u8]) -> &'static str {
        if bytes.len() > METADATA_MAX {
            return "unknown";
        }
        match serde_json::from_slice::<MetadataHeader>(bytes)
            .ok()
            .map(|h| h.kind)
            .as_deref()
        {
            Some("tcp") => "tcp",
            Some("ip-session") => "ip-session",
            Some("ip-data") => "ip-data",
            _ => "unknown",
        }
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NetworkError> {
        if bytes.len() > METADATA_MAX {
            return Err(NetworkError::InvalidMetadata);
        }
        let header: MetadataHeader =
            serde_json::from_slice(bytes).map_err(|_| NetworkError::InvalidMetadata)?;
        if header.v != NETWORK_VERSION {
            return Err(NetworkError::UnsupportedVersion);
        }
        if !matches!(header.kind.as_str(), "tcp" | "ip-session" | "ip-data") {
            return Err(NetworkError::UnsupportedType);
        }
        let mut value: Self =
            serde_json::from_slice(bytes).map_err(|_| NetworkError::InvalidMetadata)?;
        let v = match &value {
            Self::Tcp { v, .. } | Self::IpSession { v, .. } | Self::IpData { v, .. } => *v,
        };
        if v != NETWORK_VERSION {
            return Err(NetworkError::UnsupportedVersion);
        }
        match &mut value {
            Self::Tcp { host, port, .. } => {
                if *port == 0 || !valid_host(host) {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            Self::IpSession {
                families,
                max_mtu,
                channels,
                ..
            } => {
                families.sort_unstable();
                validate_families(families)?;
                if !(576..=1500).contains(max_mtu)
                    || families.contains(&6) && *max_mtu < 1280
                    || !(1..=8).contains(channels)
                {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            Self::IpData { channel, .. } => {
                if *channel >= 8 {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
        }
        Ok(value)
    }
    pub fn encode(&self) -> Result<Vec<u8>, NetworkError> {
        let b = serde_json::to_vec(self).map_err(|_| NetworkError::InvalidMetadata)?;
        Self::decode(&b)?;
        Ok(b)
    }
}
fn valid_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 || !host.is_ascii() || host.contains('%') {
        return false;
    }
    if let Ok(address) = host.parse::<IpAddr>() {
        return address.to_canonical() == address && address.to_string() == host;
    }
    if host.contains(':') || host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return false;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !label.starts_with('-')
            && !label.ends_with('-')
    })
}
pub fn validate_families(families: &[u8]) -> Result<(), NetworkError> {
    if !matches!(families, [4] | [6] | [4, 6]) {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum Accept {
    #[serde(rename = "tcp")]
    Tcp { v: u8, status: String },
    #[serde(rename = "ip-session")]
    IpSession { v: u8, session: SessionId },
    #[serde(rename = "ip-data")]
    IpData {
        v: u8,
        session: SessionId,
        channel: u8,
    },
}
impl Accept {
    pub fn encode(&self) -> Result<Vec<u8>, NetworkError> {
        let bytes = serde_json::to_vec(self).map_err(|_| NetworkError::InvalidMetadata)?;
        Self::decode(&bytes)?;
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NetworkError> {
        if bytes.len() > METADATA_MAX {
            return Err(NetworkError::InvalidMetadata);
        }
        let a: Self = serde_json::from_slice(bytes).map_err(|_| NetworkError::InvalidMetadata)?;
        match &a {
            Self::Tcp { v, status } if *v == 2 && status == "connected" => {}
            Self::IpSession { v, .. } if *v == 2 => {}
            Self::IpData { v, channel, .. } if *v == 2 && *channel < 8 => {}
            _ => return Err(NetworkError::InvalidMetadata),
        }
        Ok(a)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Egress {
    pub ipv4: String,
    pub ipv6: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    pub session: SessionId,
    pub families: Vec<u8>,
    pub source_grants: Vec<IpPrefix>,
    pub routes: Vec<IpPrefix>,
    pub dns_servers: Vec<IpAddr>,
    pub mtu: u16,
    pub channels: u8,
    pub packet_queue_bytes: usize,
    pub packet_queue_records: usize,
    pub setup_timeout_ms: u64,
    pub egress: Egress,
}
impl SessionConfig {
    pub fn validate(&self) -> Result<(), NetworkError> {
        validate_families(&self.families)?;
        if !(576..=1500).contains(&self.mtu)
            || self.families.contains(&6) && self.mtu < 1280
            || !(1..=8).contains(&self.channels)
            || !(1..=8).contains(&self.source_grants.len())
            || !(1..=32).contains(&self.routes.len())
            || !(1..=4).contains(&self.dns_servers.len())
            || self.packet_queue_bytes < usize::from(self.mtu) + 8
            || self.packet_queue_bytes > 262144
            || !(1..=256).contains(&self.packet_queue_records)
            || self.setup_timeout_ms == 0
            || self.setup_timeout_ms > 15000
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        if self
            .source_grants
            .iter()
            .any(|g| !self.families.contains(&g.family()))
            || self
                .routes
                .iter()
                .any(|r| !self.families.contains(&r.family()))
            || self.dns_servers.iter().any(|a| {
                !self.families.contains(&(if a.is_ipv4() { 4 } else { 6 }))
                    || a.to_canonical() != *a
            })
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        for list in [&self.source_grants, &self.routes] {
            let mut unique = list.clone();
            unique.sort_unstable();
            unique.dedup();
            if unique.len() != list.len()
                || list
                    .iter()
                    .any(|p| String::from(*p).parse::<IpPrefix>().ok() != Some(*p))
            {
                return Err(NetworkError::InvalidConfiguration);
            }
        }
        for (i, p) in self.source_grants.iter().enumerate() {
            if self
                .source_grants
                .iter()
                .skip(i + 1)
                .any(|q| p.contains(q.address) || q.contains(p.address))
            {
                return Err(NetworkError::InvalidConfiguration);
            }
        }
        let mut dns = self.dns_servers.clone();
        dns.sort_unstable();
        dns.dedup();
        if dns.len() != self.dns_servers.len() {
            return Err(NetworkError::InvalidConfiguration);
        }
        for family in &self.families {
            if !self.source_grants.iter().any(|g| g.family() == *family) {
                return Err(NetworkError::InvalidConfiguration);
            }
        }
        if self.families.contains(&4) == (self.egress.ipv4 == "none")
            || self.families.contains(&6) == (self.egress.ipv6 == "none")
            || !matches!(self.egress.ipv4.as_str(), "nat44" | "routed" | "none")
            || !matches!(self.egress.ipv6.as_str(), "routed" | "none")
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSignal {
    pub session: SessionId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionError {
    pub session: SessionId,
    pub error: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Control {
    Config(SessionConfig),
    Ready(SessionSignal),
    Active(SessionSignal),
    Close(SessionError),
    Error(SessionError),
}
impl Control {
    pub fn encode(&self) -> Result<Vec<u8>, NetworkError> {
        match self {
            Self::Close(s)
                if !matches!(s.error.as_str(), "user_stop" | "mode_change" | "shutdown") =>
            {
                return Err(NetworkError::InvalidRecord);
            }
            Self::Error(s)
                if !matches!(
                    s.error.as_str(),
                    "invalid_record"
                        | "invalid_state"
                        | "invalid_configuration"
                        | "local_setup_failed"
                        | "network_unavailable"
                        | "timeout"
                        | "transport_lost"
                        | "forbidden"
                        | "overloaded"
                ) =>
            {
                return Err(NetworkError::InvalidRecord);
            }
            _ => {}
        }
        let (kind, payload) = match self {
            Self::Config(c) => {
                c.validate()?;
                (1, serde_json::to_vec(c))
            }
            Self::Ready(c) => (2, serde_json::to_vec(c)),
            Self::Active(c) => (3, serde_json::to_vec(c)),
            Self::Close(c) => (4, serde_json::to_vec(c)),
            Self::Error(c) => (5, serde_json::to_vec(c)),
        };
        encode_record(kind, &payload.map_err(|_| NetworkError::InvalidRecord)?)
    }
    pub fn decode(record: &Record) -> Result<Self, NetworkError> {
        if record.payload.is_empty() || record.payload.len() > CONTROL_MAX {
            return Err(NetworkError::InvalidRecord);
        }
        let parse = |r: Result<SessionSignal, serde_json::Error>| {
            r.map_err(|_| NetworkError::InvalidRecord)
        };
        match record.kind {
            1 => {
                let config: SessionConfig = serde_json::from_slice(&record.payload)
                    .map_err(|_| NetworkError::InvalidRecord)?;
                config.validate()?;
                Ok(Self::Config(config))
            }
            2 => Ok(Self::Ready(parse(serde_json::from_slice(&record.payload))?)),
            3 => Ok(Self::Active(parse(serde_json::from_slice(
                &record.payload,
            ))?)),
            4 | 5 => {
                let value: SessionError = serde_json::from_slice(&record.payload)
                    .map_err(|_| NetworkError::InvalidRecord)?;
                let valid = if record.kind == 4 {
                    matches!(
                        value.error.as_str(),
                        "user_stop" | "mode_change" | "shutdown"
                    )
                } else {
                    matches!(
                        value.error.as_str(),
                        "invalid_record"
                            | "invalid_state"
                            | "invalid_configuration"
                            | "local_setup_failed"
                            | "network_unavailable"
                            | "timeout"
                            | "transport_lost"
                            | "forbidden"
                            | "overloaded"
                    )
                };
                if !valid {
                    return Err(NetworkError::InvalidRecord);
                }
                Ok(if record.kind == 4 {
                    Self::Close(value)
                } else {
                    Self::Error(value)
                })
            }
            _ => Err(NetworkError::InvalidRecord),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub kind: u8,
    pub payload: Vec<u8>,
    pub end_offset: u64,
}
pub fn encode_record(kind: u8, payload: &[u8]) -> Result<Vec<u8>, NetworkError> {
    let limit = match kind {
        1..=5 => CONTROL_MAX,
        16 => 65575,
        _ => return Err(NetworkError::InvalidRecord),
    };
    if payload.is_empty() || payload.len() > limit {
        return Err(NetworkError::InvalidRecord);
    }
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&[kind, 0, 0, 0]);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    Ok(out)
}
/// The parser admits bounded bytes before allocating complete record payloads.
pub struct RecordParser {
    bytes: VecDeque<u8>,
    offset: u64,
    received: u64,
    max_payload: usize,
    control: bool,
    capacity: usize,
}
impl RecordParser {
    pub fn new(control: bool, max_payload: usize, capacity: usize) -> Result<Self, NetworkError> {
        if max_payload == 0
            || max_payload > if control { CONTROL_MAX } else { 65575 }
            || max_payload.checked_add(8).is_none_or(|n| capacity < n)
            || capacity > 65536
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(Self {
            bytes: VecDeque::new(),
            offset: 0,
            received: 0,
            max_payload,
            control,
            capacity,
        })
    }
    pub fn push(&mut self, offset: u64, bytes: &[u8]) -> Result<(), NetworkError> {
        if offset != self.received || bytes.len() > self.capacity.saturating_sub(self.bytes.len()) {
            return Err(NetworkError::InvalidRecord);
        }
        self.received = self
            .received
            .checked_add(bytes.len() as u64)
            .ok_or(NetworkError::InvalidRecord)?;
        self.bytes.extend(bytes);
        self.header()?;
        Ok(())
    }
    fn header(&self) -> Result<Option<(u8, usize)>, NetworkError> {
        if self.bytes.len() < 8 {
            return Ok(None);
        }
        let mut h = [0; 8];
        for (target, byte) in h.iter_mut().zip(&self.bytes) {
            *target = *byte;
        }
        let length = u32::from_be_bytes(h[4..8].try_into().unwrap()) as usize;
        if h[1..4] != [0, 0, 0]
            || length == 0
            || length > self.max_payload
            || if self.control {
                !(1..=5).contains(&h[0])
            } else {
                h[0] != 16
            }
        {
            return Err(NetworkError::InvalidRecord);
        }
        Ok(Some((h[0], length)))
    }
    pub(crate) fn next_record_bytes(&self) -> Result<Option<usize>, NetworkError> {
        Ok(self
            .header()?
            .and_then(|(_, len)| (self.bytes.len() >= len + 8).then_some(len + 8)))
    }
    pub fn next_record(&mut self) -> Result<Option<Record>, NetworkError> {
        let Some((kind, len)) = self.header()? else {
            return Ok(None);
        };
        if self.bytes.len() < len + 8 {
            return Ok(None);
        }
        self.bytes.drain(..8);
        let payload = self.bytes.drain(..len).collect();
        self.offset = self
            .offset
            .checked_add((len + 8) as u64)
            .ok_or(NetworkError::InvalidRecord)?;
        Ok(Some(Record {
            kind,
            payload,
            end_offset: self.offset,
        }))
    }
    pub fn buffered_bytes(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rejection {
    pub v: u8,
    #[serde(rename = "type")]
    pub kind: String,
    pub error: String,
}
impl Rejection {
    pub fn decode(bytes: &[u8]) -> Result<Self, NetworkError> {
        let value = crate::local_api::parse_strict_json_bounded(bytes, METADATA_MAX)?;
        let r: Self = serde_json::from_value(value).map_err(|_| NetworkError::InvalidMetadata)?;
        if r.v != 2
            || !matches!(
                r.kind.as_str(),
                "tcp" | "ip-session" | "ip-data" | "unknown"
            )
            || !matches!(
                r.error.as_str(),
                "unsupported_version"
                    | "unsupported_type"
                    | "invalid_request"
                    | "forbidden"
                    | "overloaded"
                    | "unsupported_family"
                    | "network_unavailable"
                    | "timeout"
            )
        {
            return Err(NetworkError::InvalidMetadata);
        }
        Ok(r)
    }
    pub fn network_error(&self) -> NetworkError {
        match self.error.as_str() {
            "unsupported_version" => NetworkError::UnsupportedVersion,
            "unsupported_type" => NetworkError::UnsupportedType,
            "forbidden" => NetworkError::Forbidden,
            "overloaded" => NetworkError::Overloaded,
            "timeout" => NetworkError::Timeout,
            "network_unavailable" => {
                NetworkError::Runtime(skvoz_core::runtime::RuntimeError::PeerUnavailable)
            }
            _ => NetworkError::InvalidConfiguration,
        }
    }
}
