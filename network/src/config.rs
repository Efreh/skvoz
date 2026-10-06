//! Shared strict startup and helper policy configuration.
use crate::{EngineConfig, IpPrefix, NetworkError, validate_families};
use serde::{Deserialize, Serialize};
use skvoz_core::runtime::{Authentication, Membership, RuntimeConfig, Trust};
use skvoz_core::{Config, ManagerConfig, PeerId};
use std::{collections::BTreeSet, net::IpAddr, path::PathBuf, time::Duration};
pub fn required_option<'de, D, T>(decoder: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(decoder)
}
pub fn canonical_ip<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<IpAddr, D::Error> {
    let raw = String::deserialize(decoder)?;
    parse_canonical_ip(&raw).map_err(serde::de::Error::custom)
}
pub fn canonical_ip_list<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Vec<IpAddr>, D::Error> {
    Vec::<String>::deserialize(decoder)?
        .into_iter()
        .map(|raw| parse_canonical_ip(&raw).map_err(serde::de::Error::custom))
        .collect()
}
fn parse_canonical_ip(raw: &str) -> Result<IpAddr, NetworkError> {
    let ip: IpAddr = raw
        .parse()
        .map_err(|_| NetworkError::InvalidConfiguration)?;
    if ip.to_canonical() != ip || ip.to_string() != raw {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(ip)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Client,
    Server,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupConfig {
    pub v: u8,
    pub role: Role,
    pub core: CoreConfig,
    pub network: NetworkConfig,
    #[serde(deserialize_with = "required_option")]
    pub server: Option<ServerConfig>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoreConfig {
    pub url: String,
    #[serde(deserialize_with = "required_option")]
    pub tls_server_name: Option<String>,
    pub trust: String,
    #[serde(deserialize_with = "required_option")]
    pub ca_file: Option<PathBuf>,
    pub username: String,
    pub password: String,
    pub namespace: String,
    pub peer_id: String,
    pub membership: String,
    pub allowed_peers: Vec<String>,
    pub initiate: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub families: Vec<u8>,
    pub max_mtu: u16,
    pub channels: u8,
    pub limits: Limits,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub ip_sessions: usize,
    pub core_streams: usize,
    pub streams_per_peer: usize,
    pub lease_identities: usize,
    pub receive_window: u32,
    pub max_frame: u32,
    pub core_receive_bytes: usize,
    pub core_receive_peer_bytes: usize,
    pub core_send_bytes: usize,
    pub core_send_peer_bytes: usize,
    pub packet_queue_bytes: usize,
    pub packet_queue_records: usize,
    pub control_queue_bytes: usize,
    pub control_queue_records: usize,
    pub runtime_buffer_bytes: usize,
    pub runtime_buffer_records: usize,
    pub api_queue_bytes: usize,
    pub api_queue_records: usize,
    pub subscription_frames: usize,
    pub join_frames: usize,
    pub client_frames: usize,
    pub core_shards: usize,
    pub flow_buckets: usize,
    pub setup_timeout_ms: u64,
    pub teardown_timeout_ms: u64,
    pub packet_io_timeout_ms: u64,
}
impl Limits {
    pub fn canonical(role: Role) -> Self {
        let server = role == Role::Server;
        Self {
            ip_sessions: if server { 128 } else { 1 },
            core_streams: if server { 2048 } else { 512 },
            streams_per_peer: 512,
            lease_identities: if server { 4096 } else { 1 },
            receive_window: 65536,
            max_frame: 16384,
            core_receive_bytes: if server { 134217728 } else { 33554432 },
            core_receive_peer_bytes: 33554432,
            core_send_bytes: if server { 67108864 } else { 2097152 },
            core_send_peer_bytes: 2097152,
            packet_queue_bytes: 262144,
            packet_queue_records: 256,
            control_queue_bytes: 32768,
            control_queue_records: 8,
            runtime_buffer_bytes: if server { 536870912 } else { 100663296 },
            runtime_buffer_records: if server { 131072 } else { 16384 },
            api_queue_bytes: 131072,
            api_queue_records: 128,
            subscription_frames: 64,
            join_frames: 32,
            client_frames: 16,
            core_shards: 8,
            flow_buckets: 1024,
            setup_timeout_ms: 15000,
            teardown_timeout_ms: 3000,
            packet_io_timeout_ms: 1000,
        }
    }
    pub fn validate(&self, role: Role) -> Result<(), NetworkError> {
        let c = Self::canonical(role);
        // Fixed transport/SEND values are part of the supported progress profile.
        if self.receive_window != c.receive_window
            || self.max_frame != c.max_frame
            || self.core_send_bytes != c.core_send_bytes
            || self.core_send_peer_bytes != c.core_send_peer_bytes
            || self.subscription_frames != 64
            || self.join_frames != 32
            || self.client_frames != 16
            || self.core_shards != 8
            || self.flow_buckets != 1024
            || self.setup_timeout_ms != 15000
            || self.teardown_timeout_ms != 3000
            || self.packet_io_timeout_ms != 1000
            || !(1..=c.ip_sessions).contains(&self.ip_sessions)
            || !(2..=c.core_streams).contains(&self.core_streams)
            || !(2..=c.streams_per_peer).contains(&self.streams_per_peer)
            || !(1..=c.lease_identities).contains(&self.lease_identities)
            || !(131072..=c.core_receive_bytes).contains(&self.core_receive_bytes)
            || !(131072..=c.core_receive_peer_bytes).contains(&self.core_receive_peer_bytes)
            || !(1508..=262144).contains(&self.packet_queue_bytes)
            || !(1..=256).contains(&self.packet_queue_records)
            || !(16392..=32768).contains(&self.control_queue_bytes)
            || !(1..=8).contains(&self.control_queue_records)
            || !(8388608..=c.runtime_buffer_bytes).contains(&self.runtime_buffer_bytes)
            || !(128..=c.runtime_buffer_records).contains(&self.runtime_buffer_records)
            || !(crate::local_api::BODY_MAX + 2048..=131072).contains(&self.api_queue_bytes)
            || !(6..=128).contains(&self.api_queue_records)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        if self.runtime_buffer_bytes < self.minimum_runtime_bytes(role, false)? {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
    pub fn fixed_backing(&self, role: Role, helper: bool) -> Result<usize, NetworkError> {
        let receive = self
            .core_streams
            .checked_mul(self.receive_window as usize)
            .map(|n| n.min(self.core_receive_bytes))
            .ok_or(NetworkError::InvalidConfiguration)?;
        self.transport_reservation(role)
            .checked_add(
                receive
                    .checked_mul(2)
                    .ok_or(NetworkError::InvalidConfiguration)?,
            )
            .and_then(|n| n.checked_add(self.core_send_bytes))
            .and_then(|n| n.checked_add(524288))
            .and_then(|n| {
                n.checked_add(if helper {
                    crate::local_api::BODY_MAX * 40
                } else {
                    0
                })
            })
            .ok_or(NetworkError::InvalidConfiguration)
    }
    /// Fixed backing plus one maximum command and a fully populated API queue.
    pub fn minimum_runtime_bytes(&self, role: Role, helper: bool) -> Result<usize, NetworkError> {
        self.fixed_backing(role, helper)?
            .checked_add(crate::local_api::BODY_MAX * 32 + 32768)
            .and_then(|n| n.checked_add(self.api_queue_bytes))
            .and_then(|n| n.checked_add(self.api_queue_records.checked_mul(256)?))
            .ok_or(NetworkError::InvalidConfiguration)
    }
    pub fn engine(&self) -> EngineConfig {
        EngineConfig {
            ip_sessions: self.ip_sessions,
            packet_queue_bytes: self.packet_queue_bytes,
            packet_queue_records: self.packet_queue_records,
            control_queue_bytes: self.control_queue_bytes,
            control_queue_records: self.control_queue_records,
        }
    }
    pub fn manager(&self, role: Role) -> ManagerConfig {
        ManagerConfig {
            stream: Config {
                receive_window: self.receive_window,
                max_frame: self.max_frame,
                max_pending_frames: 128,
                max_metadata: 512,
                open_timeout_ms: 15000,
            },
            max_peers: if role == Role::Server { 128 } else { 1 },
            max_streams: self.core_streams,
            max_streams_per_peer: self.streams_per_peer,
            receive_budget: self.core_receive_bytes,
            receive_budget_per_peer: self.core_receive_peer_bytes,
            send_budget: self.core_send_bytes,
            send_budget_per_peer: self.core_send_peer_bytes,
        }
    }
    pub fn transport_reservation(&self, role: Role) -> usize {
        let lanes = if role == Role::Server { 8 } else { 1 };
        (2 * self.subscription_frames * lanes + self.join_frames + self.client_frames * (lanes + 1))
            * 65588
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(deserialize_with = "required_option")]
    pub ipv4: Option<BackendConfig>,
    #[serde(deserialize_with = "required_option")]
    pub ipv6: Option<BackendConfig>,
    #[serde(deserialize_with = "canonical_ip_list")]
    pub dns_servers: Vec<IpAddr>,
    pub allow: Vec<PolicyRule>,
    pub deny: Vec<PolicyRule>,
    pub service_prefixes: Vec<ServicePrefix>,
    pub lease_store: PathBuf,
    #[serde(deserialize_with = "canonical_ip_list")]
    pub server_addresses: Vec<IpAddr>,
    pub management_endpoints: Vec<ManagementEndpoint>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub pool: IpPrefix,
    pub egress: String,
    pub interface: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyRule {
    pub cidr: IpPrefix,
    pub protocols: Protocols,
    #[serde(deserialize_with = "required_option")]
    pub ports: Option<Vec<u16>>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Protocols {
    Any(String),
    Numbers(Vec<u8>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServicePrefix {
    pub peer: String,
    pub prefix: IpPrefix,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementEndpoint {
    #[serde(deserialize_with = "canonical_ip")]
    pub address: IpAddr,
    pub protocol: u8,
    pub port: u16,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperPolicy {
    pub network: NetworkConfig,
    pub server: ServerConfig,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelperConfig {
    pub v: u8,
    pub role: Role,
    pub state_dir: PathBuf,
    #[serde(deserialize_with = "required_option")]
    pub policy: Option<HelperPolicy>,
}

pub fn parse_peer(value: &str) -> Result<PeerId, NetworkError> {
    let id: u64 = value
        .parse()
        .map_err(|_| NetworkError::InvalidConfiguration)?;
    if id.to_string() != value {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(PeerId(id))
}
pub fn validate_ifname(name: &str) -> Result<(), NetworkError> {
    if name.is_empty()
        || name.len() > 15
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
    {
        return Err(NetworkError::InvalidConfiguration);
    }
    Ok(())
}
impl NetworkConfig {
    pub fn validate(&self, role: Role) -> Result<(), NetworkError> {
        if !(role == Role::Server && self.families.is_empty()) {
            validate_families(&self.families)?;
        }
        if !(576..=1500).contains(&self.max_mtu)
            || self.families.contains(&6) && self.max_mtu < 1280
            || !(1..=8).contains(&self.channels)
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        self.limits.validate(role)?;
        if self.limits.runtime_buffer_bytes
            < self
                .limits
                .minimum_runtime_bytes(role, role == Role::Server && !self.families.is_empty())?
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
}
impl PolicyRule {
    pub fn validate(&self) -> Result<(), NetworkError> {
        match &self.protocols {
            Protocols::Any(v) if v == "any" && self.ports.is_none() => {}
            Protocols::Numbers(v)
                if !v.is_empty()
                    && v.len() <= 256
                    && v.iter().collect::<BTreeSet<_>>().len() == v.len() =>
            {
                if let Some(ports) = &self.ports
                    && (v.len() != 1
                        || !matches!(v[0], 6 | 17)
                        || ports.is_empty()
                        || ports.len() > 64
                        || ports.contains(&0)
                        || ports.iter().collect::<BTreeSet<_>>().len() != ports.len())
                {
                    return Err(NetworkError::InvalidConfiguration);
                }
            }
            _ => return Err(NetworkError::InvalidConfiguration),
        }
        Ok(())
    }
    pub fn matches(&self, ip: IpAddr, protocol: u8, port: Option<u16>) -> bool {
        self.cidr.contains(ip)
            && match &self.protocols {
                Protocols::Any(_) => true,
                Protocols::Numbers(v) => v.contains(&protocol),
            }
            && self
                .ports
                .as_ref()
                .is_none_or(|p| port.is_some_and(|n| p.contains(&n)))
    }
}
impl ServerConfig {
    pub fn validate(&self, network: &NetworkConfig) -> Result<(), NetworkError> {
        network.validate(Role::Server)?;
        let mut families = Vec::new();
        for (family, backend) in [(4, &self.ipv4), (6, &self.ipv6)] {
            if let Some(b) = backend {
                families.push(family);
                validate_ifname(&b.interface)?;
                if b.pool.family() != family
                    || (family == 4
                        && (!(16..=24).contains(&b.pool.bits)
                            || !matches!(b.egress.as_str(), "nat44" | "routed")))
                    || (family == 6 && (b.pool.bits != 64 || b.egress != "routed"))
                {
                    return Err(NetworkError::InvalidConfiguration);
                }
            }
        }
        if families != network.families
            || self.dns_servers.len() > 4
            || (!families.is_empty() && self.dns_servers.is_empty())
            || (families.is_empty() && !self.dns_servers.is_empty())
            || self.dns_servers.iter().any(|ip| {
                ip.to_canonical() != *ip || !families.contains(&if ip.is_ipv4() { 4 } else { 6 })
            })
            || self.allow.len() > 128
            || self.deny.len() > 128
            || self.service_prefixes.len() > 128
            || !self.lease_store.is_absolute()
            || self.server_addresses.len() > 32
            || self.management_endpoints.len() > 32
            || !families.is_empty()
                && (self.server_addresses.is_empty() || self.management_endpoints.is_empty())
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        for rule in self.allow.iter().chain(&self.deny) {
            rule.validate()?;
        }
        if self.server_addresses.iter().any(|a| a.to_canonical() != *a)
            || self.server_addresses.iter().collect::<BTreeSet<_>>().len()
                != self.server_addresses.len()
            || self.management_endpoints.iter().any(|e| {
                e.address.to_canonical() != e.address
                    || !matches!(e.protocol, 6 | 17)
                    || e.port == 0
            })
            || self
                .management_endpoints
                .iter()
                .map(|e| (e.address, e.protocol, e.port))
                .collect::<BTreeSet<_>>()
                .len()
                != self.management_endpoints.len()
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        let mut by_peer = std::collections::BTreeMap::new();
        for (i, s) in self.service_prefixes.iter().enumerate() {
            parse_peer(&s.peer)?;
            let n = by_peer.entry(&s.peer).or_insert(0);
            *n += 1;
            if *n > 8 {
                return Err(NetworkError::InvalidConfiguration);
            }
            if !families.contains(&s.prefix.family())
                || self.service_prefixes[..i]
                    .iter()
                    .any(|p| prefix_overlap(p.prefix, s.prefix))
                || self
                    .ipv4
                    .iter()
                    .chain(self.ipv6.iter())
                    .any(|b| prefix_overlap(b.pool, s.prefix))
            {
                return Err(NetworkError::InvalidConfiguration);
            }
        }
        Ok(())
    }
}
pub fn prefix_overlap(a: IpPrefix, b: IpPrefix) -> bool {
    a.contains(b.address) || b.contains(a.address)
}
impl HelperConfig {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let value = crate::local_api::parse_strict_json(bytes)?;
        let c: Self =
            serde_json::from_value(value).map_err(|_| NetworkError::InvalidConfiguration)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 || !self.state_dir.is_absolute() {
            return Err(NetworkError::InvalidConfiguration);
        }
        match (self.role, &self.policy) {
            (Role::Client, None) => Ok(()),
            (Role::Server, Some(p)) if !p.network.families.is_empty() => {
                p.server.validate(&p.network)
            }
            _ => Err(NetworkError::InvalidConfiguration),
        }
    }
}
impl StartupConfig {
    pub fn parse_json(bytes: &[u8]) -> Result<Self, NetworkError> {
        let value = crate::local_api::parse_strict_json(bytes)?;
        let config: Self =
            serde_json::from_value(value).map_err(|_| NetworkError::InvalidConfiguration)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.v != 1 {
            return Err(NetworkError::UnsupportedVersion);
        }
        self.network.validate(self.role)?;
        match (self.role, &self.server) {
            (Role::Client, None) => {}
            (Role::Server, Some(s)) => s.validate(&self.network)?,
            _ => return Err(NetworkError::InvalidConfiguration),
        }
        let c = &self.core;
        let id = parse_peer(&c.peer_id)?;
        if !c.url.starts_with("tls://")
            || c.url.len() > 2048
            || c.url.contains('@')
            || c.username.is_empty()
            || c.password.is_empty()
            || c.username.len() > 4096
            || c.password.len() > 4096
            || c.namespace.len() > 256
            || c.namespace.split('.').any(|t| {
                t.is_empty()
                    || t.len() > 64
                    || t.bytes()
                        .any(|b| !b.is_ascii_alphanumeric() && !b"_-".contains(&b))
            })
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        if c.tls_server_name.as_ref().is_some_and(|n| {
            crate::Metadata::Tcp {
                v: crate::NETWORK_VERSION,
                host: n.clone(),
                port: 443,
            }
            .encode()
            .is_err()
        }) {
            return Err(NetworkError::InvalidConfiguration);
        }
        match (c.trust.as_str(), &c.ca_file) {
            ("system", None) => {}
            ("managed_ca", Some(path)) if path.is_absolute() => {}
            _ => return Err(NetworkError::InvalidConfiguration),
        }
        if c.allowed_peers.len() > 128 || c.initiate.len() > 128 {
            return Err(NetworkError::InvalidConfiguration);
        }
        for p in c.allowed_peers.iter().chain(&c.initiate) {
            parse_peer(p)?;
        }
        if c.allowed_peers.iter().collect::<BTreeSet<_>>().len() != c.allowed_peers.len()
            || c.initiate.iter().collect::<BTreeSet<_>>().len() != c.initiate.len()
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        match self.role {
            Role::Client => {
                if id == PeerId(0)
                    || c.membership != "allowlist"
                    || c.allowed_peers != ["0"]
                    || c.initiate != ["0"]
                {
                    return Err(NetworkError::InvalidConfiguration);
                }
            }
            Role::Server => {
                if id != PeerId(0)
                    || c.membership != "broker_authorized"
                    || !c.allowed_peers.is_empty()
                    || !c.initiate.is_empty()
                {
                    return Err(NetworkError::InvalidConfiguration);
                }
            }
        }
        Ok(())
    }
    pub fn core_runtime(&self) -> Result<RuntimeConfig, NetworkError> {
        self.validate()?;
        let c = &self.core;
        Ok(RuntimeConfig {
            url: c.url.clone(),
            tls_server_name: c.tls_server_name.clone(),
            trust: if c.trust == "system" {
                Trust::System
            } else {
                Trust::ManagedCa(
                    c.ca_file
                        .clone()
                        .ok_or(NetworkError::InvalidConfiguration)?,
                )
            },
            authentication: Authentication {
                username: c.username.clone(),
                password: c.password.clone(),
            },
            namespace: c.namespace.clone(),
            id: parse_peer(&c.peer_id)?,
            membership: if c.membership == "broker_authorized" {
                Membership::BrokerAuthorized
            } else {
                Membership::Allowlist(
                    c.allowed_peers
                        .iter()
                        .map(|p| parse_peer(p))
                        .collect::<Result<_, _>>()?,
                )
            },
            initiate: c
                .initiate
                .iter()
                .map(|p| parse_peer(p))
                .collect::<Result<_, _>>()?,
            shards: 8,
            subscription_capacity: 64,
            join_capacity: 32,
            client_capacity: 16,
            // The shared server drains aggregate peer tails between native chunks.
            max_incoming_per_turn: if self.role == Role::Server { 256 } else { 32 },
            max_outgoing_per_turn: 32,
            io_timeout: Duration::from_secs(3),
            heartbeat_interval: Duration::from_secs(2),
            peer_timeout: Duration::from_secs(10),
            join_timeout: Duration::from_secs(10),
            retry_initial: Duration::from_millis(250),
            retry_max: Duration::from_secs(5),
            max_retries: 24,
            terminal_drain_timeout: Duration::from_secs(3),
        })
    }
}
