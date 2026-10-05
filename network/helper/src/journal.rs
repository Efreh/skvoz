use crate::{HelperError, Result, registry::Store};
use serde::{Deserialize, Serialize};
use skvoz_network::IpPrefix;

/// Only helper-generated object identities and validated grants are durable.
/// Intent is persisted before effects. A journal is not proof that a foreign
/// object of the same name is ours: recovery also checks the kernel owner tag.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Journal {
    pub v: u8,
    pub token: String,
    pub interface: String,
    pub prepared: bool,
    pub guarded: bool,
    pub peers: Vec<JournalPeer>,
    #[serde(deserialize_with = "required_option")]
    pub operation: Option<String>,
    pub namespace_device: u64,
    pub namespace_inode: u64,
    pub route_table: u32,
    #[serde(deserialize_with = "required_option")]
    pub client: Option<skvoz_network::local_api::PrepareClientArgs>,
    #[serde(deserialize_with = "required_option")]
    pub restored_handle: Option<skvoz_network::SessionId>,
    pub settings: Vec<NamespaceSetting>,
    pub underlay_routes: Vec<UnderlayRoute>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceSetting {
    pub name: String,
    pub previous: String,
    pub applied: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnderlayRoute {
    pub address: std::net::IpAddr,
    pub interface: String,
    #[serde(deserialize_with = "required_option")]
    pub gateway: Option<std::net::IpAddr>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalPeer {
    pub peer: String,
    pub session: String,
    pub grants: Vec<IpPrefix>,
    pub mtu: u16,
    pub active: bool,
}
impl Journal {
    pub fn fresh() -> Result<Self> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(|_| HelperError::InvalidState)?;
        Ok(Self {
            v: 2,
            namespace_device: namespace()?.0,
            namespace_inode: namespace()?.1,
            route_table: 100_000
                + u32::from_be_bytes(bytes[..4].try_into().expect("four bytes")) % 1_000_000_000,
            token: bytes.iter().map(|b| format!("{b:02x}")).collect(),
            interface: "skvoz0".into(),
            ..Self::default()
        })
    }
    pub fn load(store: &impl Store) -> Result<Option<Self>> {
        let Some(bytes) = store.read("journal.json", 1024 * 1024)? else {
            return Ok(None);
        };
        let value = skvoz_network::local_api::parse_strict_json_bounded(&bytes, 1024 * 1024)
            .map_err(|_| HelperError::LeaseStoreInvalid)?;
        let j: Self = serde_json::from_value(value).map_err(|_| HelperError::LeaseStoreInvalid)?;
        let mut peers = std::collections::BTreeSet::new();
        let mut sessions = std::collections::BTreeSet::new();
        let mut endpoints = std::collections::BTreeSet::new();
        if j.v != 2
            || j.namespace_device == 0
            || j.namespace_inode == 0
            || !(100_000..1_000_100_000).contains(&j.route_table)
            || !j.settings.is_empty()
            || j.underlay_routes.len() > 32
            || j.client.as_ref().is_some_and(|c| {
                c.transport_endpoints
                    .iter()
                    .any(|e| !j.underlay_routes.iter().any(|r| r.address == e.ip))
            })
            || j.underlay_routes.iter().any(|r| {
                if !endpoints.insert(r.address) {
                    return true;
                }
                skvoz_network::config::validate_ifname(&r.interface).is_err()
                    || r.interface == "skvoz0"
                    || r.address.to_canonical() != r.address
                    || r.gateway.is_some_and(|g| {
                        g.to_canonical() != g || g.is_ipv4() != r.address.is_ipv4()
                    })
                    || !j
                        .client
                        .as_ref()
                        .is_some_and(|c| c.transport_endpoints.iter().any(|e| e.ip == r.address))
            })
            || j.client.as_ref().is_some_and(|a| {
                skvoz_network::local_api::HelperRequest::new(
                    1,
                    skvoz_network::local_api::HelperOperation::PrepareClient,
                    serde_json::to_value(a).unwrap_or_default(),
                )
                .is_err()
            })
            || j.operation.as_deref().is_some_and(|op| {
                ![
                    "cleanup",
                    "prepare_server",
                    "activate_peer",
                    "retire_peer",
                    "prepare_client",
                    "activate_client",
                    "abort_client",
                    "restore_client",
                ]
                .contains(&op)
            })
            || j.token.len() != 32
            || !j
                .token
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || j.interface != "skvoz0"
            || j.peers.len() > 128
            || j.peers.iter().any(|p| {
                !peers.insert(&p.peer)
                    || !sessions.insert(&p.session)
                    || p.peer == "0"
                    || p.grants.is_empty()
                    || p.grants.len() > 8
                    || !(576..=1500).contains(&p.mtu)
                    || p.grants.iter().any(|g| g.family() == 6) && p.mtu < 1280
                    || {
                        let mut grants = p.grants.clone();
                        grants.sort();
                        grants
                            .windows(2)
                            .any(|w| skvoz_network::config::prefix_overlap(w[0], w[1]))
                    }
                    || skvoz_network::config::parse_peer(&p.peer).is_err()
                    || p.session.len() != 32
                    || !p
                        .session
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        {
            return Err(HelperError::LeaseStoreInvalid);
        }
        let mut grants = j
            .peers
            .iter()
            .flat_map(|p| p.grants.iter().copied())
            .collect::<Vec<_>>();
        grants.sort();
        if grants
            .windows(2)
            .any(|w| skvoz_network::config::prefix_overlap(w[0], w[1]))
        {
            return Err(HelperError::LeaseStoreInvalid);
        }
        Ok(Some(j))
    }
    pub fn persist(&self, store: &impl Store) -> Result<()> {
        let body = serde_json::to_vec(self).map_err(|_| HelperError::LeaseStoreInvalid)?;
        if body.len() > 1024 * 1024 {
            return Err(HelperError::Overloaded);
        }
        store.replace("journal.json", &body)?;
        Ok(())
    }
}

pub fn namespace() -> Result<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata("/proc/self/ns/net")?;
    Ok((metadata.dev(), metadata.ino()))
}

fn required_option<'de, D: serde::Deserializer<'de>, T: serde::Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}
