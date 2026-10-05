use crate::{HelperError, Result};
use serde::{Deserialize, Serialize};
use skvoz_network::{
    IpPrefix,
    config::{NetworkConfig, ServerConfig, parse_peer, prefix_overlap},
};
use skvoz_network_native::SecureStateDir;
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

pub const REGISTRY_MAX: usize = 8 * 1024 * 1024;
const MARKER: &[u8] = b"SKVOZ network lease registry 1\n";

/// Storage implementations must atomically replace and fsync file+parent before
/// returning. Production uses an exclusive locked, root-owned directory anchor.
pub trait Store {
    fn read(&self, name: &str, cap: usize) -> std::io::Result<Option<Vec<u8>>>;
    fn replace(&self, name: &str, body: &[u8]) -> std::io::Result<()>;
    fn entries(&self) -> std::io::Result<Vec<String>>;
}
impl Store for SecureStateDir {
    fn read(&self, name: &str, cap: usize) -> std::io::Result<Option<Vec<u8>>> {
        self.read(name, cap)
    }
    fn replace(&self, name: &str, body: &[u8]) -> std::io::Result<()> {
        self.replace(name, body)
    }
    fn entries(&self) -> std::io::Result<Vec<String>> {
        self.entries()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub grants: Vec<IpPrefix>,
    pub tombstone: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    pub v: u8,
    pub pools: Vec<IpPrefix>,
    pub peers: BTreeMap<String, Lease>,
}

impl Registry {
    pub fn load_or_initialize(
        store: &impl Store,
        name: &str,
        server: &ServerConfig,
    ) -> Result<Self> {
        let pools = server
            .ipv4
            .iter()
            .chain(server.ipv6.iter())
            .map(|b| b.pool)
            .collect::<Vec<_>>();
        let marker = store.read("initialized", 128)?;
        let body = store.read(name, REGISTRY_MAX)?;
        match (marker, body) {
            (None, None) => {
                if store.entries()?.iter().any(|n| n != "lock") {
                    return Err(HelperError::LeaseStoreInvalid);
                }
                let registry = Self {
                    v: 1,
                    pools,
                    peers: BTreeMap::new(),
                };
                registry.persist(store, name)?;
                store.replace("initialized", MARKER)?;
                Ok(registry)
            }
            (Some(m), Some(bytes)) if m == MARKER => {
                // A custom visitor rejects duplicate map keys even in the
                // persistent trust boundary; serde's map alone would overwrite.
                let value =
                    skvoz_network::local_api::parse_strict_json_bounded(&bytes, REGISTRY_MAX)
                        .map_err(|_| HelperError::LeaseStoreInvalid)?;
                let registry: Self =
                    serde_json::from_value(value).map_err(|_| HelperError::LeaseStoreInvalid)?;
                registry.validate(server)?;
                if registry.pools != pools {
                    return Err(HelperError::LeaseStoreInvalid);
                }
                Ok(registry)
            }
            _ => Err(HelperError::LeaseStoreInvalid),
        }
    }

    pub fn validate(&self, server: &ServerConfig) -> Result<()> {
        if self.v != 1 || self.peers.len() > 4096 {
            return Err(HelperError::LeaseStoreInvalid);
        }
        let mut all = Vec::new();
        for (peer, lease) in &self.peers {
            if parse_peer(peer).is_err()
                || peer == "0"
                || lease.grants.is_empty()
                || lease.grants.len() > 8
            {
                return Err(HelperError::LeaseStoreInvalid);
            }
            let mut normal_families = std::collections::BTreeSet::new();
            for grant in &lease.grants {
                let normal = self.pools.iter().any(|p| {
                    p.contains(grant.address)
                        && grant.bits == if grant.family() == 4 { 32 } else { 128 }
                });
                let service = server
                    .service_prefixes
                    .iter()
                    .any(|s| s.peer == *peer && s.prefix == *grant);
                if (!normal && !service) || normal && !normal_families.insert(grant.family()) {
                    return Err(HelperError::LeaseStoreInvalid);
                }
                // Never accept the network, reserved gateway or IPv4 broadcast.
                if normal
                    && self
                        .pools
                        .iter()
                        .any(|p| p.contains(grant.address) && reserved(*p, grant.address))
                {
                    return Err(HelperError::LeaseStoreInvalid);
                }
                all.push(*grant);
            }
        }
        all.sort();
        if all.windows(2).any(|pair| prefix_overlap(pair[0], pair[1])) {
            return Err(HelperError::LeaseStoreInvalid);
        }
        Ok(())
    }

    pub fn reserve(
        &mut self,
        store: &impl Store,
        name: &str,
        server: &ServerConfig,
        network: &NetworkConfig,
        peer: &str,
        families: &[u8],
    ) -> Result<Vec<IpPrefix>> {
        if peer == "0"
            || parse_peer(peer).is_err()
            || skvoz_network::validate_families(families).is_err()
            || families.iter().any(|f| !network.families.contains(f))
        {
            return Err(HelperError::InvalidRequest);
        }
        if !self.peers.contains_key(peer)
            && self.peers.len() >= network.limits.lease_identities.min(4096)
        {
            return Err(HelperError::Overloaded);
        }
        let mut next = self.clone();
        let mut grants = next
            .peers
            .get(peer)
            .map(|l| l.grants.clone())
            .unwrap_or_default();
        for family in families {
            let pool = next
                .pools
                .iter()
                .find(|p| p.family() == *family)
                .ok_or(HelperError::InvalidRequest)?;
            if !grants
                .iter()
                .any(|g| g.family() == *family && pool.contains(g.address))
            {
                let address = allocate(
                    *pool,
                    next.peers
                        .values()
                        .flat_map(|l| l.grants.iter())
                        .chain(grants.iter()),
                )?;
                grants.push(IpPrefix {
                    address,
                    bits: if *family == 4 { 32 } else { 128 },
                });
            }
        }
        for service in server
            .service_prefixes
            .iter()
            .filter(|s| s.peer == peer && families.contains(&s.prefix.family()))
        {
            if !grants.contains(&service.prefix) {
                grants.push(service.prefix)
            }
        }
        if grants.len() > 8 {
            return Err(HelperError::Overloaded);
        }
        grants.sort();
        next.peers.insert(
            peer.to_owned(),
            Lease {
                grants: grants.clone(),
                tombstone: false,
            },
        );
        next.validate(server)?;
        next.persist(store, name)?;
        *self = next;
        Ok(grants
            .into_iter()
            .filter(|g| families.contains(&g.family()))
            .collect())
    }

    pub fn tombstone(&mut self, store: &impl Store, name: &str, peer: &str) -> Result<()> {
        let mut next = self.clone();
        next.peers
            .get_mut(peer)
            .ok_or(HelperError::InvalidState)?
            .tombstone = true;
        next.persist(store, name)?;
        *self = next;
        Ok(())
    }
    fn persist(&self, store: &impl Store, name: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self).map_err(|_| HelperError::LeaseStoreInvalid)?;
        if bytes.len() > REGISTRY_MAX {
            return Err(HelperError::Overloaded);
        }
        store.replace(name, &bytes)?;
        Ok(())
    }
}

fn reserved(pool: IpPrefix, address: IpAddr) -> bool {
    match (pool.address, address) {
        (IpAddr::V4(p), IpAddr::V4(a)) => {
            let n = u32::from(a) - u32::from(p);
            n <= 1 || n == (1u32 << (32 - pool.bits)) - 1
        }
        (IpAddr::V6(p), IpAddr::V6(a)) => u128::from(a) - u128::from(p) <= 1,
        _ => true,
    }
}
fn allocate<'a>(pool: IpPrefix, used: impl Iterator<Item = &'a IpPrefix>) -> Result<IpAddr> {
    let base = match pool.address {
        IpAddr::V4(a) => u32::from(a) as u128,
        IpAddr::V6(a) => u128::from(a),
    };
    let capacity = if pool.family() == 4 {
        (1u128 << (32 - pool.bits)) - 1
    } else {
        4098
    };
    let end = base + capacity;
    let mut intervals = used
        .filter(|p| p.family() == pool.family() && prefix_overlap(pool, **p))
        .map(|p| {
            let start = match p.address {
                IpAddr::V4(a) => u32::from(a) as u128,
                IpAddr::V6(a) => u128::from(a),
            };
            let width = if p.family() == 4 {
                32 - p.bits
            } else {
                128 - p.bits
            };
            let last = start
                | if width == 128 {
                    u128::MAX
                } else {
                    (1u128 << width) - 1
                };
            (start, last)
        })
        .collect::<Vec<_>>();
    intervals.sort_unstable();
    let mut candidate = base + 2;
    for (first, last) in intervals {
        if last < candidate {
            continue;
        }
        if first > candidate {
            break;
        }
        candidate = last.checked_add(1).ok_or(HelperError::Overloaded)?;
        if candidate >= end {
            return Err(HelperError::Overloaded);
        }
    }
    if candidate >= end {
        return Err(HelperError::Overloaded);
    }
    Ok(if pool.family() == 4 {
        IpAddr::V4(Ipv4Addr::from(candidate as u32))
    } else {
        IpAddr::V6(Ipv6Addr::from(candidate))
    })
}
