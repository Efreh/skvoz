//! Addressed routing control. Application bytes remain in the network engine.
mod state;
#[cfg(feature = "portable-runtime")]
pub(crate) mod transport;
use crate::{Metadata, NetworkError, SessionId};
use serde::{Deserialize, Serialize};
pub use state::*;

pub const ROUTE_VERSION: u8 = 1;
pub const MESSAGE_MAX: usize = 4096;
pub const DEVICE_MAX: usize = 128;
pub const NODE_MAX: usize = 8;
pub const MEMBERS_PER_CHUNK: usize = 48;
pub const LEASE_MS: u64 = 6000;
pub const FENCE_MS: u64 = 9000;
pub const RENEW_MS: u64 = 2000;
pub const RESERVATION_MS: u64 = 15000;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub authority: SessionId,
    pub device: u64,
    pub epoch: SessionId,
    pub sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub authority: SessionId,
    pub sequence: u64,
    pub token: SessionId,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assignment {
    pub attempt: Attempt,
    pub owner: u64,
    pub owner_epoch: SessionId,
    pub token: SessionId,
}
impl Assignment {
    pub fn admission(&self) -> Admission {
        Admission {
            authority: self.attempt.authority.clone(),
            sequence: self.attempt.sequence,
            token: self.token.clone(),
        }
    }
}
pub fn epoch(value: u128) -> SessionId {
    SessionId::try_from(format!("{value:032x}")).expect("fixed epoch encoding")
}
pub fn epoch_number(value: &SessionId) -> u128 {
    u128::from_str_radix(value.as_str(), 16).expect("validated epoch")
}
pub fn device_id(value: u64) -> bool {
    value > 0 && value < 1 << 63
}
pub fn node_id(value: u64) -> bool {
    value > 1 << 63 && value.is_multiple_of(8)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    pub revision: u64,
    pub devices: Vec<u64>,
    pub nodes: Vec<u64>,
}
impl Registry {
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.devices.len() > DEVICE_MAX
            || self.nodes.len() > NODE_MAX
            || self.devices.iter().any(|id| !device_id(*id))
            || self.nodes.iter().any(|id| !node_id(*id))
            || self
                .devices
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.devices.len()
            || self
                .nodes
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.nodes.len()
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
}
/// Compact tuples bound three chunks of 48 rows below the route message limit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member(pub u64, pub SessionId, pub u64);
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Body {
    ClientLease,
    NodeLease {
        families: Vec<u8>,
        tcp: usize,
        vpn: usize,
        revision: u64,
        watermark: u64,
        membership: u64,
    },
    Lease {
        authority: SessionId,
        remaining_ms: u64,
    },
    Snapshot {
        authority: SessionId,
        node_epoch: SessionId,
        revision: u64,
        part: u8,
        parts: u8,
        members: Vec<Member>,
    },
    Reserve {
        attempt: Attempt,
        target: Metadata,
    },
    Dispatch {
        command: u64,
        assignment: Assignment,
        target: Metadata,
    },
    Reserved {
        command: u64,
        assignment: Assignment,
        error: Option<String>,
    },
    Assigned {
        assignment: Assignment,
    },
    Cancel {
        attempt: Attempt,
    },
    CancelOwner {
        command: u64,
        attempt: Attempt,
    },
    Query {
        command: u64,
        attempt: Attempt,
    },
    Terminal {
        attempt: Attempt,
        watermark: u64,
        absent: bool,
    },
    Refused {
        error: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub v: u8,
    pub from: u64,
    pub epoch: SessionId,
    pub request: u64,
    pub body: Body,
}
impl Envelope {
    pub fn encode(&self) -> Result<Vec<u8>, NetworkError> {
        let bytes = serde_json::to_vec(self).map_err(|_| NetworkError::InvalidRecord)?;
        Self::decode(&bytes)?;
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, NetworkError> {
        let value = crate::local_api::parse_strict_json_bounded(bytes, MESSAGE_MAX)?;
        let envelope: Self =
            serde_json::from_value(value).map_err(|_| NetworkError::InvalidRecord)?;
        if envelope.v != ROUTE_VERSION {
            return Err(NetworkError::UnsupportedVersion);
        }
        if epoch_number(&envelope.epoch) == 0 || envelope.request == 0 {
            return Err(NetworkError::InvalidRecord);
        }
        match &envelope.body {
            Body::Reserve { target, .. } | Body::Dispatch { target, .. } => {
                target.encode()?;
                if !matches!(target, Metadata::Tcp { .. } | Metadata::IpSession { .. }) {
                    return Err(NetworkError::InvalidMetadata);
                }
            }
            Body::Snapshot {
                parts,
                part,
                members,
                ..
            } if *parts == 0
                || *parts > 3
                || *part >= *parts
                || members.len() > MEMBERS_PER_CHUNK =>
            {
                return Err(NetworkError::InvalidRecord);
            }
            Body::Lease { remaining_ms, .. } if *remaining_ms > LEASE_MS => {
                return Err(NetworkError::InvalidRecord);
            }
            _ => {}
        }
        Ok(envelope)
    }
}
