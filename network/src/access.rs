//! The full server's bounded local set of assigned device identities.
use crate::NetworkError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const DEVICE_MAX: usize = 128;
pub fn device_id(id: u64) -> bool {
    id > 0 && id <= i64::MAX as u64
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceSet {
    pub revision: u64,
    pub devices: Vec<u64>,
    pub revoke: Vec<u64>,
}
impl DeviceSet {
    pub fn validate(&self) -> Result<(), NetworkError> {
        if self.revision == 0
            || self.revision > i64::MAX as u64
            || [&self.devices, &self.revoke].into_iter().any(|ids| {
                ids.len() > DEVICE_MAX
                    || ids.iter().any(|id| !device_id(*id))
                    || ids.windows(2).any(|pair| pair[0] >= pair[1])
            })
            || self
                .devices
                .iter()
                .any(|id| self.revoke.binary_search(id).is_ok())
        {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(())
    }
    pub fn transition(&self, next: &Self) -> Result<(), NetworkError> {
        next.validate()?;
        if next.revision == self.revision && next == self {
            return Ok(());
        }
        let removed: Vec<_> = self
            .devices
            .iter()
            .copied()
            .filter(|id| next.devices.binary_search(id).is_err())
            .collect();
        if next.revision <= self.revision || next.revoke != removed {
            return Err(NetworkError::InvalidState);
        }
        Ok(())
    }
    pub(crate) fn peers(&self) -> BTreeSet<skvoz_core::PeerId> {
        self.devices
            .iter()
            .map(|id| skvoz_core::PeerId(*id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn update_requires_complete_revocation_and_monotonic_exact_replay() {
        let current = DeviceSet {
            revision: 1,
            devices: vec![1, 2],
            revoke: vec![],
        };
        let next = DeviceSet {
            revision: 2,
            devices: vec![2, 3],
            revoke: vec![1],
        };
        assert!(current.transition(&next).is_ok());
        assert!(next.transition(&next).is_ok());
        for bad in [
            DeviceSet {
                revoke: vec![],
                ..next.clone()
            },
            DeviceSet {
                revision: 1,
                ..next.clone()
            },
            DeviceSet {
                revoke: vec![1, 4],
                ..next.clone()
            },
        ] {
            assert!(current.transition(&bad).is_err());
        }
    }
    #[test]
    fn set_is_finite_sorted_disjoint_and_device_ids_never_include_server() {
        let valid = DeviceSet {
            revision: 1,
            devices: (1..=128).collect(),
            revoke: vec![],
        };
        assert!(valid.validate().is_ok());
        for ids in [
            vec![0],
            vec![1, 1],
            vec![2, 1],
            vec![1 << 63],
            (1..=129).collect(),
        ] {
            assert!(
                DeviceSet {
                    devices: ids,
                    ..valid.clone()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            DeviceSet {
                revoke: vec![1],
                ..valid
            }
            .validate()
            .is_err()
        );
    }
}
