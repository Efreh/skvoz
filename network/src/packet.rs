use crate::NetworkError;
use serde::{Deserialize, Serialize};
use std::{net::IpAddr, str::FromStr};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IpPrefix {
    pub address: IpAddr,
    pub bits: u8,
}
impl IpPrefix {
    pub fn contains(self, address: IpAddr) -> bool {
        if self.bits > if self.address.is_ipv4() { 32 } else { 128 } {
            return false;
        }
        match (self.address, address) {
            (IpAddr::V4(a), IpAddr::V4(b)) => {
                mask32(u32::from(a), self.bits) == mask32(u32::from(b), self.bits)
            }
            (IpAddr::V6(a), IpAddr::V6(b)) => {
                mask128(u128::from(a), self.bits) == mask128(u128::from(b), self.bits)
            }
            _ => false,
        }
    }
    pub fn family(self) -> u8 {
        if self.address.is_ipv4() { 4 } else { 6 }
    }
}
fn mask32(value: u32, bits: u8) -> u32 {
    if bits == 0 {
        0
    } else {
        value & (u32::MAX << (32 - bits))
    }
}
fn mask128(value: u128, bits: u8) -> u128 {
    if bits == 0 {
        0
    } else {
        value & (u128::MAX << (128 - bits))
    }
}
impl FromStr for IpPrefix {
    type Err = NetworkError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (host, bits) = value
            .split_once('/')
            .ok_or(NetworkError::InvalidConfiguration)?;
        let address: IpAddr = host
            .parse()
            .map_err(|_| NetworkError::InvalidConfiguration)?;
        let bits: u8 = bits
            .parse()
            .map_err(|_| NetworkError::InvalidConfiguration)?;
        let max = if address.is_ipv4() { 32 } else { 128 };
        if bits > max {
            return Err(NetworkError::InvalidConfiguration);
        }
        let result = Self { address, bits };
        let canonical = match address {
            IpAddr::V4(a) => u32::from(a) == mask32(u32::from(a), bits),
            IpAddr::V6(a) => u128::from(a) == mask128(u128::from(a), bits),
        };
        if !canonical || String::from(result) != value || address.to_canonical() != address {
            return Err(NetworkError::InvalidConfiguration);
        }
        Ok(result)
    }
}
impl TryFrom<String> for IpPrefix {
    type Error = NetworkError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}
impl From<IpPrefix> for String {
    fn from(p: IpPrefix) -> Self {
        format!("{}/{}", p.address, p.bits)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PacketInfo {
    pub family: u8,
    pub source: IpAddr,
    pub destination: IpAddr,
    pub protocol: u8,
}
pub fn validate_packet(
    packet: &[u8],
    mtu: u16,
    families: &[u8],
) -> Result<PacketInfo, NetworkError> {
    if packet.is_empty() || packet.len() > usize::from(mtu) {
        return Err(NetworkError::InvalidPacket);
    }
    let family = packet[0] >> 4;
    if !families.contains(&family) {
        return Err(NetworkError::InvalidPacket);
    }
    match family {
        4 => {
            if packet.len() < 20 {
                return Err(NetworkError::InvalidPacket);
            }
            let header = usize::from(packet[0] & 15) * 4;
            if !(20..=60).contains(&header)
                || header > packet.len()
                || usize::from(u16::from_be_bytes([packet[2], packet[3]])) != packet.len()
            {
                return Err(NetworkError::InvalidPacket);
            }
            let mut sum: u32 = packet[..header]
                .chunks_exact(2)
                .map(|b| u32::from(u16::from_be_bytes([b[0], b[1]])))
                .sum();
            while sum > 65535 {
                sum = (sum & 65535) + (sum >> 16);
            }
            if sum != 65535 {
                return Err(NetworkError::InvalidPacket);
            }
            Ok(PacketInfo {
                family,
                source: IpAddr::V4([packet[12], packet[13], packet[14], packet[15]].into()),
                destination: IpAddr::V4([packet[16], packet[17], packet[18], packet[19]].into()),
                protocol: packet[9],
            })
        }
        6 => {
            if packet.len() < 40
                || usize::from(u16::from_be_bytes([packet[4], packet[5]])) + 40 != packet.len()
            {
                return Err(NetworkError::InvalidPacket);
            }
            Ok(PacketInfo {
                family,
                source: IpAddr::V6(<[u8; 16]>::try_from(&packet[8..24]).unwrap().into()),
                destination: IpAddr::V6(<[u8; 16]>::try_from(&packet[24..40]).unwrap().into()),
                protocol: packet[6],
            })
        }
        _ => Err(NetworkError::InvalidPacket),
    }
}
/// Stable address-only channel selection also keeps IP fragments together.
pub fn packet_channel(info: PacketInfo, seed: u64, channels: u8) -> Result<u8, NetworkError> {
    if !(1..=8).contains(&channels) {
        return Err(NetworkError::InvalidConfiguration);
    }
    let mut hash = seed ^ u64::from(info.family);
    for address in [info.source, info.destination] {
        let octets = match address {
            IpAddr::V4(a) => {
                let mut octets = [0; 16];
                octets[..4].copy_from_slice(&a.octets());
                (octets, 4)
            }
            IpAddr::V6(a) => (a.octets(), 16),
        };
        for b in &octets.0[..octets.1] {
            hash = (hash ^ u64::from(*b)).wrapping_mul(0x100000001b3);
        }
    }
    Ok((hash % u64::from(channels)) as u8)
}

#[cfg(test)]
mod hash_tests {
    use super::*;
    #[test]
    fn allocation_free_address_hash_preserves_both_family_vectors() {
        for (family, source, destination, expected) in [
            (4, "192.0.2.10", "198.51.100.10", [0, 1, 2, 3, 1, 5, 4, 3]),
            (
                6,
                "2001:db8::10",
                "2001:db8:1::20",
                [0, 1, 2, 3, 2, 5, 1, 3],
            ),
        ] {
            let info = PacketInfo {
                family,
                source: source.parse().unwrap(),
                destination: destination.parse().unwrap(),
                protocol: 6,
            };
            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(packet_channel(info, 42, index as u8 + 1).unwrap(), expected);
                // Protocol/fragment headers are deliberately absent from hash.
                assert_eq!(
                    packet_channel(
                        PacketInfo {
                            protocol: 143,
                            ..info
                        },
                        42,
                        index as u8 + 1
                    )
                    .unwrap(),
                    expected
                );
            }
        }
    }
}
