//! Destination policy shared with kernel-helper configuration.
use crate::{IpPrefix, config::ServerConfig};
use std::net::IpAddr;
pub const NON_PUBLIC: &[&str] = &[
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.88.99.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "100::/64",
    "2001::/23",
    "2001:db8::/32",
    "2002::/16",
    "3fff::/20",
    "fc00::/7",
    "fe80::/10",
    "ff00::/8",
];
pub const NEVER: &[&str] = &[
    "0.0.0.0/32",
    "224.0.0.0/4",
    "255.255.255.255/32",
    "::/128",
    "ff00::/8",
];
pub const MANDATORY: &[&str] = &[
    "0.0.0.0/8",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "::/128",
    "::1/128",
    "fe80::/10",
    "ff00::/8",
];
pub fn matches_any(prefixes: &[&str], ip: IpAddr) -> bool {
    prefixes
        .iter()
        .any(|p| p.parse::<IpPrefix>().is_ok_and(|p| p.contains(ip)))
}
fn protected_destination(
    config: &ServerConfig,
    ip: IpAddr,
    protocol: u8,
    port: Option<u16>,
) -> bool {
    ip.to_canonical() != ip
        || config
            .management_endpoints
            .iter()
            .any(|e| e.address == ip && e.protocol == protocol && port == Some(e.port))
        || config
            .ipv4
            .iter()
            .chain(config.ipv6.iter())
            .any(|b| b.pool.contains(ip))
        || config
            .service_prefixes
            .iter()
            .any(|p| p.prefix.contains(ip))
}
pub fn mandatory_denied(
    config: &ServerConfig,
    ip: IpAddr,
    protocol: u8,
    port: Option<u16>,
) -> bool {
    protected_destination(config, ip, protocol, port) || matches_any(MANDATORY, ip)
}
pub fn allowed(config: &ServerConfig, ip: IpAddr, protocol: u8, port: Option<u16>) -> bool {
    if mandatory_denied(config, ip, protocol, port)
        || config.deny.iter().any(|r| r.matches(ip, protocol, port))
    {
        return false;
    }
    permitted_destination(config, ip, protocol, port)
}
pub fn tcp_allowed(config: &ServerConfig, ip: IpAddr, port: u16) -> bool {
    // A native target socket may reach an explicitly allowed local service.
    // L3 packet destinations still use the unconditional loopback prohibition.
    if protected_destination(config, ip, 6, Some(port))
        || (!ip.is_loopback() && matches_any(MANDATORY, ip))
        || (ip.is_loopback()
            && config
                .management_endpoints
                .iter()
                .any(|e| e.address.is_loopback() && e.protocol == 6 && e.port == port))
        || config.deny.iter().any(|r| r.matches(ip, 6, Some(port)))
    {
        return false;
    }
    permitted_destination(config, ip, 6, Some(port))
}
fn permitted_destination(
    config: &ServerConfig,
    ip: IpAddr,
    protocol: u8,
    port: Option<u16>,
) -> bool {
    if config.allow.iter().any(|r| r.matches(ip, protocol, port)) {
        return true;
    }
    !config.server_addresses.contains(&ip)
        && !matches_any(NON_PUBLIC, ip)
        && match ip {
            IpAddr::V4(_) => true,
            IpAddr::V6(_) => "2000::/3".parse::<IpPrefix>().unwrap().contains(ip),
        }
}
