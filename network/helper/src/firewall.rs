//! Generated nft grammar contains constants and validated numeric/config data.
use crate::{HelperError, Result, journal::Journal};
use skvoz_network::{
    IpPrefix,
    config::{HelperPolicy, PolicyRule, Protocols},
    policy::{MANDATORY, NON_PUBLIC},
};

fn address_match(prefix: IpPrefix) -> String {
    format!(
        "{} daddr {}",
        if prefix.family() == 4 { "ip" } else { "ip6" },
        String::from(prefix)
    )
}
fn rule_match(rule: &PolicyRule) -> String {
    let mut text = address_match(rule.cidr);
    if let Protocols::Numbers(numbers) = &rule.protocols {
        text.push_str(&format!(
            " meta l4proto {{ {} }}",
            numbers
                .iter()
                .map(u8::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(ports) = &rule.ports {
        text.push_str(&format!(
            " {} dport {{ {} }}",
            if matches!(&rule.protocols,Protocols::Numbers(n)if n==&[6]) {
                "tcp"
            } else {
                "udp"
            },
            ports
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    text
}
pub fn server_rules(policy: &HelperPolicy, journal: &Journal) -> Result<String> {
    policy
        .server
        .validate(&policy.network)
        .map_err(|_| HelperError::InvalidRequest)?;
    if journal.token.len() != 32
        || !journal
            .token
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        || journal.interface != "skvoz0"
    {
        return Err(HelperError::InvalidState);
    }
    let mut text = format!(
        "table inet skvoz_network {{\n comment \"skvoz:{}\"\n",
        journal.token
    );
    text.push_str(" set live4 { type ipv4_addr; flags interval; size 4096; }\n set live6 { type ipv6_addr; flags interval; size 4096; }\n");
    text.push_str(" set flows4 { type ipv4_addr; flags dynamic; size 32768; }\n set flows6 { type ipv6_addr; flags dynamic; size 32768; }\n");
    text.push_str(" chain limits {\n ct count over 65536 drop\n meta nfproto ipv4 add @flows4 { ip saddr ct count over 1024 } drop\n meta nfproto ipv4 ip saddr != @flows4 drop\n meta nfproto ipv6 add @flows6 { ip6 saddr ct count over 1024 } drop\n meta nfproto ipv6 ip6 saddr != @flows6 drop\n }\n");
    text.push_str(" chain admission {\n meta nfproto ipv4 ip saddr != @live4 drop\n meta nfproto ipv6 ip6 saddr != @live6 drop\n ct state invalid drop\n");
    // Destination management and all mandatory exclusions precede established
    // shortcuts and explicit allows, including old conntrack after retirement.
    for endpoint in &policy.server.management_endpoints {
        text.push_str(&format!(
            " {} daddr {} meta l4proto {} {} dport {} drop\n",
            if endpoint.address.is_ipv4() {
                "ip"
            } else {
                "ip6"
            },
            endpoint.address,
            endpoint.protocol,
            if endpoint.protocol == 6 { "tcp" } else { "udp" },
            endpoint.port
        ));
    }
    for prefix in MANDATORY
        .iter()
        .map(|s| s.parse::<IpPrefix>().expect("static prefix"))
        .chain(
            policy
                .server
                .ipv4
                .iter()
                .chain(policy.server.ipv6.iter())
                .map(|b| b.pool),
        )
        .chain(policy.server.service_prefixes.iter().map(|s| s.prefix))
    {
        text.push_str(&format!(" {} drop\n", address_match(prefix)));
    }
    for rule in &policy.server.deny {
        text.push_str(&format!(" {} drop\n", rule_match(rule)));
    }
    // RELATED ICMP errors have the grant as their destination, so are allowed
    // in the reverse path, not by bypassing destination policy on TUN ingress.
    text.push_str(" ct state new jump limits\n");
    for rule in &policy.server.allow {
        text.push_str(&format!(" {} accept\n", rule_match(rule)));
    }
    for address in &policy.server.server_addresses {
        text.push_str(&format!(
            " {} daddr {} drop\n",
            if address.is_ipv4() { "ip" } else { "ip6" },
            address
        ));
    }
    for prefix in NON_PUBLIC
        .iter()
        .map(|s| s.parse::<IpPrefix>().expect("static prefix"))
    {
        text.push_str(&format!(" {} drop\n", address_match(prefix)));
    }
    text.push_str(" meta nfproto ipv4 accept\n ip6 daddr 2000::/3 accept\n drop\n }\n");
    text.push_str(" chain input { type filter hook input priority 0; policy accept; iifname \"skvoz0\" jump admission; }\n");
    text.push_str(" chain forward { type filter hook forward priority 0; policy accept; iifname \"skvoz0\" jump admission; oifname \"skvoz0\" jump reverse; }\n");
    text.push_str(" chain output { type filter hook output priority 0; policy accept; oifname \"skvoz0\" jump reverse; }\n");
    text.push_str(" chain reverse {\n meta nfproto ipv4 ip daddr != @live4 drop\n meta nfproto ipv6 ip6 daddr != @live6 drop\n ct state established,related accept\n drop\n }\n");
    if let Some(v4) = &policy.server.ipv4
        && v4.egress == "nat44"
    {
        text.push_str(&format!(" chain postrouting {{ type nat hook postrouting priority 100; policy accept; ip saddr @live4 oifname \"{}\" masquerade; }}\n",v4.interface));
    }
    text.push_str("}\n");
    Ok(text)
}

/// A full live-set transaction closes retired grant access before any route or
/// conntrack deletion. The journal bounds the entire generated transaction.
pub fn live_sets(journal: &Journal) -> Result<String> {
    if journal.peers.len() > 128 {
        return Err(HelperError::Overloaded);
    }
    let mut text =
        "flush set inet skvoz_network live4\nflush set inet skvoz_network live6\n".to_string();
    for family in [4, 6] {
        let grants = journal
            .peers
            .iter()
            .filter(|p| p.active)
            .flat_map(|p| p.grants.iter())
            .filter(|g| g.family() == family)
            .copied()
            .collect::<Vec<_>>();
        if grants.len() > 4096 {
            return Err(HelperError::Overloaded);
        }
        if !grants.is_empty() {
            text.push_str(&format!(
                "add element inet skvoz_network live{family} {{ {} }}\n",
                grants
                    .iter()
                    .map(|g| String::from(*g))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(text)
}

fn client_guard(journal: &Journal, active: bool) -> Result<String> {
    let client = journal.client.as_ref().ok_or(HelperError::InvalidState)?;
    let mut text = String::from("oifname \"lo\" accept\n");
    // Underlay configuration protocols are explicit exceptions from capture.
    text.push_str("meta nfproto ipv4 udp sport 68 udp dport 67 accept\nmeta nfproto ipv6 udp sport 546 udp dport 547 accept\n");
    text.push_str("meta nfproto ipv6 icmpv6 type { nd-router-solicit, nd-router-advert, nd-neighbor-solicit, nd-neighbor-advert } accept\n");
    for endpoint in &client.transport_endpoints {
        text.push_str(&format!(
            "{} daddr {} tcp dport {} accept\n",
            if endpoint.ip.is_ipv4() { "ip" } else { "ip6" },
            endpoint.ip,
            endpoint.port
        ));
    }
    if active {
        for family in &client.config.families {
            text.push_str(&format!(
                "oifname \"skvoz0\" meta nfproto {} accept\n",
                if *family == 4 { "ipv4" } else { "ipv6" }
            ));
        }
    }
    text.push_str("drop\n");
    Ok(text)
}
pub fn client_rules(journal: &Journal, active: bool) -> Result<String> {
    Ok(format!(
        "table inet skvoz_network {{\ncomment \"skvoz:{}\"\nchain guard {{type filter hook output priority -150; policy accept;\n{} }}\n}}\n",
        journal.token,
        client_guard(journal, active)?
    ))
}
pub fn client_replace(journal: &Journal, active: bool) -> Result<String> {
    Ok(format!(
        "flush chain inet skvoz_network guard\nadd rule inet skvoz_network guard {}",
        client_guard(journal, active)?
            .lines()
            .collect::<Vec<_>>()
            .join("\nadd rule inet skvoz_network guard ")
    ))
}
