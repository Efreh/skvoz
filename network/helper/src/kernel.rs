use crate::{
    HelperError, Result,
    command::{self, Tool},
    firewall,
    journal::{Journal, JournalPeer, UnderlayRoute},
};
use serde_json::Value;
use skvoz_network::{
    IpPrefix,
    config::{HelperPolicy, prefix_overlap},
    local_api::PrepareClientArgs,
};
use skvoz_network_native::{TunDevice, duplicate_cloexec};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    time::Instant,
};

pub trait Kernel {
    fn begin(&mut self, deadline: Instant);
    fn prepare_server(
        &mut self,
        policy: &HelperPolicy,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd>;
    fn plan_client(
        &mut self,
        args: &PrepareClientArgs,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<Vec<UnderlayRoute>>;
    fn prepare_client(
        &mut self,
        args: &PrepareClientArgs,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd>;
    fn activate_client(&mut self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<()>;
    fn update_access(&mut self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<()>;
    fn add_routes(&mut self, peer: &JournalPeer, owner: Option<BorrowedFd<'_>>) -> Result<()>;
    fn retire_routes(&mut self, peer: &JournalPeer, owner: Option<BorrowedFd<'_>>) -> Result<()>;
    fn recover(
        &mut self,
        policy: Option<&HelperPolicy>,
        journal: &Journal,
        keep_guard: bool,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()>;
}

pub struct LinuxKernel {
    tun: Option<TunDevice>,
    deadline: Instant,
    table: u32,
}
impl Default for LinuxKernel {
    fn default() -> Self {
        Self {
            tun: None,
            deadline: Instant::now(),
            table: 0,
        }
    }
}
impl LinuxKernel {
    fn run(
        &self,
        tool: Tool,
        args: Vec<String>,
        body: &[u8],
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<command::Output> {
        command::run(tool, &args, body, owner, self.deadline)
    }
    fn checked(
        &self,
        tool: Tool,
        args: Vec<String>,
        body: &[u8],
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<Vec<u8>> {
        let out = self.run(tool, args, body, owner)?;
        if !out.success {
            eprintln!(
                "kernel command rejected: {}",
                String::from_utf8_lossy(&out.stderr[..out.stderr.len().min(512)])
            );
            return Err(HelperError::InvalidState);
        }
        Ok(out.stdout)
    }
    fn ip(&self, mut args: Vec<String>, owner: Option<BorrowedFd<'_>>) -> Result<Vec<u8>> {
        // Ignore administrator aliases in rt_tables/rt_protos for ownership
        // comparisons; JSON can still represent numeric values as strings.
        if args.iter().any(|arg| arg == "-j") {
            args.insert(0, "-N".into());
        }
        self.checked(Tool::Ip, args, &[], owner)
    }
    fn nft(&self, body: &str, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        self.checked(
            Tool::Nft,
            vec!["-f".into(), "-".into()],
            body.as_bytes(),
            owner,
        )?;
        Ok(())
    }
    fn check_table(&self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<bool> {
        let out = self.checked(
            Tool::Nft,
            vec!["-j".into(), "list".into(), "tables".into()],
            &[],
            owner,
        )?;
        let v: Value = serde_json::from_slice(&out).map_err(|_| HelperError::InvalidState)?;
        let found = v
            .get("nftables")
            .and_then(Value::as_array)
            .ok_or(HelperError::InvalidState)?
            .iter()
            .filter_map(|i| i.get("table"))
            .find(|t| {
                t.get("family") == Some(&Value::String("inet".into()))
                    && t.get("name") == Some(&Value::String("skvoz_network".into()))
            });
        if found.is_some() {
            // nft 1.0.6 omits table comments from JSON. The text header exposes
            // the owning declaration on both supported distributions; terse
            // output still avoids dumping dynamic set elements.
            let out = self.checked(
                Tool::Nft,
                vec![
                    "-t".into(),
                    "list".into(),
                    "table".into(),
                    "inet".into(),
                    "skvoz_network".into(),
                ],
                &[],
                owner,
            )?;
            if !owned_table_header(&out, &journal.token) {
                return Err(HelperError::Forbidden);
            }
            return Ok(true);
        }
        Ok(false)
    }
    fn links(&self, owner: Option<BorrowedFd<'_>>) -> Result<Vec<Value>> {
        // iproute2 only includes IFLA_IFALIAS in the detailed dump. The alias
        // is our ownership proof for activation and crash cleanup.
        let bytes = self.ip(
            vec!["-d".into(), "-j".into(), "address".into(), "show".into()],
            owner,
        )?;
        serde_json::from_slice(&bytes).map_err(|_| HelperError::InvalidState)
    }
    fn own_interface(&self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<bool> {
        let links = self.links(owner)?;
        if let Some(link) = links
            .iter()
            .find(|l| l.get("ifname").and_then(Value::as_str) == Some(&journal.interface))
        {
            if link.get("ifalias").and_then(Value::as_str)
                != Some(format!("skvoz:{}", journal.token).as_str())
            {
                return Err(HelperError::Forbidden);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn route(
        &self,
        verb: &str,
        prefix: IpPrefix,
        mtu: Option<u16>,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        let mut args = vec![
            format!("-{}", prefix.family()),
            "route".into(),
            verb.into(),
            String::from(prefix),
            "dev".into(),
            "skvoz0".into(),
            "proto".into(),
            "186".into(),
            "table".into(),
            self.table.to_string(),
            "metric".into(),
            self.table.to_string(),
        ];
        if let Some(mtu) = mtu {
            args.extend(["mtu".into(), mtu.to_string()]);
        }
        self.ip(args, owner)?;
        Ok(())
    }
    fn rules(&self, family: u8, owner: Option<BorrowedFd<'_>>) -> Result<Vec<Value>> {
        let bytes = self.ip(
            vec![
                format!("-{family}"),
                "-j".into(),
                "rule".into(),
                "show".into(),
            ],
            owner,
        )?;
        serde_json::from_slice(&bytes).map_err(|_| HelperError::InvalidState)
    }
    fn routes(&self, family: u8, table: u32, owner: Option<BorrowedFd<'_>>) -> Result<Vec<Value>> {
        let bytes = self.ip(
            vec![
                format!("-{family}"),
                "-j".into(),
                "route".into(),
                "show".into(),
                "table".into(),
                "all".into(),
            ],
            owner,
        )?;
        // An uninitialized custom table has no FIB. Querying it directly can
        // fail instead of returning an empty array, including on first startup
        // and after complete cleanup. A dump of all tables represents that
        // absence without conflating a real command failure with empty state.
        let routes: Vec<Value> =
            serde_json::from_slice(&bytes).map_err(|_| HelperError::InvalidState)?;
        Ok(routes
            .into_iter()
            .filter(|route| number(route.get("table")) == Some(u64::from(table)))
            .collect())
    }
    fn assert_empty_routing(
        &self,
        journal: &Journal,
        families: &[u8],
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        for family in [4, 6] {
            if !self.routes(family, journal.route_table, owner)?.is_empty() {
                return Err(HelperError::Forbidden);
            }
            if families.contains(&family)
                && self.rules(family, owner)?.iter().any(|r| {
                    number(r.get("priority")) == Some(19760)
                        || number(r.get("table")) == Some(journal.route_table as u64)
                })
            {
                return Err(HelperError::Forbidden);
            }
        }
        Ok(())
    }
    fn install_rules(
        &self,
        journal: &Journal,
        families: &[u8],
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        for family in families {
            self.ip(
                vec![
                    format!("-{family}"),
                    "rule".into(),
                    "add".into(),
                    "priority".into(),
                    "19760".into(),
                    "table".into(),
                    journal.route_table.to_string(),
                    "protocol".into(),
                    "186".into(),
                ],
                owner,
            )?;
        }
        Ok(())
    }
    fn routing_present(&self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<bool> {
        for family in [4, 6] {
            if !self.routes(family, journal.route_table, owner)?.is_empty()
                || self
                    .rules(family, owner)?
                    .iter()
                    .any(|r| number(r.get("table")) == Some(journal.route_table as u64))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    fn validate_owned_routing(
        &self,
        policy: Option<&HelperPolicy>,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        for family in [4, 6] {
            for r in self.rules(family, owner)? {
                if number(r.get("table")) == Some(journal.route_table as u64)
                    && (number(r.get("priority")) != Some(19760)
                        || number(r.get("protocol")) != Some(186)
                        || r.get("src")
                            .and_then(Value::as_str)
                            .is_some_and(|s| s != "all")
                        || r.get("dst").is_some()
                        || r.get("fwmark").is_some()
                        || !r.as_object().is_some_and(|o| {
                            o.keys().all(|k| {
                                ["priority", "src", "table", "protocol"].contains(&k.as_str())
                            })
                        }))
                {
                    return Err(HelperError::Forbidden);
                }
            }
            for r in self.routes(family, journal.route_table, owner)? {
                let prefix = parse_route_prefix(&r, family)?;
                let allowed = if let Some(client) = &journal.client {
                    if let Some(exception) = journal.underlay_routes.iter().find(|e| {
                        e.address == prefix.address
                            && prefix.bits == if family == 4 { 32 } else { 128 }
                    }) {
                        r.get("dev").and_then(Value::as_str) == Some(exception.interface.as_str())
                            && r.get("gateway")
                                .and_then(Value::as_str)
                                .map(|g| g.parse::<IpAddr>())
                                .transpose()
                                .map_err(|_| HelperError::Forbidden)?
                                == exception.gateway
                    } else {
                        client.config.routes.contains(&prefix)
                            && r.get("dev").and_then(Value::as_str)
                                == Some(journal.interface.as_str())
                            && r.get("gateway").is_none()
                    }
                } else {
                    let pool = policy.is_some_and(|p| {
                        p.server
                            .ipv4
                            .iter()
                            .chain(p.server.ipv6.iter())
                            .any(|b| b.pool == prefix)
                    });
                    if pool {
                        unreachable_pool_route(&r)
                    } else {
                        journal.peers.iter().any(|p| p.grants.contains(&prefix))
                            && r.get("dev").and_then(Value::as_str)
                                == Some(journal.interface.as_str())
                            && r.get("gateway").is_none()
                    }
                };
                if !allowed
                    || number(r.get("protocol")) != Some(186)
                    || number(r.get("metric")) != Some(journal.route_table as u64)
                {
                    return Err(HelperError::Forbidden);
                }
            }
        }
        Ok(())
    }
    fn remove_routing(&self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        for family in [4, 6] {
            for r in self.rules(family, owner)? {
                if number(r.get("table")) == Some(journal.route_table as u64) {
                    self.ip(
                        vec![
                            format!("-{family}"),
                            "rule".into(),
                            "del".into(),
                            "priority".into(),
                            "19760".into(),
                            "table".into(),
                            journal.route_table.to_string(),
                            "protocol".into(),
                            "186".into(),
                        ],
                        owner,
                    )?;
                }
            }
            if !self.routes(family, journal.route_table, owner)?.is_empty() {
                self.ip(
                    vec![
                        format!("-{family}"),
                        "route".into(),
                        "flush".into(),
                        "table".into(),
                        journal.route_table.to_string(),
                    ],
                    owner,
                )?;
            }
        }
        Ok(())
    }
    fn delete_conntrack(&self, peer: &JournalPeer, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        for grant in &peer.grants {
            let out = self.run(
                Tool::Conntrack,
                vec![
                    "-D".into(),
                    "-f".into(),
                    if grant.family() == 4 { "ipv4" } else { "ipv6" }.into(),
                    "--orig-src".into(),
                    String::from(*grant),
                ],
                &[],
                owner,
            )?;
            if !out.success && !empty_conntrack_match(&out.stderr) {
                return Err(HelperError::InvalidState);
            }
        }
        Ok(())
    }
    fn preflight(&self, policy: &HelperPolicy, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        let links = self.links(owner)?;
        if links
            .iter()
            .any(|l| l.get("ifname").and_then(Value::as_str) == Some("skvoz0"))
        {
            return Err(HelperError::Forbidden);
        }
        for backend in policy.server.ipv4.iter().chain(policy.server.ipv6.iter()) {
            let link = links
                .iter()
                .find(|l| l.get("ifname").and_then(Value::as_str) == Some(&backend.interface))
                .ok_or(HelperError::InvalidRequest)?;
            if !link
                .get("flags")
                .and_then(Value::as_array)
                .is_some_and(|a| a.iter().any(|v| v.as_str() == Some("UP")))
            {
                return Err(HelperError::InvalidState);
            }
            if backend.pool.family() == 6
                && !link
                    .get("addr_info")
                    .and_then(Value::as_array)
                    .is_some_and(|addresses| {
                        addresses.iter().any(|a| {
                            a.get("local")
                                .and_then(Value::as_str)
                                .and_then(|s| s.parse::<IpAddr>().ok())
                                .is_some_and(|ip| {
                                    ip.is_ipv6() && !ip.is_unspecified() && !ip.is_multicast()
                                })
                                && !a.get("tentative").and_then(Value::as_bool).unwrap_or(false)
                                && !a.get("dadfailed").and_then(Value::as_bool).unwrap_or(false)
                        })
                    })
            {
                return Err(HelperError::InvalidState);
            }
            for link in &links {
                for info in link
                    .get("addr_info")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let (Some(local), Some(bits)) = (
                        info.get("local").and_then(Value::as_str),
                        info.get("prefixlen").and_then(Value::as_u64),
                    ) {
                        let ip: IpAddr = local.parse().map_err(|_| HelperError::InvalidState)?;
                        if let Ok(prefix) = canonical_network(
                            ip,
                            u8::try_from(bits).map_err(|_| HelperError::InvalidState)?,
                        ) && prefix_overlap(prefix, backend.pool)
                        {
                            return Err(HelperError::InvalidRequest);
                        }
                    }
                }
            }
            let routes = self.ip(
                vec![
                    format!("-{}", backend.pool.family()),
                    "-j".into(),
                    "route".into(),
                    "show".into(),
                    "table".into(),
                    "all".into(),
                ],
                owner,
            )?;
            let routes: Vec<Value> =
                serde_json::from_slice(&routes).map_err(|_| HelperError::InvalidState)?;
            if backend.pool.family() == 6
                && !routes.iter().any(|r| {
                    r.get("dst").and_then(Value::as_str) == Some("default")
                        && r.get("dev").and_then(Value::as_str) == Some(backend.interface.as_str())
                        && r.get("type")
                            .and_then(Value::as_str)
                            .is_none_or(|t| t == "unicast")
                })
            {
                return Err(HelperError::InvalidState);
            }
            for route in routes {
                if let Some(dst) = route.get("dst").and_then(Value::as_str)
                    && dst != "default"
                {
                    let prefix: IpPrefix = if dst.contains('/') {
                        dst.parse()
                    } else {
                        format!(
                            "{dst}/{}",
                            if backend.pool.family() == 4 { 32 } else { 128 }
                        )
                        .parse()
                    }
                    .map_err(|_| HelperError::InvalidState)?;
                    if prefix_overlap(prefix, backend.pool) {
                        return Err(HelperError::InvalidRequest);
                    }
                }
            }
            if policy
                .server
                .allow
                .iter()
                .any(|r| prefix_overlap(r.cidr, backend.pool))
                || policy
                    .server
                    .server_addresses
                    .iter()
                    .any(|a| backend.pool.contains(*a))
                || policy
                    .server
                    .management_endpoints
                    .iter()
                    .any(|e| backend.pool.contains(e.address))
            {
                return Err(HelperError::InvalidRequest);
            }
        }
        Ok(())
    }
}

impl Kernel for LinuxKernel {
    fn begin(&mut self, deadline: Instant) {
        self.deadline = deadline;
    }
    fn prepare_server(
        &mut self,
        policy: &HelperPolicy,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd> {
        if self.check_table(journal, owner)? {
            return Err(HelperError::Forbidden);
        }
        self.preflight(policy, owner)?;
        validate_namespace_limits(policy)?;
        self.assert_empty_routing(journal, &policy.network.families, owner)?;
        self.table = journal.route_table;
        self.nft(&firewall::server_rules(policy, journal)?, owner)?;
        let tun = TunDevice::create("skvoz0", usize::from(policy.network.max_mtu))?;
        self.tun = Some(tun);
        self.ip(
            vec![
                "link".into(),
                "set".into(),
                "dev".into(),
                "skvoz0".into(),
                "alias".into(),
                format!("skvoz:{}", journal.token),
            ],
            owner,
        )?;
        for backend in policy.server.ipv4.iter().chain(policy.server.ipv6.iter()) {
            let gateway = match backend.pool.address {
                IpAddr::V4(ip) => IpAddr::V4(Ipv4Addr::from(u32::from(ip) + 1)),
                IpAddr::V6(ip) => IpAddr::V6(Ipv6Addr::from(u128::from(ip) + 1)),
            };
            self.ip(
                vec![
                    format!("-{}", backend.pool.family()),
                    "address".into(),
                    "add".into(),
                    format!("{gateway}/{}", if gateway.is_ipv4() { 32 } else { 128 }),
                    "dev".into(),
                    "skvoz0".into(),
                ],
                owner,
            )?;
            self.ip(
                vec![
                    format!("-{}", backend.pool.family()),
                    "route".into(),
                    "add".into(),
                    "unreachable".into(),
                    String::from(backend.pool),
                    "proto".into(),
                    "186".into(),
                    "metric".into(),
                    journal.route_table.to_string(),
                    "table".into(),
                    journal.route_table.to_string(),
                ],
                owner,
            )?;
        }
        self.install_rules(journal, &policy.network.families, owner)?;
        self.ip(
            vec![
                "link".into(),
                "set".into(),
                "dev".into(),
                "skvoz0".into(),
                "up".into(),
            ],
            owner,
        )?;
        Ok(duplicate_cloexec(
            self.tun.as_ref().ok_or(HelperError::InvalidState)?.as_fd(),
        )?)
    }
    fn plan_client(
        &mut self,
        args: &PrepareClientArgs,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<Vec<UnderlayRoute>> {
        let guarded_table = self.check_table(journal, owner)?;
        if guarded_table && !journal.guarded
            || self
                .links(owner)?
                .iter()
                .any(|l| l.get("ifname").and_then(Value::as_str) == Some("skvoz0"))
        {
            return Err(HelperError::Forbidden);
        }
        self.assert_empty_routing(journal, &args.config.families, owner)?;
        let links = self.links(owner)?;
        for grant in &args.config.source_grants {
            if args
                .transport_endpoints
                .iter()
                .any(|e| grant.contains(e.ip))
            {
                return Err(HelperError::InvalidRequest);
            }
            for link in &links {
                for addr in link
                    .get("addr_info")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let (Some(ip), Some(bits)) = (
                        addr.get("local").and_then(Value::as_str),
                        addr.get("prefixlen").and_then(Value::as_u64),
                    ) {
                        let ip = ip.parse().map_err(|_| HelperError::InvalidState)?;
                        let prefix = canonical_network(
                            ip,
                            u8::try_from(bits).map_err(|_| HelperError::InvalidState)?,
                        )?;
                        if prefix_overlap(prefix, *grant) {
                            return Err(HelperError::InvalidRequest);
                        }
                    }
                }
            }
        }
        // Resolve every literal's underlay route before any capture/guard effects.
        let mut exceptions = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for endpoint in &args.transport_endpoints {
            if !seen.insert(endpoint.ip) {
                continue;
            }
            let bytes = self.ip(
                vec![
                    if endpoint.ip.is_ipv4() { "-4" } else { "-6" }.into(),
                    "-j".into(),
                    "route".into(),
                    "get".into(),
                    endpoint.ip.to_string(),
                ],
                owner,
            )?;
            let routes: Vec<Value> =
                serde_json::from_slice(&bytes).map_err(|_| HelperError::InvalidState)?;
            if routes.len() != 1 {
                return Err(HelperError::InvalidState);
            }
            let r = &routes[0];
            let dev = r
                .get("dev")
                .and_then(Value::as_str)
                .ok_or(HelperError::InvalidState)?;
            skvoz_network::config::validate_ifname(dev).map_err(|_| HelperError::InvalidState)?;
            if dev == "skvoz0" {
                return Err(HelperError::Forbidden);
            }
            let gateway = r
                .get("gateway")
                .and_then(Value::as_str)
                .map(|g| g.parse::<IpAddr>())
                .transpose()
                .map_err(|_| HelperError::InvalidState)?;
            if gateway
                .is_some_and(|g| g.is_ipv4() != endpoint.ip.is_ipv4() || g.to_canonical() != g)
            {
                return Err(HelperError::InvalidState);
            }
            if args.config.routes.iter().any(|p| {
                p.address == endpoint.ip && p.bits == if endpoint.ip.is_ipv4() { 32 } else { 128 }
            }) {
                return Err(HelperError::InvalidRequest);
            }
            exceptions.push(UnderlayRoute {
                address: endpoint.ip,
                interface: dev.to_owned(),
                gateway,
            });
        }
        Ok(exceptions)
    }
    fn prepare_client(
        &mut self,
        args: &PrepareClientArgs,
        journal: &Journal,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd> {
        let guarded_table = self.check_table(journal, owner)?;
        self.table = journal.route_table;
        if guarded_table {
            self.nft(&firewall::client_replace(journal, false)?, owner)?;
        } else {
            self.nft(&firewall::client_rules(journal, false)?, owner)?;
        }
        self.tun = Some(TunDevice::create("skvoz0", usize::from(args.config.mtu))?);
        self.ip(
            vec![
                "link".into(),
                "set".into(),
                "dev".into(),
                "skvoz0".into(),
                "alias".into(),
                format!("skvoz:{}", journal.token),
            ],
            owner,
        )?;
        for grant in &args.config.source_grants {
            if grant.bits != if grant.family() == 4 { 32 } else { 128 } {
                continue;
            }
            self.ip(
                vec![
                    format!("-{}", grant.family()),
                    "address".into(),
                    "add".into(),
                    String::from(*grant),
                    "dev".into(),
                    "skvoz0".into(),
                ],
                owner,
            )?;
        }
        self.ip(
            vec![
                "link".into(),
                "set".into(),
                "dev".into(),
                "skvoz0".into(),
                "up".into(),
            ],
            owner,
        )?;
        for exception in &journal.underlay_routes {
            let ip = exception.address;
            let dev = exception.interface.clone();
            let gateway = exception.gateway;
            let mut command = vec![
                if ip.is_ipv4() { "-4" } else { "-6" }.into(),
                "route".into(),
                "add".into(),
                format!("{ip}/{}", if ip.is_ipv4() { 32 } else { 128 }),
                "table".into(),
                journal.route_table.to_string(),
                "proto".into(),
                "186".into(),
                "metric".into(),
                journal.route_table.to_string(),
                "dev".into(),
                dev,
            ];
            if let Some(gateway) = gateway {
                command.extend(["via".into(), gateway.to_string()]);
            }
            self.ip(command, owner)?;
        }
        for prefix in &args.config.routes {
            self.route("add", *prefix, Some(args.config.mtu), owner)?;
        }
        self.install_rules(journal, &args.config.families, owner)?;
        let mut dns = vec!["dns".into(), "skvoz0".into()];
        dns.extend(args.config.dns_servers.iter().map(ToString::to_string));
        self.checked(Tool::Resolvectl, dns, &[], owner)?;
        self.checked(
            Tool::Resolvectl,
            vec!["domain".into(), "skvoz0".into(), "~.".into()],
            &[],
            owner,
        )?;
        self.checked(
            Tool::Resolvectl,
            vec!["default-route".into(), "skvoz0".into(), "yes".into()],
            &[],
            owner,
        )?;
        Ok(duplicate_cloexec(
            self.tun.as_ref().ok_or(HelperError::InvalidState)?.as_fd(),
        )?)
    }
    fn activate_client(&mut self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        if !self.check_table(journal, owner)? || !self.own_interface(journal, owner)? {
            return Err(HelperError::InvalidState);
        }
        self.nft(&firewall::client_replace(journal, true)?, owner)
    }
    fn update_access(&mut self, journal: &Journal, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        if !self.check_table(journal, owner)? {
            return Err(HelperError::InvalidState);
        }
        self.nft(&firewall::live_sets(journal)?, owner)
    }
    fn add_routes(&mut self, peer: &JournalPeer, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        for grant in &peer.grants {
            self.route("add", *grant, Some(peer.mtu), owner)?;
        }
        Ok(())
    }
    fn retire_routes(&mut self, peer: &JournalPeer, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        self.delete_conntrack(peer, owner)?;
        for grant in &peer.grants {
            self.route("del", *grant, None, owner)?;
        }
        Ok(())
    }
    fn recover(
        &mut self,
        policy: Option<&HelperPolicy>,
        journal: &Journal,
        keep_guard: bool,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        let table = self.check_table(journal, owner)?;
        let same_namespace =
            crate::journal::namespace()? == (journal.namespace_device, journal.namespace_inode);
        if !same_namespace {
            if table
                || self.own_interface(journal, owner)?
                || self.routing_present(journal, owner)?
            {
                return Err(HelperError::Forbidden);
            }
            return Ok(());
        }
        if keep_guard && !table && journal.client.is_some() {
            self.nft(&firewall::client_rules(journal, false)?, owner)?;
        }
        // Revoke packet access before routing or conntrack retirement.
        if table {
            if journal.client.is_some() {
                self.nft(&firewall::client_replace(journal, false)?, owner)?;
            } else {
                self.nft(
                    "flush set inet skvoz_network live4\nflush set inet skvoz_network live6\n",
                    owner,
                )?;
            }
        }
        // Ownership conflicts in other objects must not retain live access in
        // a proven-owned firewall table. Revocation above is independently safe.
        let interface = self.own_interface(journal, owner)?;
        self.validate_owned_routing(policy, journal, owner)?;
        for peer in &journal.peers {
            self.delete_conntrack(peer, owner)?;
        }
        if interface {
            if journal.client.is_some() {
                self.checked(
                    Tool::Resolvectl,
                    vec!["revert".into(), journal.interface.clone()],
                    &[],
                    owner,
                )?;
            }
            self.ip(
                vec![
                    "link".into(),
                    "delete".into(),
                    "dev".into(),
                    journal.interface.clone(),
                ],
                owner,
            )?;
        }
        self.tun = None;
        self.remove_routing(journal, owner)?;
        if table && !keep_guard {
            self.nft("delete table inet skvoz_network\n", owner)?;
        }

        Ok(())
    }
}
fn canonical_network(ip: IpAddr, bits: u8) -> Result<IpPrefix> {
    match ip {
        IpAddr::V4(v) if bits <= 32 => Ok(IpPrefix {
            address: IpAddr::V4(Ipv4Addr::from(
                u32::from(v)
                    & if bits == 0 {
                        0
                    } else {
                        u32::MAX << (32 - bits)
                    },
            )),
            bits,
        }),
        IpAddr::V6(v) if bits <= 128 => Ok(IpPrefix {
            address: IpAddr::V6(Ipv6Addr::from(
                u128::from(v)
                    & if bits == 0 {
                        0
                    } else {
                        u128::MAX << (128 - bits)
                    },
            )),
            bits,
        }),
        _ => Err(HelperError::InvalidState),
    }
}

fn number(v: Option<&Value>) -> Option<u64> {
    v.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}
fn owned_table_header(output: &[u8], token: &str) -> bool {
    let Ok(text) = std::str::from_utf8(output) else {
        return false;
    };
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    lines.next() == Some("table inet skvoz_network {")
        && lines.next() == Some(format!("comment \"skvoz:{token}\"").as_str())
}
fn unreachable_pool_route(route: &Value) -> bool {
    // -N selects numeric route types as well as table/protocol identifiers.
    // The IPv6 kernel dump associates non-forwarding routes with lo even when
    // the creation command did not name a device. Neither form is a live route.
    let unreachable = route.get("type").is_some_and(|value| {
        number(Some(value)) == Some(7) || value.as_str() == Some("unreachable")
    });
    unreachable
        && route.get("gateway").is_none()
        && route
            .get("dev")
            .is_none_or(|value| value.as_str() == Some("lo"))
}
fn parse_route_prefix(r: &Value, family: u8) -> Result<IpPrefix> {
    let dst = r
        .get("dst")
        .and_then(Value::as_str)
        .ok_or(HelperError::InvalidState)?;
    if dst == "default" {
        return if family == 4 { "0.0.0.0/0" } else { "::/0" }
            .parse()
            .map_err(|_| HelperError::InvalidState);
    }
    if dst.contains('/') {
        dst.parse()
    } else {
        format!("{dst}/{}", if family == 4 { 32 } else { 128 }).parse()
    }
    .map_err(|_| HelperError::InvalidState)
}
fn validate_namespace_limits(policy: &HelperPolicy) -> Result<()> {
    // Compose provisions these namespace-local values. Changing ip_forward here
    // could reset unrelated settings; the helper only checks observed bounds.
    let mut settings = Vec::new();
    if policy.network.families.contains(&4) {
        settings.extend([
            ("/proc/sys/net/ipv4/ip_forward", 1u64, 1u64),
            ("/proc/sys/net/ipv4/ipfrag_high_thresh", 1, 4194304),
            ("/proc/sys/net/ipv4/ipfrag_time", 1, 15),
        ]);
    }
    if policy.network.families.contains(&6) {
        settings.extend([
            ("/proc/sys/net/ipv6/conf/all/forwarding", 1, 1),
            ("/proc/sys/net/ipv6/ip6frag_high_thresh", 1, 4194304),
            ("/proc/sys/net/ipv6/ip6frag_time", 1, 15),
        ]);
        for path in [
            "/proc/sys/net/netfilter/nf_conntrack_frag6_high_thresh",
            "/proc/sys/net/netfilter/nf_conntrack_frag6_timeout",
        ] {
            match std::fs::read_to_string(path) {
                Ok(v) => {
                    let max = if path.ends_with("timeout") {
                        15
                    } else {
                        4194304
                    };
                    let value = v
                        .trim()
                        .parse::<u64>()
                        .map_err(|_| HelperError::InvalidState)?;
                    if value == 0 || value > max {
                        return Err(HelperError::InvalidState);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    for (path, min, max) in settings {
        let value = std::fs::read_to_string(path)?
            .trim()
            .parse::<u64>()
            .map_err(|_| HelperError::InvalidState)?;
        if !(min..=max).contains(&value) {
            return Err(HelperError::InvalidState);
        }
    }
    Ok(())
}

fn empty_conntrack_match(stderr: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(stderr) else {
        return false;
    };
    let mut lines = text.trim().lines();
    let Some(line) = lines.next() else {
        return false;
    };
    lines.next().is_none()
        && line.starts_with("conntrack v")
        && line.ends_with("(conntrack-tools): 0 flow entries have been deleted.")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nft_ownership_requires_exact_table_header_comment() {
        let token = "00000000000000000000000000000000";
        let comment = format!("comment \"skvoz:{token}\"");
        assert!(owned_table_header(
            format!("table inet skvoz_network {{\n\t{comment}\n\tchain guard {{\n}}\n}}\n")
                .as_bytes(),
            token
        ));
        for text in [
            format!("table ip skvoz_network {{\n{comment}\n}}"),
            format!("table inet foreign {{\n{comment}\n}}"),
            "table inet skvoz_network {\ncomment \"skvoz:foreign\"\n}".into(),
            format!("table inet skvoz_network {{\nchain guard {{\n{comment}\n}}\n}}"),
            format!("table inet skvoz_network {{\n{comment} extra\n}}"),
            "table inet skvoz_network {\n}".into(),
        ] {
            assert!(!owned_table_header(text.as_bytes(), token));
        }
        assert!(!owned_table_header(b"\xff", token));
    }
    #[test]
    fn empty_conntrack_is_not_inferred_from_a_partial_error_diagnostic() {
        assert!(empty_conntrack_match(
            b"conntrack v1.4.7 (conntrack-tools): 0 flow entries have been deleted.\n"
        ));
        for body in [b"0 flow entries have been deleted".as_slice(),
            b"conntrack v1.4.7 (conntrack-tools): 0 flow entries have been deleted.\nPermission denied\n",
            b"Permission denied: 0 flow entries have been deleted"] {
            assert!(!empty_conntrack_match(body));
        }
    }
    #[test]
    fn numerical_json_and_route_prefixes_are_strict() {
        assert_eq!(number(Some(&serde_json::json!("186"))), Some(186));
        assert_eq!(number(Some(&serde_json::json!(186))), Some(186));
        assert_eq!(number(Some(&serde_json::json!("boot"))), None);
        assert_eq!(
            parse_route_prefix(&serde_json::json!({"dst":"default"}), 6).unwrap(),
            "::/0".parse().unwrap()
        );
        assert_eq!(
            parse_route_prefix(&serde_json::json!({"dst":"10.203.0.2"}), 4).unwrap(),
            "10.203.0.2/32".parse().unwrap()
        );
        assert!(parse_route_prefix(&serde_json::json!({"dst":"somewhere"}), 4).is_err());
    }
    #[test]
    fn kernel_unreachable_pool_routes_accept_numeric_types_and_ipv6_loopback() {
        for route in [
            serde_json::json!({"type":"7"}),
            serde_json::json!({"type":7,"dev":"lo"}),
            serde_json::json!({"type":"unreachable","dev":"lo"}),
        ] {
            assert!(unreachable_pool_route(&route));
        }
        for route in [
            serde_json::json!({"type":"unicast","dev":"lo"}),
            serde_json::json!({"type":"7","dev":"eth0"}),
            serde_json::json!({"type":"7","gateway":"192.0.2.1"}),
            serde_json::json!({"dev":"lo"}),
        ] {
            assert!(!unreachable_pool_route(&route));
        }
    }
}
