use skvoz_network::config::{
    BackendConfig, Limits, ManagementEndpoint, NetworkConfig, Role, ServerConfig,
};
use skvoz_network_helper::registry::Store;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    io,
};

#[derive(Default, Clone)]
pub struct Memory {
    pub files: std::rc::Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
    pub fail: std::rc::Rc<Cell<bool>>,
}
impl Store for Memory {
    fn read(&self, n: &str, c: usize) -> io::Result<Option<Vec<u8>>> {
        let v = self.files.borrow().get(n).cloned();
        if v.as_ref().is_some_and(|b| b.len() > c) {
            return Err(io::Error::other("cap"));
        }
        Ok(v)
    }
    fn replace(&self, n: &str, b: &[u8]) -> io::Result<()> {
        if self.fail.get() {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.files.borrow_mut().insert(n.into(), b.to_vec());
        Ok(())
    }
    fn entries(&self) -> io::Result<Vec<String>> {
        Ok(self.files.borrow().keys().cloned().collect())
    }
}
#[allow(dead_code)]
pub fn policy() -> (ServerConfig, NetworkConfig) {
    (
        ServerConfig {
            ipv4: Some(BackendConfig {
                pool: "10.203.0.0/24".parse().unwrap(),
                egress: "nat44".into(),
                interface: "eth0".into(),
            }),
            ipv6: None,
            dns_servers: vec!["1.1.1.1".parse().unwrap()],
            allow: vec![],
            deny: vec![],
            service_prefixes: vec![],
            lease_store: "/var/lib/skvoz-network/leases.json".into(),
            server_addresses: vec!["198.51.100.10".parse().unwrap()],
            management_endpoints: vec![ManagementEndpoint {
                address: "198.51.100.10".parse().unwrap(),
                protocol: 6,
                port: 4222,
            }],
        },
        NetworkConfig {
            families: vec![4],
            max_mtu: 1500,
            channels: 1,
            limits: Limits::canonical(Role::Server),
        },
    )
}
