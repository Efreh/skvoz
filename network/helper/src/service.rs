use crate::{
    HelperError, Result,
    journal::{Journal, JournalPeer},
    kernel::Kernel,
    registry::{Registry, Store},
};
use serde_json::{Value, json};
use skvoz_network::{
    Egress,
    config::{HelperConfig, Role},
    local_api::*,
};
use std::{
    collections::VecDeque,
    os::fd::{BorrowedFd, OwnedFd},
    time::{Duration, Instant},
};

pub struct Reply {
    pub result: Value,
    pub fd: Option<OwnedFd>,
}
pub struct Service<S: Store, K: Kernel> {
    config: HelperConfig,
    store: S,
    kernel: K,
    registry: Option<Registry>,
    lease_name: String,
    journal: Journal,
    hello: bool,
    last_id: u32,
    retired: VecDeque<(String, String)>,
    terminal: bool,
}
impl<S: Store, K: Kernel> Service<S, K> {
    pub fn new(config: HelperConfig, store: S, mut kernel: K) -> Result<Self> {
        config.validate().map_err(|_| HelperError::InvalidRequest)?;
        if config
            .policy
            .as_ref()
            .is_some_and(|p| p.network.families.is_empty())
        {
            return Err(HelperError::InvalidRequest);
        }
        let mut lease_name = String::new();
        let registry = if let Some(policy) = &config.policy {
            if policy.server.lease_store.parent() != Some(config.state_dir.as_path()) {
                return Err(HelperError::LeaseStoreInvalid);
            }
            lease_name = policy
                .server
                .lease_store
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or(HelperError::LeaseStoreInvalid)?
                .to_owned();
            if ["lock", "initialized", "journal.json"].contains(&lease_name.as_str()) {
                return Err(HelperError::LeaseStoreInvalid);
            }
            Some(Registry::load_or_initialize(
                &store,
                &lease_name,
                &policy.server,
            )?)
        } else {
            None
        };
        let mut journal = match Journal::load(&store)? {
            Some(j) => j,
            None => Journal::fresh()?,
        };
        if (config.role == Role::Server
            && (journal.client.is_some() || journal.guarded || journal.restored_handle.is_some()))
            || (config.role == Role::Client && !journal.peers.is_empty())
        {
            return Err(HelperError::LeaseStoreInvalid);
        }
        if let Some(policy) = &config.policy
            && journal.peers.iter().any(|p| {
                p.mtu > policy.network.max_mtu
                    || p.grants
                        .iter()
                        .any(|g| !policy.network.families.contains(&g.family()))
            })
        {
            return Err(HelperError::LeaseStoreInvalid);
        }
        if let Some(registry) = &registry
            && journal.peers.iter().any(|p| {
                !registry
                    .peers
                    .get(&p.peer)
                    .is_some_and(|l| p.grants.iter().all(|g| l.grants.contains(g)))
            })
        {
            return Err(HelperError::LeaseStoreInvalid);
        }
        if journal.prepared
            || journal.guarded
            || journal.operation.is_some()
            || crate::journal::namespace()? != (journal.namespace_device, journal.namespace_inode)
        {
            kernel.begin(Instant::now() + Duration::from_secs(3));
            kernel.recover(
                config.policy.as_ref(),
                &journal,
                config.role == Role::Client,
                None,
            )?;
            if crate::journal::namespace()? != (journal.namespace_device, journal.namespace_inode) {
                // The old namespace is gone and the kernel verified absence of
                // all tagged objects in this new namespace. Leases remain.
                journal = Journal::fresh()?;
            }
            journal.guarded = config.role == Role::Client && journal.client.is_some();
            journal.peers.clear();
            journal.prepared = false;
            journal.operation = None;
        }
        journal.persist(&store)?;
        Ok(Self {
            config,
            store,
            kernel,
            registry,
            lease_name,
            journal,
            hello: false,
            last_id: 0,
            retired: VecDeque::new(),
            terminal: false,
        })
    }
    pub fn handle(
        &mut self,
        request: &HelperRequest,
        owner: Option<BorrowedFd<'_>>,
    ) -> Result<Reply> {
        if self.terminal {
            return Err(HelperError::InvalidState);
        }
        request
            .validate()
            .map_err(|_| HelperError::InvalidRequest)?;
        if request.id <= self.last_id || (!self.hello && request.op != HelperOperation::Hello) {
            self.terminal = true;
            return Err(HelperError::InvalidRequest);
        }
        self.last_id = request.id;
        self.kernel.begin(Instant::now() + Duration::from_secs(4));
        let result = self.dispatch(request, owner);
        if result.is_err()
            && (self.journal.operation.is_some()
                || matches!(
                    &result,
                    Err(HelperError::Io(_) | HelperError::LeaseStoreInvalid)
                ))
        {
            self.terminal = true;
        }
        result
    }
    pub fn terminal(&self) -> bool {
        self.terminal
    }
    pub fn owner_eof(&mut self) -> Result<()> {
        self.terminal = true;
        self.kernel.begin(Instant::now() + Duration::from_secs(3));
        self.cleanup(self.config.role == Role::Client, None)
    }
    fn intent(&mut self, operation: &str) -> Result<()> {
        self.journal.operation = Some(operation.into());
        self.journal.persist(&self.store)
    }
    fn complete(&mut self) -> Result<()> {
        self.journal.operation = None;
        self.journal.persist(&self.store)
    }
    fn cleanup(&mut self, keep_guard: bool, owner: Option<BorrowedFd<'_>>) -> Result<()> {
        // Durability failure still requires best-effort fail-closed revocation
        // of known-owned effects. It never becomes a successful acknowledgment.
        let intent = self.intent("cleanup");
        self.kernel.recover(
            self.config.policy.as_ref(),
            &self.journal,
            keep_guard,
            owner,
        )?;
        intent?;
        self.journal.peers.clear();
        self.journal.prepared = false;
        self.journal.guarded = keep_guard && self.journal.client.is_some();
        if !keep_guard {
            self.journal.restored_handle = self.journal.client.as_ref().map(|c| c.handle.clone());
            self.journal.client = None;
            self.journal.underlay_routes.clear();
        }
        self.journal.settings.clear();
        self.complete()
    }
    fn dispatch(&mut self, r: &HelperRequest, owner: Option<BorrowedFd<'_>>) -> Result<Reply> {
        let nofd = |v| {
            Ok(Reply {
                result: v,
                fd: None,
            })
        };
        match r.op {
            HelperOperation::PrepareClient => {
                if self.config.role != Role::Client || self.journal.prepared {
                    return Err(HelperError::InvalidState);
                }
                let args: PrepareClientArgs =
                    arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                let underlay = self.kernel.plan_client(&args, &self.journal, owner)?;
                self.journal.client = Some(args.clone());
                self.journal.underlay_routes = underlay;
                self.intent("prepare_client")?;
                let fd = self.kernel.prepare_client(&args, &self.journal, owner)?;
                self.journal.prepared = true;
                self.journal.guarded = true;
                self.complete()?;
                Ok(Reply {
                    result: serde_json::to_value(ClientPrepared {
                        handle: args.handle,
                        interface: self.journal.interface.clone(),
                        mtu: args.config.mtu,
                    })
                    .map_err(|_| HelperError::InvalidState)?,
                    fd: Some(fd),
                })
            }
            HelperOperation::ActivateClient => {
                let a: HandleArgs = arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                if self.config.role != Role::Client
                    || !self.journal.prepared
                    || self
                        .journal
                        .client
                        .as_ref()
                        .is_none_or(|c| c.handle != a.handle)
                {
                    return Err(HelperError::Forbidden);
                }
                self.intent("activate_client")?;
                self.kernel.activate_client(&self.journal, owner)?;
                self.complete()?;
                nofd(handle_result(&a.handle))
            }
            HelperOperation::AbortClient => {
                let a: HandleArgs = arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                if self.config.role != Role::Client
                    || self
                        .journal
                        .client
                        .as_ref()
                        .is_none_or(|c| c.handle != a.handle)
                {
                    return Err(HelperError::Forbidden);
                }
                self.cleanup(true, owner)?;
                nofd(handle_result(&a.handle))
            }
            HelperOperation::RestoreClient => {
                let a: StopArgs = arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                if self.config.role == Role::Client
                    && self.journal.client.is_none()
                    && self.journal.restored_handle.as_ref() == Some(&a.handle)
                {
                    return nofd(handle_result(&a.handle));
                }
                if self.config.role != Role::Client
                    || self
                        .journal
                        .client
                        .as_ref()
                        .is_none_or(|c| c.handle != a.handle)
                {
                    return Err(HelperError::Forbidden);
                }
                self.cleanup(false, owner)?;
                nofd(handle_result(&a.handle))
            }
            HelperOperation::Hello => {
                if self.hello {
                    return Err(HelperError::InvalidState);
                }
                self.hello = true;
                nofd(json!({"api":1,"network":4,"role":self.config.role}))
            }
            HelperOperation::PrepareServer => {
                if self.config.role != Role::Server || self.journal.prepared {
                    return Err(HelperError::InvalidState);
                }
                let a: PrepareServerArgs =
                    arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                let p = self
                    .config
                    .policy
                    .as_ref()
                    .ok_or(HelperError::InvalidState)?;
                // Both sides share the same typed serializer. Equal logical
                // configs therefore produce identical canonical bytes.
                if serde_json::to_vec(&a.network).ok() != serde_json::to_vec(&p.network).ok()
                    || serde_json::to_vec(&a.server).ok() != serde_json::to_vec(&p.server).ok()
                {
                    return Err(HelperError::Forbidden);
                }
                self.intent("prepare_server")?;
                let p = self
                    .config
                    .policy
                    .as_ref()
                    .ok_or(HelperError::InvalidState)?;
                let fd = self.kernel.prepare_server(p, &self.journal, owner)?;
                let mtu = p.network.max_mtu;
                self.journal.prepared = true;
                self.complete()?;
                Ok(Reply {
                    result: json!({"interface":self.journal.interface,"mtu":mtu}),
                    fd: Some(fd),
                })
            }
            HelperOperation::ReservePeer => {
                if self.config.role != Role::Server || !self.journal.prepared {
                    return Err(HelperError::InvalidState);
                }
                let a: ReservePeerArgs =
                    arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                let p = self
                    .config
                    .policy
                    .as_ref()
                    .ok_or(HelperError::InvalidState)?;
                if a.mtu > p.network.max_mtu || a.peer == "0" {
                    return Err(HelperError::InvalidRequest);
                }
                if self
                    .journal
                    .peers
                    .iter()
                    .any(|p| p.peer == a.peer || p.session == a.session.as_str())
                {
                    return Err(HelperError::InvalidState);
                }
                if self.journal.peers.len() >= p.network.limits.ip_sessions.min(128) {
                    return Err(HelperError::Overloaded);
                }
                let grants = self
                    .registry
                    .as_mut()
                    .ok_or(HelperError::InvalidState)?
                    .reserve(
                        &self.store,
                        &self.lease_name,
                        &p.server,
                        &p.network,
                        &a.peer,
                        &a.families,
                    )?;
                let egress = Egress {
                    ipv4: if a.families.contains(&4) {
                        p.server
                            .ipv4
                            .as_ref()
                            .map(|b| b.egress.as_str())
                            .unwrap_or("none")
                    } else {
                        "none"
                    }
                    .into(),
                    ipv6: if a.families.contains(&6) {
                        "routed"
                    } else {
                        "none"
                    }
                    .into(),
                };
                self.journal.peers.push(JournalPeer {
                    peer: a.peer,
                    session: a.session.as_str().to_owned(),
                    grants: grants.clone(),
                    mtu: a.mtu,
                    active: false,
                });
                self.journal.persist(&self.store)?;
                nofd(
                    serde_json::to_value(PeerReserved {
                        session: a.session,
                        source_grants: grants,
                        egress,
                    })
                    .map_err(|_| HelperError::InvalidState)?,
                )
            }
            HelperOperation::ActivatePeer => {
                let a: PeerSessionArgs =
                    arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                if self.config.role != Role::Server || !self.journal.prepared {
                    return Err(HelperError::InvalidState);
                }
                let index = self
                    .journal
                    .peers
                    .iter()
                    .position(|p| p.peer == a.peer && p.session == a.session.as_str())
                    .ok_or(HelperError::Forbidden)?;
                if !self.journal.peers[index].active {
                    self.intent("activate_peer")?;
                    self.kernel.add_routes(&self.journal.peers[index], owner)?;
                    self.journal.peers[index].active = true;
                    self.kernel.update_access(&self.journal, owner)?;
                    self.complete()?;
                }
                nofd(json!({"session":a.session}))
            }
            HelperOperation::RetirePeer => {
                let a: PeerSessionArgs =
                    arguments(&r.args).map_err(|_| HelperError::InvalidRequest)?;
                if self.config.role != Role::Server {
                    return Err(HelperError::InvalidState);
                }
                if self
                    .retired
                    .contains(&(a.peer.clone(), a.session.as_str().to_owned()))
                {
                    return nofd(json!({"session":a.session}));
                }
                let index = self
                    .journal
                    .peers
                    .iter()
                    .position(|p| p.peer == a.peer && p.session == a.session.as_str())
                    .ok_or(HelperError::Forbidden)?;
                self.intent("retire_peer")?;
                let was_active = self.journal.peers[index].active;
                self.journal.peers[index].active = false;
                // Transaction revokes sources first, before deleting routes or
                // scoped conntrack. No retired source remains admitted on error.
                self.kernel.update_access(&self.journal, owner)?;
                if was_active {
                    self.kernel
                        .retire_routes(&self.journal.peers[index], owner)?;
                }
                self.registry
                    .as_mut()
                    .ok_or(HelperError::InvalidState)?
                    .tombstone(&self.store, &self.lease_name, &a.peer)?;
                self.journal.peers.remove(index);
                self.complete()?;
                if self.retired.len() == 128 {
                    self.retired.pop_front();
                }
                self.retired
                    .push_back((a.peer, a.session.as_str().to_owned()));
                nofd(json!({"session":a.session}))
            }
            HelperOperation::StopServer => {
                if self.config.role != Role::Server {
                    return Err(HelperError::InvalidState);
                }
                self.cleanup(false, owner)?;
                nofd(json!({"state":"idle"}))
            }
            HelperOperation::Recover => {
                let keep = self.config.role == Role::Client && self.journal.guarded;
                self.cleanup(keep, owner)?;
                nofd(json!({"state":if keep{"guarded"}else{"idle"},
                    "handle":self.journal.client.as_ref().map(|c|&c.handle)}))
            }
        }
    }
}
