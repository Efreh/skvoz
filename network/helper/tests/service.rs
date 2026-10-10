mod support;
use serde_json::{Value, json};
use skvoz_network::{
    Egress, SessionConfig, SessionId,
    config::{HelperConfig, HelperPolicy, Role},
    local_api::*,
};
use skvoz_network_helper::{
    HelperError, Result,
    journal::{Journal, JournalPeer},
    kernel::Kernel,
    service::Service,
};
use std::{
    cell::RefCell,
    os::fd::{BorrowedFd, OwnedFd},
    rc::Rc,
    time::Instant,
};
use support::{Memory, policy};
#[derive(Default, Clone)]
struct FakeKernel {
    trace: Rc<RefCell<Vec<String>>>,
}
impl Kernel for FakeKernel {
    fn begin(&mut self, _: Instant) {}
    fn prepare_server(
        &mut self,
        _: &HelperPolicy,
        _: &Journal,
        _: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd> {
        self.trace.borrow_mut().push("prepare-server".into());
        Ok(std::fs::File::open("/dev/null")?.into())
    }
    fn plan_client(
        &mut self,
        a: &PrepareClientArgs,
        _: &Journal,
        _: Option<BorrowedFd<'_>>,
    ) -> Result<Vec<skvoz_network_helper::journal::UnderlayRoute>> {
        Ok(a.transport_endpoints
            .iter()
            .map(|e| skvoz_network_helper::journal::UnderlayRoute {
                address: e.ip,
                interface: "eth0".into(),
                gateway: None,
            })
            .collect())
    }
    fn prepare_client(
        &mut self,
        _: &PrepareClientArgs,
        _: &Journal,
        _: Option<BorrowedFd<'_>>,
    ) -> Result<OwnedFd> {
        self.trace
            .borrow_mut()
            .push("prepare-client-guarded".into());
        Ok(std::fs::File::open("/dev/null")?.into())
    }
    fn activate_client(&mut self, _: &Journal, _: Option<BorrowedFd<'_>>) -> Result<()> {
        self.trace.borrow_mut().push("activate-client".into());
        Ok(())
    }
    fn update_access(&mut self, j: &Journal, _: Option<BorrowedFd<'_>>) -> Result<()> {
        self.trace.borrow_mut().push(format!(
            "access:{}",
            j.peers.iter().filter(|p| p.active).count()
        ));
        Ok(())
    }
    fn add_routes(&mut self, _: &JournalPeer, _: Option<BorrowedFd<'_>>) -> Result<()> {
        self.trace.borrow_mut().push("add-routes".into());
        Ok(())
    }
    fn retire_routes(&mut self, _: &JournalPeer, _: Option<BorrowedFd<'_>>) -> Result<()> {
        self.trace.borrow_mut().push("retire-routes".into());
        Ok(())
    }
    fn recover(
        &mut self,
        _: Option<&HelperPolicy>,
        _: &Journal,
        keep: bool,
        _: Option<BorrowedFd<'_>>,
    ) -> Result<()> {
        self.trace
            .borrow_mut()
            .push(format!("cleanup-guard:{keep}"));
        Ok(())
    }
}
fn server() -> HelperConfig {
    let (server, network) = policy();
    HelperConfig {
        v: 1,
        role: Role::Server,
        state_dir: "/var/lib/skvoz-network".into(),
        policy: Some(HelperPolicy { server, network }),
    }
}
fn request(id: u32, op: HelperOperation, args: Value) -> HelperRequest {
    HelperRequest::new(id, op, args).unwrap()
}
fn hello<S: skvoz_network_helper::registry::Store, K: Kernel>(s: &mut Service<S, K>) {
    s.handle(
        &request(1, HelperOperation::Hello, json!({"api":1,"network":5})),
        None,
    )
    .unwrap();
}
fn sid(byte: u8) -> SessionId {
    SessionId::try_from(format!("{byte:02x}").repeat(16)).unwrap()
}
fn client_args(byte: u8) -> PrepareClientArgs {
    let session = sid(byte);
    PrepareClientArgs {
        handle: session.clone(),
        config: SessionConfig {
            session,
            families: vec![4],
            source_grants: vec!["10.203.0.2/32".parse().unwrap()],
            routes: vec!["0.0.0.0/0".parse().unwrap()],
            dns_servers: vec!["1.1.1.1".parse().unwrap()],
            mtu: 1500,
            channels: 1,
            packet_queue_bytes: 262144,
            packet_queue_records: 256,
            setup_timeout_ms: 15000,
            egress: Egress {
                ipv4: "nat44".into(),
                ipv6: "none".into(),
            },
        },
        transport_endpoints: vec![TransportEndpoint {
            ip: "198.51.100.10".parse().unwrap(),
            port: 4222,
        }],
    }
}
#[test]
fn server_reserve_has_no_access_and_retire_revokes_before_routes() {
    let c = server();
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let mut s = Service::new(c.clone(), Memory::default(), k).unwrap();
    hello(&mut s);
    let p = c.policy.unwrap();
    s.handle(
        &request(
            2,
            HelperOperation::PrepareServer,
            serde_json::to_value(PrepareServerArgs {
                network: p.network,
                server: p.server,
            })
            .unwrap(),
        ),
        None,
    )
    .unwrap();
    let session = sid(1);
    let a = ReservePeerArgs {
        peer: "1".into(),
        session: session.clone(),
        families: vec![4],
        mtu: 1500,
    };
    let result = s
        .handle(
            &request(
                3,
                HelperOperation::ReservePeer,
                serde_json::to_value(a).unwrap(),
            ),
            None,
        )
        .unwrap();
    assert_eq!(result.result["source_grants"][0], "10.203.0.2/32");
    assert_eq!(&*trace.borrow(), &["prepare-server"]);
    let args = json!({"peer":"1","session":session});
    s.handle(
        &request(4, HelperOperation::ActivatePeer, args.clone()),
        None,
    )
    .unwrap();
    s.handle(&request(5, HelperOperation::RetirePeer, args.clone()), None)
        .unwrap();
    s.handle(&request(6, HelperOperation::RetirePeer, args), None)
        .unwrap();
    assert_eq!(
        &*trace.borrow(),
        &[
            "prepare-server",
            "add-routes",
            "access:1",
            "access:0",
            "retire-routes"
        ]
    );
}
#[test]
fn stale_session_cannot_revoke_current_or_duplicate_activation() {
    let c = server();
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let mut s = Service::new(c.clone(), Memory::default(), k).unwrap();
    hello(&mut s);
    let p = c.policy.unwrap();
    s.handle(
        &request(
            2,
            HelperOperation::PrepareServer,
            json!({"server":p.server,"network":p.network}),
        ),
        None,
    )
    .unwrap();
    let session = sid(2);
    s.handle(
        &request(
            3,
            HelperOperation::ReservePeer,
            json!({"peer":"1","session":session,"families":[4],"mtu":1500}),
        ),
        None,
    )
    .unwrap();
    assert!(matches!(
        s.handle(
            &request(
                4,
                HelperOperation::RetirePeer,
                json!({"peer":"1","session":sid(3)})
            ),
            None
        ),
        Err(HelperError::Forbidden)
    ));
    for id in [5, 6] {
        s.handle(
            &request(
                id,
                HelperOperation::ActivatePeer,
                json!({"peer":"1","session":session}),
            ),
            None,
        )
        .unwrap();
    }
    assert_eq!(
        &*trace.borrow(),
        &["prepare-server", "add-routes", "access:1"]
    );
}
#[test]
fn owner_eof_is_terminal_and_preserves_client_guard() {
    let c = HelperConfig {
        v: 1,
        role: Role::Client,
        state_dir: "/var/lib/skvoz-network-helper/1000".into(),
        policy: None,
    };
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let mut s = Service::new(c, Memory::default(), k).unwrap();
    hello(&mut s);
    let args = client_args(4);
    s.handle(
        &request(
            2,
            HelperOperation::PrepareClient,
            serde_json::to_value(&args).unwrap(),
        ),
        None,
    )
    .unwrap();
    s.owner_eof().unwrap();
    assert!(s.terminal());
    assert_eq!(trace.borrow().last().unwrap(), "cleanup-guard:true");
    assert!(
        s.handle(
            &request(
                3,
                HelperOperation::ActivateClient,
                json!({"handle":args.handle})
            ),
            None
        )
        .is_err()
    );
}
#[test]
fn abort_recover_restore_and_new_handle_do_not_remove_unrelated_guard() {
    let c = HelperConfig {
        v: 1,
        role: Role::Client,
        state_dir: "/var/lib/skvoz-network-helper/1000".into(),
        policy: None,
    };
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let mut s = Service::new(c, Memory::default(), k).unwrap();
    hello(&mut s);
    let a = client_args(5);
    s.handle(
        &request(
            2,
            HelperOperation::PrepareClient,
            serde_json::to_value(&a).unwrap(),
        ),
        None,
    )
    .unwrap();
    s.handle(
        &request(
            3,
            HelperOperation::ActivateClient,
            json!({"handle":a.handle}),
        ),
        None,
    )
    .unwrap();
    s.handle(
        &request(4, HelperOperation::AbortClient, json!({"handle":a.handle})),
        None,
    )
    .unwrap();
    let response = s
        .handle(&request(5, HelperOperation::Recover, json!({})), None)
        .unwrap();
    assert_eq!(
        response.result,
        json!({"state":"guarded","handle":a.handle})
    );
    assert!(
        s.handle(
            &request(
                6,
                HelperOperation::RestoreClient,
                json!({"handle":sid(9),"reason":"user_stop"})
            ),
            None
        )
        .is_err()
    );
    for id in [7, 8] {
        s.handle(
            &request(
                id,
                HelperOperation::RestoreClient,
                json!({"handle":a.handle,"reason":"user_stop"}),
            ),
            None,
        )
        .unwrap();
    }
    assert_eq!(
        trace
            .borrow()
            .iter()
            .filter(|s| s.as_str() == "cleanup-guard:false")
            .count(),
        1
    );
    let b = client_args(6);
    s.handle(
        &request(
            9,
            HelperOperation::PrepareClient,
            serde_json::to_value(&b).unwrap(),
        ),
        None,
    )
    .unwrap();
    assert!(
        s.handle(
            &request(
                10,
                HelperOperation::RestoreClient,
                json!({"handle":a.handle,"reason":"shutdown"})
            ),
            None
        )
        .is_err()
    );
}
#[test]
fn malformed_order_and_role_mismatch_cannot_reach_kernel() {
    let mut s = Service::new(server(), Memory::default(), FakeKernel::default()).unwrap();
    assert!(
        s.handle(&request(1, HelperOperation::Recover, json!({})), None)
            .is_err()
    );
    assert!(s.terminal());
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let mut s = Service::new(server(), Memory::default(), k).unwrap();
    hello(&mut s);
    assert!(
        s.handle(
            &request(
                2,
                HelperOperation::PrepareClient,
                serde_json::to_value(client_args(7)).unwrap()
            ),
            None
        )
        .is_err()
    );
    assert!(trace.borrow().is_empty());
    assert!(
        s.handle(
            &request(1, HelperOperation::Hello, json!({"api":1,"network":5})),
            None
        )
        .is_err()
    );
    assert!(s.terminal());
}

#[test]
fn failed_journal_durability_still_attempts_fail_closed_cleanup() {
    let memory = Memory::default();
    let observer = memory.clone();
    let k = FakeKernel::default();
    let trace = k.trace.clone();
    let c = server();
    let p = c.policy.clone().unwrap();
    let mut s = Service::new(c, memory, k).unwrap();
    hello(&mut s);
    s.handle(
        &request(
            2,
            HelperOperation::PrepareServer,
            json!({"server":p.server,"network":p.network}),
        ),
        None,
    )
    .unwrap();
    observer.fail.set(true);
    assert!(s.owner_eof().is_err());
    assert_eq!(trace.borrow().last().unwrap(), "cleanup-guard:false");
    assert!(s.terminal());
}

#[test]
fn restarted_authorized_owner_recovers_exact_guarded_handle_before_restore() {
    let memory = Memory::default();
    let copy = memory.clone();
    let config = HelperConfig {
        v: 1,
        role: Role::Client,
        state_dir: "/var/lib/skvoz-network-helper/1000".into(),
        policy: None,
    };
    let args = client_args(8);
    let mut first = Service::new(config.clone(), memory, FakeKernel::default()).unwrap();
    hello(&mut first);
    first
        .handle(
            &request(
                2,
                HelperOperation::PrepareClient,
                serde_json::to_value(&args).unwrap(),
            ),
            None,
        )
        .unwrap();
    first.owner_eof().unwrap();
    drop(first);
    let mut second = Service::new(config, copy, FakeKernel::default()).unwrap();
    hello(&mut second);
    let status = second
        .handle(&request(2, HelperOperation::Recover, json!({})), None)
        .unwrap();
    assert_eq!(
        status.result,
        json!({"state":"guarded","handle":args.handle})
    );
    second
        .handle(
            &request(
                3,
                HelperOperation::RestoreClient,
                json!({"handle":args.handle,"reason":"user_stop"}),
            ),
            None,
        )
        .unwrap();
    let status = second
        .handle(&request(4, HelperOperation::Recover, json!({})), None)
        .unwrap();
    assert_eq!(status.result, json!({"state":"idle","handle":null}));
}
