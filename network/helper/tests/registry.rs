mod support;
use skvoz_network_helper::{HelperError, registry::Registry};
use support::{Memory, policy};

#[test]
fn durable_tombstone_never_transfers_and_identity_rejoins() {
    let store = Memory::default();
    let (s, n) = policy();
    let mut r = Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    let a = r.reserve(&store, "leases.json", &s, &n, "1", &[4]).unwrap();
    assert_eq!(String::from(a[0]), "10.203.0.2/32");
    r.tombstone(&store, "leases.json", "1").unwrap();
    let mut r = Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    let b = r.reserve(&store, "leases.json", &s, &n, "2", &[4]).unwrap();
    assert_eq!(String::from(b[0]), "10.203.0.3/32");
    assert_eq!(
        r.reserve(&store, "leases.json", &s, &n, "1", &[4]).unwrap(),
        a
    );
}

#[test]
fn initialized_missing_or_duplicate_registry_is_fatal() {
    let store = Memory::default();
    let (s, _) = policy();
    Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    store.files.borrow_mut().remove("leases.json");
    assert!(matches!(
        Registry::load_or_initialize(&store, "leases.json", &s),
        Err(HelperError::LeaseStoreInvalid)
    ));
    store.files.borrow_mut().insert(
        "leases.json".into(),
        br#"{"v":1,"v":1,"pools":["10.203.0.0/24"],"peers":{}}"#.to_vec(),
    );
    assert!(matches!(
        Registry::load_or_initialize(&store, "leases.json", &s),
        Err(HelperError::LeaseStoreInvalid)
    ));
}

#[test]
fn failed_durability_does_not_publish_inmemory_grant() {
    let store = Memory::default();
    let (s, n) = policy();
    let mut r = Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    store.fail.set(true);
    assert!(r.reserve(&store, "leases.json", &s, &n, "1", &[4]).is_err());
    assert!(r.peers.is_empty());
    store.fail.set(false);
    assert_eq!(
        r.reserve(&store, "leases.json", &s, &n, "2", &[4]).unwrap()[0]
            .address
            .to_string(),
        "10.203.0.2"
    );
}

#[test]
fn pool_exhaustion_preserves_tombstones() {
    let store = Memory::default();
    let (s, n) = policy();
    let mut r = Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    for peer in 1..=253 {
        r.reserve(&store, "leases.json", &s, &n, &peer.to_string(), &[4])
            .unwrap();
        r.tombstone(&store, "leases.json", &peer.to_string())
            .unwrap();
    }
    assert!(matches!(
        r.reserve(&store, "leases.json", &s, &n, "254", &[4]),
        Err(HelperError::Overloaded)
    ));
    assert_eq!(r.peers.len(), 253);
}

#[test]
fn foreign_state_first_run_and_changed_pool_fail_closed() {
    let store = Memory::default();
    let (s, _) = policy();
    store.files.borrow_mut().insert("foreign".into(), vec![]);
    assert!(Registry::load_or_initialize(&store, "leases.json", &s).is_err());
    store.files.borrow_mut().clear();
    Registry::load_or_initialize(&store, "leases.json", &s).unwrap();
    let mut other = s;
    other.ipv4.as_mut().unwrap().pool = "10.204.0.0/24".parse().unwrap();
    assert!(Registry::load_or_initialize(&store, "leases.json", &other).is_err());
}
