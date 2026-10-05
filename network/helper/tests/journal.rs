mod support;
use skvoz_network_helper::{
    HelperError,
    journal::{Journal, JournalPeer},
};
use support::Memory;
#[test]
fn durable_journal_rejects_missing_nullable_duplicate_and_overlap() {
    let store = Memory::default();
    let mut journal = Journal::fresh().unwrap();
    journal.peers.push(JournalPeer {
        peer: "1".into(),
        session: "1".repeat(32),
        grants: vec!["10.203.0.2/32".parse().unwrap()],
        mtu: 1500,
        active: false,
    });
    journal.persist(&store).unwrap();
    assert!(Journal::load(&store).unwrap().is_some());
    let original = store.files.borrow()["journal.json"].clone();
    for field in ["operation", "client", "restored_handle"] {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        value.as_object_mut().unwrap().remove(field);
        store
            .files
            .borrow_mut()
            .insert("journal.json".into(), serde_json::to_vec(&value).unwrap());
        assert!(matches!(
            Journal::load(&store),
            Err(HelperError::LeaseStoreInvalid)
        ));
    }
    journal.peers.push(journal.peers[0].clone());
    journal.persist(&store).unwrap();
    assert!(Journal::load(&store).is_err());
    journal.peers.pop();
    let duplicate = journal.peers[0].grants[0];
    journal.peers[0].grants.push(duplicate);
    journal.persist(&store).unwrap();
    assert!(Journal::load(&store).is_err());
    journal.peers[0].grants.pop();
    let mut other = journal.peers[0].clone();
    other.peer = "2".into();
    other.session = "2".repeat(32);
    journal.peers.push(other);
    journal.persist(&store).unwrap();
    assert!(Journal::load(&store).is_err());
    store
        .files
        .borrow_mut()
        .insert("journal.json".into(), b"{\"v\":2,\"v\":2}".to_vec());
    assert!(Journal::load(&store).is_err());
}
#[test]
fn corrupted_role_neutral_fields_cannot_change_arbitrary_objects() {
    let store = Memory::default();
    let journal = Journal::fresh().unwrap();
    journal.persist(&store).unwrap();
    let original = store.files.borrow()["journal.json"].clone();
    for (field, value) in [
        ("interface", serde_json::json!("eth0")),
        ("token", serde_json::json!("A".repeat(32))),
        ("operation", serde_json::json!("exec")),
        ("namespace_inode", serde_json::json!(0)),
        ("route_table", serde_json::json!(254)),
        (
            "settings",
            serde_json::json!([{"name":"ipv4_forward","previous":"0","applied":"1"}]),
        ),
    ] {
        let mut body: serde_json::Value = serde_json::from_slice(&original).unwrap();
        body[field] = value;
        store
            .files
            .borrow_mut()
            .insert("journal.json".into(), serde_json::to_vec(&body).unwrap());
        assert!(Journal::load(&store).is_err(), "{field}");
    }
}
