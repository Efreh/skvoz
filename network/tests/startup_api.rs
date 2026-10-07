use serde_json::{Value, json};
use skvoz_network::{NetworkError, config::*, local_api::*, policy};
const CLIENT: &[u8] = include_bytes!("fixtures/client-startup.json");
#[test]
fn shared_runtime_profiles_bound_aggregate_server_input_without_growing_queues() {
    let mut client = StartupConfig::parse_json(CLIENT).unwrap();
    let runtime = client.core_runtime().unwrap();
    assert_eq!(runtime.max_incoming_per_turn, 32);
    assert_eq!(runtime.max_outgoing_per_turn, 32);
    client.role = Role::Server;
    client.core.peer_id = "0".into();
    client.core.membership = "broker_authorized".into();
    client.core.allowed_peers.clear();
    client.core.initiate.clear();
    client.network.limits = Limits::canonical(Role::Server);
    client.network.families.clear();
    client.server = Some(serde_json::from_value(json!({"ipv4":null,"ipv6":null,"dns_servers":[],"allow":[],"deny":[],"service_prefixes":[],"lease_store":std::env::temp_dir().join("leases.json"),"server_addresses":[],"management_endpoints":[]})).unwrap());
    let runtime = client.core_runtime().unwrap();
    assert_eq!(runtime.max_incoming_per_turn, 256);
    assert_eq!(runtime.max_outgoing_per_turn, 32);
    assert_eq!(runtime.subscription_capacity, 64);
    assert_eq!(runtime.shards, 8);
}
#[test]
fn canonical_capacity_preserves_all_ip_reservations_and_dual_stack_client() {
    let limits = Limits::canonical(Role::Server);
    let budget = skvoz_network::budget::Budget::new(
        limits.runtime_buffer_bytes,
        limits.runtime_buffer_records,
    );
    let _fixed = budget
        .reserve(limits.fixed_backing(Role::Server, true).unwrap(), 32)
        .unwrap();
    let bytes =
        2 * limits.packet_queue_bytes + 65536 + 32768 + 2 * limits.control_queue_bytes + 65536;
    let records = 2 * limits.packet_queue_records + 128 + 32;
    let sessions: Vec<_> = (0..128)
        .map(|_| budget.reserve(bytes, records).unwrap())
        .collect();
    assert_eq!(sessions.len(), limits.ip_sessions);
    let mut client = StartupConfig::parse_json(CLIENT).unwrap();
    client.network.families = vec![4, 6];
    assert!(client.network.validate(Role::Client).is_ok());
}

fn wire(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}
#[test]
fn canonical_runtime_backs_cancelled_backend_payload_shadow_and_native_pool() {
    for role in [Role::Client, Role::Server] {
        let limits = Limits::canonical(role);
        let minimum = limits
            .minimum_runtime_bytes(role, role == Role::Server)
            .unwrap();
        assert!(
            minimum <= limits.runtime_buffer_bytes,
            "{role:?}: {minimum}"
        );
        // Both independently retained owners plus the cancelled backend shadow.
        let payload = 2 * limits.core_receive_bytes + 512 * limits.max_frame as usize;
        assert!(limits.fixed_backing(role, false).unwrap() + limits.core_receive_bytes >= payload);
        println!(
            "{role:?} minimum_runtime_bytes={minimum} limit={}",
            limits.runtime_buffer_bytes
        );
    }
}
#[test]
fn tuned_profiles_reject_unbacked_fixed_reservations_before_startup() {
    for role in [Role::Client, Role::Server] {
        let mut limits = Limits::canonical(role);
        limits.api_queue_bytes = BODY_MAX + 2048;
        limits.api_queue_records = 6;
        limits.core_streams = 2;
        limits.streams_per_peer = 2;
        limits.core_receive_bytes = 131072;
        limits.core_receive_peer_bytes = 131072;
        let fixed = limits.minimum_runtime_bytes(role, false).unwrap();
        limits.runtime_buffer_bytes = fixed;
        assert!(limits.validate(role).is_ok());
        limits.runtime_buffer_bytes -= 1;
        assert_eq!(
            limits.validate(role),
            Err(NetworkError::InvalidConfiguration)
        );
        let mut network = serde_json::from_slice::<StartupConfig>(CLIENT)
            .unwrap()
            .network;
        network.limits = limits;
        network.limits.runtime_buffer_bytes = network
            .limits
            .minimum_runtime_bytes(role, role == Role::Server)
            .unwrap();
        assert!(network.validate(role).is_ok());
        network.limits.runtime_buffer_bytes -= 1;
        assert!(network.validate(role).is_err());
    }
}

#[test]
fn api_profile_requires_room_for_a_response_and_deferred_terminal_progress() {
    let mut limits = Limits::canonical(Role::Client);
    limits.api_queue_bytes = BODY_MAX + 2048;
    limits.api_queue_records = 6;
    assert!(limits.validate(Role::Client).is_ok());
    limits.api_queue_bytes -= 1;
    assert!(limits.validate(Role::Client).is_err());
    limits.api_queue_bytes += 1;
    limits.api_queue_records = 5;
    assert!(limits.validate(Role::Client).is_err());
}

#[test]
fn strict_startup_rejects_missing_nullable_duplicate_unknown_and_profile_changes() {
    let config = StartupConfig::parse_json(CLIENT).unwrap();
    assert_eq!(
        config.network.limits.transport_reservation(Role::Client),
        13322464
    );
    assert_eq!(
        Limits::canonical(Role::Server).transport_reservation(Role::Server),
        82340624
    );
    let value: Value = serde_json::from_slice(CLIENT).unwrap();
    for (parent, key) in [
        ("", "server"),
        ("core", "tls_server_name"),
        ("core", "ca_file"),
    ] {
        let mut bad = value.clone();
        let object = if parent.is_empty() {
            bad.as_object_mut().unwrap()
        } else {
            bad[parent].as_object_mut().unwrap()
        };
        object.remove(key);
        assert!(StartupConfig::parse_json(&wire(&bad)).is_err());
    }
    let duplicate = String::from_utf8(CLIENT.to_vec())
        .unwrap()
        .replace("\"v\": 1", "\"v\": 1, \"v\": 1");
    assert!(StartupConfig::parse_json(duplicate.as_bytes()).is_err());
    let mut bad = value.clone();
    bad["core"]["ipc_path"] = json!("unused.sock");
    assert!(StartupConfig::parse_json(&wire(&bad)).is_err());
    for key in [
        "receive_window",
        "max_frame",
        "core_send_bytes",
        "core_send_peer_bytes",
    ] {
        let mut bad = value.clone();
        bad["network"]["limits"][key] = json!(1);
        assert!(StartupConfig::parse_json(&wire(&bad)).is_err());
    }
    let mut namespaced = value;
    namespaced["core"]["namespace"] = json!("skvoz.runtime.fixture.ffi");
    assert!(StartupConfig::parse_json(&wire(&namespaced)).is_ok());
}

#[test]
fn request_exact_args_rights_and_nested_duplicate_rules() {
    let proxy = json!({"v":1,"id":1,"op":"START_PROXY","args":{"http_bind":null,"socks_bind":null},"fd_count":0});
    assert!(Request::parse_json(&wire(&proxy)).is_ok());
    for key in ["http_bind", "socks_bind"] {
        let mut bad = proxy.clone();
        bad["args"].as_object_mut().unwrap().remove(key);
        assert!(Request::parse_json(&wire(&bad)).is_err());
    }
    let mut bad = proxy.clone();
    bad["fd_count"] = json!(1);
    assert!(Request::parse_json(&wire(&bad)).is_err());
    let duplicate=br#"{"v":1,"id":1,"op":"OPEN_TCP","args":{"host":"example.org","host":"evil.example","port":443},"fd_count":0}"#;
    assert!(Request::parse_json(duplicate).is_err());
    assert!(parse_strict_json(br#"{"a":{"b":1,"b":2}}"#).is_err());
    assert!(parse_strict_json(br#"{} {}"#).is_err());
    let large = serde_json::to_vec(&json!({"padding":"a".repeat(40000)})).unwrap();
    assert!(parse_strict_json(&large).is_err());
    assert!(parse_strict_json_bounded(&large, 65536).is_ok());
}

#[test]
fn helper_exact_arguments_and_nullable_response_fields() {
    let prepare = json!({"v":1,"id":1,"op":"RESERVE_PEER","args":{"peer":"7","session":"0123456789abcdef0123456789abcdef","families":[4],"mtu":1500},"fd_count":0});
    assert!(HelperRequest::parse_json(&wire(&prepare)).is_ok());
    let mut bad = prepare.clone();
    bad["args"]["peer"] = json!("07");
    assert!(HelperRequest::parse_json(&wire(&bad)).is_err());
    let mut bad = prepare;
    bad["args"]["command"] = json!("ignored");
    assert!(HelperRequest::parse_json(&wire(&bad)).is_err());
    assert!(HelperResponse::parse_json(br#"{"v":1,"id":1,"result":{},"fd_count":0}"#).is_err());
    assert!(
        HelperResponse::parse_json(br#"{"v":1,"id":1,"result":{},"error":null,"fd_count":0}"#)
            .is_ok()
    );
}

#[test]
fn management_and_mandatory_addresses_cannot_be_allowed_over() {
    let config:ServerConfig=serde_json::from_value(json!({"ipv4":null,"ipv6":null,"dns_servers":[],
        "allow":[{"cidr":"10.0.0.0/8","protocols":[6],"ports":null},{"cidr":"127.0.0.0/8","protocols":"any","ports":null}],"deny":[],
        "service_prefixes":[],"lease_store":"/var/lib/skvoz-network/leases.json",
        "server_addresses":["10.2.0.1"],"management_endpoints":[{"address":"10.2.0.1","protocol":6,"port":4222}]})).unwrap();
    assert!(!policy::allowed(
        &config,
        "10.2.0.1".parse().unwrap(),
        6,
        Some(4222)
    ));
    assert!(policy::allowed(
        &config,
        "10.2.0.1".parse().unwrap(),
        6,
        Some(443)
    ));
    assert!(!policy::allowed(
        &config,
        "127.0.0.1".parse().unwrap(),
        6,
        Some(443)
    ));
    assert!(!policy::allowed(
        &config,
        "169.254.0.1".parse().unwrap(),
        6,
        Some(443)
    ));
    assert!(policy::allowed(
        &config,
        "8.8.8.8".parse().unwrap(),
        6,
        Some(443)
    ));
    let mut network = StartupConfig::parse_json(CLIENT).unwrap().network;
    network.limits = Limits::canonical(Role::Server);
    network.families.clear();
    assert!(config.validate(&network).is_ok());
    let mut missing = config;
    missing.ipv4 = Some(BackendConfig {
        pool: "10.203.0.0/16".parse().unwrap(),
        egress: "nat44".into(),
        interface: "eth0".into(),
    });
    network.families = vec![4];
    missing.dns_servers = vec!["1.1.1.1".parse().unwrap()];
    missing.management_endpoints.clear();
    assert_eq!(
        missing.validate(&network),
        Err(NetworkError::InvalidConfiguration)
    );
}

#[test]
fn native_tcp_local_service_rules_preserve_packet_and_management_isolation() {
    let mut config: ServerConfig = serde_json::from_value(json!({
        "ipv4":null,"ipv6":null,"dns_servers":[],
        "allow":[
            {"cidr":"127.0.0.0/8","protocols":[6],"ports":[443,4222]},
            {"cidr":"::1/128","protocols":[6],"ports":[443,4222]}
        ],
        "deny":[],"service_prefixes":[],"lease_store":"/var/lib/skvoz-network/leases.json",
        "server_addresses":["127.0.0.1","::1"],
        "management_endpoints":[{"address":"127.0.0.1","protocol":6,"port":4222}]
    }))
    .unwrap();
    for address in ["127.0.0.1", "127.2.3.4", "::1"] {
        let ip = address.parse().unwrap();
        assert!(policy::tcp_allowed(&config, ip, 443));
        assert!(!policy::tcp_allowed(&config, ip, 4222));
        assert!(!policy::tcp_allowed(&config, ip, 8443));
        assert!(!policy::allowed(&config, ip, 6, Some(443)));
    }
    assert!(!policy::tcp_allowed(
        &config,
        "::ffff:127.0.0.1".parse().unwrap(),
        443
    ));
    config.deny.push(
        serde_json::from_value(json!({
            "cidr":"127.0.0.0/8","protocols":[6],"ports":[443]
        }))
        .unwrap(),
    );
    assert!(!policy::tcp_allowed(
        &config,
        "127.0.0.1".parse().unwrap(),
        443
    ));
    config.allow.clear();
    assert!(!policy::tcp_allowed(&config, "::1".parse().unwrap(), 443));
}

#[test]
fn certificate_identity_accepts_canonical_numeric_san_but_rejects_aliases() {
    let mut value: serde_json::Value =
        serde_json::from_slice(include_bytes!("fixtures/client-startup.json")).unwrap();
    for identity in ["192.0.2.1", "2001:db8::1", "broker.example"] {
        value["core"]["tls_server_name"] = serde_json::json!(identity);
        let config =
            skvoz_network::config::StartupConfig::parse_json(&serde_json::to_vec(&value).unwrap())
                .unwrap();
        config
            .core_runtime()
            .unwrap()
            .validate_profile(config.network.limits.manager(config.role))
            .unwrap();
    }
    for identity in [
        "[2001:db8::1]",
        "2001:0db8::1",
        "192.000.2.1",
        "fe80::1%eth0",
        "::ffff:192.0.2.1",
        "",
    ] {
        value["core"]["tls_server_name"] = serde_json::json!(identity);
        assert!(
            skvoz_network::config::StartupConfig::parse_json(&serde_json::to_vec(&value).unwrap())
                .is_err(),
            "{identity}"
        );
    }
}

#[test]
fn events_have_exact_typed_data_and_allow_independent_local_session_handle() {
    use serde_json::json;
    use skvoz_network::local_api::{Counters, Event};
    let envelope = |kind: &str, data: serde_json::Value| json!({"v":1,"seq":1,"event":kind,"data":data,"fd_count":0});
    for value in [
        envelope("STATS", json!({"counters":Counters::default()})),
        envelope("RUNTIME_STATE", json!({"state":"starting","error":null})),
        envelope(
            "ACTIVE",
            json!({"handle":"0123456789abcdef0123456789abcdef"}),
        ),
        envelope(
            "CLOSED",
            json!({"handle":"0123456789abcdef0123456789abcdef","error":"forbidden"}),
        ),
        envelope(
            "REQUEST",
            json!({"id":1,"protocol":"SOCKS5","host":"example.com","port":443,"result":"active","uploaded":0,"downloaded":0}),
        ),
    ] {
        assert!(Event::parse_json(&serde_json::to_vec(&value).unwrap()).is_ok());
    }
    for value in [
        envelope("STATS", json!({"counters":{}})),
        envelope("RUNTIME_STATE", json!({"state":"ready"})),
        envelope("RUNTIME_STATE", json!({"state":"failed","error":null})),
        envelope(
            "CLOSED",
            json!({"handle":"0123456789abcdef0123456789abcdef","error":null,"payload":"forbidden"}),
        ),
        envelope(
            "REQUEST",
            json!({"id":1,"protocol":"SOCKS5","host":"example.com","port":443,"result":"raw_errno","uploaded":0,"downloaded":0}),
        ),
        envelope("UNKNOWN", json!({})),
    ] {
        assert!(Event::parse_json(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut value = envelope(
        "ACTIVE",
        json!({"handle":"0123456789abcdef0123456789abcdef"}),
    );
    value["seq"] = json!(u64::MAX);
    assert!(Event::parse_json(&serde_json::to_vec(&value).unwrap()).is_err());
    value["seq"] = json!(1);
    value["fd_count"] = json!(1);
    assert!(Event::parse_json(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(Event::parse_json(br#"{"v":1,"seq":1,"event":"ACTIVE","data":{"handle":"0123456789abcdef0123456789abcdef","handle":"0123456789abcdef0123456789abcdef"},"fd_count":0}"#).is_err());
}
