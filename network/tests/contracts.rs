use skvoz_network::*;
fn session() -> SessionId {
    "0123456789abcdef0123456789abcdef"
        .to_owned()
        .try_into()
        .unwrap()
}
fn config() -> SessionConfig {
    SessionConfig {
        session: session(),
        families: vec![4, 6],
        source_grants: vec![
            "192.0.2.10/32".parse().unwrap(),
            "2001:db8::10/128".parse().unwrap(),
        ],
        routes: vec!["0.0.0.0/0".parse().unwrap(), "::/0".parse().unwrap()],
        dns_servers: vec!["192.0.2.53".parse().unwrap()],
        mtu: 1500,
        channels: 1,
        packet_queue_bytes: 262144,
        packet_queue_records: 256,
        setup_timeout_ms: 15000,
        egress: Egress {
            ipv4: "nat44".into(),
            ipv6: "routed".into(),
        },
    }
}
fn ipv4(options: bool) -> Vec<u8> {
    let len = if options { 28 } else { 24 };
    let mut p = vec![0; len];
    p[0] = if options { 0x46 } else { 0x45 };
    p[2..4].copy_from_slice(&(len as u16).to_be_bytes());
    p[8] = 64;
    p[9] = 143;
    p[12..16].copy_from_slice(&[192, 0, 2, 10]);
    p[16..20].copy_from_slice(&[198, 51, 100, 10]);
    let header = (p[0] & 15) as usize * 4;
    let mut sum: u32 = p[..header]
        .chunks_exact(2)
        .map(|b| u32::from(u16::from_be_bytes([b[0], b[1]])))
        .sum();
    while sum > 65535 {
        sum = (sum & 65535) + (sum >> 16);
    }
    p[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    p
}
#[test]
fn exact_metadata_and_accept_contract() {
    for input in [
        r#"{"v":2,"type":"tcp","host":"example.org","port":443}"#,
        r#"{"v":2,"type":"ip-session","families":[6,4],"max_mtu":1500,"channels":1}"#,
        r#"{"v":2,"type":"ip-data","session":"0123456789abcdef0123456789abcdef","channel":0}"#,
    ] {
        let value = Metadata::decode(input.as_bytes()).unwrap();
        assert_eq!(Metadata::decode(&value.encode().unwrap()).unwrap(), value);
    }
    let accept = Accept::IpSession {
        v: 2,
        session: session(),
    };
    assert_eq!(Accept::decode(&accept.encode().unwrap()).unwrap(), accept);
    assert!(
        Accept::IpSession {
            v: 1,
            session: session()
        }
        .encode()
        .is_err()
    );
}
#[test]
fn metadata_rejects_duplicates_unknown_version_and_types() {
    for input in [
        r#"{"v":2,"v":2,"type":"tcp","host":"example.org","port":443}"#,
        r#"{"v":2,"type":"tcp","type":"tcp","host":"example.org","port":443}"#,
        r#"{"v":2,"type":"tcp","host":"example.org","port":443,"unknown":0}"#,
        r#"{"v":1,"type":"tcp","host":"example.org","port":443}"#,
        r#"{"v":2,"type":"ip-session","families":[4,4],"max_mtu":1500,"channels":1}"#,
        r#"{"v":2,"type":"ip-session","families":[4,6],"max_mtu":1000,"channels":1}"#,
        r#"{"v":2,"type":"ip-data","session":"0123456789ABCDEF0123456789abcdef","channel":0}"#,
    ] {
        assert!(Metadata::decode(input.as_bytes()).is_err(), "{input}");
    }
    assert!(Metadata::decode(&vec![b' '; 513]).is_err());
}
#[test]
fn destination_rejects_ambiguous_scoped_mapped_and_invalid_names() {
    for host in [
        "01.2.3.4",
        "127.1",
        "::ffff:192.0.2.1",
        "fe80::1%eth0",
        "bad\nname",
        "-bad.example",
        "example..org",
        "abc_def.example",
    ] {
        assert!(
            Metadata::Tcp {
                v: 2,
                host: host.into(),
                port: 443
            }
            .encode()
            .is_err(),
            "{host}"
        );
    }
    assert!(
        Metadata::Tcp {
            v: 2,
            host: "::1".into(),
            port: 0
        }
        .encode()
        .is_err()
    );
}
#[test]
fn strict_config_overlaps_family_egress_and_budgets() {
    let c = config();
    c.validate().unwrap();
    for mutate in [0, 1, 2, 3, 4, 5, 6] {
        let mut c = c.clone();
        match mutate {
            0 => c.source_grants.push("192.0.2.0/24".parse().unwrap()),
            1 => c.source_grants.push(c.source_grants[0]),
            2 => c.egress.ipv6 = "none".into(),
            3 => c.packet_queue_bytes = 1500,
            4 => c.channels = 9,
            5 => c.dns_servers.push(c.dns_servers[0]),
            _ => c.source_grants[0].bits = 255,
        }
        assert!(c.validate().is_err());
    }
}
#[test]
fn control_roundtrip_and_exact_fields() {
    let c = Control::Config(config());
    let b = c.encode().unwrap();
    let mut p = RecordParser::new(true, CONTROL_MAX, 65536).unwrap();
    p.push(0, &b).unwrap();
    assert_eq!(
        Control::decode(&p.next_record().unwrap().unwrap()).unwrap(),
        c
    );
    let b=encode_record(2,br#"{"session":"0123456789abcdef0123456789abcdef","session":"0123456789abcdef0123456789abcdef"}"#).unwrap();
    let mut p = RecordParser::new(true, CONTROL_MAX, 65536).unwrap();
    p.push(0, &b).unwrap();
    assert!(Control::decode(&p.next_record().unwrap().unwrap()).is_err());
    assert!(
        Control::Close(SessionError {
            session: session(),
            error: "transport_lost".into()
        })
        .encode()
        .is_err()
    );
}
#[test]
fn split_and_coalesced_records_keep_absolute_completion_offsets() {
    let a = encode_record(16, &ipv4(false)).unwrap();
    let b = encode_record(16, &ipv4(true)).unwrap();
    let all = [a.clone(), b.clone()].concat();
    for cut in 0..=all.len() {
        let mut p = RecordParser::new(false, 1500, 65536).unwrap();
        p.push(0, &all[..cut]).unwrap();
        let first = p.next_record().unwrap();
        p.push(cut as u64, &all[cut..]).unwrap();
        let first = first.or_else(|| p.next_record().unwrap()).unwrap();
        assert_eq!(first.end_offset, a.len() as u64);
        let second = p.next_record().unwrap().unwrap();
        assert_eq!(second.end_offset, all.len() as u64);
        assert!(p.is_empty());
    }
}
#[test]
fn parser_rejects_header_before_payload_allocation() {
    for h in [
        [16, 1, 0, 0, 0, 0, 0, 24],
        [16, 0, 0, 1, 0, 0, 0, 24],
        [16, 0, 0, 0, 0, 0, 5, 221],
        [16, 0, 0, 0, 0, 0, 0, 0],
        [2, 0, 0, 0, 0, 0, 0, 24],
    ] {
        let mut p = RecordParser::new(false, 1500, 65536).unwrap();
        assert!(p.push(0, &h).is_err());
    }
    assert!(RecordParser::new(false, usize::MAX, 65536).is_err());
    let mut p = RecordParser::new(false, 1500, 65536).unwrap();
    assert!(p.push(1, &[16]).is_err());
    assert!(p.push(0, &vec![0; 65537]).is_err());
}
#[test]
fn ipv4_options_and_checksum_and_family_checks() {
    for options in [false, true] {
        let p = ipv4(options);
        assert_eq!(validate_packet(&p, 1500, &[4]).unwrap().protocol, 143);
        let mut bad = p.clone();
        bad[10] ^= 1;
        assert!(validate_packet(&bad, 1500, &[4]).is_err());
        assert!(validate_packet(&p, 1500, &[6]).is_err());
        assert!(validate_packet(&p, 20, &[4]).is_err());
        let mut bad = p.clone();
        bad.pop();
        assert!(validate_packet(&bad, 1500, &[4]).is_err());
    }
}
#[test]
fn ipv6_full_packet_and_no_jumbo() {
    let mut p = vec![0; 48];
    p[0] = 0x60;
    p[4..6].copy_from_slice(&8u16.to_be_bytes());
    p[6] = 44;
    p[8..24].copy_from_slice(
        &"2001:db8::10"
            .parse::<std::net::Ipv6Addr>()
            .unwrap()
            .octets(),
    );
    p[24..40].copy_from_slice(
        &"2001:db8::20"
            .parse::<std::net::Ipv6Addr>()
            .unwrap()
            .octets(),
    );
    assert_eq!(validate_packet(&p, 1500, &[6]).unwrap().protocol, 44);
    p[4..6].fill(0);
    assert!(validate_packet(&p, 1500, &[6]).is_err());
}
#[test]
fn canonical_prefix_and_stable_fragment_channel() {
    for p in [
        "192.0.2.1/24",
        "192.0.2.0/024",
        "2001:0db8::/32",
        "::ffff:192.0.2.0/120",
        "::/129",
    ] {
        assert!(p.parse::<IpPrefix>().is_err(), "{p}");
    }
    let p = ipv4(false);
    let info = validate_packet(&p, 1500, &[4]).unwrap();
    for k in 1..=8 {
        let channel = packet_channel(info, 99, k).unwrap();
        assert_eq!(packet_channel(info, 99, k).unwrap(), channel);
    }
}

#[test]
fn explicit_unknown_protocol_version_and_type_errors() {
    assert_eq!(
        Metadata::decode(br#"{"v":1,"type":"tcp","host":"example.org","port":443}"#),
        Err(NetworkError::UnsupportedVersion)
    );
    assert_eq!(
        Metadata::decode(br#"{"v":2,"type":"ethernet"}"#),
        Err(NetworkError::UnsupportedType)
    );
    let record = Record {
        kind: 2,
        payload: vec![b' '; CONTROL_MAX + 1],
        end_offset: 0,
    };
    assert!(Control::decode(&record).is_err());
    assert!(RecordParser::new(true, CONTROL_MAX + 1, 65536).is_err());
}
