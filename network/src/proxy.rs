use crate::{Metadata, NETWORK_VERSION};
use std::{
    collections::BTreeSet,
    net::{Ipv4Addr, Ipv6Addr},
};

pub const HANDSHAKE_MAX: usize = 64 * 1024;
const HTTP_BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub(crate) const HTTP_OVERLOADED: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const HTTP_FAILED: &[u8] =
    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const HTTP_CONNECTED: &[u8] = b"HTTP/1.1 200 Connection Established\r\n\r\n";
const SOCKS_FAILED: &[u8] = &[5, 1, 0, 1, 0, 0, 0, 0, 0, 0];
const SOCKS_CONNECTED: &[u8] = &[5, 0, 0, 1, 0, 0, 0, 0, 0, 0];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    Http,
    Socks,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    Read,
    Write(Vec<u8>),
    Open {
        host: String,
        port: u16,
        initial: Vec<u8>,
        success: Vec<u8>,
        failure: Vec<u8>,
    },
}

#[derive(Clone, Copy)]
enum Stage {
    Http,
    Greeting,
    Request,
    Finished,
}

pub struct Handshake {
    protocol: Protocol,
    input: Vec<u8>,
    received: usize,
    scanned: usize,
    stage: Stage,
    negotiated: bool,
}

impl Handshake {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            input: Vec::new(),
            received: 0,
            scanned: 0,
            stage: match protocol {
                Protocol::Http => Stage::Http,
                Protocol::Socks => Stage::Greeting,
            },
            negotiated: false,
        }
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }
    /// Bound input growth plus concurrently rewritten HTTP fields/header/payload.
    pub fn feed_reservation(&self, incoming: usize) -> usize {
        let capacity = self
            .input
            .capacity()
            .saturating_mul(2)
            .max(self.input.len().saturating_add(incoming))
            .min(HANDSHAKE_MAX);
        1024 + 4 * capacity + 32768
    }
    pub fn bad_request(&self) -> Vec<u8> {
        match self.protocol {
            Protocol::Http => HTTP_BAD_REQUEST.to_vec(),
            Protocol::Socks if !self.negotiated => vec![5, 255],
            Protocol::Socks => SOCKS_FAILED.to_vec(),
        }
    }

    pub fn open_failure(&self, error: crate::local_api::ApiError, parsed: Vec<u8>) -> Vec<u8> {
        if self.protocol == Protocol::Socks {
            parsed
        } else {
            self.setup_failure(error)
        }
    }
    pub fn setup_failure(&self, error: crate::local_api::ApiError) -> Vec<u8> {
        match self.protocol {
            Protocol::Http if error == crate::local_api::ApiError::Overloaded => {
                HTTP_OVERLOADED.to_vec()
            }
            Protocol::Http => HTTP_FAILED.to_vec(),
            Protocol::Socks => self.bad_request(),
        }
    }

    /// A Write result must be written completely before calling feed again.
    /// Open consumes the handshake once. HTTP prepends the rewritten header;
    /// bytes following any handshake remain unchanged in initial.
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Progress, ()> {
        if matches!(self.stage, Stage::Finished) {
            return Err(());
        }
        if bytes.len() > HANDSHAKE_MAX.saturating_sub(self.received) {
            self.stage = Stage::Finished;
            self.input.clear();
            return Err(());
        }
        self.received += bytes.len();
        self.input.extend_from_slice(bytes);
        let result = match self.stage {
            Stage::Http => self.http(),
            Stage::Greeting => self.greeting(),
            Stage::Request => self.socks(),
            Stage::Finished => Err(()),
        };
        if result.is_err() {
            self.stage = Stage::Finished;
            self.input.clear();
        }
        result
    }

    fn http(&mut self) -> Result<Progress, ()> {
        let end = self.input[self.scanned..]
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .map(|offset| self.scanned + offset + 4);
        let Some(end) = end else {
            self.scanned = self.input.len().saturating_sub(3);
            return if self.received == HANDSHAKE_MAX {
                Err(())
            } else {
                Ok(Progress::Read)
            };
        };
        let (host, port, mut initial, tunnel) = parse_http(&self.input[..end])?;
        initial.extend_from_slice(&self.input[end..]);
        self.input.clear();
        self.stage = Stage::Finished;
        Ok(Progress::Open {
            host,
            port,
            initial,
            success: if tunnel {
                HTTP_CONNECTED.to_vec()
            } else {
                Vec::new()
            },
            failure: HTTP_FAILED.to_vec(),
        })
    }

    fn greeting(&mut self) -> Result<Progress, ()> {
        if self.input.len() < 2 {
            return Ok(Progress::Read);
        }
        if self.input[0] != 5 || self.input[1] == 0 {
            return Err(());
        }
        let end = 2 + usize::from(self.input[1]);
        if self.input.len() < end {
            return Ok(Progress::Read);
        }
        if !self.input[2..end].contains(&0) {
            return Err(());
        }
        self.input.drain(..end);
        self.negotiated = true;
        self.stage = Stage::Request;
        Ok(Progress::Write(vec![5, 0]))
    }

    fn socks(&mut self) -> Result<Progress, ()> {
        if self.input.len() < 4 {
            return Ok(Progress::Read);
        }
        if self.input[..3] != [5, 1, 0] {
            return Err(());
        }
        let (size, offset) = match self.input[3] {
            1 => (4, 4),
            4 => (16, 4),
            3 => {
                if self.input.len() < 5 {
                    return Ok(Progress::Read);
                }
                if self.input[4] == 0 {
                    return Err(());
                }
                (usize::from(self.input[4]), 5)
            }
            _ => return Err(()),
        };
        let end = offset + size + 2;
        if self.input.len() < end {
            return Ok(Progress::Read);
        }
        let address = &self.input[offset..offset + size];
        let host = match self.input[3] {
            1 => Ipv4Addr::from(<[u8; 4]>::try_from(address).map_err(|_| ())?).to_string(),
            4 => Ipv6Addr::from(<[u8; 16]>::try_from(address).map_err(|_| ())?).to_string(),
            _ => std::str::from_utf8(address).map_err(|_| ())?.to_owned(),
        };
        let port = u16::from_be_bytes([self.input[end - 2], self.input[end - 1]]);
        validate_destination(&host, port)?;
        let initial = self.input.split_off(end);
        self.input.clear();
        self.stage = Stage::Finished;
        Ok(Progress::Open {
            host,
            port,
            initial,
            success: SOCKS_CONNECTED.to_vec(),
            failure: SOCKS_FAILED.to_vec(),
        })
    }
}

fn validate_destination(host: &str, port: u16) -> Result<(), ()> {
    Metadata::Tcp {
        v: NETWORK_VERSION,
        host: host.to_owned(),
        port,
    }
    .encode()
    .map(|_| ())
    .map_err(|_| ())
}

fn authority(value: &str, default: Option<u16>) -> Result<(String, u16), ()> {
    if value.bytes().any(|byte| byte <= 32 || byte >= 127) || value.contains('@') {
        return Err(());
    }
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (address, suffix) = rest.split_once(']').ok_or(())?;
        let host = address.parse::<Ipv6Addr>().map_err(|_| ())?.to_string();
        let port = if suffix.is_empty() {
            default.ok_or(())?
        } else {
            parse_port(suffix.strip_prefix(':').ok_or(())?)?
        };
        (host, port)
    } else {
        let (host, port) = match value.rsplit_once(':') {
            Some((host, port)) => (host, parse_port(port)?),
            None => (value, default.ok_or(())?),
        };
        // HTTP IPv6 authorities must use brackets.
        if host.contains(':') {
            return Err(());
        }
        (host.to_owned(), port)
    };
    validate_destination(&host, port)?;
    Ok((host, port))
}

fn parse_port(value: &str) -> Result<u16, ()> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(());
    }
    value
        .parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or(())
}

fn token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn parse_http(header: &[u8]) -> Result<(String, u16, Vec<u8>, bool), ()> {
    let text = std::str::from_utf8(&header[..header.len() - 4]).map_err(|_| ())?;
    let mut lines = text.split("\r\n");
    let mut request = lines.next().ok_or(())?.split(' ');
    let method = request.next().ok_or(())?;
    let target = request.next().ok_or(())?;
    let version = request.next().ok_or(())?;
    if request.next().is_some()
        || !token(method)
        || method.len() > 32
        || !["HTTP/1.0", "HTTP/1.1"].contains(&version)
        || target.is_empty()
        || target.bytes().any(|byte| byte <= 32 || byte >= 127)
    {
        return Err(());
    }
    let mut fields = Vec::new();
    let mut nominated = BTreeSet::new();
    let (mut lengths, mut transfers, mut hosts) = (0, 0, 0);
    for line in lines {
        if fields.len() >= 256 {
            return Err(());
        }
        let (name, value) = line.split_once(':').ok_or(())?;
        if !token(name)
            || value
                .bytes()
                .any(|byte| byte < 32 && byte != 9 || byte == 127)
        {
            return Err(());
        }
        let lower = name.to_ascii_lowercase();
        let value = value.trim_matches([' ', '\t']);
        match lower.as_str() {
            "content-length" => {
                lengths += 1;
                if value.is_empty()
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                    || value.parse::<u64>().is_err()
                {
                    return Err(());
                }
            }
            "transfer-encoding" => {
                transfers += 1;
                if version != "HTTP/1.1" || !value.eq_ignore_ascii_case("chunked") {
                    return Err(());
                }
            }
            "host" => hosts += 1,
            "connection" => {
                for part in value.split(',') {
                    let part = part.trim_matches([' ', '\t']);
                    if !token(part) {
                        return Err(());
                    }
                    if nominated.len() >= 256 {
                        return Err(());
                    }
                    nominated.insert(part.to_ascii_lowercase());
                }
            }
            _ => {}
        }
        fields.push((lower, name, value));
    }
    if lengths > 1
        || transfers > 1
        || hosts > 1
        || lengths > 0 && transfers > 0
        || ["content-length", "transfer-encoding", "host"]
            .iter()
            .any(|name| nominated.contains(*name))
    {
        return Err(());
    }
    if method == "CONNECT" {
        if lengths > 0 || transfers > 0 {
            return Err(());
        }
        let (host, port) = authority(target, None)?;
        return Ok((host, port, Vec::new(), true));
    }
    let rest = target.strip_prefix("http://").ok_or(())?;
    if rest.contains('#') || rest.contains('\\') {
        return Err(());
    }
    let boundary = rest.find(['/', '?']).unwrap_or(rest.len());
    let (host, port) = authority(&rest[..boundary], Some(80))?;
    let suffix = &rest[boundary..];
    let origin = if suffix.is_empty() {
        "/".to_owned()
    } else if suffix.starts_with('?') {
        format!("/{suffix}")
    } else {
        suffix.to_owned()
    };
    let mut output = format!("{method} {origin} {version}\r\n");
    for (lower, name, value) in fields {
        if !nominated.contains(&lower)
            && ![
                "host",
                "proxy-authorization",
                "proxy-connection",
                "connection",
                "keep-alive",
                "te",
                "upgrade",
            ]
            .contains(&lower.as_str())
        {
            output.push_str(name);
            output.push_str(": ");
            output.push_str(value);
            output.push_str("\r\n");
        }
    }
    let host_header = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    let port_header = if port == 80 {
        String::new()
    } else {
        format!(":{port}")
    };
    output.push_str(&format!(
        "Host: {host_header}{port_header}\r\nConnection: close\r\n\r\n"
    ));
    Ok((host, port, output.into_bytes(), false))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open(progress: Progress) -> (String, u16, Vec<u8>, Vec<u8>) {
        match progress {
            Progress::Open {
                host,
                port,
                initial,
                success,
                ..
            } => (host, port, initial, success),
            _ => panic!("expected open"),
        }
    }

    #[test]
    fn incremental_http_rewrites_and_keeps_binary_prefix() {
        let request = b"POST http://example.org:8080/a?q=1 HTTP/1.1\r\nHost: ignored\r\nContent-Length: 3\r\nProxy-Connection: keep-alive\r\nTE: trailers\r\nConnection: x-secret\r\nX-Secret: remove\r\nX-Keep: yes\r\n\r\n";
        for split in 0..request.len() {
            let mut parser = Handshake::new(Protocol::Http);
            assert_eq!(parser.feed(&request[..split]), Ok(Progress::Read));
            let mut rest = request[split..].to_vec();
            rest.extend_from_slice(&[0, 255, 1]);
            let (host, port, initial, success) = open(parser.feed(&rest).unwrap());
            assert_eq!((host.as_str(), port), ("example.org", 8080));
            assert_eq!(initial, b"POST /a?q=1 HTTP/1.1\r\nContent-Length: 3\r\nX-Keep: yes\r\nHost: example.org:8080\r\nConnection: close\r\n\r\n\0\xff\x01");
            assert!(success.is_empty());
            assert!(parser.feed(&[]).is_err());
        }
    }

    #[test]
    fn connect_preserves_pipelined_tls_prefix() {
        let mut parser = Handshake::new(Protocol::Http);
        let (host, port, initial, success) = open(
            parser
                .feed(b"CONNECT [2001:db8::1]:443 HTTP/1.1\r\n\r\n\x16\x03\x01")
                .unwrap(),
        );
        assert_eq!((host.as_str(), port), ("2001:db8::1", 443));
        assert_eq!(initial, [22, 3, 1]);
        assert_eq!(success, HTTP_CONNECTED);
    }

    #[test]
    fn http_empty_path_query_and_ipv6_host() {
        for (uri, first_line, host_header) in [
            ("http://example.org", "GET / HTTP/1.0", "Host: example.org"),
            (
                "http://[2001:db8::1]:81?x",
                "GET /?x HTTP/1.0",
                "Host: [2001:db8::1]:81",
            ),
        ] {
            let mut parser = Handshake::new(Protocol::Http);
            let (_, _, initial, _) = open(
                parser
                    .feed(format!("GET {uri} HTTP/1.0\r\n\r\n").as_bytes())
                    .unwrap(),
            );
            let text = String::from_utf8(initial).unwrap();
            assert!(text.starts_with(first_line));
            assert!(text.contains(host_header));
        }
    }

    #[test]
    fn http_rejects_ambiguous_framing_and_invalid_targets() {
        for header in [
            "Content-Length: 1\r\nContent-Length: 1",
            "Content-Length: 1\r\nTransfer-Encoding: chunked",
            "Transfer-Encoding: gzip, chunked",
            "Host: a\r\nHost: b",
            "Connection: host",
            "Bad : value",
            " folded: value",
            "X: value\nInjected: yes",
        ] {
            let mut parser = Handshake::new(Protocol::Http);
            assert!(
                parser
                    .feed(
                        format!("GET http://example.org/ HTTP/1.1\r\n{header}\r\n\r\n").as_bytes()
                    )
                    .is_err(),
                "{header}"
            );
            assert_eq!(parser.bad_request(), HTTP_BAD_REQUEST);
        }
        for target in [
            "https://example.org/",
            "http://user@example.org/",
            "http://example.org/#f",
            "http://127.1/",
            "http://[::ffff:127.0.0.1]/",
            "http://[fe80::1%eth0]/",
            "/origin",
            "http://example.org:0/",
            "http://example.org\\evil/",
        ] {
            assert!(
                Handshake::new(Protocol::Http)
                    .feed(format!("GET {target} HTTP/1.1\r\n\r\n").as_bytes())
                    .is_err(),
                "{target}"
            );
        }
    }

    #[test]
    fn socks_coalesced_handshake_never_loses_payload() {
        let mut parser = Handshake::new(Protocol::Socks);
        assert_eq!(
            parser.feed(b"\x05\x02\x02\x00\x05\x01\x00\x03\x0bexample.org\x01\xbbpayload"),
            Ok(Progress::Write(vec![5, 0]))
        );
        let (host, port, initial, success) = open(parser.feed(&[]).unwrap());
        assert_eq!((host.as_str(), port), ("example.org", 443));
        assert_eq!(initial, b"payload");
        assert_eq!(success, SOCKS_CONNECTED);
    }

    #[test]
    fn socks_incremental_greeting_and_ipv4_ipv6_request() {
        for address in [
            Ipv4Addr::new(127, 0, 0, 1).octets().to_vec(),
            "2001:db8::1".parse::<Ipv6Addr>().unwrap().octets().to_vec(),
        ] {
            let mut wire = vec![5, 1, 0, if address.len() == 4 { 1 } else { 4 }];
            wire.extend_from_slice(&address);
            wire.extend_from_slice(&80u16.to_be_bytes());
            for split in 0..wire.len() {
                let mut parser = Handshake::new(Protocol::Socks);
                assert_eq!(parser.feed(&[5]), Ok(Progress::Read));
                assert_eq!(parser.feed(&[1]), Ok(Progress::Read));
                assert_eq!(parser.feed(&[0]), Ok(Progress::Write(vec![5, 0])));
                assert_eq!(parser.feed(&wire[..split]), Ok(Progress::Read));
                let (_, port, initial, _) = open(parser.feed(&wire[split..]).unwrap());
                assert_eq!(port, 80);
                assert!(initial.is_empty());
            }
        }
    }

    #[test]
    fn socks_rejects_auth_commands_and_noncanonical_addresses() {
        for greeting in [vec![4, 1, 0], vec![5, 0], vec![5, 1, 2]] {
            let mut parser = Handshake::new(Protocol::Socks);
            assert!(parser.feed(&greeting).is_err());
            assert_eq!(parser.bad_request(), [5, 255]);
        }
        for request in [
            vec![5, 2, 0, 1],
            vec![5, 1, 1, 1],
            vec![5, 1, 0, 9],
            vec![5, 1, 0, 3, 0],
            vec![5, 1, 0, 1, 127, 0, 0, 1, 0, 0],
            b"\x05\x01\x00\x03\x05bad%z\x00\x50".to_vec(),
        ] {
            let mut parser = Handshake::new(Protocol::Socks);
            parser.feed(&[5, 1, 0]).unwrap();
            assert!(parser.feed(&request).is_err());
            assert_eq!(parser.bad_request(), SOCKS_FAILED);
        }
        let mut parser = Handshake::new(Protocol::Socks);
        parser.feed(&[5, 1, 0]).unwrap();
        let mut mapped = vec![5, 1, 0, 4];
        mapped.extend_from_slice(&"::ffff:192.0.2.1".parse::<Ipv6Addr>().unwrap().octets());
        mapped.extend_from_slice(&80u16.to_be_bytes());
        assert!(parser.feed(&mapped).is_err());
    }

    #[test]
    fn cap_is_cumulative_across_socks_greeting() {
        let mut parser = Handshake::new(Protocol::Http);
        assert_eq!(
            parser.feed(&vec![b'x'; HANDSHAKE_MAX - 1]),
            Ok(Progress::Read)
        );
        assert!(parser.feed(b"x").is_err());
        assert!(parser.feed(b"").is_err());
        let mut parser = Handshake::new(Protocol::Socks);
        parser.feed(&[5, 1, 0]).unwrap();
        assert!(parser.feed(&vec![0; HANDSHAKE_MAX - 2]).is_err());
        let mut parser = Handshake::new(Protocol::Http);
        let mut wire = b"CONNECT example.org:80 HTTP/1.1\r\n\r\n".to_vec();
        wire.resize(HANDSHAKE_MAX, 255);
        let (_, _, initial, _) = open(parser.feed(&wire).unwrap());
        assert_eq!(
            initial.len(),
            HANDSHAKE_MAX - b"CONNECT example.org:80 HTTP/1.1\r\n\r\n".len()
        );
    }
    #[test]
    fn header_and_connection_nomination_counts_are_bounded() {
        let mut headers = b"GET http://example.com/ HTTP/1.1\r\n".to_vec();
        for _ in 0..257 {
            headers.extend_from_slice(b"X: 1\r\n");
        }
        headers.extend_from_slice(b"\r\n");
        assert!(Handshake::new(Protocol::Http).feed(&headers).is_err());
        let names = (0..257)
            .map(|n| format!("x{n}"))
            .collect::<Vec<_>>()
            .join(",");
        let request = format!("GET http://example.com/ HTTP/1.1\r\nConnection: {names}\r\n\r\n");
        assert!(
            Handshake::new(Protocol::Http)
                .feed(request.as_bytes())
                .is_err()
        );
    }
    #[test]
    fn overload_and_post_parse_socks_failures_preserve_the_protocol_phase() {
        let fresh = Handshake::new(Protocol::Socks);
        assert_eq!(
            fresh.setup_failure(crate::local_api::ApiError::Overloaded),
            vec![5, 255]
        );
        assert_eq!(
            fresh.open_failure(
                crate::local_api::ApiError::Overloaded,
                SOCKS_FAILED.to_vec()
            ),
            SOCKS_FAILED
        );
        let http = Handshake::new(Protocol::Http);
        assert_eq!(
            http.open_failure(crate::local_api::ApiError::Overloaded, HTTP_FAILED.to_vec()),
            HTTP_OVERLOADED
        );
        assert_eq!(
            http.open_failure(
                crate::local_api::ApiError::NetworkUnavailable,
                HTTP_FAILED.to_vec()
            ),
            HTTP_FAILED
        );
    }
}
