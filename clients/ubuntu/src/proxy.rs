use crate::{
    Error, Result,
    ipc::{CLOSED, DATA, OPENED, Owner, REMOTE_FINISHED, WRITABLE},
    settings::{host_name, port},
};
use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
#[derive(Clone, Copy)]
pub struct Budgets {
    pub connections: usize,
    pub frames: usize,
    pub bytes: usize,
}
impl Default for Budgets {
    fn default() -> Self {
        Self {
            connections: 62,
            frames: 128,
            bytes: 2 * crate::RECEIVE_WINDOW,
        }
    }
}
pub struct Proxies {
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    pub active: Arc<AtomicUsize>,
}
struct Guard(Arc<AtomicUsize>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Proxies {
    pub async fn start(
        path: PathBuf,
        http: u16,
        socks: u16,
        budget: Budgets,
        telemetry: Arc<crate::telemetry::Telemetry>,
    ) -> Result<Self> {
        if !(1..=62).contains(&budget.connections)
            || !(1..=128).contains(&budget.frames)
            || !(1024..=2 * crate::RECEIVE_WINDOW).contains(&budget.bytes)
        {
            return Err(Error("invalid_budgets"));
        }
        let http = TcpListener::bind((Ipv4Addr::LOCALHOST, http))
            .await
            .map_err(|_| Error("port_busy"))?;
        let socks = TcpListener::bind((Ipv4Addr::LOCALHOST, socks))
            .await
            .map_err(|_| Error("port_busy"))?;
        let (stop, mut stop_rx) = watch::channel(false);
        let active = Arc::new(AtomicUsize::new(0));
        let count = active.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = stop_rx.changed() => break,
                    Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                    accepted = http.accept() => {
                        if let Ok((socket, _)) = accepted { admit(&mut tasks, socket, false, &path, budget, &count, &telemetry); }
                    },
                    accepted = socks.accept() => {
                        if let Ok((socket, _)) = accepted { admit(&mut tasks, socket, true, &path, budget, &count, &telemetry); }
                    },
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        });
        Ok(Self { stop, task, active })
    }
    pub fn abort(&self) {
        self.task.abort();
    }
    pub async fn close(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}
fn admit(
    tasks: &mut JoinSet<()>,
    socket: TcpStream,
    socks: bool,
    path: &std::path::Path,
    budget: Budgets,
    active: &Arc<AtomicUsize>,
    telemetry: &Arc<crate::telemetry::Telemetry>,
) {
    if active.load(Ordering::Relaxed) >= budget.connections {
        return;
    }
    active.fetch_add(1, Ordering::Relaxed);
    let guard = Guard(active.clone());
    let path = path.to_owned();
    let flow = telemetry.flow();
    tasks.spawn(async move {
        let _guard = guard;
        let _ = connection(socket, socks, path, budget, flow).await;
    });
}

pub fn authority(input: &str, default: Option<u16>) -> Result<(String, u16)> {
    if input.bytes().any(|byte| byte <= 32 || byte >= 127) || input.contains('@') {
        return Err(Error("invalid_proxy_request"));
    }
    let (host, number) = if let Some(rest) = input.strip_prefix('[') {
        let end = rest.find(']').ok_or(Error("invalid_proxy_request"))?;
        rest[..end]
            .parse::<Ipv6Addr>()
            .map_err(|_| Error("invalid_proxy_request"))?;
        let suffix = &rest[end + 1..];
        (
            &rest[..end],
            if suffix.is_empty() {
                default.ok_or(Error("invalid_port"))?
            } else {
                port(
                    suffix
                        .strip_prefix(':')
                        .ok_or(Error("invalid_proxy_request"))?,
                    false,
                )?
            },
        )
    } else {
        if input.matches(':').count() > 1 {
            return Err(Error("invalid_proxy_request"));
        }
        match input.rsplit_once(':') {
            Some((host, number)) => (host, port(number, false)?),
            None => (input, default.ok_or(Error("invalid_port"))?),
        }
    };
    Ok((host_name(host)?, number))
}
#[derive(Debug)]
pub struct Http {
    pub host: String,
    pub port: u16,
    pub initial: Vec<u8>,
    pub tunnel: bool,
}
pub fn http_header(header: &[u8]) -> Result<Http> {
    if header.len() > 16384 || !header.ends_with(b"\r\n\r\n") {
        return Err(Error("invalid_proxy_request"));
    }
    let text = std::str::from_utf8(&header[..header.len() - 4])
        .map_err(|_| Error("invalid_proxy_request"))?;
    let mut lines = text.split("\r\n");
    let request: Vec<_> = lines
        .next()
        .ok_or(Error("invalid_proxy_request"))?
        .split(' ')
        .collect();
    if request.len() != 3
        || !["HTTP/1.0", "HTTP/1.1"].contains(&request[2])
        || request[0].is_empty()
        || request[0].len() > 32
        || !request[0].bytes().all(|byte| byte.is_ascii_alphabetic())
    {
        return Err(Error("invalid_proxy_request"));
    }
    let mut fields = Vec::new();
    let mut lengths = 0;
    let mut transfers = 0;
    let mut hosts = 0;
    let mut nominated = BTreeSet::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(Error("invalid_proxy_request"))?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
            || value
                .bytes()
                .any(|byte| byte < 32 && byte != 9 || byte == 127)
        {
            return Err(Error("invalid_proxy_request"));
        }
        let lower = name.to_ascii_lowercase();
        let value = value.trim();
        match lower.as_str() {
            "content-length" => {
                lengths += 1;
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(Error("invalid_proxy_request"));
                }
            }
            "transfer-encoding" => {
                transfers += 1;
                if request[2] != "HTTP/1.1" || !value.eq_ignore_ascii_case("chunked") {
                    return Err(Error("invalid_proxy_request"));
                }
            }
            "host" => hosts += 1,
            "connection" => {
                nominated.extend(
                    value
                        .split(',')
                        .map(|part| part.trim().to_ascii_lowercase()),
                );
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
        return Err(Error("invalid_proxy_request"));
    }
    if request[0] == "CONNECT" {
        if lengths > 0 || transfers > 0 {
            return Err(Error("invalid_proxy_request"));
        }
        let (host, port) = authority(request[1], None)?;
        return Ok(Http {
            host,
            port,
            initial: Vec::new(),
            tunnel: true,
        });
    }
    let url = url::Url::parse(request[1]).map_err(|_| Error("invalid_proxy_request"))?;
    if url.scheme() != "http"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error("invalid_proxy_request"));
    }
    // Validate the original authority before URL normalization can accept short IPs.
    let original = request[1]
        .strip_prefix("http://")
        .ok_or(Error("invalid_proxy_request"))?
        .split(['/', '?', '#'])
        .next()
        .ok_or(Error("invalid_proxy_request"))?;
    let (host, port) = authority(original, Some(80))?;
    let target = format!(
        "{}{}",
        url.path(),
        url.query()
            .map(|query| format!("?{query}"))
            .unwrap_or_default()
    );
    let mut output = format!("{} {} {}\r\n", request[0], target, request[2]);
    for (lower, name, value) in fields {
        if !nominated.contains(&lower)
            && ![
                "host",
                "proxy-authorization",
                "proxy-connection",
                "connection",
                "keep-alive",
                "upgrade",
            ]
            .contains(&lower.as_str())
        {
            output.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    let host_header = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.clone()
    };
    output.push_str(&format!(
        "Host: {host_header}{}\r\nConnection: close\r\n\r\n",
        if port != 80 {
            format!(":{port}")
        } else {
            String::new()
        }
    ));
    Ok(Http {
        host,
        port,
        initial: output.into_bytes(),
        tunnel: false,
    })
}
async fn http_handshake(socket: &mut TcpStream) -> Result<Http> {
    let mut header = Vec::new();
    loop {
        let byte = socket.read_u8().await?;
        header.push(byte);
        if header.len() > 16384 {
            return Err(Error("invalid_proxy_request"));
        }
        if header.ends_with(b"\r\n\r\n") {
            return http_header(&header);
        }
    }
}
async fn socks_handshake(socket: &mut TcpStream, negotiated: &mut bool) -> Result<(String, u16)> {
    let version = socket.read_u8().await?;
    let count = socket.read_u8().await?;
    if version != 5 || count == 0 {
        return Err(Error("socks_greeting"));
    }
    let mut methods = vec![0; count as usize];
    socket.read_exact(&mut methods).await?;
    if !methods.contains(&0) {
        socket.write_all(&[5, 255]).await?;
        return Err(Error("socks_replied"));
    }
    socket.write_all(&[5, 0]).await?;
    *negotiated = true;
    let mut request = [0; 4];
    socket.read_exact(&mut request).await?;
    if request[..3] != [5, 1, 0] {
        return Err(Error("invalid_proxy_request"));
    }
    let host = match request[3] {
        1 => {
            let mut ip = [0; 4];
            socket.read_exact(&mut ip).await?;
            IpAddr::V4(Ipv4Addr::from(ip)).to_string()
        }
        4 => {
            let mut ip = [0; 16];
            socket.read_exact(&mut ip).await?;
            IpAddr::V6(Ipv6Addr::from(ip)).to_string()
        }
        3 => {
            let size = socket.read_u8().await?;
            let mut name = vec![0; size as usize];
            socket.read_exact(&mut name).await?;
            host_name(std::str::from_utf8(&name).map_err(|_| Error("invalid_proxy_request"))?)?
        }
        _ => return Err(Error("invalid_proxy_request")),
    };
    let port = socket.read_u16().await?;
    if port == 0 {
        return Err(Error("invalid_port"));
    }
    Ok((host, port))
}

async fn connection(
    mut socket: TcpStream,
    socks: bool,
    path: PathBuf,
    budget: Budgets,
    mut flow: crate::telemetry::Flow,
) -> Result<()> {
    // Tokio writes directly into the finite OS send buffer; no application output queue.
    socket.set_nodelay(true)?;
    let mut negotiated = false;
    let handshake = async {
        let http = if socks {
            let (host, port) = socks_handshake(&mut socket, &mut negotiated).await?;
            Http {
                host,
                port,
                initial: Vec::new(),
                tunnel: true,
            }
        } else {
            http_handshake(&mut socket).await?
        };
        flow.destination(
            if socks {
                "SOCKS5"
            } else if http.tunnel {
                "CONNECT"
            } else {
                "HTTP"
            },
            &http.host,
            http.port,
        );
        let mut session = Owner::connect(&path, budget.frames, budget.bytes).await?;
        let mut metadata = 0u64.to_be_bytes().to_vec();
        metadata.extend_from_slice(
            &serde_json::to_vec(
                &serde_json::json!({"v":1,"type":"tcp","host":http.host,"port":http.port}),
            )
            .map_err(|_| Error("invalid_proxy_request"))?,
        );
        let reply = session.commands.request(2, 0, &metadata, &[0]).await?;
        let accounted = session.events.recv().await.ok_or(Error("ipc_failed"))?;
        let event = &accounted.event;
        let connected = event.kind == OPENED
            && event.handle == reply.handle
            && serde_json::from_slice::<serde_json::Value>(&event.payload).is_ok_and(|value| {
                value == serde_json::json!({"v":1,"type":"tcp","status":"connected"})
            });
        drop(accounted);
        if !connected {
            return Err(Error("destination_failed"));
        }
        Ok((http, session, reply.handle))
    };
    let established = tokio::time::timeout(Duration::from_secs(10), handshake).await;
    let (http, mut session, handle) = match established {
        Ok(Ok(value)) => value,
        other => {
            let error = match other {
                Err(_) => Error("proxy_timeout"),
                Ok(Err(error)) => error,
                _ => Error("invalid_proxy_request"),
            };
            if !socks || negotiated {
                let bytes: &[u8] = if socks {
                    &[5, 1, 0, 1, 0, 0, 0, 0, 0, 0]
                } else {
                    b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                let _ = tokio::time::timeout(Duration::from_secs(1), socket.write_all(bytes)).await;
            }
            flow.finish(Err(error));
            return Err(error);
        }
    };
    flow.opened();
    if socks {
        socket.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
    } else if http.tunnel {
        socket
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
    }
    let result = relay(&mut socket, &mut session, handle, http.initial, &flow).await;
    flow.finish(result);
    let _ = session.commands.request(8, handle, &[], &[0, 4]).await;
    let _ = socket.shutdown().await;
    result
}
async fn relay(
    socket: &mut TcpStream,
    owner: &mut Owner,
    handle: u128,
    initial: Vec<u8>,
    flow: &crate::telemetry::Flow,
) -> Result<()> {
    let (mut reader, mut writer) = socket.split();
    let commands = owner.commands.clone();
    let send = async {
        let mut pending = initial;
        let mut buffer = [0; crate::DATA_BLOCK];
        loop {
            if pending.is_empty() {
                let count = reader.read(&mut buffer).await?;
                if count == 0 {
                    commands.request(7, handle, &[], &[0, 4]).await?;
                    return Ok::<(), Error>(());
                }
                pending.extend_from_slice(&buffer[..count]);
            }
            let part = pending.len().min(crate::DATA_BLOCK);
            let reply = commands
                .request(5, handle, &pending[..part], &[0, 1, 4])
                .await?;
            if reply.code == 4 || reply.value > part as u64 {
                return Err(Error("ipc_failed"));
            }
            flow.upload(reply.value);
            pending.drain(..reply.value as usize);
            if reply.value == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    };
    let commands = owner.commands.clone();
    let events = &mut owner.events;
    let failed = &mut owner.failed;
    let receive = async {
        let mut offset = 0u64;
        let mut remote_fin = false;
        while let Some(accounted) = events.recv().await {
            let event = &accounted.event;
            if event.handle != handle {
                return Err(Error("ipc_failed"));
            }
            match event.kind {
                DATA => {
                    if remote_fin
                        || event.payload.len() < 8
                        || u64::from_be_bytes(
                            event.payload[..8]
                                .try_into()
                                .map_err(|_| Error("ipc_failed"))?,
                        ) != offset
                    {
                        return Err(Error("ipc_failed"));
                    }
                    writer.write_all(&event.payload[8..]).await?;
                    let bytes = event.payload.len() as u64 - 8;
                    flow.download(bytes);
                    offset += bytes;
                    commands
                        .request(6, handle, &offset.to_be_bytes(), &[0, 4])
                        .await?;
                }
                WRITABLE => {}
                REMOTE_FINISHED => {
                    remote_fin = true;
                    writer.shutdown().await?;
                }
                CLOSED => {
                    return if remote_fin {
                        Ok(())
                    } else {
                        Err(Error("stream_cancelled"))
                    };
                }
                _ => return Err(Error("ipc_failed")),
            };
        }
        Err(Error("ipc_failed"))
    };
    tokio::pin!(send);
    tokio::pin!(receive);
    let transfer = async {
        tokio::select! {
            result = &mut send => { result?; receive.await },
            result = &mut receive => result,
        }
    };
    // Owner failure must interrupt a blocked consumer write even after local FIN.
    tokio::select! {
        result = transfer => result,
        _ = failed.changed() => Err(Error("ipc_failed")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn owner_failure_interrupts_a_blocked_consumer_after_local_fin() {
        use tokio::net::UnixStream;
        async fn request(socket: &mut UnixStream) -> (u16, u64) {
            let length = socket.read_u32().await.unwrap() as usize;
            let mut bytes = vec![0; length];
            socket.read_exact(&mut bytes).await.unwrap();
            (
                u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
                u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            )
        }
        async fn frame(
            socket: &mut UnixStream,
            kind: u16,
            sequence: u64,
            handle: u128,
            payload: &[u8],
        ) {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&((32 + payload.len()) as u32).to_be_bytes());
            bytes.extend_from_slice(b"SKI1");
            bytes.extend_from_slice(&1u16.to_be_bytes());
            bytes.extend_from_slice(&kind.to_be_bytes());
            bytes.extend_from_slice(&sequence.to_be_bytes());
            bytes.extend_from_slice(&handle.to_be_bytes());
            bytes.extend_from_slice(payload);
            socket.write_all(&bytes).await.unwrap();
        }
        let path = std::env::temp_dir().join(format!(
            "skvoz-blocked-{}",
            crate::settings::token().unwrap()
        ));
        let ipc = tokio::net::UnixListener::bind(&path).unwrap();
        let (fault, failed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = ipc.accept().await.unwrap();
            let (kind, sequence) = request(&mut socket).await;
            assert_eq!(kind, 1);
            let mut hello = vec![0; 56];
            hello[11] = 1;
            frame(&mut socket, 0x8000, sequence, 0, &hello).await;
            let (kind, sequence) = request(&mut socket).await;
            assert_eq!(kind, 7);
            frame(&mut socket, 0x8000, sequence, 1, &[0; 10]).await;
            for index in 0..4096u64 {
                let mut payload = (index * 16384).to_be_bytes().to_vec();
                payload.resize(16392, 42);
                frame(&mut socket, DATA, 0, 1, &payload).await;
                let answer =
                    tokio::time::timeout(Duration::from_millis(200), request(&mut socket)).await;
                let Ok((kind, sequence)) = answer else {
                    // The local TCP buffer is full. Closing IPC must wake the
                    // relay although it is blocked writing and SEND has finished.
                    drop(socket);
                    let _ = fault.send(());
                    return;
                };
                assert_eq!(kind, 6);
                frame(&mut socket, 0x8000, sequence, 1, &[0; 10]).await;
            }
            panic!("Consumer did not apply backpressure within the finite workload");
        });
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let (client, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut client = client.unwrap();
        let mut socket = accepted.unwrap().0;
        let mut owner = Owner::connect(&path, 32, 65536).await.unwrap();
        client.shutdown().await.unwrap();
        let transfer = tokio::spawn(async move {
            relay(
                &mut socket,
                &mut owner,
                1,
                Vec::new(),
                &crate::telemetry::Telemetry::new(false).flow(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), failed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), transfer)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
            Error("ipc_failed")
        );
        server.await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn http_routing() {
        let request=http_header(b"POST http://example.org:8081/p?q=1 HTTP/1.1\r\nHost: wrong\r\nProxy-Authorization: secret\r\nContent-Length: 3\r\n\r\n").unwrap();
        let text = String::from_utf8(request.initial).unwrap();
        assert!(text.contains("POST /p?q=1"));
        assert!(text.contains("Host: example.org:8081"));
        assert!(!text.contains("secret"));
    }
    #[test]
    fn framing_rejection() {
        for fields in [
            "Content-Length: 1\r\nContent-Length: 1",
            "Content-Length: 1\r\nTransfer-Encoding: chunked",
            "Content-Length: 1\r\nConnection: content-length",
            "Host: a\r\nConnection: host",
        ] {
            assert!(
                http_header(
                    format!("POST http://example.org/ HTTP/1.1\r\n{fields}\r\n\r\n").as_bytes()
                )
                .is_err()
            );
        }
        assert!(http_header(b"GET http://127.1/ HTTP/1.1\r\n\r\n").is_err());
        assert_eq!(authority("[::1]:443", None), Ok(("::1".into(), 443)));
    }
}
