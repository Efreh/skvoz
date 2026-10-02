//! Bounded, exclusive Core owner with independent local IPC sessions.
use crate::{
    config::Profile,
    endpoint::Endpoint,
    protocol::{self, Decoder, Frame},
};
use skvoz_core::{
    CloseReason, Error, Event, ManagerError, PeerId, SendOutcome,
    runtime::{Lifecycle, NatsRuntime, RuntimeError, RuntimeKey},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    time::{Duration, Instant},
};
use tokio::net::UnixStream;

struct Output {
    bytes: Vec<u8>,
    written: usize,
    delivered: Option<(u128, u64)>,
}
struct Owner {
    socket: UnixStream,
    decoder: Decoder,
    hello: bool,
    last_request: u64,
    started: Instant,
    partial_since: Option<Instant>,
    output_since: Instant,
    output: VecDeque<Output>,
    output_bytes: usize,
    streams: BTreeSet<u128>,
}
struct Binding {
    owner: u64,
    key: RuntimeKey,
    delivered: u64,
}
struct Host {
    runtime: NatsRuntime,
    profile: Profile,
    owners: BTreeMap<u64, Owner>,
    bindings: BTreeMap<u128, Binding>,
    keys: BTreeMap<RuntimeKey, u128>,
    acceptor: Option<u64>,
    epoch: u64,
    sequence: u64,
    session: u64,
}
fn error_code(e: RuntimeError) -> u16 {
    match e {
        RuntimeError::Admission => 5,
        RuntimeError::PeerUnavailable => 6,
        RuntimeError::StaleKey | RuntimeError::Manager(ManagerError::UnknownStream) => 4,
        RuntimeError::Manager(ManagerError::Stream(Error::InvalidConsumption)) => 8,
        RuntimeError::Manager(ManagerError::Stream(Error::InvalidState)) => 7,
        RuntimeError::Manager(_) | RuntimeError::Config | RuntimeError::Authorization => 2,
        RuntimeError::Protocol => 10,
        _ => 9,
    }
}
impl Host {
    fn remove_binding(&mut self, handle: u128) {
        if let Some(b) = self.bindings.remove(&handle) {
            self.keys.remove(&b.key);
            if let Some(o) = self.owners.get_mut(&b.owner) {
                o.streams.remove(&handle);
            }
        }
    }
    fn disconnect(&mut self, owner: u64) {
        if self.acceptor == Some(owner) {
            self.acceptor = None;
        }
        if let Some(o) = self.owners.remove(&owner) {
            for handle in o.streams {
                if let Some(b) = self.bindings.get(&handle) {
                    // A retiring generation may reject close; terminal events still drain.
                    let _ = self.runtime.close(b.key);
                }
                self.remove_binding(handle);
            }
        }
    }
    fn enqueue(&mut self, owner: u64, frame: Frame, delivered: Option<(u128, u64)>) -> bool {
        let bytes = frame.encode();
        let Some(o) = self.owners.get_mut(&owner) else {
            return false;
        };
        if o.output.len() >= self.profile.output_frames
            || bytes.len() > self.profile.output_bytes.saturating_sub(o.output_bytes)
        {
            self.disconnect(owner);
            return false;
        }
        if o.output.is_empty() {
            o.output_since = Instant::now();
        }
        o.output_bytes += bytes.len();
        o.output.push_back(Output {
            bytes,
            written: 0,
            delivered,
        });
        true
    }
    fn flush(&mut self, owner: u64) {
        let mut budget = 16384;
        while budget != 0 {
            let Some(o) = self.owners.get_mut(&owner) else {
                break;
            };
            let Some(front) = o.output.front_mut() else {
                break;
            };
            let remaining = (front.bytes.len() - front.written).min(budget);
            match o
                .socket
                .try_write(&front.bytes[front.written..front.written + remaining])
            {
                Ok(0) => {
                    self.disconnect(owner);
                    break;
                }
                Ok(n) => {
                    front.written += n;
                    budget -= n;
                    o.output_since = Instant::now();
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.disconnect(owner);
                    break;
                }
            }
            if front.written == front.bytes.len() {
                let finished = o.output.pop_front().unwrap();
                o.output_bytes -= finished.bytes.len();
                if let Some((handle, end)) = finished.delivered
                    && let Some(b) = self.bindings.get_mut(&handle)
                {
                    b.delivered = end;
                }
            }
        }
    }
    fn bind(&mut self, owner: u64, key: RuntimeKey) -> Option<u128> {
        if self.bindings.len() >= self.profile.manager.max_streams
            || self.owners.get(&owner)?.streams.len() >= self.profile.streams_per_owner
        {
            return None;
        }
        self.sequence = self.sequence.checked_add(1)?;
        let handle = ((self.epoch as u128) << 64) | self.sequence as u128;
        self.bindings.insert(
            handle,
            Binding {
                owner,
                key,
                delivered: 0,
            },
        );
        self.keys.insert(key, handle);
        self.owners.get_mut(&owner)?.streams.insert(handle);
        Some(handle)
    }
    fn key(&self, owner: u64, handle: u128) -> Result<RuntimeKey, u16> {
        self.bindings
            .get(&handle)
            .filter(|b| b.owner == owner)
            .map(|b| b.key)
            .ok_or(4)
    }
    fn command(&mut self, owner: u64, f: Frame) {
        let Some(o) = self.owners.get_mut(&owner) else {
            return;
        };
        if f.request == 0 || f.request <= o.last_request || !(1..=11).contains(&f.kind) {
            self.disconnect(owner);
            return;
        }
        o.last_request = f.request;
        if !o.hello && f.kind != 1 {
            self.disconnect(owner);
            return;
        }
        let hello = o.hello;
        let owned_count = o.streams.len();
        let mut handle = f.handle;
        let mut value = 0;
        let mut extra = Vec::new();
        let result: Result<(), u16> = (|| {
            if [1, 2, 9, 10, 11].contains(&f.kind) && f.handle != 0 {
                return Err(2);
            }
            match f.kind {
                1 => {
                    if hello || f.payload.len() != 5 || f.payload[4] > 1 {
                        return Err(2);
                    }
                    let min = u16::from_be_bytes(f.payload[..2].try_into().unwrap());
                    let max = u16::from_be_bytes(f.payload[2..4].try_into().unwrap());
                    if min > 1 || max < 1 || min > max {
                        return Err(3);
                    }
                    if f.payload[4] == 1 {
                        if self.acceptor.is_some() {
                            return Err(5);
                        }
                        self.acceptor = Some(owner);
                    }
                    self.owners.get_mut(&owner).unwrap().hello = true;
                    extra.extend_from_slice(&1u16.to_be_bytes());
                    for n in [owner, self.epoch] {
                        extra.extend_from_slice(&n.to_be_bytes());
                    }
                    extra.extend_from_slice(&15u32.to_be_bytes());
                    for n in [
                        protocol::MAX_PAYLOAD,
                        512,
                        self.profile.manager.stream.receive_window as usize,
                        self.profile.streams_per_owner,
                        self.profile.output_frames,
                        self.profile.output_bytes,
                    ] {
                        extra.extend_from_slice(&(n as u32).to_be_bytes());
                    }
                    Ok(())
                }
                2 => {
                    if f.payload.len() < 8 || f.payload.len() > 520 {
                        return Err(2);
                    }
                    if self.sequence == u64::MAX
                        || owned_count >= self.profile.streams_per_owner
                        || self.bindings.len() >= self.profile.manager.max_streams
                    {
                        return Err(5);
                    }
                    let key = self
                        .runtime
                        .open(
                            PeerId(protocol::number(&f.payload[..8]).unwrap()),
                            &f.payload[8..],
                        )
                        .map_err(error_code)?;
                    handle = match self.bind(owner, key) {
                        Some(handle) => handle,
                        None => {
                            let _ = self.runtime.close(key);
                            return Err(5);
                        }
                    };
                    Ok(())
                }
                3..=8 => {
                    let key = self.key(owner, f.handle)?;
                    match f.kind {
                        3 | 4 => {
                            if f.payload.len() > 512 {
                                return Err(2);
                            }
                            if f.kind == 3 {
                                self.runtime.accept(key, &f.payload)
                            } else {
                                self.runtime.reject(key, &f.payload)
                            }
                            .map_err(error_code)
                        }
                        5 => match self.runtime.send(key, &f.payload).map_err(error_code)? {
                            SendOutcome::Accepted(n) => {
                                value = n as u64;
                                Ok(())
                            }
                            SendOutcome::WouldBlock => Err(1),
                        },
                        6 => {
                            let end = protocol::number(&f.payload).ok_or(2u16)?;
                            if end > self.bindings[&f.handle].delivered {
                                return Err(8);
                            }
                            self.runtime.consume_through(key, end).map_err(error_code)
                        }
                        7 | 8 => {
                            if !f.payload.is_empty() {
                                return Err(2);
                            }
                            if f.kind == 7 {
                                self.runtime.finish(key)
                            } else {
                                self.runtime.close(key)
                            }
                            .map_err(error_code)
                        }
                        _ => unreachable!(),
                    }
                }
                9 => {
                    if !f.payload.is_empty() {
                        return Err(2);
                    }
                    let status = self.runtime.status();
                    extra.push(match status.lifecycle {
                        Lifecycle::Connecting => 0,
                        Lifecycle::Ready => 1,
                        Lifecycle::Recovering => 2,
                        Lifecycle::Failed => 3,
                        Lifecycle::ShuttingDown => 4,
                        Lifecycle::Closed => 5,
                    });
                    let queued_count: usize = self.owners.values().map(|o| o.output.len()).sum();
                    let queued_bytes: usize = self.owners.values().map(|o| o.output_bytes).sum();
                    for n in [
                        self.owners.len() as u64,
                        self.bindings.len() as u64,
                        queued_count as u64,
                        queued_bytes as u64,
                        status.resources.streams as u64,
                        status.resources.reserved_receive_bytes as u64,
                        status.resources.pending_send_bytes as u64,
                        status.active_peers as u64,
                        status.membership_slots as u64,
                        status.connections as u64,
                        status.counters.shard_failures,
                        status.counters.peer_timeouts,
                    ] {
                        extra.extend_from_slice(&n.to_be_bytes());
                    }
                    Ok(())
                }
                10 | 11 => {
                    let peer = PeerId(protocol::number(&f.payload).ok_or(2u16)?);
                    if f.kind == 10 {
                        if self.runtime.peer_ready(peer) {
                            Ok(())
                        } else {
                            self.runtime.join_peer(peer).map_err(error_code)
                        }
                    } else {
                        extra.push(self.runtime.peer_ready(peer) as u8);
                        Ok(())
                    }
                }
                _ => Err(2),
            }
        })();
        self.enqueue(
            owner,
            protocol::response(f.request, handle, result.err().unwrap_or(0), value, &extra),
            None,
        );
        // HELLO negotiation failure is returned before a bounded timeout disconnect.
    }
    fn events(&mut self) -> usize {
        let events = self.runtime.poll_events(128);
        let count = events.len();
        for e in events {
            let handle = if let Event::IncomingOpen { .. } = e.event {
                match self.acceptor.and_then(|owner| self.bind(owner, e.key)) {
                    Some(handle) => handle,
                    None => {
                        let _ = self.runtime.reject(e.key, b"no local acceptor capacity");
                        continue;
                    }
                }
            } else {
                match self.keys.get(&e.key).copied() {
                    Some(handle) => handle,
                    None => continue,
                }
            };
            let Some(b) = self.bindings.get(&handle) else {
                continue;
            };
            let owner = b.owner;
            let mut delivered = None;
            let mut terminal = false;
            let (kind, payload) = match e.event {
                Event::IncomingOpen { metadata } => {
                    let mut p = e.key.stream.peer.0.to_be_bytes().to_vec();
                    p.extend_from_slice(&metadata);
                    (protocol::INCOMING, p)
                }
                Event::Opened { metadata } => (protocol::OPENED, metadata.into_vec()),
                Event::Rejected { reason } => (protocol::REJECTED, reason.into_vec()),
                Event::Data { offset, bytes } => {
                    delivered = Some((handle, offset + bytes.len() as u64));
                    let mut p = offset.to_be_bytes().to_vec();
                    p.extend_from_slice(&bytes);
                    (protocol::DATA, p)
                }
                Event::Writable => (protocol::WRITABLE, vec![]),
                Event::RemoteFinished => (protocol::REMOTE_FINISHED, vec![]),
                Event::Closed { reason } => {
                    terminal = true;
                    let reason: u16 = match reason {
                        CloseReason::Finished => 1,
                        CloseReason::Rejected => 2,
                        CloseReason::Cancelled => 3,
                        CloseReason::TransportLost => 4,
                        CloseReason::ProtocolError => 5,
                        CloseReason::OpenTimeout => 6,
                    };
                    (protocol::CLOSED, reason.to_be_bytes().to_vec())
                }
            };
            self.enqueue(
                owner,
                Frame {
                    kind,
                    request: 0,
                    handle,
                    payload,
                },
                delivered,
            );
            if terminal {
                self.remove_binding(handle);
            }
            // Healthy writable sockets are serviced during the bounded batch too.
            self.flush(owner);
        }
        count
    }
    fn owner_turn(&mut self, owner: u64) -> usize {
        self.flush(owner);
        let Some(o) = self.owners.get_mut(&owner) else {
            return 0;
        };
        let now = Instant::now();
        if (!o.hello && now.duration_since(o.started) > self.profile.timeout)
            || o.partial_since
                .is_some_and(|t| now.duration_since(t) > self.profile.timeout)
            || (!o.output.is_empty() && now.duration_since(o.output_since) > self.profile.timeout)
        {
            self.disconnect(owner);
            return 0;
        }
        let result = o.decoder.read(&o.socket);
        if o.decoder.partial() {
            o.partial_since.get_or_insert(now);
        } else {
            o.partial_since = None;
        }
        match result {
            Ok(Some(f)) => {
                self.command(owner, f);
                self.flush(owner);
                1
            }
            Ok(None) => 0,
            Err(_) => {
                self.disconnect(owner);
                0
            }
        }
    }
}

pub async fn run(profile: Profile, uid: u32) -> Result<(), (u8, &'static str)> {
    let endpoint = Endpoint::bind(&profile.ipc_path, uid)
        .map_err(|_| (2, "unsafe or occupied IPC endpoint"))?;
    let runtime = NatsRuntime::connect(profile.runtime.clone(), profile.manager)
        .await
        .map_err(|e| {
            let message = match e {
                RuntimeError::Tls => "TLS failed",
                RuntimeError::Authentication => "authentication failed",
                RuntimeError::Authorization => "authorization failed",
                RuntimeError::Config => "invalid runtime profile",
                _ => "runtime connection failed",
            };
            (3, message)
        })?;
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).map_err(|_| (4, "identity randomness failed"))?;
    let epoch = u64::from_be_bytes(bytes);
    if epoch == 0 {
        return Err((4, "identity randomness failed"));
    }
    let mut host = Host {
        runtime,
        profile,
        owners: BTreeMap::new(),
        bindings: BTreeMap::new(),
        keys: BTreeMap::new(),
        acceptor: None,
        epoch,
        sequence: 0,
        session: 0,
    };
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| (4, "signal setup failed"))?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|_| (4, "signal setup failed"))?;
    println!("READY ipc=1");
    let mut failed = false;
    loop {
        let stopping = tokio::select! { biased; _ = term.recv() => true, _ = interrupt.recv() => true, _ = std::future::ready(()) => false };
        if stopping {
            break;
        }
        match endpoint.listener.accept() {
            Ok((socket, _)) => {
                if host.owners.len() < host.profile.owners {
                    socket
                        .set_nonblocking(true)
                        .map_err(|_| (4, "IPC setup failed"))?;
                    let socket =
                        UnixStream::from_std(socket).map_err(|_| (4, "IPC setup failed"))?;
                    if socket.peer_cred().map(|c| c.uid()).ok() == Some(uid) {
                        host.session = host
                            .session
                            .checked_add(1)
                            .ok_or((4, "session identity exhausted"))?;
                        let now = Instant::now();
                        host.owners.insert(
                            host.session,
                            Owner {
                                socket,
                                decoder: Decoder::default(),
                                hello: false,
                                last_request: 0,
                                started: now,
                                partial_since: None,
                                output_since: now,
                                output: VecDeque::new(),
                                output_bytes: 0,
                                streams: BTreeSet::new(),
                            },
                        );
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            Err(_) => {
                failed = true;
                break;
            }
        }
        let owners: Vec<_> = host.owners.keys().copied().collect();
        let mut progress = 0;
        for owner in owners {
            progress += host.owner_turn(owner);
        }
        // Never cancel a runtime turn: its output guards would fail touched lanes.
        if let Ok(n) = host.runtime.turn(Duration::ZERO).await {
            progress += n;
        }
        progress += host.events();
        if host.runtime.status().lifecycle == Lifecycle::Failed {
            failed = true;
            break;
        }
        if progress == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
    // One deadline covers every terminal turn/drain/shutdown operation. Once this
    // future is cancelled, the host is dropped and its runtime is never reused.
    let _ = tokio::time::timeout(host.profile.shutdown_timeout, async {
        for owner in host.owners.keys().copied().collect::<Vec<_>>() {
            host.disconnect(owner);
        }
        while host.runtime.status().resources.streams != 0 {
            let _ = host.runtime.turn(Duration::ZERO).await;
            host.events();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let _ = host.runtime.shutdown().await;
        while !host.runtime.poll_events(256).is_empty() {}
    })
    .await;
    println!("STOPPED ipc=1");
    if failed {
        Err((4, "runtime stopped"))
    } else {
        Ok(())
    }
}

#[cfg(all(test, feature = "real-nats"))]
mod tests {
    use super::*;
    use skvoz_core::{
        ManagerConfig,
        runtime::{Authentication, Membership, RuntimeConfig, Trust},
    };
    #[tokio::test]
    async fn exhausted_handle_rejects_before_runtime_stream_allocation() {
        let namespace = format!(
            "skvoz.runtime.{}.exhaustion",
            std::env::var("SKVOZ_NATS_RUN_TOKEN").expect("use the real broker runner")
        );
        let config = |id| {
            RuntimeConfig::new(
                std::env::var("SKVOZ_NATS_URL").unwrap(),
                Trust::ManagedCa(std::env::var("SKVOZ_NATS_CA").unwrap().into()),
                Authentication {
                    username: format!("p{id}"),
                    password: std::env::var(format!("SKVOZ_NATS_P{id}_PASSWORD")).unwrap(),
                },
                namespace.clone(),
                PeerId(id),
                Membership::Allowlist(BTreeSet::from([PeerId(1 - id)])),
            )
        };
        let limits = ManagerConfig::default();
        let mut server = NatsRuntime::connect(config(0), limits).await.unwrap();
        let mut runtime = NatsRuntime::connect(config(1), limits).await.unwrap();
        runtime.join_peer(PeerId(0)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !runtime.peer_ready(PeerId(0)) || !server.peer_ready(PeerId(1)) {
            assert!(Instant::now() < deadline);
            server.turn(Duration::ZERO).await.unwrap();
            runtime.turn(Duration::ZERO).await.unwrap();
            server.poll_events(256);
            runtime.poll_events(256);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let profile = Profile {
            ipc_path: std::path::PathBuf::new(),
            runtime: config(1),
            manager: limits,
            owners: 2,
            streams_per_owner: 2,
            output_frames: 8,
            output_bytes: 8192,
            timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        };
        let (socket, _reader) = UnixStream::pair().unwrap();
        let now = Instant::now();
        let owner = Owner {
            socket,
            decoder: Decoder::default(),
            hello: true,
            last_request: 0,
            started: now,
            partial_since: None,
            output_since: now,
            output: VecDeque::new(),
            output_bytes: 0,
            streams: BTreeSet::new(),
        };
        let mut host = Host {
            runtime,
            profile,
            owners: BTreeMap::from([(1, owner)]),
            bindings: BTreeMap::new(),
            keys: BTreeMap::new(),
            acceptor: None,
            epoch: 1,
            sequence: u64::MAX,
            session: 1,
        };
        host.command(
            1,
            Frame {
                kind: 2,
                request: 1,
                handle: 0,
                payload: 0u64.to_be_bytes().to_vec(),
            },
        );
        assert_eq!(host.runtime.status().resources.streams, 0);
        assert_eq!(host.runtime.status().resources.reserved_receive_bytes, 0);
        assert!(host.bindings.is_empty());
        let reply = Frame::decode(host.owners[&1].output[0].bytes[4..].to_vec()).unwrap();
        assert_eq!(
            u16::from_be_bytes(reply.payload[..2].try_into().unwrap()),
            5
        );
        host.runtime.shutdown().await.unwrap();
        server.shutdown().await.unwrap();
    }
}
