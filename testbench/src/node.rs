use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use skvoz_core::{CloseReason, Config, Event, Frame, SendOutcome, Snapshot, State, Stream, wire};
use tokio::sync::Notify;

pub type BenchError = Box<dyn std::error::Error + Send + Sync>;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    User,
    Consumer,
}

impl Role {
    pub fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Consumer => "consumer",
        }
    }
    fn bit(self) -> u64 {
        match self {
            Self::User => 0,
            Self::Consumer => 1,
        }
    }
    fn peer(self) -> Self {
        match self {
            Self::User => Self::Consumer,
            Self::Consumer => Self::User,
        }
    }
}

pub struct ConnectionConfig {
    pub url: String,
    pub ca: PathBuf,
    pub password: String,
    pub run_token: String,
    pub role: Role,
}

impl ConnectionConfig {
    pub fn from_env(role: Role) -> Result<Self, BenchError> {
        Ok(Self {
            url: std::env::var("SKVOZ_NATS_URL")?,
            ca: std::env::var("SKVOZ_NATS_CA")?.into(),
            password: std::env::var(match role {
                Role::User => "SKVOZ_NATS_USER_PASSWORD",
                Role::Consumer => "SKVOZ_NATS_CONSUMER_PASSWORD",
            })?,
            run_token: std::env::var("SKVOZ_NATS_RUN_TOKEN")?,
            role,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BenchConfig {
    pub stream: Config,
    pub max_streams: usize,
    pub subscription_capacity: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            stream: Config {
                receive_window: 8192,
                max_frame: 1024,
                max_pending_frames: 8,
                max_metadata: 256,
                open_timeout_ms: 1000,
            },
            max_streams: 32,
            subscription_capacity: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Disconnected = 1,
    SlowConsumer = 2,
    ServerError = 3,
    ClientError = 4,
}

#[derive(Default)]
struct Failure {
    kind: AtomicU8,
    notify: Notify,
}

impl Failure {
    fn mark(&self, kind: FailureKind) {
        let _ = self
            .kind
            .compare_exchange(0, kind as u8, Ordering::SeqCst, Ordering::SeqCst);
        self.notify.notify_one();
    }
    fn kind(&self) -> Option<FailureKind> {
        match self.kind.load(Ordering::SeqCst) {
            1 => Some(FailureKind::Disconnected),
            2 => Some(FailureKind::SlowConsumer),
            3 => Some(FailureKind::ServerError),
            4 => Some(FailureKind::ClientError),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct NodeEvent {
    pub stream_id: u64,
    pub event: Event,
}

/// A bounded experimental driver for two fixed owners sharing multiple streams.
pub struct Node {
    client: async_nats::Client,
    subscription: async_nats::Subscriber,
    inbox: String,
    peer_inbox: String,
    role: Role,
    config: BenchConfig,
    started: Instant,
    streams: BTreeMap<u64, Stream>,
    local_sequence: u64,
    remote_sequence: u64,
    failure: Arc<Failure>,
}

fn error(message: &str) -> BenchError {
    std::io::Error::other(message).into()
}

async fn bounded<T, E: Into<BenchError>>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, BenchError> {
    tokio::time::timeout(IO_TIMEOUT, future)
        .await
        .map_err(|_| error("NATS operation timed out"))?
        .map_err(Into::into)
}

fn token_valid(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 64
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

impl Node {
    pub async fn connect(
        connection: ConnectionConfig,
        case_token: &str,
        config: BenchConfig,
    ) -> Result<Self, BenchError> {
        config.stream.validate()?;
        if !token_valid(&connection.run_token)
            || !token_valid(case_token)
            || config.max_streams == 0
            || config.max_streams > 32
            || config.subscription_capacity == 0
            || config.subscription_capacity > 256
            || config.stream.receive_window > 65536
            || config.stream.max_frame > 8192
            || config.stream.max_pending_frames > 16
            || config.stream.max_metadata > 1024
        {
            return Err(error("testbench configuration exceeds its bounded scope"));
        }
        let namespace = format!("skvoz.bench.{}.{}", connection.run_token, case_token);
        let inbox = format!("{namespace}.{}", connection.role.label());
        let peer_inbox = format!("{namespace}.{}", connection.role.peer().label());
        let failure = Arc::new(Failure::default());
        let callback_failure = failure.clone();
        let client = bounded(
            async_nats::ConnectOptions::with_user_and_password(
                connection.role.label().into(),
                connection.password,
            )
            .name(format!("skvoz-{case_token}-{}", connection.role.label()))
            .require_tls(true)
            .tls_first()
            .add_root_certificates(connection.ca)
            .connection_timeout(IO_TIMEOUT)
            .ping_interval(Duration::from_millis(100))
            .max_reconnects(1)
            .client_capacity(32)
            .subscription_capacity(config.subscription_capacity)
            .ignore_discovered_servers()
            .event_callback(move |event| {
                let failure = callback_failure.clone();
                async move {
                    match event {
                        async_nats::Event::Disconnected | async_nats::Event::Closed => {
                            failure.mark(FailureKind::Disconnected)
                        }
                        async_nats::Event::SlowConsumer(_) => {
                            failure.mark(FailureKind::SlowConsumer)
                        }
                        async_nats::Event::ServerError(_) => failure.mark(FailureKind::ServerError),
                        async_nats::Event::ClientError(_) => failure.mark(FailureKind::ClientError),
                        _ => {}
                    }
                }
            })
            .connect(connection.url),
        )
        .await?;
        if client.max_payload() < wire::MAX_PACKET_BYTES {
            return Err(error(
                "broker max_payload is below the testbench packet limit",
            ));
        }
        let subscription = bounded(client.subscribe(inbox.clone())).await?;
        bounded(client.flush()).await?;
        Ok(Self {
            client,
            subscription,
            inbox,
            peer_inbox,
            role: connection.role,
            config,
            started: Instant::now(),
            streams: BTreeMap::new(),
            local_sequence: 0,
            remote_sequence: 0,
            failure,
        })
    }

    pub fn failure_kind(&self) -> Option<FailureKind> {
        self.failure.kind()
    }
    pub fn slot_count(&self) -> usize {
        self.streams.len()
    }
    pub fn snapshot(&self, id: u64) -> Option<Snapshot> {
        self.streams.get(&id).map(Stream::snapshot)
    }
    fn now_ms(&self) -> Result<u64, BenchError> {
        Ok(self.started.elapsed().as_millis().try_into()?)
    }
    fn stream(&mut self, id: u64) -> Result<&mut Stream, BenchError> {
        self.streams
            .get_mut(&id)
            .ok_or_else(|| error("unknown testbench stream"))
    }

    fn apply_failure(&mut self) -> bool {
        if self.failure.kind().is_none() {
            return false;
        }
        for stream in self.streams.values_mut() {
            stream.transport_lost();
        }
        true
    }

    fn require_transport(&mut self) -> Result<(), BenchError> {
        if self.apply_failure() {
            Err(error("testbench transport is terminal; create a new node"))
        } else {
            let now = self.now_ms()?;
            for stream in self.streams.values_mut() {
                stream.tick(now)?;
            }
            Ok(())
        }
    }

    pub async fn open(&mut self, metadata: &[u8]) -> Result<u64, BenchError> {
        self.require_transport()?;
        if self.streams.len() >= self.config.max_streams {
            return Err(error("testbench stream slot limit reached"));
        }
        let sequence = self
            .local_sequence
            .checked_add(1)
            .ok_or_else(|| error("stream identity exhausted"))?;
        let id = sequence
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.role.bit()))
            .ok_or_else(|| error("stream identity exhausted"))?;
        let mut stream = Stream::new(self.config.stream)?;
        stream.open(metadata, self.now_ms()?)?;
        self.local_sequence = sequence;
        self.streams.insert(id, stream);
        self.flush_outgoing().await?;
        Ok(id)
    }

    pub async fn accept(&mut self, id: u64, metadata: &[u8]) -> Result<(), BenchError> {
        self.require_transport()?;
        self.stream(id)?.accept(metadata)?;
        self.flush_outgoing().await
    }
    pub async fn reject(&mut self, id: u64, reason: &[u8]) -> Result<(), BenchError> {
        self.require_transport()?;
        self.stream(id)?.reject(reason)?;
        self.flush_outgoing().await
    }
    pub async fn send(&mut self, id: u64, bytes: &[u8]) -> Result<SendOutcome, BenchError> {
        self.require_transport()?;
        let outcome = self.stream(id)?.send(bytes)?;
        self.flush_outgoing().await?;
        Ok(outcome)
    }
    pub async fn finish(&mut self, id: u64) -> Result<(), BenchError> {
        self.require_transport()?;
        self.stream(id)?.finish()?;
        self.flush_outgoing().await
    }
    pub async fn consume_through(&mut self, id: u64, offset: u64) -> Result<(), BenchError> {
        self.require_transport()?;
        self.stream(id)?.consume_through(offset)?;
        self.flush_outgoing().await
    }
    pub async fn close(&mut self, id: u64) -> Result<(), BenchError> {
        self.stream(id)?.close(CloseReason::Cancelled)?;
        if self.apply_failure() {
            Ok(())
        } else {
            self.flush_outgoing().await
        }
    }

    pub fn poll_events(&mut self) -> Vec<NodeEvent> {
        self.apply_failure();
        self.streams
            .iter_mut()
            .flat_map(|(&stream_id, stream)| {
                stream
                    .poll_events(256)
                    .into_iter()
                    .map(move |event| NodeEvent { stream_id, event })
            })
            .collect()
    }

    /// Drive one actual broker message or a bounded wait; no background stream tasks.
    pub async fn turn(&mut self, wait: Duration) -> Result<usize, BenchError> {
        if self.apply_failure() {
            return Ok(0);
        }
        let now = self.now_ms()?;
        for stream in self.streams.values_mut() {
            stream.tick(now)?;
        }
        self.flush_outgoing().await?;
        let message = tokio::select! {
            message = self.subscription.next() => message,
            _ = self.failure.notify.notified() => None,
            _ = tokio::time::sleep(wait) => return Ok(0),
        };
        if self.apply_failure() {
            return Ok(0);
        }
        let Some(message) = message else {
            self.failure.mark(FailureKind::Disconnected);
            self.apply_failure();
            return Ok(0);
        };
        let packet = match wire::decode(&message.payload) {
            Ok(packet) => packet,
            Err(cause) => {
                for stream in self.streams.values_mut() {
                    stream.close(CloseReason::ProtocolError)?;
                }
                self.flush_outgoing().await?;
                return Err(cause.into());
            }
        };
        if !self.streams.contains_key(&packet.stream_id) {
            if !matches!(packet.frame, Frame::Open { .. }) {
                return Ok(1);
            }
            let sequence = packet.stream_id >> 1;
            if packet.stream_id & 1 != self.role.peer().bit() || sequence == 0 {
                return Err(error("OPEN has an invalid fixed-owner origin"));
            }
            if sequence <= self.remote_sequence {
                return Ok(1);
            }
            self.remote_sequence = sequence;
            if self.streams.len() >= self.config.max_streams {
                self.publish_frame(
                    packet.stream_id,
                    &Frame::Reject {
                        reason: b"stream slot limit".as_slice().into(),
                    },
                )
                .await?;
                bounded(self.client.flush()).await?;
                return Ok(1);
            }
            self.streams
                .insert(packet.stream_id, Stream::new(self.config.stream)?);
        }
        let now = self.now_ms()?;
        let outcome = self.stream(packet.stream_id)?.receive(&packet.frame, now);
        self.flush_outgoing().await?;
        outcome?;
        Ok(1)
    }

    async fn publish_frame(&mut self, id: u64, frame: &Frame) -> Result<(), BenchError> {
        let bytes = wire::encode(id, frame)?;
        if let Err(cause) =
            bounded(self.client.publish(self.peer_inbox.clone(), bytes.into())).await
        {
            self.failure.mark(FailureKind::Disconnected);
            self.apply_failure();
            return Err(cause);
        }
        Ok(())
    }

    async fn flush_outgoing(&mut self) -> Result<(), BenchError> {
        self.require_transport()?;
        let ids: Vec<_> = self.streams.keys().copied().collect();
        let mut published = false;
        loop {
            let mut progress = false;
            for &id in &ids {
                if let Some(frame) = self.stream(id)?.poll_frames(1).pop() {
                    self.publish_frame(id, &frame).await?;
                    progress = true;
                    published = true;
                }
            }
            if !progress {
                break;
            }
        }
        if published && let Err(cause) = bounded(self.client.flush()).await {
            self.failure.mark(FailureKind::Disconnected);
            self.apply_failure();
            return Err(cause);
        }
        self.require_transport()
    }

    /// Fault injection still publishes through the real authenticated connection.
    pub async fn inject_packet(&self, bytes: Vec<u8>) -> Result<(), BenchError> {
        bounded(self.client.publish(self.peer_inbox.clone(), bytes.into())).await?;
        bounded(self.client.flush()).await
    }

    pub async fn attempt_forbidden_publish(&self) -> Result<(), BenchError> {
        bounded(
            self.client
                .publish(self.inbox.clone(), b"forbidden".to_vec().into()),
        )
        .await?;
        bounded(self.client.flush()).await
    }

    pub async fn shutdown(mut self) -> Result<(), BenchError> {
        if !self.apply_failure() {
            for stream in self.streams.values_mut() {
                if stream.state() != State::Closed {
                    stream.close(CloseReason::Cancelled)?;
                }
            }
            self.flush_outgoing().await?;
        }
        bounded(self.subscription.unsubscribe()).await?;
        bounded(self.client.drain()).await
    }
}
