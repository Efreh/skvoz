//! Optional reusable NATS driver for the same universal Core manager.
use crate::{
    CloseReason, ManagedEvent, Manager, ManagerConfig, PeerId, Resources, SendOutcome, Snapshot,
    StreamKey, wire,
};
use futures_util::{FutureExt, StreamExt};
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
use tokio::sync::Notify;
pub type NatsError = Box<dyn std::error::Error + Send + Sync>;
#[derive(Clone, Debug)]
pub struct PeerRoute {
    pub id: PeerId,
    pub session: String,
}
#[derive(Clone)]
pub struct NatsConfig {
    pub url: String,
    pub ca: PathBuf,
    pub username: String,
    pub password: String,
    pub namespace: String,
    pub id: PeerId,
    pub session: String,
    pub peers: Vec<PeerRoute>,
    pub name: String,
    pub subscription_capacity: usize,
    pub client_capacity: usize,
    pub max_outgoing_per_turn: usize,
    pub max_incoming_per_turn: usize,
    pub io_timeout: Duration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
// A polled frame is no longer owned by Manager. Abandoning its publish/flush
// future must never leave a healthy node with a gap in the ordered stream.
struct OutputGuard {
    failure: Arc<Failure>,
    completed: bool,
}
impl Drop for OutputGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.failure.mark(FailureKind::ClientError);
        }
    }
}
fn error(message: &str) -> NatsError {
    std::io::Error::other(message).into()
}
fn token_valid(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
async fn bounded<T, E: Into<NatsError>>(
    timeout: Duration,
    f: impl Future<Output = Result<T, E>>,
) -> Result<T, NatsError> {
    tokio::time::timeout(timeout, f)
        .await
        .map_err(|_| error("NATS operation timed out"))?
        .map_err(Into::into)
}
/// No per-stream tasks or connections. Connector APIs enqueue; `turn` performs
/// bounded I/O. Session/credential provisioning belongs to the embedding host.
pub struct NatsNode {
    manager: Manager,
    client: async_nats::Client,
    subscription: async_nats::Subscriber,
    config: NatsConfig,
    peers: BTreeMap<PeerId, String>,
    started: Instant,
    failure: Arc<Failure>,
}
impl NatsNode {
    pub async fn connect(config: NatsConfig, limits: ManagerConfig) -> Result<Self, NatsError> {
        if !token_valid(&config.session)
            || config.namespace.len() > 256
            || config.namespace.split('.').any(|s| !token_valid(s))
            || config.subscription_capacity == 0
            || config.subscription_capacity > 65536
            || config.client_capacity == 0
            || config.client_capacity > 65536
            || config.max_outgoing_per_turn == 0
            || config.max_outgoing_per_turn > 256
            || config.max_incoming_per_turn == 0
            || config.max_incoming_per_turn > 256
            || config.io_timeout.is_zero()
            || config.peers.len() > limits.max_peers
        {
            return Err(error("invalid NATS node configuration"));
        }
        let mut manager = Manager::new(limits)?;
        let mut peers = BTreeMap::new();
        for p in &config.peers {
            if p.id == config.id
                || !token_valid(&p.session)
                || peers.insert(p.id, p.session.clone()).is_some()
            {
                return Err(error("invalid or duplicate peer route"));
            }
            manager.register_peer(p.id, config.id > p.id)?;
        }
        let failure = Arc::new(Failure::default());
        let callback = failure.clone();
        let client = bounded(
            config.io_timeout,
            async_nats::ConnectOptions::with_user_and_password(
                config.username.clone(),
                config.password.clone(),
            )
            .name(config.name.clone())
            .require_tls(true)
            .add_root_certificates(config.ca.clone())
            .connection_timeout(config.io_timeout)
            .ping_interval(Duration::from_millis(100))
            .max_reconnects(1)
            .client_capacity(config.client_capacity)
            .subscription_capacity(config.subscription_capacity)
            .ignore_discovered_servers()
            .event_callback(move |event| {
                let failure = callback.clone();
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
            .connect(config.url.clone()),
        )
        .await?;
        if client.max_payload() < wire::MAX_PACKET_BYTES {
            return Err(error("broker max_payload is below the Core packet limit"));
        }
        let inbox = format!(
            "{}.{}.{}.*.*",
            config.namespace, config.id.0, config.session
        );
        let subscription = bounded(config.io_timeout, client.subscribe(inbox)).await?;
        bounded(config.io_timeout, client.flush()).await?;
        Ok(Self {
            manager,
            client,
            subscription,
            config,
            peers,
            started: Instant::now(),
            failure,
        })
    }
    fn now(&self) -> Result<u64, NatsError> {
        Ok(self.started.elapsed().as_millis().try_into()?)
    }
    fn apply_failure(&mut self) -> bool {
        if self.failure.kind().is_some() {
            self.manager.transport_lost();
            true
        } else {
            false
        }
    }
    fn prepare(&mut self) -> Result<u64, NatsError> {
        if self.apply_failure() {
            return Err(error("NATS node transport is terminal"));
        }
        let now = self.now()?;
        self.manager.tick(now)?;
        Ok(now)
    }
    pub fn failure_kind(&self) -> Option<FailureKind> {
        self.failure.kind()
    }
    pub fn resources(&self) -> Resources {
        self.manager.resources()
    }
    pub fn snapshot(&self, key: StreamKey) -> Option<Snapshot> {
        self.manager.snapshot(key)
    }
    pub fn open(&mut self, peer: PeerId, metadata: &[u8]) -> Result<StreamKey, NatsError> {
        let now = self.prepare()?;
        Ok(self.manager.open(peer, metadata, now)?)
    }
    pub fn accept(&mut self, key: StreamKey, metadata: &[u8]) -> Result<(), NatsError> {
        self.prepare()?;
        Ok(self.manager.accept(key, metadata)?)
    }
    pub fn reject(&mut self, key: StreamKey, reason: &[u8]) -> Result<(), NatsError> {
        self.prepare()?;
        Ok(self.manager.reject(key, reason)?)
    }
    pub fn send(&mut self, key: StreamKey, bytes: &[u8]) -> Result<SendOutcome, NatsError> {
        self.prepare()?;
        Ok(self.manager.send(key, bytes)?)
    }
    pub fn finish(&mut self, key: StreamKey) -> Result<(), NatsError> {
        self.prepare()?;
        Ok(self.manager.finish(key)?)
    }
    pub fn consume_through(&mut self, key: StreamKey, offset: u64) -> Result<(), NatsError> {
        self.prepare()?;
        Ok(self.manager.consume_through(key, offset)?)
    }
    pub fn close(&mut self, key: StreamKey) -> Result<(), NatsError> {
        self.apply_failure();
        Ok(self.manager.close(key, CloseReason::Cancelled)?)
    }
    pub fn poll_events(&mut self, max: usize) -> Vec<ManagedEvent> {
        self.apply_failure();
        self.manager.poll_events(max)
    }
    pub fn peer_lost(&mut self, peer: PeerId) {
        self.manager.peer_lost(peer);
    }
    fn subject(&self, peer: PeerId) -> Result<String, NatsError> {
        let session = self
            .peers
            .get(&peer)
            .ok_or_else(|| error("unknown NATS peer"))?;
        Ok(format!(
            "{}.{}.{}.{}.{}",
            self.config.namespace, peer.0, session, self.config.id.0, self.config.session
        ))
    }
    fn sender(&self, subject: &str) -> Option<PeerId> {
        let tail = subject.strip_prefix(&format!(
            "{}.{}.{}.",
            self.config.namespace, self.config.id.0, self.config.session
        ))?;
        let (id, session) = tail.split_once('.')?;
        let peer = PeerId(id.parse().ok()?);
        (self.peers.get(&peer).is_some_and(|s| s == session)).then_some(peer)
    }
    async fn publish(&mut self, subject: String, payload: Vec<u8>) -> Result<(), NatsError> {
        if let Err(e) = bounded(
            self.config.io_timeout,
            self.client.publish(subject, payload.into()),
        )
        .await
        {
            self.failure.mark(FailureKind::Disconnected);
            self.apply_failure();
            return Err(e);
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), NatsError> {
        if let Err(e) = bounded(self.config.io_timeout, self.client.flush()).await {
            self.failure.mark(FailureKind::Disconnected);
            self.apply_failure();
            return Err(e);
        }
        Ok(())
    }
    async fn output(&mut self, remaining: &mut usize) -> Result<usize, NatsError> {
        let mut sent = 0;
        let mut guard = None;
        while *remaining > 0 {
            let Some(frame) = self.manager.poll_frames(1).pop() else {
                break;
            };
            guard.get_or_insert_with(|| OutputGuard {
                failure: self.failure.clone(),
                completed: false,
            });
            *remaining -= 1;
            sent += 1;
            self.publish(
                self.subject(frame.key.peer)?,
                wire::encode(frame.key.stream_id, &frame.frame)?,
            )
            .await?;
        }
        if sent > 0 {
            self.flush().await?;
        }
        if let Some(guard) = &mut guard {
            guard.completed = true;
        }
        Ok(sent)
    }
    /// Publish one bounded output batch without consuming inbound messages.
    pub async fn flush_pending(&mut self) -> Result<usize, NatsError> {
        if self.apply_failure() {
            return Ok(0);
        }
        self.prepare()?;
        let mut remaining = self.config.max_outgoing_per_turn;
        self.output(&mut remaining).await
    }
    /// At most configured input/output messages, regardless of total idle streams.
    /// Dropping this future during publish/flush permanently fails the node:
    /// the next owner API/turn/poll applies transport loss. Idle input wait is
    /// cancellation-safe; already dispatched output is never transparently retried.
    pub async fn turn(&mut self, wait: Duration) -> Result<usize, NatsError> {
        if self.apply_failure() {
            return Ok(0);
        }
        let now = self.prepare()?;
        let mut remaining = self.config.max_outgoing_per_turn;
        let mut progress = self.output(&mut remaining).await?;
        let deadline_wait = self
            .manager
            .next_deadline()
            .map(|d| Duration::from_millis(d.saturating_sub(now)))
            .unwrap_or(wait);
        let actual_wait = if progress > 0 {
            Duration::ZERO
        } else {
            wait.min(deadline_wait)
        };
        for i in 0..self.config.max_incoming_per_turn {
            let message = if i == 0 && !actual_wait.is_zero() {
                tokio::select! {m=self.subscription.next()=>m,_=self.failure.notify.notified()=>None,_=tokio::time::sleep(actual_wait)=>break}
            } else {
                match self.subscription.next().now_or_never() {
                    Some(m) => m,
                    None => break,
                }
            };
            if self.apply_failure() {
                return Ok(progress);
            }
            let Some(message) = message else {
                self.failure.mark(FailureKind::Disconnected);
                self.apply_failure();
                return Ok(progress);
            };
            progress += 1;
            let Some(peer) = self.sender(message.subject.as_str()) else {
                continue;
            };
            let packet = match wire::decode(&message.payload) {
                Ok(p) => p,
                Err(e) => {
                    self.manager.protocol_error(peer);
                    self.output(&mut remaining).await?;
                    return Err(e.into());
                }
            };
            let now = self.now()?;
            let outcome = self.manager.receive(
                StreamKey {
                    peer,
                    stream_id: packet.stream_id,
                },
                &packet.frame,
                now,
            );
            if let Err(e) = outcome {
                self.output(&mut remaining).await?;
                return Err(e.into());
            }
        }
        self.manager.tick(self.now()?)?;
        progress += self.output(&mut remaining).await?;
        self.apply_failure();
        Ok(progress)
    }
    /// Diagnostic injection for broker/ACL tests; normal traffic uses `turn`.
    pub async fn inject_packet(&mut self, peer: PeerId, payload: Vec<u8>) -> Result<(), NatsError> {
        self.publish(self.subject(peer)?, payload).await?;
        self.flush().await
    }
    pub async fn inject_subject(
        &mut self,
        subject: String,
        payload: Vec<u8>,
    ) -> Result<(), NatsError> {
        self.publish(subject, payload).await?;
        self.flush().await
    }
    pub async fn shutdown(mut self) -> Result<(), NatsError> {
        self.manager.transport_lost();
        bounded(self.config.io_timeout, self.subscription.unsubscribe()).await?;
        bounded(self.config.io_timeout, self.client.drain()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn dropped_output_future_latches_failure_but_completed_batch_does_not() {
        let failure = Arc::new(Failure::default());
        let f = failure.clone();
        let mut pending = Box::pin(async move {
            let _guard = OutputGuard {
                failure: f,
                completed: false,
            };
            std::future::pending::<()>().await;
        });
        assert!(pending.as_mut().now_or_never().is_none());
        drop(pending);
        assert_eq!(failure.kind(), Some(FailureKind::ClientError));
        let healthy = Arc::new(Failure::default());
        let mut guard = OutputGuard {
            failure: healthy.clone(),
            completed: false,
        };
        guard.completed = true;
        drop(guard);
        assert_eq!(healthy.kind(), None);
    }
}
