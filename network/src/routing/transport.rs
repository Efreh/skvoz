//! One bounded control task per authority/participant, independent of DATA I/O.
use super::*;
use crate::config::CoreConfig;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, mpsc};

pub(crate) struct View {
    pub epoch: SessionId,
    pub lease: LocalLease,
    pub egress: Arc<Mutex<Egress>>,
    pub results: BTreeMap<u64, Result<Assignment, NetworkError>>,
    pub pending: BTreeMap<u64, (Metadata, Instant)>,
    pub alive: bool,
    pub ever_ready: bool,
}
impl View {
    pub fn ready(&self) -> bool {
        self.alive && self.lease.ready(Instant::now())
    }
}
enum Local {
    Assign(u64, Metadata),
    Cancel(Attempt),
}
pub(crate) struct Participant {
    pub view: Arc<Mutex<View>>,
    commands: mpsc::Sender<Local>,
    sequence: Arc<AtomicU64>,
    signal: Arc<Notify>,
    id: u64,
}
impl Participant {
    pub fn submit(&self, target: Metadata) -> Result<u64, NetworkError> {
        target.encode()?;
        let mut view = self.view.lock().map_err(|_| NetworkError::InvalidState)?;
        if !view.ready() {
            return Err(NetworkError::InvalidState);
        }
        if view.pending.len() + view.results.len() >= 32 {
            return Err(NetworkError::Overloaded);
        }
        let sequence = next(&self.sequence)?;
        view.pending
            .insert(sequence, (target.clone(), Instant::now()));
        if self
            .commands
            .try_send(Local::Assign(sequence, target))
            .is_err()
        {
            view.pending.remove(&sequence);
            return Err(NetworkError::Overloaded);
        }
        self.signal.notify_one();
        Ok(sequence)
    }
    pub fn result(&self, sequence: u64) -> Option<Result<Assignment, NetworkError>> {
        self.view.lock().ok()?.results.remove(&sequence)
    }
    pub fn cancel(&self, attempt: Attempt) {
        let _ = self.commands.try_send(Local::Cancel(attempt));
        self.signal.notify_one();
    }
    pub fn abandon(&self, sequence: u64) {
        let mut view = self.view.lock().unwrap();
        view.pending.remove(&sequence);
        let assignment = view.results.remove(&sequence).and_then(Result::ok);
        let attempt = assignment.map(|a| a.attempt).or_else(|| {
            view.lease.authority.clone().map(|authority| Attempt {
                authority,
                device: self.id,
                epoch: view.epoch.clone(),
                sequence,
            })
        });
        drop(view);
        if let Some(attempt) = attempt {
            self.cancel(attempt);
        }
    }
}
fn next(sequence: &AtomicU64) -> Result<u64, NetworkError> {
    sequence
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
        .map(|n| n + 1)
        .map_err(|_| NetworkError::InvalidState)
}
struct Connection {
    client: async_nats::Client,
    incoming: mpsc::Receiver<async_nats::Message>,
    _subscriptions: Vec<async_nats::SubscriptionHandle>,
    failure: Arc<AtomicU64>,
}
async fn connect(config: &CoreConfig, subjects: Vec<String>) -> Result<Connection, NetworkError> {
    tokio::time::timeout(Duration::from_secs(3), connect_inner(config, subjects))
        .await
        .map_err(|_| NetworkError::Timeout)?
}
async fn connect_inner(
    config: &CoreConfig,
    subjects: Vec<String>,
) -> Result<Connection, NetworkError> {
    let failure = Arc::new(AtomicU64::new(0));
    let latch = failure.clone();
    let mut options = async_nats::ConnectOptions::with_user_and_password(
        config.username.clone(),
        config.password.clone(),
    )
    .require_tls(true)
    .max_reconnects(0)
    .ignore_discovered_servers()
    .connection_timeout(Duration::from_secs(3))
    .client_capacity(16)
    .subscription_capacity(64)
    .subscription_backpressure_timeout(Duration::from_secs(3))
    .raw_message_limit(MESSAGE_MAX)
    .event_callback(move |event| {
        let latch = latch.clone();
        async move {
            if matches!(
                event,
                async_nats::Event::Disconnected
                    | async_nats::Event::Closed
                    | async_nats::Event::SlowConsumer(_)
                    | async_nats::Event::ServerError(_)
                    | async_nats::Event::ClientError(_)
            ) {
                latch.store(1, Ordering::Release);
            }
        }
    });
    if let Some(identity) = config.tls_server_name.as_deref() {
        let trust = match &config.ca_file {
            Some(path) => skvoz_core::runtime::Trust::ManagedCa(path.clone()),
            None => skvoz_core::runtime::Trust::System,
        };
        options =
            options.tls_client_config(skvoz_core::runtime::verified_tls_config(&trust, identity)?);
    } else if let Some(ca) = &config.ca_file {
        options = options.add_root_certificates(ca.clone());
    }
    let client = tokio::time::timeout(Duration::from_secs(3), options.connect(config.url.clone()))
        .await
        .map_err(|_| NetworkError::Timeout)?
        .map_err(|_| NetworkError::InvalidState)?;
    let (sender, incoming) = mpsc::channel(64);
    let mut subscriptions = Vec::new();
    for subject in subjects {
        subscriptions.push(
            client
                .subscribe_into(subject, sender.clone())
                .await
                .map_err(|_| NetworkError::InvalidState)?,
        );
    }
    drop(sender);
    client
        .flush()
        .await
        .map_err(|_| NetworkError::InvalidState)?;
    if failure.load(Ordering::Acquire) != 0
        || client.connection_state() != async_nats::connection::State::Connected
    {
        return Err(NetworkError::InvalidState);
    }
    Ok(Connection {
        client,
        incoming,
        _subscriptions: subscriptions,
        failure,
    })
}
impl Connection {
    fn failed(&self) -> bool {
        self.failure.load(Ordering::Acquire) != 0
            || self.client.connection_state() != async_nats::connection::State::Connected
            || self.incoming.is_closed()
    }
}
type Outbox = VecDeque<(String, Vec<u8>)>;
fn queue(
    outbox: &mut Outbox,
    subject: String,
    from: u64,
    epoch: SessionId,
    request: u64,
    body: Body,
) -> Result<(), NetworkError> {
    if outbox.len() >= 16 {
        return Err(NetworkError::Overloaded);
    }
    let bytes = Envelope {
        v: ROUTE_VERSION,
        from,
        epoch,
        request,
        body,
    }
    .encode()?;
    outbox.push_back((subject, bytes));
    Ok(())
}
async fn publish(connection: &Connection, outbox: &mut Outbox) -> Result<(), NetworkError> {
    for _ in 0..16 {
        let Some((subject, bytes)) = outbox.front() else {
            break;
        };
        // A slow command channel cannot block lease/fence inspection. Keep the
        // bounded owned record until a subsequent tick successfully enqueues it.
        match tokio::time::timeout(
            Duration::from_millis(25),
            connection
                .client
                .publish(subject.clone(), bytes.clone().into()),
        )
        .await
        {
            Ok(Ok(())) => {
                outbox.pop_front();
            }
            Ok(Err(_)) => return Err(NetworkError::InvalidState),
            Err(_) => break,
        }
    }
    Ok(())
}
pub(crate) struct Routing {
    pub authority: Option<Arc<Mutex<Authority>>>,
    pub participant: Option<Participant>,
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<Result<(), NetworkError>>>,
    pub control_ready: Arc<AtomicBool>,
}
impl Routing {
    pub fn new() -> Self {
        Self {
            authority: None,
            participant: None,
            stop: Arc::new(AtomicBool::new(false)),
            tasks: Vec::new(),
            control_ready: Arc::new(AtomicBool::new(false)),
        }
    }
    pub fn authority(
        &mut self,
        config: CoreConfig,
        registry: Registry,
        wake: Arc<Notify>,
    ) -> Result<(), NetworkError> {
        let authority = Arc::new(Mutex::new(Authority::new(registry)?));
        let shared = authority.clone();
        let stop = self.stop.clone();
        let ready = self.control_ready.clone();
        self.tasks.push(tokio::spawn(async move {
            let result = run_authority(config, shared, stop, wake.clone(), ready.clone()).await;
            ready.store(false, Ordering::Release);
            wake.notify_one();
            result
        }));
        self.authority = Some(authority);
        Ok(())
    }
    pub fn participant(
        &mut self,
        config: CoreConfig,
        epoch: SessionId,
        families: Option<Vec<u8>>,
        wake: Arc<Notify>,
    ) {
        let id = config.peer_id.parse().unwrap();
        let view = Arc::new(Mutex::new(View {
            epoch: epoch.clone(),
            lease: LocalLease::default(),
            egress: Arc::new(Mutex::new(Egress::new(epoch))),
            results: BTreeMap::new(),
            pending: BTreeMap::new(),
            alive: true,
            ever_ready: false,
        }));
        let (sender, receiver) = mpsc::channel(32);
        let sequence = Arc::new(AtomicU64::new(0));
        let shared = view.clone();
        let count = sequence.clone();
        let stop = self.stop.clone();
        let notify = wake.clone();
        let signal = Arc::new(Notify::new());
        let task_signal = signal.clone();
        self.tasks.push(tokio::spawn(async move {
            let result = run_participant(
                config,
                shared.clone(),
                families,
                count,
                receiver,
                ParticipantSignals {
                    stop,
                    wake: notify.clone(),
                    signal: task_signal,
                },
            )
            .await;
            if let Ok(mut view) = shared.lock() {
                if let Err(error) = &result {
                    eprintln!(
                        "Routing participant failed: time_ms={} id={id} error={error:?} lease={:?}",
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis(),
                        view.lease
                    );
                }
                view.alive = false;
                view.lease.clear();
                view.egress.lock().unwrap().fence();
            }
            notify.notify_one();
            result
        }));
        self.participant = Some(Participant {
            view,
            commands: sender,
            sequence,
            signal,
            id,
        });
    }
    pub fn failed(&self) -> bool {
        self.tasks.iter().any(tokio::task::JoinHandle::is_finished)
    }
    pub fn fence(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(participant) = self.participant.as_ref() {
            let mut view = participant.view.lock().unwrap();
            view.alive = false;
            view.lease.clear();
            view.egress.lock().unwrap().fence();
            participant.signal.notify_one();
        }
        for task in &self.tasks {
            task.abort();
        }
    }
    pub async fn shutdown(&mut self, deadline: Instant) {
        self.stop.store(true, Ordering::Release);
        for mut task in self.tasks.drain(..) {
            if tokio::time::timeout_at(deadline.into(), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
    }
}
impl Drop for Routing {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for task in &self.tasks {
            task.abort();
        }
    }
}

async fn run_authority(
    config: CoreConfig,
    shared: Arc<Mutex<Authority>>,
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    ready: Arc<AtomicBool>,
) -> Result<(), NetworkError> {
    let ns = &config.namespace;
    let mut connection = connect(
        &config,
        vec![format!("{ns}.route.client.*"), format!("{ns}.route.node.*")],
    )
    .await?;
    ready.store(true, Ordering::Release);
    wake.notify_one();
    let mut outbox = Outbox::new();
    let mut query_due = Instant::now();
    let mut next_message = None;
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if connection.failed() {
            wake.notify_one();
            return Err(NetworkError::InvalidState);
        }
        if Instant::now() >= query_due {
            let mut authority = shared.lock().map_err(|_| NetworkError::InvalidState)?;
            authority.expire(Instant::now());
            // Deadlines precede record intake, including rejected records.
            // Eight queries fit with existing lease/admission headroom; only
            // queued queries advance the command watermark/cursor.
            if outbox.len() <= 8 {
                let epoch = authority.epoch.clone();
                for (owner, _, command, attempt) in authority.queries() {
                    queue(
                        &mut outbox,
                        format!("{ns}.route.command.{owner}"),
                        0,
                        epoch.clone(),
                        command,
                        Body::Query { command, attempt },
                    )?;
                }
            }
            query_due = Instant::now() + Duration::from_millis(250);
        }
        // Leave four records for a full lease+membership response. Backpressure
        // defers ordinary admission instead of terminating the authority.
        if outbox.len() <= 8
            && let Some(message) = next_message
                .take()
                .or_else(|| connection.incoming.try_recv().ok())
        {
            'record: {
                let envelope = match Envelope::decode(&message.payload) {
                    Ok(v) => v,
                    Err(_) => break 'record,
                };
                let source = message.subject.as_str();
                let client_source = source == format!("{ns}.route.client.{}", envelope.from)
                    && device_id(envelope.from);
                let node_source = source == format!("{ns}.route.node.{}", envelope.from)
                    && node_id(envelope.from);
                if !client_source && !node_source {
                    break 'record;
                }
                let now = Instant::now();
                let mut authority = shared.lock().map_err(|_| NetworkError::InvalidState)?;
                let authority_epoch = authority.epoch.clone();
                let reply = format!(
                    "{ns}.route.reply.{}.{}",
                    if client_source { "client" } else { "node" },
                    envelope.from
                );
                let result = match envelope.body {
                    Body::ClientLease if client_source => authority
                        .client_lease(envelope.from, &envelope.epoch, envelope.request, now)
                        .map(|_| Body::Lease {
                            authority: authority_epoch.clone(),
                            remaining_ms: LEASE_MS,
                        }),
                    Body::NodeLease {
                        families,
                        tcp,
                        vpn,
                        revision,
                        watermark,
                        membership,
                    } if node_source => {
                        match authority.node_lease(
                            envelope.from,
                            &envelope.epoch,
                            envelope.request,
                            families,
                            tcp,
                            vpn,
                            revision,
                            watermark,
                            membership,
                            now,
                        ) {
                            Ok(()) => {
                                for body in authority.snapshots(envelope.from, now)? {
                                    queue(
                                        &mut outbox,
                                        reply.clone(),
                                        0,
                                        authority_epoch.clone(),
                                        envelope.request,
                                        body,
                                    )?;
                                }
                                Ok(Body::Lease {
                                    authority: authority_epoch.clone(),
                                    remaining_ms: LEASE_MS,
                                })
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Body::Reserve { attempt, target }
                        if client_source
                            && attempt.device == envelope.from
                            && attempt.epoch == envelope.epoch
                            && attempt.sequence == envelope.request =>
                    {
                        match authority.reserve(attempt, target.clone(), now) {
                            Ok((assignment, command)) => {
                                if authority.assigned(&assignment.attempt) {
                                    queue(
                                        &mut outbox,
                                        reply.clone(),
                                        0,
                                        authority_epoch.clone(),
                                        envelope.request,
                                        Body::Assigned { assignment },
                                    )?;
                                    break 'record;
                                }
                                queue(
                                    &mut outbox,
                                    format!("{ns}.route.command.{}", assignment.owner),
                                    0,
                                    authority_epoch.clone(),
                                    envelope.request,
                                    Body::Dispatch {
                                        command,
                                        assignment,
                                        target,
                                    },
                                )?;
                                break 'record;
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Body::Reserved {
                        command,
                        assignment,
                        error,
                    } if node_source
                        && assignment.owner == envelope.from
                        && assignment.owner_epoch == envelope.epoch =>
                    {
                        if authority
                            .reserved(
                                envelope.from,
                                &envelope.epoch,
                                &assignment,
                                command,
                                error.is_none(),
                                now,
                            )
                            .is_ok()
                        {
                            queue(
                                &mut outbox,
                                format!("{ns}.route.reply.client.{}", assignment.attempt.device),
                                0,
                                authority_epoch.clone(),
                                assignment.attempt.sequence,
                                if error.is_none() {
                                    Body::Assigned { assignment }
                                } else {
                                    Body::Refused {
                                        error: "forbidden".into(),
                                    }
                                },
                            )?;
                        }
                        break 'record;
                    }
                    Body::Cancel { attempt }
                        if client_source
                            && attempt.device == envelope.from
                            && attempt.epoch == envelope.epoch =>
                    {
                        if let Ok((owner, _, command)) = authority.cancel(&attempt) {
                            queue(
                                &mut outbox,
                                format!("{ns}.route.command.{owner}"),
                                0,
                                authority_epoch.clone(),
                                envelope.request,
                                Body::CancelOwner { command, attempt },
                            )?;
                        }
                        break 'record;
                    }
                    Body::Terminal {
                        attempt,
                        watermark,
                        absent,
                    } if node_source => {
                        authority.terminal(
                            envelope.from,
                            &envelope.epoch,
                            &attempt,
                            watermark,
                            absent,
                        );
                        break 'record;
                    }
                    _ => break 'record,
                };
                queue(
                    &mut outbox,
                    reply,
                    0,
                    authority_epoch,
                    envelope.request,
                    result.unwrap_or_else(|error| Body::Refused {
                        error: match error {
                            NetworkError::Forbidden => "forbidden",
                            NetworkError::Overloaded => "overloaded",
                            NetworkError::UnsupportedFamily => "unsupported_family",
                            _ => "invalid_state",
                        }
                        .into(),
                    }),
                )?;
            }
        }
        publish(&connection, &mut outbox).await?;
        if outbox.is_empty() {
            tokio::select! { message = connection.incoming.recv() => { next_message = Some(message.ok_or(NetworkError::InvalidState)?); }, _ = tokio::time::sleep_until(query_due.into()) => {} }
        } else {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

struct ParticipantSignals {
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    signal: Arc<Notify>,
}

async fn run_participant(
    config: CoreConfig,
    shared: Arc<Mutex<View>>,
    families: Option<Vec<u8>>,
    sequence: Arc<AtomicU64>,
    mut commands: mpsc::Receiver<Local>,
    signals: ParticipantSignals,
) -> Result<(), NetworkError> {
    let ParticipantSignals { stop, wake, signal } = signals;
    let ns = &config.namespace;
    let id: u64 = config
        .peer_id
        .parse()
        .map_err(|_| NetworkError::InvalidConfiguration)?;
    let node = families.is_some();
    let incoming = if node {
        vec![
            format!("{ns}.route.reply.node.{id}"),
            format!("{ns}.route.command.{id}"),
        ]
    } else {
        vec![format!("{ns}.route.reply.client.{id}")]
    };
    let mut connection = connect(&config, incoming).await?;
    let subject = format!("{ns}.route.{}.{id}", if node { "node" } else { "client" });
    let mut outbox = Outbox::new();
    let mut due = Instant::now();
    let mut revision = 0u64;
    let mut seen_epoch = shared.lock().unwrap().epoch.clone();
    let mut next_message = None;
    let mut next_command = None;
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if connection.failed() {
            return Err(NetworkError::InvalidState);
        }
        let now = Instant::now();
        {
            let mut view = shared.lock().map_err(|_| NetworkError::InvalidState)?;
            if view.epoch != seen_epoch {
                seen_epoch = view.epoch.clone();
                due = now;
                outbox.clear();
            }
            if !view.lease.ready(now) {
                view.egress.lock().unwrap().fence();
            }
            if now >= due && outbox.len() < 8 {
                let request = next(&sequence)?;
                revision = revision.checked_add(1).ok_or(NetworkError::InvalidState)?;
                let body = if let Some(families) = &families {
                    let egress = view.egress.lock().unwrap();
                    let (tcp, vpn) = egress.counts();
                    Body::NodeLease {
                        families: families.clone(),
                        tcp,
                        vpn,
                        revision,
                        watermark: egress.watermark,
                        membership: egress.revision,
                    }
                } else {
                    Body::ClientLease
                };
                view.lease.sent(request, now);
                queue(
                    &mut outbox,
                    subject.clone(),
                    id,
                    seen_epoch.clone(),
                    request,
                    body,
                )?;
                due = now + Duration::from_millis(RENEW_MS);
            }
            let expired: Vec<_> = view
                .pending
                .iter()
                .filter(|(_, (_, sent))| now.duration_since(*sent) >= Duration::from_secs(2))
                .map(|(id, _)| *id)
                .collect();
            for request in expired {
                view.pending.remove(&request);
                view.results.insert(request, Err(NetworkError::Timeout));
                if outbox.len() < 16
                    && let Some(authority) = view.lease.authority.clone()
                {
                    let attempt = Attempt {
                        authority,
                        device: id,
                        epoch: seen_epoch.clone(),
                        sequence: request,
                    };
                    queue(
                        &mut outbox,
                        subject.clone(),
                        id,
                        seen_epoch.clone(),
                        next(&sequence)?,
                        Body::Cancel { attempt },
                    )?;
                }
            }
        }
        if outbox.len() < 8
            && let Some(command) = next_command.take().or_else(|| commands.try_recv().ok())
        {
            let view = shared.lock().map_err(|_| NetworkError::InvalidState)?;
            match command {
                Local::Assign(request, target)
                    if view.pending.contains_key(&request) && view.ready() =>
                {
                    let authority = view
                        .lease
                        .authority
                        .clone()
                        .ok_or(NetworkError::InvalidState)?;
                    let attempt = Attempt {
                        authority,
                        device: id,
                        epoch: seen_epoch.clone(),
                        sequence: request,
                    };
                    queue(
                        &mut outbox,
                        subject.clone(),
                        id,
                        seen_epoch.clone(),
                        request,
                        Body::Reserve { attempt, target },
                    )?;
                }
                Local::Cancel(attempt) => {
                    queue(
                        &mut outbox,
                        subject.clone(),
                        id,
                        seen_epoch.clone(),
                        next(&sequence)?,
                        Body::Cancel { attempt },
                    )?;
                }
                _ => {}
            }
        }
        let mut changed = false;
        for _ in 0..64 {
            if outbox.len() >= 12 {
                break;
            }
            let Some(message) = next_message
                .take()
                .or_else(|| connection.incoming.try_recv().ok())
            else {
                break;
            };
            if message.subject.as_str()
                != format!(
                    "{ns}.route.reply.{}.{id}",
                    if node { "node" } else { "client" }
                )
                && (!node || message.subject.as_str() != format!("{ns}.route.command.{id}"))
            {
                continue;
            }
            let envelope = match Envelope::decode(&message.payload) {
                Ok(v) if v.from == 0 => v,
                _ => continue,
            };
            let mut view = shared.lock().map_err(|_| NetworkError::InvalidState)?;
            match envelope.body {
                Body::Lease {
                    authority,
                    remaining_ms,
                } if authority == envelope.epoch => {
                    let old = view.lease.authority.clone();
                    if view.lease.acknowledge(
                        envelope.request,
                        authority.clone(),
                        remaining_ms,
                        now,
                    ) {
                        if old.as_ref().is_some_and(|a| *a != authority) {
                            view.egress.lock().unwrap().fence();
                            view.pending.clear();
                            view.alive = false;
                        }
                        view.ever_ready = true;
                        changed = true;
                    }
                }
                Body::Snapshot {
                    authority,
                    node_epoch,
                    revision,
                    part,
                    parts,
                    members,
                } if node && authority == envelope.epoch => {
                    if view.ever_ready
                        && view
                            .lease
                            .authority
                            .as_ref()
                            .is_some_and(|old| *old != authority)
                    {
                        view.alive = false;
                        view.egress.lock().unwrap().fence();
                        changed = true;
                        continue;
                    }
                    if let Some(sent) = view.lease.send_time(envelope.request)
                        && view
                            .egress
                            .lock()
                            .unwrap()
                            .snapshot(
                                authority,
                                &node_epoch,
                                revision,
                                part,
                                parts,
                                members,
                                sent,
                                now,
                            )
                            .unwrap_or(false)
                    {
                        changed = true;
                    }
                }
                Body::Dispatch {
                    command,
                    assignment,
                    target,
                } if node
                    && view.ready()
                    && view.lease.authority.as_ref() == Some(&envelope.epoch)
                    && assignment.owner == id
                    && assignment.owner_epoch == view.epoch
                    && assignment.attempt.authority == envelope.epoch =>
                {
                    let error = view
                        .egress
                        .lock()
                        .unwrap()
                        .reserve(command, assignment.clone(), target, now)
                        .err()
                        .map(|_| "forbidden".to_string());
                    changed = true;
                    queue(
                        &mut outbox,
                        subject.clone(),
                        id,
                        seen_epoch.clone(),
                        envelope.request,
                        Body::Reserved {
                            command,
                            assignment,
                            error,
                        },
                    )?;
                }
                Body::CancelOwner { command, attempt }
                    if node
                        && view.lease.authority.as_ref() == Some(&envelope.epoch)
                        && attempt.authority == envelope.epoch =>
                {
                    view.egress.lock().unwrap().cancel(command, &attempt);
                    changed = true;
                }
                Body::Query { command, attempt }
                    if node
                        && view.lease.authority.as_ref() == Some(&envelope.epoch)
                        && attempt.authority == envelope.epoch =>
                {
                    let absent = view.egress.lock().unwrap().query(command, &attempt);
                    let watermark = view.egress.lock().unwrap().watermark;
                    queue(
                        &mut outbox,
                        subject.clone(),
                        id,
                        seen_epoch.clone(),
                        envelope.request,
                        Body::Terminal {
                            attempt,
                            watermark,
                            absent,
                        },
                    )?;
                }
                Body::Assigned { assignment }
                    if !node
                        && view.ready()
                        && view.lease.authority.as_ref() == Some(&envelope.epoch)
                        && assignment.attempt.authority == envelope.epoch
                        && assignment.attempt.device == id
                        && assignment.attempt.epoch == view.epoch
                        && assignment.attempt.sequence == envelope.request =>
                {
                    if view.pending.remove(&envelope.request).is_some() {
                        view.results.insert(envelope.request, Ok(assignment));
                        changed = true;
                    }
                }
                Body::Refused { error }
                    if !node && view.lease.authority.as_ref() == Some(&envelope.epoch) =>
                {
                    if view.pending.remove(&envelope.request).is_some() {
                        view.results.insert(
                            envelope.request,
                            Err(match error.as_str() {
                                "forbidden" => NetworkError::Forbidden,
                                "overloaded" => NetworkError::Overloaded,
                                "unsupported_family" => NetworkError::UnsupportedFamily,
                                _ => NetworkError::InvalidState,
                            }),
                        );
                        changed = true;
                    }
                }
                _ => {}
            }
        }
        publish(&connection, &mut outbox).await?;
        if changed {
            wake.notify_one();
        }
        let until = {
            let view = shared.lock().unwrap();
            let expiry = view.lease.deadline().filter(|d| *d > now).unwrap_or(due);
            let pending = view
                .pending
                .values()
                .map(|(_, sent)| *sent + Duration::from_secs(2))
                .min()
                .unwrap_or(due);
            due.min(expiry).min(pending)
        };
        let until = if outbox.is_empty() {
            until
        } else {
            until.min(Instant::now() + Duration::from_millis(10))
        };
        tokio::select! {
            message = connection.incoming.recv(), if outbox.len()<12 && next_message.is_none() => { next_message = Some(message.ok_or(NetworkError::InvalidState)?); },
            command = commands.recv(), if outbox.len()<8 && next_command.is_none() => { next_command = command; if next_command.is_none() { return Ok(()); } },
            _ = signal.notified() => {},
            _ = tokio::time::sleep_until(until.into()) => {},
        }
    }
}
