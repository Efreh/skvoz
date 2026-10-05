//! One owner actor shared by the executable and native embedding boundary.
use crate::{
    budget::{Budget, Reservation},
    config::{Role, StartupConfig},
    local_api::{self, ApiError, Operation, Request, Response},
    tcp::{Socket, TcpConnection},
    *,
};
use serde_json::{Value, json};
use skvoz_core::PeerId;
use skvoz_core::runtime::{Lifecycle, NatsRuntime, RuntimeKey};
use skvoz_network_native::{IncrementalUnix, TunDevice};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    os::fd::{AsFd, OwnedFd},
    os::unix::net::UnixStream,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore, mpsc},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFailure {
    InvalidArgument,
    Closed,
    Overloaded,
    Internal,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollError {
    InsufficientBuffer { required: usize },
    Timeout,
    Closed,
    Internal,
}
#[derive(Debug)]
pub struct OwnedMessage {
    pub json: Vec<u8>,
    pub fd: Option<OwnedFd>,
}
struct Queued {
    message: OwnedMessage,
    _global: Reservation,
    _api: Reservation,
    stats: bool,
    advertised: bool,
    request_id: Option<u64>,
    terminal_request: bool,
}
struct OutputState {
    queue: VecDeque<Queued>,
    closed: bool,
}
struct Output {
    state: Mutex<OutputState>,
    changed: Condvar,
    budget: Budget,
    api: Budget,
}
impl Output {
    fn push(&self, message: OwnedMessage) -> Result<(), RuntimeFailure> {
        let envelope = serde_json::from_slice::<Value>(&message.json).ok();
        let event = envelope
            .as_ref()
            .and_then(|v| v.get("event"))
            .and_then(Value::as_str);
        let stats = event == Some("STATS");
        let request_id = if event == Some("REQUEST") {
            envelope.as_ref().and_then(|v| v["data"]["id"].as_u64())
        } else {
            None
        };
        let terminal_request = request_id.is_some()
            && !envelope.as_ref().is_some_and(|v| {
                matches!(v["data"]["result"].as_str(), Some("opening" | "active"))
            });
        if let Some(id) = request_id {
            let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal)?;
            // Coalesce only transient states; terminal and advertised records
            // retain exact ownership until the owner receives them.
            state
                .queue
                .retain(|q| q.advertised || q.terminal_request || q.request_id != Some(id));
        }
        if stats {
            let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal)?;
            if state.queue.iter().any(|q| q.stats && q.advertised) {
                return Ok(());
            }
            state.queue.retain(|q| !q.stats);
        }
        let global = self
            .budget
            .reserve(message.json.len() + 256, 1)
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let api = self
            .api
            .reserve(message.json.len(), 1)
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let mut state = self.state.lock().map_err(|_| RuntimeFailure::Internal)?;
        if state.closed {
            return Err(RuntimeFailure::Closed);
        }
        state.queue.push_back(Queued {
            message,
            _global: global,
            _api: api,
            stats,
            advertised: false,
            request_id,
            terminal_request,
        });
        self.changed.notify_all();
        Ok(())
    }
    fn terminal_batch(&self) -> usize {
        let available = self.api.available();
        // REQUEST has bounded host/port/protocol fields and fits in 1024 bytes.
        // Leave room for lifecycle/response records while deferring in TCP state.
        available
            .records
            .saturating_sub(4)
            .min(available.bytes.saturating_sub(local_api::BODY_MAX) / 1024)
            .min(32)
    }
    fn discard(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        state.queue.clear();
        self.changed.notify_all();
    }
    fn close(&self) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.closed = true;
        self.changed.notify_all();
    }
    fn poll(&self, capacity: usize, timeout: Duration) -> Result<OwnedMessage, PollError> {
        if timeout > Duration::from_secs(1) {
            return Err(PollError::Internal);
        }
        let until = Instant::now() + timeout;
        let mut s = self.state.lock().map_err(|_| PollError::Internal)?;
        loop {
            if let Some(front) = s.queue.front_mut() {
                if front.message.json.len() > capacity {
                    front.advertised = true;
                    return Err(PollError::InsufficientBuffer {
                        required: front.message.json.len(),
                    });
                }
                return Ok(s.queue.pop_front().unwrap().message);
            }
            if s.closed {
                return Err(PollError::Closed);
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(PollError::Timeout);
            }
            s = self
                .changed
                .wait_timeout(s, left)
                .map_err(|_| PollError::Internal)?
                .0;
        }
    }
}
struct Command {
    request: Request,
    fd: Option<OwnedFd>,
    _reservation: Reservation,
}
pub struct RuntimeHandle {
    commands: Option<mpsc::Sender<Command>>,
    output: Arc<Output>,
    budget: Budget,
    busy: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    done: std::sync::mpsc::Receiver<Result<(), RuntimeFailure>>,
    thread: Option<thread::JoinHandle<()>>,
    last_id: u32,
}
impl RuntimeHandle {
    pub fn start(config: StartupConfig, helper: Option<OwnedFd>) -> Result<Self, RuntimeFailure> {
        config
            .validate()
            .map_err(|_| RuntimeFailure::InvalidArgument)?;
        let profile = config
            .core_runtime()
            .map_err(|_| RuntimeFailure::InvalidArgument)?;
        let limits = config.network.limits.manager(config.role);
        profile
            .validate_profile(limits)
            .map_err(|_| RuntimeFailure::InvalidArgument)?;
        let helper_required = config.role == Role::Server && !config.network.families.is_empty();
        if helper.is_some() != helper_required {
            return Err(RuntimeFailure::InvalidArgument);
        }
        let helper = helper
            .map(IncrementalUnix::from_owned_fd)
            .transpose()
            .map_err(|_| RuntimeFailure::InvalidArgument)?;
        if helper
            .as_ref()
            .is_some_and(|h| !h.peer_credentials().is_ok_and(|c| c.uid == 0))
        {
            return Err(RuntimeFailure::InvalidArgument);
        }
        let budget = Budget::new(
            config.network.limits.runtime_buffer_bytes,
            config.network.limits.runtime_buffer_records,
        );
        let transport = budget
            .reserve(config.network.limits.transport_reservation(config.role), 0)
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let core = budget
            .reserve(
                2 * limits
                    .receive_budget
                    .min(limits.max_streams * limits.stream.receive_window as usize)
                    + limits.send_budget,
                0,
            )
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let fixed = budget
            .reserve(
                524288
                    + if helper_required {
                        local_api::BODY_MAX * 40
                    } else {
                        0
                    },
                32,
            )
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let output = Arc::new(Output {
            state: Mutex::new(OutputState {
                queue: VecDeque::new(),
                closed: false,
            }),
            changed: Condvar::new(),
            budget: budget.clone(),
            api: Budget::new(
                config.network.limits.api_queue_bytes,
                config.network.limits.api_queue_records,
            ),
        });
        let (sender, receiver) = mpsc::channel(1);
        let (done_sender, done) = std::sync::mpsc::channel();
        let busy = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let close_output = output.clone();
        let shutdown_deadline = Arc::new(Mutex::new(None));
        let actor = Actor::new(
            config,
            helper,
            output.clone(),
            busy.clone(),
            stop.clone(),
            wake.clone(),
            shutdown_deadline.clone(),
        );
        let thread = thread::Builder::new()
            .name("skvoz-network".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|_| RuntimeFailure::Internal)?;
                    let result = runtime.block_on(actor.run(receiver));
                    runtime.shutdown_background();
                    result
                }))
                .unwrap_or(Err(RuntimeFailure::Internal));
                close_output.close();
                drop((transport, core, fixed));
                let _ = done_sender.send(result);
            })
            .map_err(|_| RuntimeFailure::Internal)?;
        Ok(Self {
            shutdown_deadline,
            commands: Some(sender),
            output,
            budget,
            busy,
            stop,
            wake,
            done,
            thread: Some(thread),
            last_id: 0,
        })
    }
    pub fn request_json(
        &mut self,
        bytes: &[u8],
        fd: Option<OwnedFd>,
    ) -> Result<u32, RuntimeFailure> {
        if self.stop.load(Ordering::Acquire) {
            return Err(RuntimeFailure::Closed);
        }
        if bytes.is_empty() || bytes.len() > local_api::BODY_MAX {
            self.abort();
            return Err(RuntimeFailure::InvalidArgument);
        }
        let reservation = self
            .budget
            .reserve(bytes.len() * 32 + 32768, 1)
            .map_err(|_| RuntimeFailure::Overloaded)?;
        let request = match Request::parse_json(bytes) {
            Ok(r) => r,
            Err(_) => {
                self.abort();
                return Err(RuntimeFailure::InvalidArgument);
            }
        };
        if request.id <= self.last_id || usize::from(request.fd_count) != usize::from(fd.is_some())
        {
            self.abort();
            return Err(RuntimeFailure::InvalidArgument);
        }
        if self.busy.swap(true, Ordering::AcqRel) {
            return Err(RuntimeFailure::Overloaded);
        }
        let id = request.id;
        match self
            .commands
            .as_ref()
            .ok_or(RuntimeFailure::Closed)?
            .try_send(Command {
                request,
                fd,
                _reservation: reservation,
            }) {
            Ok(()) => {
                self.last_id = id;
                self.wake.notify_one();
                Ok(id)
            }
            Err(e) => {
                self.busy.store(false, Ordering::Release);
                Err(match e {
                    mpsc::error::TrySendError::Closed(_) => RuntimeFailure::Closed,
                    mpsc::error::TrySendError::Full(_) => RuntimeFailure::Overloaded,
                })
            }
        }
    }
    pub fn next_message(
        &mut self,
        capacity: usize,
        timeout: Duration,
    ) -> Result<OwnedMessage, PollError> {
        self.output.poll(capacity, timeout)
    }
    fn abort(&mut self) {
        self.shutdown_deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| Instant::now() + Duration::from_secs(3));
        self.stop.store(true, Ordering::Release);
        self.wake.notify_one();
        self.commands.take();
    }
    fn finish(&mut self) -> Result<(), RuntimeFailure> {
        self.abort();
        let result = match self.done.recv_timeout(Duration::from_millis(3500)) {
            Ok(r) => r,
            Err(_) => {
                self.output.discard();
                self.thread.take();
                return Err(RuntimeFailure::Internal);
            }
        };
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| RuntimeFailure::Internal)?;
        }
        result
    }
    pub fn shutdown(mut self) -> Result<(), RuntimeFailure> {
        self.finish()
    }
}
impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let _ = self.finish();
        }
    }
}

type Connect = Pin<Box<dyn Future<Output = Result<std::net::TcpStream, ApiError>> + Send>>;
struct Connecting {
    key: RuntimeKey,
    future: Option<Connect>,
    host: String,
    port: u16,
    _reservation: Reservation,
    deadline: Instant,
}
struct HelperPending {
    request: local_api::HelperRequest,
    kind: HelperKind,
    _reservation: Reservation,
    deadline: Instant,
}
enum HelperKind {
    Hello,
    Prepare,
    Reserve {
        key: RuntimeKey,
        session: SessionId,
        families: Vec<u8>,
        mtu: u16,
        channels: u8,
    },
    Activate {
        key: RuntimeKey,
        session: SessionId,
    },
    Retire,
    Stop,
}
struct PendingStop {
    id: u32,
    target: Option<SessionId>,
    keys: Vec<RuntimeKey>,
    deadline: Instant,
}
#[derive(Clone)]
struct Journal {
    id: u64,
    protocol: &'static str,
    host: String,
    port: u16,
    uploaded: u64,
    downloaded: u64,
    terminal: bool,
}
struct ProxyPending {
    socket: std::net::TcpStream,
    parser: crate::proxy::Handshake,
    output: Vec<u8>,
    cursor: usize,
    deadline: Instant,
    _reservation: Reservation,
    terminal: bool,
}
struct Actor {
    config: StartupConfig,
    policy: Option<Arc<crate::config::ServerConfig>>,
    engine: Option<NetworkEngine>,
    helper: Option<IncrementalUnix>,
    helper_queue: VecDeque<HelperPending>,
    helper_pending: Option<HelperPending>,
    helper_id: u32,
    helper_ready: bool,
    tun: Option<TunDevice>,
    packet_pending: Option<(ReceivedPacket, Instant)>,
    budget: Budget,
    output: Arc<Output>,
    busy: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    wake: Arc<Notify>,
    hello: bool,
    hello_deadline: Instant,
    seq: u64,
    lifecycle: &'static str,
    ever_ready: bool,
    mode: &'static str,
    tcp: BTreeMap<RuntimeKey, TcpConnection>,
    journal: BTreeMap<RuntimeKey, Journal>,
    request_seq: u64,
    tcp_uploaded: u64,
    tcp_downloaded: u64,
    connects: Vec<Connecting>,
    tcp_stopping: bool,
    http: Option<TcpListener>,
    socks: Option<TcpListener>,
    proxy_pending: Vec<ProxyPending>,
    ip: Option<SessionId>,
    configured: bool,
    active: bool,
    ip_guard: Option<Reservation>,
    known_ip: VecDeque<SessionId>,
    stats: Instant,
    ready_deadline: Instant,
    cleanup_done: bool,
    pending_stop: Option<PendingStop>,
    native_cursor: usize,
    proxy_cursor: usize,
    connect_peer: PeerId,
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    counters: local_api::Counters,
    admission_errors: [u64; 10],
    terminal_wait: Option<Instant>,
}
impl Actor {
    fn new(
        config: StartupConfig,
        helper: Option<IncrementalUnix>,
        output: Arc<Output>,
        busy: Arc<AtomicBool>,
        stop: Arc<AtomicBool>,
        wake: Arc<Notify>,
        shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    ) -> Self {
        let budget = output.budget.clone();
        let policy = config.server.clone().map(Arc::new);
        let mode = if config.role == Role::Server {
            "server"
        } else {
            "idle"
        };
        Self {
            config,
            policy,
            engine: None,
            helper,
            helper_queue: VecDeque::new(),
            helper_pending: None,
            helper_id: 0,
            helper_ready: false,
            tun: None,
            packet_pending: None,
            budget,
            output,
            busy,
            stop,
            wake,
            hello: false,
            hello_deadline: Instant::now() + Duration::from_secs(5),
            seq: 0,
            lifecycle: "starting",
            ever_ready: false,
            mode,
            tcp: BTreeMap::new(),
            journal: BTreeMap::new(),
            request_seq: 0,
            tcp_uploaded: 0,
            tcp_downloaded: 0,
            connects: Vec::new(),
            tcp_stopping: false,
            http: None,
            socks: None,
            proxy_pending: Vec::new(),
            ip: None,
            configured: false,
            active: false,
            ip_guard: None,
            known_ip: VecDeque::new(),
            stats: Instant::now(),
            ready_deadline: Instant::now() + Duration::from_secs(30),
            cleanup_done: false,
            pending_stop: None,
            native_cursor: 0,
            proxy_cursor: 0,
            connect_peer: PeerId(0),
            shutdown_deadline,
            counters: local_api::Counters::default(),
            admission_errors: [0; 10],
            terminal_wait: None,
        }
    }
    fn message<T: serde::Serialize>(
        &self,
        value: &T,
        fd: Option<OwnedFd>,
    ) -> Result<(), RuntimeFailure> {
        self.output.push(OwnedMessage {
            json: serde_json::to_vec(value).map_err(|_| RuntimeFailure::Internal)?,
            fd,
        })
    }
    fn respond(
        &self,
        id: u32,
        result: Result<Value, ApiError>,
        fd: Option<OwnedFd>,
    ) -> Result<(), RuntimeFailure> {
        let response = match result {
            Ok(v) => Response::success(id, v, fd.is_some()),
            Err(e) => Response::failure(id, e),
        };
        let result = self.message(&response, fd);
        self.busy.store(false, Ordering::Release);
        result
    }
    fn event(&mut self, name: &str, data: Value) -> Result<(), RuntimeFailure> {
        self.seq = self
            .seq
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(RuntimeFailure::Internal)?;
        self.message(
            &local_api::Event {
                v: 1,
                seq: self.seq,
                event: name.into(),
                data,
                fd_count: 0,
            },
            None,
        )
    }
    fn state(
        &mut self,
        state: &'static str,
        error: Option<ApiError>,
    ) -> Result<(), RuntimeFailure> {
        self.lifecycle = state;
        if state == "ready" {
            self.ever_ready = true;
        }
        self.event("RUNTIME_STATE", json!({"state":state,"error":error}))
    }
    async fn run(mut self, commands: mpsc::Receiver<Command>) -> Result<(), RuntimeFailure> {
        let result = self.run_loop(commands).await;
        if let Err(error) = result {
            eprintln!("Network actor stopped: {error:?}");
        }
        let cleanup = self.cleanup().await;
        let closed = self.state(
            "closed",
            if result.is_err() || cleanup.is_err() {
                Some(ApiError::Closed)
            } else {
                None
            },
        );
        self.output.close();
        result.and(cleanup).and(closed)
    }
    async fn run_loop(
        &mut self,
        mut commands: mpsc::Receiver<Command>,
    ) -> Result<(), RuntimeFailure> {
        let profile = self
            .config
            .core_runtime()
            .map_err(|_| RuntimeFailure::InvalidArgument)?;
        let limits = self.config.network.limits.manager(self.config.role);
        let connect = NatsRuntime::connect(profile, limits);
        tokio::pin!(connect);
        let mut connecting = true;
        let mut connect_error = false;
        loop {
            if self.stop.load(Ordering::Acquire) {
                break Ok(());
            }
            if !self.hello && Instant::now() >= self.hello_deadline {
                break Err(RuntimeFailure::Closed);
            }
            while let Ok(command) = commands.try_recv() {
                self.command(command).await?;
            }
            if commands.is_closed() {
                break Ok(());
            }
            if !self.ever_ready
                && self.lifecycle == "starting"
                && Instant::now() >= self.ready_deadline
            {
                break Err(RuntimeFailure::Closed);
            }
            self.helper_turn()?;
            if connecting {
                tokio::select! {
                 result=&mut connect=>{connecting=false;match result{
                  Ok(runtime)=>{
                   let role=if self.config.role==Role::Client{EngineRole::Client}else{EngineRole::Server{grants:BTreeMap::new()}};
                   let mut engine=NetworkEngine::new(runtime,role,self.config.network.limits.engine()).map_err(|_|RuntimeFailure::Internal)?;engine.enable_tcp();engine.set_budget(self.budget.clone());
                   if self.config.role==Role::Server && self.config.network.families.is_empty(){engine.enable_dynamic_server(Vec::new(),self.config.network.max_mtu,self.config.network.channels).map_err(|_|RuntimeFailure::Internal)?;}
                   if self.helper_ready{engine.enable_dynamic_server(self.config.network.families.clone(),self.config.network.max_mtu,self.config.network.channels).map_err(|_|RuntimeFailure::Internal)?;}
                   self.engine=Some(engine);self.ip_events()?;
                  },Err(_)=>{connect_error=true;self.state("starting",Some(ApiError::NetworkUnavailable))?;}
                 }},
                 command=commands.recv()=>match command{Some(c)=>self.command(c).await?,None=>break Ok(())},
                 _=self.wake.notified()=>{},
                 _=tokio::time::sleep(Duration::from_millis(5))=>{},
                }
            } else if !connect_error {
                self.backend_turn().await?;
                self.proxy_turn().await?;
                self.native_turn()?;
                if let Some(engine) = self.engine.as_mut()
                    && let Err(error) = engine
                        .drive_with_wake(Duration::from_millis(5), self.wake.notified())
                        .await
                    && self.lifecycle != "starting"
                {
                    eprintln!(
                        "Network drive failure: {error:?} counters={:?}",
                        engine.runtime.status().counters
                    );
                    self.state("starting", Some(ApiError::NetworkUnavailable))?;
                }
                self.finish_stop()?;
                self.ip_events()?;
            } else {
                tokio::select! {command=commands.recv()=>match command{Some(c)=>self.command(c).await?,None=>break Ok(())},_=self.wake.notified()=>{},_=tokio::time::sleep(Duration::from_millis(100))=>{}}
            }
            if self.hello && self.stats.elapsed() >= Duration::from_secs(1) {
                self.stats = Instant::now();
                self.update_counters();
                self.event("STATS", json!({"counters":self.counters}))?;
            }
        }
    }
    fn update_counters(&mut self) {
        let usage = self.budget.usage();
        self.counters.buffer_bytes = usage.bytes as u64;
        self.counters.buffer_records = usage.records as u64;
        self.counters.tcp_open = self.tcp.len() as u64;
        if let Some(e) = &self.engine {
            let r = e.resources();
            self.counters.ip_sessions = r.sessions as u64;
            self.counters.queue_bytes = (r.queued_packet_bytes + r.received_packet_bytes) as u64;
            self.counters.queue_records =
                (r.queued_packet_records + r.received_packet_records) as u64;
            self.counters.packet_dropped = e.counters().packet_drops;
            self.counters.uploaded = self.tcp_uploaded.saturating_add(e.counters().uploaded);
            self.counters.downloaded = self.tcp_downloaded.saturating_add(e.counters().downloaded);
        }
    }
    async fn command(&mut self, command: Command) -> Result<(), RuntimeFailure> {
        let Command {
            request: r,
            fd,
            _reservation,
        } = command;
        if !self.hello {
            if r.op != Operation::Hello {
                self.stop.store(true, Ordering::Release);
                return self.respond(r.id, Err(ApiError::InvalidState), None);
            }
            self.hello = true;
            return self.respond(r.id,Ok(json!({"api":1,"network":2,"role":self.config.role,"capabilities":{"profiles":if self.config.network.families.is_empty(){vec!["tcp"]}else{vec!["tcp","ip"]},"families":self.config.network.families,"max_mtu":self.config.network.max_mtu,"max_channels":self.config.network.channels}})),None);
        }
        if r.op == Operation::Hello {
            return self.respond(r.id, Err(ApiError::InvalidState), None);
        }
        if self.config.role == Role::Server
            && !matches!(r.op, Operation::Status | Operation::PrepareShutdown)
        {
            return self.respond(r.id, Err(ApiError::Forbidden), None);
        }
        let result: Result<Value, ApiError> = match r.op {
            Operation::Status => {
                self.update_counters();
                let session=self.ip.as_ref().map(|id|json!({"handle":id,"state":if self.pending_stop.as_ref().is_some_and(|s|s.target.is_some()){"closing"}else{self.engine.as_ref().and_then(|e|e.sessions().into_iter().find(|s|s.peer==PeerId(0))).map(|s|session_label(s.state)).unwrap_or("closed")}}));
                Ok(
                    json!({"lifecycle":self.lifecycle,"mode":self.mode,"session":session,"counters":self.counters}),
                )
            }
            Operation::StartProxy => {
                if self.mode != "idle" {
                    Err(ApiError::InvalidState)
                } else {
                    let args: local_api::ProxyArgs = local_api::arguments(&r.args)
                        .map_err(|_| RuntimeFailure::InvalidArgument)?;
                    match bind_proxy(args).await {
                        Ok((http, socks, http_uri, socks_uri)) => {
                            self.http = http;
                            self.socks = socks;
                            self.mode = "proxy";
                            Ok(json!({"http":http_uri,"socks":socks_uri}))
                        }
                        Err(e) => Err(e),
                    }
                }
            }
            Operation::StopProxy => {
                if self.mode == "ip" {
                    Err(ApiError::InvalidState)
                } else {
                    let keys = self.tcp.keys().copied().collect();
                    self.stop_proxy();
                    self.pending_stop = Some(PendingStop {
                        id: r.id,
                        target: None,
                        keys,
                        deadline: Instant::now() + Duration::from_secs(3),
                    });
                    return Ok(());
                }
            }
            Operation::OpenTcp => {
                if self.mode != "proxy" {
                    Err(ApiError::InvalidState)
                } else if self
                    .engine
                    .as_ref()
                    .is_none_or(|e| !e.peer_ready(PeerId(0)))
                {
                    Err(ApiError::NetworkUnavailable)
                } else {
                    let args: local_api::TcpArgs = local_api::arguments(&r.args)
                        .map_err(|_| RuntimeFailure::InvalidArgument)?;
                    match self.open_api_tcp(r.id, args) {
                        Ok(()) => return Ok(()),
                        Err(e) => Err(e),
                    }
                }
            }
            Operation::StartIp => {
                if self.mode != "idle" {
                    Err(ApiError::InvalidState)
                } else if self
                    .engine
                    .as_ref()
                    .is_none_or(|e| !e.peer_ready(PeerId(0)))
                {
                    Err(ApiError::NetworkUnavailable)
                } else {
                    let a: local_api::IpArgs = local_api::arguments(&r.args)
                        .map_err(|_| RuntimeFailure::InvalidArgument)?;
                    if a.families
                        .iter()
                        .any(|f| !self.config.network.families.contains(f))
                    {
                        Err(ApiError::UnsupportedVersion)
                    } else {
                        match self.engine.as_mut().unwrap().open_ip(
                            PeerId(0),
                            a.families,
                            a.max_mtu,
                            a.channels,
                        ) {
                            Ok(_key) => {
                                let handle =
                                    SessionId::random().map_err(|_| RuntimeFailure::Internal)?;
                                self.mode = "ip";
                                self.ip = Some(handle.clone());
                                self.configured = false;
                                self.active = false;
                                Ok(local_api::handle_result(&handle))
                            }
                            Err(e) => Err(e.into()),
                        }
                    }
                }
            }
            Operation::AttachIp => {
                let a: local_api::AttachArgs =
                    local_api::arguments(&r.args).map_err(|_| RuntimeFailure::InvalidArgument)?;
                if self.ip.as_ref() != Some(&a.handle) {
                    Err(ApiError::UnknownHandle)
                } else if !self.configured || self.tun.is_some() {
                    Err(ApiError::InvalidState)
                } else {
                    let config = self
                        .engine
                        .as_ref()
                        .and_then(|e| e.sessions().into_iter().find(|s| s.peer == PeerId(0)))
                        .and_then(|s| s.config);
                    if config.is_none_or(|c| c.mtu != a.mtu) {
                        Err(ApiError::InvalidRequest)
                    } else {
                        match TunDevice::from_owned_fd(
                            fd.ok_or(RuntimeFailure::InvalidArgument)?,
                            usize::from(a.mtu),
                        ) {
                            Ok(tun) if tun.name() == a.interface => {
                                self.tun = Some(tun);
                                Ok(local_api::handle_result(&a.handle))
                            }
                            _ => Err(ApiError::LocalSetupFailed),
                        }
                    }
                }
            }
            Operation::LocalReady => {
                let a: local_api::HandleArgs =
                    local_api::arguments(&r.args).map_err(|_| RuntimeFailure::InvalidArgument)?;
                if self.ip.as_ref() != Some(&a.handle) {
                    Err(ApiError::UnknownHandle)
                } else if self.tun.is_none() {
                    Err(ApiError::InvalidState)
                } else {
                    {
                        let remote = self.remote_ip().ok_or(RuntimeFailure::Internal)?;
                        self.engine
                            .as_mut()
                            .ok_or(RuntimeFailure::Internal)?
                            .local_ready(&remote)
                    }
                    .map(|_| local_api::handle_result(&a.handle))
                    .map_err(Into::into)
                }
            }
            Operation::StopIp => {
                let a: local_api::StopArgs =
                    local_api::arguments(&r.args).map_err(|_| RuntimeFailure::InvalidArgument)?;
                if self.ip.as_ref() == Some(&a.handle) {
                    self.tun.take();
                    self.packet_pending.take();
                    let remote = self.remote_ip().ok_or(RuntimeFailure::Internal)?;
                    let engine = self.engine.as_mut().ok_or(RuntimeFailure::Internal)?;
                    let keys = engine
                        .session_streams(&remote)
                        .map_err(|_| RuntimeFailure::Internal)?;
                    match engine.close_session(&remote) {
                        Ok(()) => {
                            self.pending_stop = Some(PendingStop {
                                id: r.id,
                                target: Some(a.handle),
                                keys,
                                deadline: Instant::now() + Duration::from_secs(3),
                            });
                            return Ok(());
                        }
                        Err(e) => Err(e.into()),
                    }
                } else if self.known_ip.contains(&a.handle) {
                    Ok(local_api::handle_result(&a.handle))
                } else {
                    Err(ApiError::UnknownHandle)
                }
            }
            Operation::PrepareShutdown => {
                self.shutdown_deadline
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get_or_insert_with(|| Instant::now() + Duration::from_secs(3));
                self.lifecycle = "closing";
                let cleanup = self.cleanup().await;
                let response = match cleanup {
                    Ok(()) => Ok(json!({})),
                    Err(_) => Err(ApiError::Timeout),
                };
                self.respond(r.id, response, None)?;
                self.stop.store(true, Ordering::Release);
                return Ok(());
            }
            Operation::Hello => Err(ApiError::InvalidState),
        };
        self.respond(r.id, result, None)
    }
    fn remote_ip(&self) -> Option<SessionId> {
        self.engine
            .as_ref()
            .and_then(|e| e.sessions().into_iter().find(|s| s.peer == PeerId(0)))
            .map(|s| s.session)
    }
    fn remember_ip(&mut self, id: SessionId) {
        if self.known_ip.len() >= 128 {
            self.known_ip.pop_front();
        }
        self.known_ip.push_back(id);
    }
    fn record_request(
        &mut self,
        key: RuntimeKey,
        protocol: &'static str,
        host: String,
        port: u16,
    ) -> Result<(), RuntimeFailure> {
        self.request_seq = self
            .request_seq
            .checked_add(1)
            .ok_or(RuntimeFailure::Internal)?;
        self.journal.insert(
            key,
            Journal {
                id: self.request_seq,
                protocol,
                host,
                port,
                uploaded: 0,
                downloaded: 0,
                terminal: false,
            },
        );
        self.request_event(key, "opening")
    }
    fn request_event(&mut self, key: RuntimeKey, result: &str) -> Result<(), RuntimeFailure> {
        let terminal = !matches!(result, "opening" | "active");
        if let Some(j) = self.journal.get(&key).filter(|j| !j.terminal).cloned() {
            self.event("REQUEST",json!({"id":j.id,"protocol":j.protocol,"host":j.host,"port":j.port,"result":result,"uploaded":j.uploaded,"downloaded":j.downloaded}))?;
            if terminal {
                self.journal.get_mut(&key).unwrap().terminal = true;
            }
        }
        Ok(())
    }
    fn failed_attempt(
        &mut self,
        protocol: &'static str,
        host: String,
        port: u16,
        reason: &'static str,
    ) -> Result<(), RuntimeFailure> {
        self.request_seq = self
            .request_seq
            .checked_add(1)
            .ok_or(RuntimeFailure::Internal)?;
        self.event("REQUEST", json!({"id":self.request_seq,"protocol":protocol,"host":host,"port":port,"result":reason,"uploaded":0,"downloaded":0}))
    }
    fn open_api_tcp(&mut self, id: u32, args: local_api::TcpArgs) -> Result<(), ApiError> {
        let (runtime, host) = UnixStream::pair().map_err(|_| ApiError::LocalSetupFailed)?;
        runtime
            .set_nonblocking(true)
            .map_err(|_| ApiError::LocalSetupFailed)?;
        skvoz_network_native::configure_socket_buffers(host.as_fd(), 131072)
            .map_err(|_| ApiError::LocalSetupFailed)?;
        host.set_nonblocking(true)
            .map_err(|_| ApiError::LocalSetupFailed)?;
        if self.tcp.len() >= self.config.network.limits.core_streams {
            self.admission_error("core_slots_or_credit");
            self.failed_attempt("TCP", args.host, args.port, "overloaded")
                .map_err(|_| ApiError::Overloaded)?;
            return Err(ApiError::Overloaded);
        }
        let key = match self
            .engine
            .as_mut()
            .ok_or(ApiError::NetworkUnavailable)?
            .open_tcp(PeerId(0), args.host.clone(), args.port)
        {
            Ok(key) => key,
            Err(error) => {
                let api_error = ApiError::from(error.clone());
                self.admission_error(if api_error == ApiError::Overloaded {
                    "core_slots_or_credit"
                } else {
                    "network_unavailable"
                });
                self.failed_attempt("TCP", args.host, args.port, api_label(api_error))
                    .map_err(|_| ApiError::Overloaded)?;
                return Err(error.into());
            }
        };
        match TcpConnection::new(key, Socket::Unix(runtime), &self.budget, Vec::new()) {
            Ok(mut connection) => {
                connection.reply = Some((
                    id,
                    SessionId::random().map_err(ApiError::from)?,
                    host.into(),
                ));
                self.tcp.insert(key, connection);
                self.record_request(key, "TCP", args.host, args.port)
                    .map_err(|_| ApiError::Overloaded)?;
                Ok(())
            }
            Err(e) => {
                self.engine.as_mut().unwrap().close_tcp(key);
                let api_error = setup_api_error(e.clone());
                self.admission_error(if api_error == ApiError::Overloaded {
                    "buffer"
                } else {
                    "local_setup"
                });
                self.failed_attempt("TCP", args.host, args.port, api_label(api_error))
                    .map_err(|_| ApiError::Overloaded)?;
                Err(e.into())
            }
        }
    }
    fn stop_proxy(&mut self) {
        self.http.take();
        self.socks.take();
        self.proxy_pending.clear();
        self.connects.clear();
        self.tcp_stopping = true;
    }
    fn helper_enqueue(
        &mut self,
        op: local_api::HelperOperation,
        args: Value,
        kind: HelperKind,
    ) -> Result<(), RuntimeFailure> {
        if self.helper_queue.len() >= 256 {
            return Err(RuntimeFailure::Overloaded);
        }
        self.helper_id = self
            .helper_id
            .checked_add(1)
            .filter(|id| *id <= i32::MAX as u32)
            .ok_or(RuntimeFailure::Internal)?;
        let request = local_api::HelperRequest::new(self.helper_id, op, args)
            .map_err(|_| RuntimeFailure::Internal)?;
        let size = serde_json::to_vec(&request)
            .map_err(|_| RuntimeFailure::Internal)?
            .len();
        let reservation = self
            .budget
            .reserve(size * 32 + 32768, 1)
            .map_err(|_| RuntimeFailure::Overloaded)?;
        self.helper_queue.push_back(HelperPending {
            request,
            kind,
            _reservation: reservation,
            deadline: Instant::now() + Duration::from_secs(5),
        });
        Ok(())
    }
    fn helper_turn(&mut self) -> Result<(), RuntimeFailure> {
        if self.helper.is_none() {
            return Ok(());
        }
        if self.helper_id == 0 {
            self.helper_enqueue(
                local_api::HelperOperation::Hello,
                json!({"api":1,"network":2}),
                HelperKind::Hello,
            )?;
        }
        if self.helper_pending.is_none()
            && let Some(pending) = self.helper_queue.pop_front()
        {
            let body =
                serde_json::to_vec(&pending.request).map_err(|_| RuntimeFailure::Internal)?;
            self.helper
                .as_mut()
                .unwrap()
                .queue_frame(body, None)
                .map_err(|_| RuntimeFailure::Internal)?;
            self.helper_pending = Some(pending);
        }
        if self
            .helper_pending
            .as_ref()
            .is_some_and(|p| Instant::now() >= p.deadline)
            || self
                .helper_queue
                .front()
                .is_some_and(|p| Instant::now() >= p.deadline)
        {
            return Err(RuntimeFailure::Internal);
        }
        let helper = self.helper.as_mut().unwrap();
        helper
            .check_deadlines()
            .map_err(|_| RuntimeFailure::Internal)?;
        match helper.try_flush() {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => return Err(RuntimeFailure::Internal),
        }
        let frame = match helper.try_receive_frame() {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
            Err(_) => return Err(RuntimeFailure::Internal),
        };
        let pending = self.helper_pending.take().ok_or(RuntimeFailure::Internal)?;
        let response = local_api::HelperResponse::parse_json(&frame.body)
            .map_err(|_| RuntimeFailure::Internal)?;
        if response.id != pending.request.id
            || usize::from(response.fd_count) != usize::from(frame.fd.is_some())
        {
            return Err(RuntimeFailure::Internal);
        }
        if let Some(error) = response.error {
            if let HelperKind::Reserve { key, .. } = pending.kind
                && matches!(
                    error.as_str(),
                    "invalid_request" | "unsupported_version" | "forbidden" | "overloaded"
                )
            {
                if let Some(engine) = self.engine.as_mut() {
                    engine.fail_reservation(key, &error);
                }
                return Ok(());
            }
            return Err(RuntimeFailure::Internal);
        }
        let result = response.result.ok_or(RuntimeFailure::Internal)?;
        if self.cleanup_done && !matches!(pending.kind, HelperKind::Stop) {
            // A prior transaction may complete while STOP is queued. It must not
            // recreate a TUN/session or enqueue work behind the final STOP.
            return Ok(());
        }
        match pending.kind {
            HelperKind::Hello => {
                if frame.fd.is_some() || result != json!({"api":1,"network":2,"role":"server"}) {
                    return Err(RuntimeFailure::Internal);
                }
                self.helper_enqueue(
                    local_api::HelperOperation::PrepareServer,
                    serde_json::to_value(local_api::PrepareServerArgs {
                        network: self.config.network.clone(),
                        server: self.config.server.clone().ok_or(RuntimeFailure::Internal)?,
                    })
                    .map_err(|_| RuntimeFailure::Internal)?,
                    HelperKind::Prepare,
                )?;
            }
            HelperKind::Prepare => {
                let prepared: local_api::ServerPrepared =
                    local_api::arguments(&result).map_err(|_| RuntimeFailure::Internal)?;
                if prepared.mtu != self.config.network.max_mtu {
                    return Err(RuntimeFailure::Internal);
                }
                let tun = TunDevice::from_owned_fd(
                    frame.fd.ok_or(RuntimeFailure::Internal)?,
                    usize::from(prepared.mtu),
                )
                .map_err(|_| RuntimeFailure::Internal)?;
                if tun.name() != prepared.interface {
                    return Err(RuntimeFailure::Internal);
                }
                self.tun = Some(tun);
                self.helper_ready = true;
                if let Some(engine) = self.engine.as_mut() {
                    engine
                        .enable_dynamic_server(
                            self.config.network.families.clone(),
                            self.config.network.max_mtu,
                            self.config.network.channels,
                        )
                        .map_err(|_| RuntimeFailure::Internal)?;
                    self.ip_events()?;
                }
            }
            HelperKind::Reserve {
                key,
                session,
                families,
                mtu,
                channels,
            } => {
                if frame.fd.is_some() {
                    return Err(RuntimeFailure::Internal);
                }
                let reserved: local_api::PeerReserved =
                    local_api::arguments(&result).map_err(|_| RuntimeFailure::Internal)?;
                if reserved.session != session {
                    return Err(RuntimeFailure::Internal);
                }
                let server = self
                    .config
                    .server
                    .as_ref()
                    .ok_or(RuntimeFailure::Internal)?;
                let config = SessionConfig {
                    session: session.clone(),
                    families: families.clone(),
                    source_grants: reserved.source_grants,
                    routes: families
                        .iter()
                        .map(|f| if *f == 4 { "0.0.0.0/0" } else { "::/0" }.parse().unwrap())
                        .collect(),
                    dns_servers: server
                        .dns_servers
                        .iter()
                        .filter(|ip| families.contains(&if ip.is_ipv4() { 4 } else { 6 }))
                        .copied()
                        .collect(),
                    mtu,
                    channels,
                    packet_queue_bytes: self.config.network.limits.packet_queue_bytes,
                    packet_queue_records: self.config.network.limits.packet_queue_records,
                    setup_timeout_ms: 15000,
                    egress: reserved.egress,
                };
                let installed = self
                    .engine
                    .as_mut()
                    .ok_or(RuntimeFailure::Internal)?
                    .complete_reservation(key, config)
                    .map_err(|_| RuntimeFailure::Internal)?;
                if !installed {
                    self.helper_enqueue(
                        local_api::HelperOperation::RetirePeer,
                        json!({"peer":key.stream.peer.0.to_string(),"session":session}),
                        HelperKind::Retire,
                    )?;
                }
            }
            HelperKind::Activate { key, session } => {
                if frame.fd.is_some() || result != json!({"session":session}) {
                    return Err(RuntimeFailure::Internal);
                }
                let active = self
                    .engine
                    .as_mut()
                    .ok_or(RuntimeFailure::Internal)?
                    .complete_activation(key, &session)
                    .map_err(|_| RuntimeFailure::Internal)?;
                if !active {
                    self.helper_enqueue(
                        local_api::HelperOperation::RetirePeer,
                        json!({"peer":key.stream.peer.0.to_string(),"session":session}),
                        HelperKind::Retire,
                    )?;
                }
            }
            HelperKind::Retire => {
                if frame.fd.is_some()
                    || result
                        != pending
                            .request
                            .args
                            .as_object()
                            .and_then(|a| a.get("session"))
                            .map(|s| json!({"session":s}))
                            .unwrap_or(Value::Null)
                {
                    return Err(RuntimeFailure::Internal);
                }
            }
            HelperKind::Stop => {
                if frame.fd.is_some() || result != json!({"state":"idle"}) {
                    return Err(RuntimeFailure::Internal);
                }
                self.helper_ready = false;
            }
        }
        Ok(())
    }
    async fn backend_turn(&mut self) -> Result<(), RuntimeFailure> {
        for _ in 0..self.output.terminal_batch() / 2 {
            let Some(event) = self.engine.as_mut().and_then(NetworkEngine::poll_backend) else {
                break;
            };
            match event {
                BackendEvent::TcpOpen { key, host, port } => {
                    if self.tcp.len() + self.connects.len()
                        >= self.config.network.limits.core_streams
                        || self.connects.len() >= 256
                        || self
                            .connects
                            .iter()
                            .filter(|c| c.key.stream.peer == key.stream.peer)
                            .count()
                            >= 64
                    {
                        self.admission_error("destination_queue");
                        self.engine
                            .as_mut()
                            .unwrap()
                            .reject_tcp(key, "overloaded")
                            .map_err(|_| RuntimeFailure::Internal)?;
                        continue;
                    }
                    let reservation = match self.budget.reserve(1024, 4) {
                        Ok(r) => r,
                        Err(_) => {
                            self.engine
                                .as_mut()
                                .unwrap()
                                .reject_tcp(key, "overloaded")
                                .map_err(|_| RuntimeFailure::Internal)?;
                            continue;
                        }
                    };
                    self.connects.push(Connecting {
                        key,
                        future: None,
                        host,
                        port,
                        _reservation: reservation,
                        deadline: Instant::now() + Duration::from_secs(10),
                    });
                }
                BackendEvent::Tcp { key, event } => {
                    let mut opened_reply = None;
                    let rejected_error = match &event {
                        skvoz_core::Event::Rejected { reason } => {
                            Some(crate::tcp::reject_error(reason))
                        }
                        _ => None,
                    };
                    let is_opened = matches!(event, skvoz_core::Event::Opened { .. });
                    let is_rejected = matches!(event, skvoz_core::Event::Rejected { .. });
                    let mut closed_reply = None;
                    if let Some(connection) = self.tcp.get_mut(&key) {
                        let opened = matches!(event, skvoz_core::Event::Opened { .. });
                        let alive = connection.event(event).unwrap_or(false);
                        if opened {
                            opened_reply = connection.reply.take();
                        }
                        if !alive {
                            if let Some(c) = self.tcp.remove(&key) {
                                closed_reply = c.reply;
                            }
                            self.engine.as_mut().unwrap().close_tcp(key);
                        }
                    }
                    if is_opened {
                        self.request_event(key, "active")?;
                    }
                    if is_rejected {
                        self.request_event(
                            key,
                            api_label(rejected_error.unwrap_or(ApiError::NetworkUnavailable)),
                        )?;
                    }
                    if !self.tcp.contains_key(&key) {
                        self.request_event(key, "cancelled")?;
                        self.journal.remove(&key);
                    }
                    if let Some((id, handle, fd)) = opened_reply {
                        self.respond(id, Ok(local_api::handle_result(&handle)), Some(fd))?;
                    }
                    if let Some((id, _, _)) = closed_reply {
                        self.respond(
                            id,
                            Err(rejected_error.unwrap_or(ApiError::NetworkUnavailable)),
                            None,
                        )?;
                    }
                }
                BackendEvent::Reserve {
                    key,
                    session,
                    families,
                    mtu,
                    channels,
                } => {
                    if !self.helper_ready {
                        self.engine
                            .as_mut()
                            .unwrap()
                            .fail_reservation(key, "network_unavailable");
                        continue;
                    }
                    self.helper_enqueue(local_api::HelperOperation::ReservePeer,json!({"peer":key.stream.peer.0.to_string(),"session":session,"families":families,"mtu":mtu}),HelperKind::Reserve{key,session,families,mtu,channels})?;
                }
                BackendEvent::Activate { key, session } => {
                    self.helper_enqueue(
                        local_api::HelperOperation::ActivatePeer,
                        json!({"peer":key.stream.peer.0.to_string(),"session":session}),
                        HelperKind::Activate { key, session },
                    )?;
                }
                BackendEvent::Retire { key, session } => {
                    if self.helper.is_some() {
                        self.helper_enqueue(
                            local_api::HelperOperation::RetirePeer,
                            json!({"peer":key.stream.peer.0.to_string(),"session":session}),
                            HelperKind::Retire,
                        )?;
                    }
                }
            }
        }
        // Canceled opens drop their queued/active futures promptly. An actual OS
        // resolver worker keeps its own permit and reservation until completion.
        self.connects.retain(|c| {
            self.engine
                .as_ref()
                .unwrap()
                .runtime
                .snapshot(c.key)
                .is_some()
        });
        // Round-robin admission by peer, with four active connects per peer.
        // One stalled peer cannot occupy all sixteen DNS/connect workers.
        loop {
            if self.connects.iter().filter(|c| c.future.is_some()).count() >= 16 {
                break;
            }
            let mut peers: Vec<_> = self
                .connects
                .iter()
                .filter(|c| c.future.is_none())
                .map(|c| c.key.stream.peer)
                .collect();
            peers.sort();
            peers.dedup();
            let start = peers.partition_point(|p| *p <= self.connect_peer);
            peers.rotate_left(start);
            let Some(peer) = peers.into_iter().find(|peer| {
                self.connects
                    .iter()
                    .filter(|c| c.key.stream.peer == *peer && c.future.is_some())
                    .count()
                    < 4
            }) else {
                break;
            };
            let c = self
                .connects
                .iter_mut()
                .filter(|c| c.key.stream.peer == peer && c.future.is_none())
                .min_by_key(|c| c.deadline)
                .unwrap();
            c.future = Some(Box::pin(connect_destination(
                self.policy.clone().ok_or(RuntimeFailure::Internal)?,
                c.host.clone(),
                c.port,
                self.budget.clone(),
            )));
            self.connect_peer = peer;
        }
        let mut index = 0;
        while index < self.connects.len() {
            let result = if Instant::now() >= self.connects[index].deadline {
                Some(Err(ApiError::Timeout))
            } else {
                match self.connects[index].future.as_mut() {
                    Some(future) => tokio::time::timeout(Duration::ZERO, future).await.ok(),
                    None => None,
                }
            };
            let Some(result) = result else {
                index += 1;
                continue;
            };
            let connecting = self.connects.swap_remove(index);
            let key = connecting.key;
            match result {
                Ok(socket) => {
                    if self
                        .engine
                        .as_ref()
                        .unwrap()
                        .runtime
                        .snapshot(key)
                        .is_none()
                    {
                        continue;
                    }
                    match TcpConnection::new(key, Socket::Tcp(socket), &self.budget, Vec::new()) {
                        Ok(mut connection) => match self.engine.as_mut().unwrap().accept_tcp(key) {
                            Ok(()) => {
                                connection.opened = true;
                                self.tcp.insert(key, connection);
                            }
                            Err(_) => self.engine.as_mut().unwrap().close_tcp(key),
                        },
                        Err(error) => {
                            self.admission_error(if error == NetworkError::Overloaded {
                                "buffer"
                            } else {
                                "local_setup"
                            });
                            let _ = self.engine.as_mut().unwrap().reject_tcp(
                                key,
                                if error == NetworkError::Overloaded {
                                    "overloaded"
                                } else {
                                    "network_unavailable"
                                },
                            );
                        }
                    }
                }
                Err(e) => {
                    self.admission_error(match e {
                        ApiError::Overloaded => "buffer",
                        ApiError::Timeout => "destination_timeout",
                        ApiError::Forbidden => "forbidden",
                        ApiError::LocalSetupFailed => "local_setup",
                        _ => "network_unavailable",
                    });
                    let _ = self.engine.as_mut().unwrap().reject_tcp(
                        key,
                        match e {
                            ApiError::Forbidden => "forbidden",
                            ApiError::Timeout => "timeout",
                            ApiError::Overloaded => "overloaded",
                            _ => "network_unavailable",
                        },
                    );
                }
            }
        }
        Ok(())
    }
    fn proxy_failure(pending: &mut ProxyPending, bytes: Vec<u8>) -> Result<(), NetworkError> {
        let protocol = pending.parser.protocol();
        pending.parser = crate::proxy::Handshake::new(protocol);
        pending.output = bytes;
        pending.cursor = 0;
        pending.terminal = true;
        pending.deadline = Instant::now() + Duration::from_secs(1);
        pending
            ._reservation
            .resize(1024 + pending.output.capacity(), 4)
    }
    async fn proxy_turn(&mut self) -> Result<(), RuntimeFailure> {
        let protocols = if self.proxy_cursor.is_multiple_of(2) {
            [crate::proxy::Protocol::Http, crate::proxy::Protocol::Socks]
        } else {
            [crate::proxy::Protocol::Socks, crate::proxy::Protocol::Http]
        };
        self.proxy_cursor = self.proxy_cursor.wrapping_add(1);
        for protocol in protocols {
            for _ in 0..8 {
                // Separate bounded failure slots allow complete replies to overload.
                if self.proxy_pending.len() >= 160 {
                    break;
                }
                let listener = match protocol {
                    crate::proxy::Protocol::Http => self.http.as_ref(),
                    crate::proxy::Protocol::Socks => self.socks.as_ref(),
                };
                let Some(listener) = listener else { break };
                let accepted = tokio::time::timeout(Duration::ZERO, listener.accept()).await;
                let socket = match accepted {
                    Ok(Ok((socket, _))) => {
                        socket.into_std().map_err(|_| RuntimeFailure::Internal)?
                    }
                    Ok(Err(_)) => return Err(RuntimeFailure::Internal),
                    Err(_) => break,
                };
                let reservation = match self.budget.reserve(1024, 4) {
                    Ok(r) => r,
                    Err(_) => {
                        self.admission_error("buffer");
                        drop(socket);
                        continue;
                    }
                };
                let full = self.proxy_pending.iter().filter(|p| !p.terminal).count() >= 128;
                let mut pending = ProxyPending {
                    socket,
                    parser: crate::proxy::Handshake::new(protocol),
                    output: Vec::new(),
                    cursor: 0,
                    deadline: Instant::now() + Duration::from_secs(10),
                    _reservation: reservation,
                    terminal: false,
                };
                if full {
                    self.admission_error("setup_queue");
                    let bytes = match protocol {
                        crate::proxy::Protocol::Http => crate::proxy::HTTP_OVERLOADED.to_vec(),
                        crate::proxy::Protocol::Socks => pending.parser.bad_request(),
                    };
                    if Self::proxy_failure(&mut pending, bytes).is_err() {
                        continue;
                    }
                }
                self.proxy_pending.push(pending);
            }
        }
        use std::io::{Read, Write};
        // Rotate the scan so incomplete handshakes never permanently lead a turn.
        let count = self.proxy_pending.len();
        if count > 0 {
            self.proxy_pending.rotate_left(self.proxy_cursor % count);
        }
        let mut index = 0;
        while index < self.proxy_pending.len() {
            if Instant::now() >= self.proxy_pending[index].deadline {
                self.admission_error("setup_timeout");
                self.proxy_pending.swap_remove(index);
                continue;
            }
            let pending = &mut self.proxy_pending[index];
            if pending.cursor < pending.output.len() {
                match pending.socket.write(&pending.output[pending.cursor..]) {
                    Ok(0) => {
                        self.proxy_pending.swap_remove(index);
                        continue;
                    }
                    Ok(n) => pending.cursor += n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        index += 1;
                        continue;
                    }
                    Err(_) => {
                        self.proxy_pending.swap_remove(index);
                        continue;
                    }
                }
                if pending.cursor < pending.output.len() {
                    index += 1;
                    continue;
                }
                pending.output = Vec::new();
                pending.cursor = 0;
            }
            if pending.terminal {
                self.proxy_pending.swap_remove(index);
                continue;
            }
            if self.output.terminal_batch() == 0 {
                // A complete local admission may emit a reliable terminal REQUEST.
                // Leave its bounded parser state intact until the owner drains.
                index += 1;
                continue;
            }
            let progress = match pending.parser.feed(&[]) {
                Ok(crate::proxy::Progress::Read) => {
                    let mut bytes = [0u8; 16384];
                    match pending.socket.read(&mut bytes) {
                        Ok(0) => {
                            self.proxy_pending.swap_remove(index);
                            continue;
                        }
                        Ok(n) => {
                            if pending
                                ._reservation
                                .resize(pending.parser.feed_reservation(n), 4)
                                .is_err()
                            {
                                let failure = pending.parser.setup_failure(ApiError::Overloaded);
                                let _ = Self::proxy_failure(pending, failure);
                                self.admission_error("buffer");
                                index += 1;
                                continue;
                            }
                            pending.parser.feed(&bytes[..n])
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            index += 1;
                            continue;
                        }
                        Err(_) => {
                            self.proxy_pending.swap_remove(index);
                            continue;
                        }
                    }
                }
                other => other,
            };
            match progress {
                Ok(crate::proxy::Progress::Read) => index += 1,
                Ok(crate::proxy::Progress::Write(bytes)) => {
                    pending.output = bytes;
                    index += 1;
                }
                Ok(crate::proxy::Progress::Open {
                    host,
                    port,
                    initial,
                    success,
                    failure,
                }) => {
                    let pending = self.proxy_pending.swap_remove(index);
                    let protocol = match pending.parser.protocol() {
                        crate::proxy::Protocol::Socks => "SOCKS5",
                        crate::proxy::Protocol::Http if success.is_empty() => "HTTP",
                        crate::proxy::Protocol::Http => "CONNECT",
                    };
                    let key = if self.tcp.len() >= self.config.network.limits.core_streams {
                        Err(NetworkError::Overloaded)
                    } else {
                        self.engine
                            .as_mut()
                            .unwrap()
                            .open_tcp(PeerId(0), host.clone(), port)
                    };
                    match key {
                        Ok(key) => {
                            let ProxyPending {
                                socket,
                                parser,
                                _reservation: reservation,
                                ..
                            } = pending;
                            drop(parser);
                            match TcpConnection::with_reservation(
                                key,
                                Socket::Tcp(socket),
                                &self.budget,
                                initial,
                                reservation,
                            ) {
                                Ok(mut connection) => {
                                    connection.success = success;
                                    connection.failure = failure;
                                    self.tcp.insert(key, connection);
                                    self.record_request(key, protocol, host, port)?;
                                }
                                Err(failed) => {
                                    let api_error = setup_api_error(failed.error.clone());
                                    self.engine.as_mut().unwrap().close_tcp(key);
                                    if let Socket::Tcp(socket) = failed.socket {
                                        let mut pending = ProxyPending {
                                            socket,
                                            parser: crate::proxy::Handshake::new(
                                                if protocol == "SOCKS5" {
                                                    crate::proxy::Protocol::Socks
                                                } else {
                                                    crate::proxy::Protocol::Http
                                                },
                                            ),
                                            output: Vec::new(),
                                            cursor: 0,
                                            deadline: Instant::now(),
                                            terminal: false,
                                            _reservation: failed.reservation,
                                        };
                                        let bytes = pending.parser.open_failure(api_error, failure);
                                        if Self::proxy_failure(&mut pending, bytes).is_ok() {
                                            self.proxy_pending.push(pending);
                                        }
                                    }
                                    self.failed_attempt(
                                        protocol,
                                        host,
                                        port,
                                        api_label(api_error),
                                    )?;
                                    self.admission_error(if api_error == ApiError::Overloaded {
                                        "buffer"
                                    } else {
                                        "local_setup"
                                    });
                                }
                            }
                        }
                        Err(error) => {
                            let api_error = ApiError::from(error);
                            let mut pending = pending;
                            let bytes = pending.parser.setup_failure(api_error);
                            let _ = Self::proxy_failure(&mut pending, bytes);
                            self.proxy_pending.push(pending);
                            self.failed_attempt(protocol, host, port, api_label(api_error))?;
                            self.admission_error(if api_error == ApiError::Overloaded {
                                "core_slots_or_credit"
                            } else {
                                "network_unavailable"
                            });
                        }
                    }
                }
                Err(_) => {
                    let bytes = pending.parser.bad_request();
                    let _ = Self::proxy_failure(pending, bytes);
                    self.admission_error("invalid_handshake");
                    index += 1;
                }
            }
        }
        Ok(())
    }
    fn admission_error(&mut self, stage: &'static str) {
        self.counters.errors = self.counters.errors.saturating_add(1);
        let index = match stage {
            "buffer" => 0,
            "setup_queue" => 1,
            "setup_timeout" => 2,
            "invalid_handshake" => 3,
            "core_slots_or_credit" => 4,
            "destination_queue" => 5,
            "destination_timeout" => 6,
            "network_unavailable" => 7,
            "local_setup" => 8,
            _ => 9,
        };
        let count = &mut self.admission_errors[index];
        *count = count.saturating_add(1);
        // Aggregate, logarithmically bounded diagnostics, without credentials or payload.
        if count.is_power_of_two() {
            eprintln!(
                "TCP admission error: stage={stage} stage_total={count} total={}",
                self.counters.errors
            );
        }
    }
    fn native_turn(&mut self) -> Result<(), RuntimeFailure> {
        let mut failed = Vec::new();
        if let Some(engine) = self.engine.as_mut() {
            let keys: Vec<_> = self.tcp.keys().copied().collect();
            let count = keys.len().min(64);
            for index in 0..count {
                let key = &keys[(self.native_cursor + index) % keys.len()];
                let c = self.tcp.get_mut(key).unwrap();
                let (before_up, before_down) = (c.uploaded, c.downloaded);
                let result = if self.tcp_stopping {
                    Err(NetworkError::InvalidState)
                } else if engine.runtime.snapshot(*key).is_none() && !c.core_finished() {
                    // Core may retire a stream before its bounded backend batch
                    // delivers Closed. Wait for that event rather than losing a
                    // graceful tail or misreporting ordinary close as I/O failure.
                    Ok(())
                } else {
                    c.turn(engine)
                };
                self.tcp_uploaded = self.tcp_uploaded.saturating_add(c.uploaded - before_up);
                self.tcp_downloaded = self
                    .tcp_downloaded
                    .saturating_add(c.downloaded - before_down);
                if let Some(j) = self.journal.get_mut(key) {
                    j.uploaded = c.uploaded;
                    j.downloaded = c.downloaded;
                }
                if let Err(error) = result {
                    failed.push((*key, error))
                }
            }
            if !keys.is_empty() {
                let cursor = self.native_cursor % keys.len();
                let wrapped = cursor + count >= keys.len();
                self.native_cursor = (cursor + count + usize::from(wrapped)) % keys.len();
                if !wrapped {
                    // Slice one native scan with complete Core turns between
                    // chunks; idle waiting belongs after the full scan wraps.
                    self.wake.notify_one();
                }
            }
        }
        // Terminal ownership is released in bounded batches, so a reading API
        // owner can drain reliable journal records during a mass close.
        let batch = self.output.terminal_batch();
        let has_pending = !failed.is_empty();
        let mut retired = 0;
        let mut remaining = batch;
        for (key, error) in failed {
            let cost = 1 + usize::from(self.tcp.get(&key).is_some_and(|c| c.reply.is_some()));
            if cost > remaining {
                break;
            }
            if !self.engine.as_mut().unwrap().close_native_tcp(key) {
                continue;
            }
            remaining -= cost;
            retired += 1;
            let rejection = self.tcp.get(&key).and_then(|c| c.rejection);
            let result = if self.tcp_stopping {
                "cancelled"
            } else if let Some(error) = rejection {
                api_label(error)
            } else if self
                .tcp
                .get(&key)
                .is_some_and(TcpConnection::gracefully_finished)
            {
                "finished"
            } else {
                match error {
                    NetworkError::Timeout => "timeout",
                    NetworkError::Overloaded => "overloaded",
                    NetworkError::Runtime(_) => "network_unavailable",
                    _ => "local_setup_failed",
                }
            };
            self.request_event(key, result)?;
            self.journal.remove(&key);
            if let Some(c) = self.tcp.remove(&key)
                && let Some((id, _, _)) = c.reply
            {
                self.respond(id, Err(ApiError::NetworkUnavailable), None)?;
            }
        }
        if has_pending && retired == 0 {
            let since = self.terminal_wait.get_or_insert_with(Instant::now);
            if since.elapsed() >= Duration::from_secs(3) {
                return Err(RuntimeFailure::Overloaded);
            }
        } else {
            self.terminal_wait = None;
        }
        if self.tcp.is_empty() {
            self.tcp_stopping = false;
        }
        let Some(tun) = self.tun.as_ref() else {
            return Ok(());
        };
        let Some(engine) = self.engine.as_mut() else {
            return Ok(());
        };
        if self.packet_pending.is_none() {
            self.packet_pending = engine
                .poll_packet()
                .map(|p| (p, Instant::now() + Duration::from_secs(1)));
        }
        for _ in 0..16 {
            let Some((packet, deadline)) = self.packet_pending.as_ref() else {
                break;
            };
            if Instant::now() >= *deadline {
                return Err(RuntimeFailure::Internal);
            }
            let (_, receive, records) = engine.native_allowance(packet.key.stream.peer);
            if receive < packet.packet.len() + 8 || records == 0 {
                break;
            }
            match tun.try_write_packet(&packet.packet) {
                Ok(()) => {
                    let (packet, _) = self.packet_pending.take().unwrap();
                    engine.account_native(packet.key.stream.peer, 0, packet.packet.len() + 8, 1);
                    engine.note_native_packet_write(packet.packet.len());
                    engine
                        .complete_packet(packet.key, packet.end_offset)
                        .map_err(|_| RuntimeFailure::Internal)?;
                    self.counters.packet_out = self.counters.packet_out.saturating_add(1);
                    self.counters.downloaded = self
                        .counters
                        .downloaded
                        .saturating_add(packet.packet.len() as u64);
                    self.packet_pending = engine
                        .poll_packet()
                        .map(|p| (p, Instant::now() + Duration::from_secs(1)));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return Err(RuntimeFailure::Internal),
            }
        }
        let mut buffer = [0u8; 1501];
        for _ in 0..16 {
            match tun.try_read_packet(&mut buffer) {
                Ok(n) => {
                    let session = if self.config.role == Role::Client {
                        engine
                            .sessions()
                            .into_iter()
                            .find(|s| s.peer == PeerId(0))
                            .map(|s| s.session)
                    } else {
                        crate::validate_packet(
                            &buffer[..n],
                            self.config.network.max_mtu,
                            &self.config.network.families,
                        )
                        .ok()
                        .and_then(|packet| {
                            engine
                                .sessions()
                                .into_iter()
                                .find(|s| {
                                    s.state == SessionState::Active
                                        && s.config.as_ref().is_some_and(|c| {
                                            c.source_grants
                                                .iter()
                                                .any(|g| g.contains(packet.destination))
                                        })
                                })
                                .map(|s| s.session)
                        })
                    };
                    if let Some(session) = session {
                        if engine.enqueue_packet(&session, &buffer[..n]).is_err() {
                            self.counters.packet_dropped =
                                self.counters.packet_dropped.saturating_add(1);
                        } else {
                            self.counters.packet_in = self.counters.packet_in.saturating_add(1);
                        }
                    } else {
                        self.counters.packet_dropped =
                            self.counters.packet_dropped.saturating_add(1);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return Err(RuntimeFailure::Internal),
            }
        }
        Ok(())
    }
    fn finish_stop(&mut self) -> Result<(), RuntimeFailure> {
        let Some(stop) = self.pending_stop.as_ref() else {
            return Ok(());
        };
        let drained = self
            .engine
            .as_ref()
            .is_none_or(|e| !e.tcp_cancellations_pending())
            && stop.keys.iter().all(|k| !self.tcp.contains_key(k))
            && self
                .engine
                .as_ref()
                .is_none_or(|e| stop.keys.iter().all(|k| e.runtime.snapshot(*k).is_none()));
        if !drained && Instant::now() < stop.deadline {
            return Ok(());
        }
        let stop = self.pending_stop.take().unwrap();
        if !drained {
            self.respond(stop.id, Err(ApiError::Timeout), None)?;
            self.stop.store(true, Ordering::Release);
            return Ok(());
        }
        self.mode = "idle";
        if let Some(handle) = stop.target {
            self.ip.take();
            self.remember_ip(handle.clone());
            self.respond(stop.id, Ok(local_api::handle_result(&handle)), None)?;
            self.event("CLOSED", json!({"handle":handle,"error":null}))?;
        } else {
            self.respond(stop.id, Ok(json!({})), None)?;
        }
        Ok(())
    }
    fn ip_events(&mut self) -> Result<(), RuntimeFailure> {
        if self
            .pending_stop
            .as_ref()
            .is_some_and(|s| s.target.is_some())
        {
            return Ok(());
        }
        if let Some(id) = self.ip.clone() {
            let session = self
                .engine
                .as_ref()
                .and_then(|e| e.sessions().into_iter().find(|s| s.peer == PeerId(0)));
            if let Some(session) = session {
                if !self.configured
                    && let Some(config) = session.config
                {
                    self.configured = true;
                    self.event("CONFIGURED", json!({"handle":id,"config":config}))?;
                }
                if !self.active && session.state == SessionState::Active {
                    self.active = true;
                    self.event("ACTIVE", json!({"handle":id}))?;
                }
            } else {
                self.ip.take();
                self.tun.take();
                self.packet_pending.take();
                self.mode = "idle";
                self.remember_ip(id.clone());
                let error = self
                    .engine
                    .as_ref()
                    .and_then(NetworkEngine::last_error)
                    .map(ApiError::from)
                    .unwrap_or(ApiError::NetworkUnavailable);
                self.event("CLOSED", json!({"handle":id,"error":error}))?;
            }
        }
        let ready = self.engine.as_ref().is_some_and(|e| {
            e.core_status().lifecycle == Lifecycle::Ready
                && (self.config.role == Role::Server || e.peer_ready(PeerId(0)))
        });
        if ready && self.lifecycle != "ready" && (self.helper.is_none() || self.helper_ready) {
            self.state("ready", None)?;
        } else if !ready && self.lifecycle == "ready" {
            if let Some(engine) = &self.engine {
                eprintln!(
                    "Network readiness lost: counters={:?}",
                    engine.runtime.status().counters
                );
            }
            self.state("starting", Some(ApiError::NetworkUnavailable))?;
        }
        Ok(())
    }
    async fn cleanup(&mut self) -> Result<(), RuntimeFailure> {
        if self.cleanup_done {
            return Ok(());
        }
        self.cleanup_done = true;
        self.stop_proxy();
        self.tun.take();
        self.packet_pending.take();
        self.ip.take();
        self.ip_guard.take();
        self.pending_stop.take();
        self.mode = if self.config.role == Role::Server {
            "server"
        } else {
            "idle"
        };
        let until = *self
            .shutdown_deadline
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert_with(|| Instant::now() + Duration::from_secs(3));
        let mut result = Ok(());
        while !self.tcp.is_empty()
            || self
                .engine
                .as_ref()
                .is_some_and(NetworkEngine::tcp_cancellations_pending)
        {
            if Instant::now() >= until {
                result = Err(RuntimeFailure::Internal);
                break;
            }
            if let Err(error) = self.native_turn() {
                result = Err(error);
                break;
            }
            if let Some(engine) = self.engine.as_mut()
                && engine.drive(Duration::ZERO).await.is_err()
            {
                result = Err(RuntimeFailure::Internal);
                break;
            }
            if let Err(error) = self.backend_turn().await {
                result = Err(error);
                break;
            }
            // Keep terminal batching observable to the reading CLI/FFI owner.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        if self.helper.is_some() {
            self.helper_queue.clear();
            if self.helper_id == 0
                && self
                    .helper_enqueue(
                        local_api::HelperOperation::Hello,
                        json!({"api":1,"network":2}),
                        HelperKind::Hello,
                    )
                    .is_err()
            {
                result = Err(RuntimeFailure::Internal)
            }
            if self
                .helper_enqueue(
                    local_api::HelperOperation::StopServer,
                    json!({}),
                    HelperKind::Stop,
                )
                .is_err()
            {
                result = Err(RuntimeFailure::Internal)
            }
        }
        let mut engine = self.engine.take();
        let shutdown = async {
            if let Some(engine) = engine.as_mut() {
                engine
                    .shutdown()
                    .await
                    .map_err(|_| RuntimeFailure::Internal)
            } else {
                Ok(())
            }
        };
        tokio::pin!(shutdown);
        let mut core_done = false;
        let mut helper_done = self.helper.is_none();
        loop {
            if core_done && helper_done {
                break;
            }
            if Instant::now() >= until {
                result = Err(RuntimeFailure::Internal);
                break;
            }
            if !helper_done {
                if self.helper_turn().is_err() {
                    result = Err(RuntimeFailure::Internal);
                    helper_done = true;
                    self.helper.take();
                    self.helper_pending.take();
                    self.helper_queue.clear();
                } else if self.helper_pending.is_none() && self.helper_queue.is_empty() {
                    helper_done = true;
                }
            }
            tokio::select! {
                core_result=&mut shutdown,if !core_done=>{core_done=true;if core_result.is_err(){result=Err(RuntimeFailure::Internal)}},
                _=tokio::time::sleep(Duration::from_millis(5))=>{},
            }
        }
        self.helper.take();
        self.helper_pending.take();
        self.helper_queue.clear();
        self.lifecycle = "closed";
        result
    }
}
async fn bind_proxy(
    args: local_api::ProxyArgs,
) -> Result<
    (
        Option<TcpListener>,
        Option<TcpListener>,
        Option<String>,
        Option<String>,
    ),
    ApiError,
> {
    let http = match &args.http_bind {
        Some(bind) => Some(
            TcpListener::bind(local_api::bind_address(bind).map_err(ApiError::from)?)
                .await
                .map_err(|_| ApiError::LocalSetupFailed)?,
        ),
        None => None,
    };
    let socks = match &args.socks_bind {
        Some(bind) => Some(
            TcpListener::bind(local_api::bind_address(bind).map_err(ApiError::from)?)
                .await
                .map_err(|_| ApiError::LocalSetupFailed)?,
        ),
        None => None,
    };
    let http_uri = args.http_bind.map(|b| format!("http://{b}"));
    let socks_uri = args.socks_bind.map(|b| format!("socks5://{b}"));
    Ok((http, socks, http_uri, socks_uri))
}
fn resolver_slots() -> Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(16))).clone()
}
async fn connect_destination(
    policy: Arc<crate::config::ServerConfig>,
    host: String,
    port: u16,
    budget: Budget,
) -> Result<std::net::TcpStream, ApiError> {
    let addresses = if let Ok(address) = host.parse::<std::net::IpAddr>() {
        vec![std::net::SocketAddr::new(address, port)]
    } else {
        let permit = resolver_slots()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::Overloaded)?;
        let reservation = budget.reserve(65536, 1).map_err(|_| ApiError::Overloaded)?;
        // Permits and reservations live in the OS resolver task, not its cancelable waiter.
        let task = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _reservation = reservation;
            use std::net::ToSocketAddrs;
            let mut addresses = Vec::new();
            for address in (host.as_str(), port)
                .to_socket_addrs()
                .map_err(|_| ApiError::NetworkUnavailable)?
            {
                if addresses.len() >= 32 {
                    return Err(ApiError::Overloaded);
                }
                if !addresses.contains(&address) {
                    addresses.push(address)
                }
            }
            Ok(addresses)
        });
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .map_err(|_| ApiError::Timeout)?
            .map_err(|_| ApiError::NetworkUnavailable)??
    };
    if addresses.is_empty() {
        return Err(ApiError::NetworkUnavailable);
    }
    if addresses
        .iter()
        .any(|a| !crate::policy::tcp_allowed(&policy, a.ip(), port))
    {
        return Err(ApiError::Forbidden);
    }
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    for address in addresses {
        if let Ok(Ok(socket)) = tokio::time::timeout_at(until, TcpStream::connect(address)).await {
            return socket.into_std().map_err(|_| ApiError::LocalSetupFailed);
        }
    }
    Err(ApiError::NetworkUnavailable)
}

fn setup_api_error(error: NetworkError) -> ApiError {
    if error == NetworkError::InvalidState {
        ApiError::LocalSetupFailed
    } else {
        error.into()
    }
}

fn api_label(error: ApiError) -> &'static str {
    match error {
        ApiError::Forbidden => "forbidden",
        ApiError::Overloaded => "overloaded",
        ApiError::Timeout => "timeout",
        ApiError::LocalSetupFailed => "local_setup_failed",
        ApiError::InvalidRequest | ApiError::UnsupportedVersion => "invalid_request",
        ApiError::Closed | ApiError::InvalidState | ApiError::UnknownHandle => "cancelled",
        ApiError::NetworkUnavailable => "network_unavailable",
    }
}

fn session_label(state: SessionState) -> &'static str {
    match state {
        SessionState::Negotiating => "negotiating",
        SessionState::Preparing => "preparing",
        SessionState::AwaitingActive => "awaiting_active",
        SessionState::Active => "active",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    fn output() -> Arc<Output> {
        Arc::new(Output {
            state: Mutex::new(OutputState {
                queue: VecDeque::new(),
                closed: false,
            }),
            changed: Condvar::new(),
            budget: Budget::new(8192, 16),
            api: Budget::new(8192, 16),
        })
    }
    #[test]
    fn transient_request_coalescing_preserves_terminal_advertised_records_and_order() {
        let out = output();
        let message = |seq, id, result| {
            OwnedMessage { json: serde_json::to_vec(&json!({"v":1,"seq":seq,"event":"REQUEST","data":{"id":id,"result":result},"fd_count":0})).unwrap(), fd:None }
        };
        out.push(message(1, 1, "opening")).unwrap();
        out.push(message(2, 2, "opening")).unwrap();
        out.push(message(3, 1, "active")).unwrap();
        let first: Value =
            serde_json::from_slice(&out.poll(32768, Duration::ZERO).unwrap().json).unwrap();
        assert_eq!(first["seq"], 2);
        assert!(matches!(
            out.poll(1, Duration::ZERO),
            Err(PollError::InsufficientBuffer { .. })
        ));
        out.push(message(4, 1, "finished")).unwrap();
        out.push(message(5, 1, "cancelled")).unwrap();
        let values: Vec<Value> = (0..3)
            .map(|_| {
                serde_json::from_slice(&out.poll(32768, Duration::ZERO).unwrap().json).unwrap()
            })
            .collect();
        assert_eq!(
            values
                .iter()
                .map(|v| v["seq"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(out.budget.usage(), crate::budget::Usage::default());
    }
    #[test]
    fn insufficient_buffer_retains_authoritative_fd_and_credits_until_transfer() {
        let output = output();
        let (sender, mut receiver) = UnixStream::pair().unwrap();
        sender.set_nonblocking(true).unwrap();
        receiver.set_nonblocking(true).unwrap();
        output
            .push(OwnedMessage {
                json: b"owned-message".to_vec(),
                fd: Some(sender.into()),
            })
            .unwrap();
        let usage = output.budget.usage();
        for _ in 0..3 {
            assert!(matches!(
                output.poll(3, Duration::ZERO),
                Err(PollError::InsufficientBuffer { required: 13 })
            ));
            assert_eq!(output.budget.usage(), usage);
        }
        let message = output.poll(13, Duration::ZERO).unwrap();
        assert!(message.fd.is_some());
        assert_eq!(output.budget.usage(), crate::budget::Usage::default());
        assert!(matches!(
            output.poll(13, Duration::ZERO),
            Err(PollError::Timeout)
        ));
        use std::io::Read;
        let mut byte = [0; 1];
        assert_eq!(
            receiver.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(message);
        assert_eq!(receiver.read(&mut byte).unwrap(), 0);
    }
    #[test]
    fn queued_fd_closes_when_owner_is_destroyed_and_stats_coalesce() {
        let output = output();
        let (sender, mut receiver) = UnixStream::pair().unwrap();
        receiver.set_nonblocking(true).unwrap();
        output
            .push(OwnedMessage {
                json: b"fd".to_vec(),
                fd: Some(sender.into()),
            })
            .unwrap();
        for seq in 1..=10 {
            output.push(OwnedMessage{json:serde_json::to_vec(&json!({"v":1,"seq":seq,"event":"STATS","data":{"counters":{}},"fd_count":0})).unwrap(),fd:None}).unwrap();
        }
        assert_eq!(output.state.lock().unwrap().queue.len(), 2);
        output.close();
        drop(output);
        use std::io::Read;
        assert_eq!(receiver.read(&mut [0]).unwrap(), 0);
    }
    #[test]
    fn client_rejects_helper_before_startup_or_any_connection() {
        let config =
            StartupConfig::parse_json(include_bytes!("../tests/fixtures/client-startup.json"))
                .unwrap();
        let (a, _b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        assert!(matches!(
            RuntimeHandle::start(config, Some(a.into())),
            Err(RuntimeFailure::InvalidArgument)
        ));
    }
    #[test]
    fn handshake_response_is_independent_of_pending_core_startup() {
        let config =
            StartupConfig::parse_json(include_bytes!("../tests/fixtures/client-startup.json"))
                .unwrap();
        let output = output();
        let (sender, mut receiver) = mpsc::channel(1);
        let busy = Arc::new(AtomicBool::new(true));
        let stop = Arc::new(AtomicBool::new(false));
        let mut actor = Actor::new(
            config,
            None,
            output.clone(),
            busy.clone(),
            stop,
            Arc::new(Notify::new()),
            Arc::new(Mutex::new(None)),
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let budget = Budget::new(65536, 4);
        let request = Request::parse_json(
            br#"{"v":1,"id":1,"op":"HELLO","args":{"api":1,"network":2},"fd_count":0}"#,
        )
        .unwrap();
        sender
            .try_send(Command {
                request,
                fd: None,
                _reservation: budget.reserve(1024, 1).unwrap(),
            })
            .unwrap();
        // No NATS future is run: the same actor command path owns the response.
        runtime
            .block_on(actor.command(receiver.try_recv().unwrap()))
            .unwrap();
        assert!(actor.engine.is_none());
        let message = output.poll(32768, Duration::ZERO).unwrap();
        let response: Response = serde_json::from_slice(&message.json).unwrap();
        assert_eq!(response.id, 1);
        assert!(response.error.is_none());
        assert_eq!(response.result.unwrap()["api"], 1);
        assert!(!busy.load(Ordering::Acquire));
    }
    #[test]
    fn output_overflow_does_not_leak_fd() {
        let output = output();
        let (a, mut b) = UnixStream::pair().unwrap();
        b.set_nonblocking(true).unwrap();
        assert_eq!(
            output.push(OwnedMessage {
                json: vec![0; 9000],
                fd: Some(a.into())
            }),
            Err(RuntimeFailure::Overloaded)
        );
        use std::io::Read;
        assert_eq!(b.read(&mut [0]).unwrap(), 0);
        assert_eq!(output.budget.usage(), crate::budget::Usage::default());
    }
    #[test]
    fn fd_mismatch_is_terminal_and_closes_duplicate() {
        let config =
            StartupConfig::parse_json(include_bytes!("../tests/fixtures/client-startup.json"))
                .unwrap();
        let (mut owner, observer) = UnixStream::pair().unwrap();
        owner.set_nonblocking(true).unwrap();
        observer.set_nonblocking(true).unwrap();
        let output = output();
        let (commands, _receiver) = mpsc::channel(1);
        let (_done_sender, done) = std::sync::mpsc::channel();
        let mut handle = RuntimeHandle {
            commands: Some(commands),
            output,
            budget: Budget::new(2_000_000, 64),
            busy: Arc::new(AtomicBool::new(false)),
            stop: Arc::new(AtomicBool::new(false)),
            wake: Arc::new(Notify::new()),
            shutdown_deadline: Arc::new(Mutex::new(None)),
            done,
            thread: None,
            last_id: 0,
        };
        let duplicate = skvoz_network_native::duplicate_cloexec(observer.as_fd()).unwrap();
        drop(observer);
        assert_eq!(
            handle.request_json(
                br#"{"v":1,"id":1,"op":"HELLO","args":{"api":1,"network":2},"fd_count":0}"#,
                Some(duplicate)
            ),
            Err(RuntimeFailure::InvalidArgument)
        );
        assert_eq!(
            handle.request_json(b"{}", None),
            Err(RuntimeFailure::Closed)
        );
        use std::io::Read;
        assert_eq!(owner.read(&mut [0]).unwrap(), 0);
        drop(config);
    }
    #[test]
    fn advertised_stats_are_retained_exactly_until_delivery() {
        let output = output();
        let original = serde_json::to_vec(
            &json!({"event":"STATS","seq":1,"data":{"counters":{"uploaded":1}}}),
        )
        .unwrap();
        output
            .push(OwnedMessage {
                json: original.clone(),
                fd: None,
            })
            .unwrap();
        assert!(matches!(
            output.poll(1, Duration::ZERO),
            Err(PollError::InsufficientBuffer { .. })
        ));
        output
            .push(OwnedMessage {
                json: serde_json::to_vec(
                    &json!({"event":"STATS","seq":2,"data":{"counters":{"uploaded":2000}}}),
                )
                .unwrap(),
                fd: None,
            })
            .unwrap();
        assert_eq!(
            output.poll(original.len(), Duration::ZERO).unwrap().json,
            original
        );
        assert!(matches!(
            output.poll(32768, Duration::ZERO),
            Err(PollError::Timeout)
        ));
    }
    #[test]
    fn timed_out_destroy_discards_queued_fd_and_forbids_late_actor_push() {
        let output = output();
        let (sender, mut receiver) = UnixStream::pair().unwrap();
        receiver.set_nonblocking(true).unwrap();
        output
            .push(OwnedMessage {
                json: b"fd".to_vec(),
                fd: Some(sender.into()),
            })
            .unwrap();
        output.discard();
        assert_eq!(
            output.push(OwnedMessage {
                json: b"late".to_vec(),
                fd: None
            }),
            Err(RuntimeFailure::Closed)
        );
        use std::io::Read;
        assert_eq!(receiver.read(&mut [0]).unwrap(), 0);
        assert!(matches!(
            output.poll(32768, Duration::ZERO),
            Err(PollError::Closed)
        ));
    }
    #[test]
    fn canceled_resolver_waiter_keeps_actual_worker_permit_and_reservation() {
        let budget = Budget::new(65536, 1);
        let slots = Arc::new(Semaphore::new(1));
        let permit = slots.clone().try_acquire_owned().unwrap();
        let reservation = budget.reserve(65536, 1).unwrap();
        let (started_send, started) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let (done_send, done) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let worker = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _reservation = reservation;
                started_send.send(()).unwrap();
                gate.recv().unwrap();
                drop((_permit, _reservation));
                done_send.send(()).unwrap();
            });
            assert!(tokio::time::timeout(Duration::ZERO, worker).await.is_err());
        });
        started.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(slots.clone().try_acquire_owned().is_err());
        assert_eq!(budget.usage().bytes, 65536);
        release.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(slots.available_permits(), 1);
        assert_eq!(budget.usage(), crate::budget::Usage::default());
        runtime.shutdown_background();
    }
    #[test]
    fn initial_deadline_is_not_reapplied_to_healthy_runtime_recovery() {
        let config =
            StartupConfig::parse_json(include_bytes!("../tests/fixtures/client-startup.json"))
                .unwrap();
        let output = output();
        let mut actor = Actor::new(
            config,
            None,
            output,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Notify::new()),
            Arc::new(Mutex::new(None)),
        );
        actor.ready_deadline = Instant::now() - Duration::from_secs(1);
        assert!(!actor.ever_ready);
        actor.state("ready", None).unwrap();
        actor
            .state("starting", Some(ApiError::NetworkUnavailable))
            .unwrap();
        assert!(actor.ever_ready);
        assert_eq!(actor.lifecycle, "starting");
        assert!(Instant::now() > actor.ready_deadline);
    }
    #[test]
    fn bounded_helper_reserve_rejection_is_nonfatal_but_activation_failure_is_fatal() {
        let config =
            StartupConfig::parse_json(include_bytes!("../tests/fixtures/client-startup.json"))
                .unwrap();
        let (a, b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let mut peer = IncrementalUnix::from_owned_fd(b.into()).unwrap();
        let mut actor = Actor::new(
            config,
            Some(IncrementalUnix::from_owned_fd(a.into()).unwrap()),
            output(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Arc::new(Notify::new()),
            Arc::new(Mutex::new(None)),
        );
        let key = RuntimeKey {
            epoch: 1,
            incarnation: 1,
            stream: skvoz_core::StreamKey {
                peer: PeerId(1),
                stream_id: 1,
            },
        };
        let session = SessionId::random().unwrap();
        actor.helper_id = 1;
        actor.helper_pending = Some(HelperPending {
            request: local_api::HelperRequest::new(
                1,
                local_api::HelperOperation::ReservePeer,
                json!({"peer":"1","session":session,"families":[4],"mtu":1500}),
            )
            .unwrap(),
            kind: HelperKind::Reserve {
                key,
                session: session.clone(),
                families: vec![4],
                mtu: 1500,
                channels: 1,
            },
            _reservation: actor.budget.reserve(4096, 1).unwrap(),
            deadline: Instant::now() + Duration::from_secs(5),
        });
        peer.queue_frame(
            serde_json::to_vec(&local_api::HelperResponse::failure(1, "overloaded")).unwrap(),
            None,
        )
        .unwrap();
        peer.try_flush().unwrap();
        assert_eq!(actor.helper_turn(), Ok(()));
        assert!(actor.helper_pending.is_none());
        actor.helper_pending = Some(HelperPending {
            request: local_api::HelperRequest::new(
                2,
                local_api::HelperOperation::ActivatePeer,
                json!({"peer":"1","session":session}),
            )
            .unwrap(),
            kind: HelperKind::Activate { key, session },
            _reservation: actor.budget.reserve(4096, 1).unwrap(),
            deadline: Instant::now() + Duration::from_secs(5),
        });
        peer.queue_frame(
            serde_json::to_vec(&local_api::HelperResponse::failure(2, "overloaded")).unwrap(),
            None,
        )
        .unwrap();
        peer.try_flush().unwrap();
        assert_eq!(actor.helper_turn(), Err(RuntimeFailure::Internal));
    }
}
