use crate::{
    Error, RUNTIME_VERSION, Result,
    enrollment::{Credentials, enroll},
    ipc::Session,
    settings::{Mode, Settings, host_name, private_dir, read_private, token, write_private},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use skvoz_network::{
    SessionId,
    config::{CoreConfig, Limits, NetworkConfig, Role, StartupConfig},
    local_api::{ClientPrepared, HelperResponse, PrepareClientArgs, TransportEndpoint},
};
use std::{
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::process::{Child, Command};
#[derive(Clone, Serialize, Debug)]
pub struct Status {
    pub state: &'static str,
    pub error: Option<&'static str>,
    pub peer_id: Option<u64>,
    pub pid: Option<u32>,
    pub runtime: Option<PathBuf>,
    pub connections: usize,
    pub info: bool,
    pub uploaded: u64,
    pub downloaded: u64,
    pub families: Vec<u8>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub requests: Vec<crate::telemetry::Request>,
}
pub type SharedSettings = Arc<Mutex<Settings>>;
pub struct Engine {
    pub settings: SharedSettings,
    executable: PathBuf,
    pub child: Option<Child>,
    control: Option<Session>,
    helper: Option<Session>,
    ip_handle: Option<SessionId>,
    families: Vec<u8>,
    pub runtime: Option<PathBuf>,
    pub peer_id: Option<u64>,
    pub state: &'static str,
    pub telemetry: Arc<crate::telemetry::Telemetry>,
    connections: usize,
    guard_retained: bool,
    uploaded_base: u64,
    downloaded_base: u64,
}
impl Engine {
    pub fn new(settings: SharedSettings, executable: PathBuf) -> Self {
        let telemetry = crate::telemetry::Telemetry::new(
            settings
                .lock()
                .map(|s| s.value.request_log)
                .unwrap_or(false),
        );
        Self {
            settings,
            executable,
            child: None,
            control: None,
            helper: None,
            ip_handle: None,
            families: Vec::new(),
            runtime: None,
            peer_id: None,
            state: "disconnected",
            telemetry,
            connections: 0,
            guard_retained: false,
            uploaded_base: 0,
            downloaded_base: 0,
        }
    }
    pub fn status(&self, error: Option<&'static str>) -> Status {
        Status {
            state: self.state,
            error,
            peer_id: self.peer_id,
            pid: self.child.as_ref().and_then(Child::id),
            runtime: self.runtime.clone(),
            connections: self.connections,
            info: false,
            uploaded: self.telemetry.uploaded.load(Ordering::Relaxed),
            downloaded: self.telemetry.downloaded.load(Ordering::Relaxed),
            families: self.families.clone(),
            requests: Vec::new(),
        }
    }
    pub fn prepare(
        &mut self,
        host: &str,
        port: u16,
        user: &str,
        password: String,
    ) -> Result<Credentials> {
        if !crate::settings::login(user) {
            return Err(Error("invalid_login"));
        }
        if !(12..=72).contains(&password.len()) {
            return Err(Error("invalid_password"));
        }
        let host = host_name(host)?;
        if port == 0 {
            return Err(Error("invalid_port"));
        }
        let mut settings = self.settings.lock().map_err(|_| Error("unsafe_settings"))?;
        let mut preferences = settings.value.clone();
        preferences.host = host.clone();
        preferences.port = port;
        preferences.username = user.into();
        preferences.remember_password(&host, port, user, password.clone())?;
        settings.save(preferences)?;
        Ok(Credentials {
            host,
            port,
            username: user.into(),
            password,
            ca_file: settings.value.ca_file.clone(),
            dial_ip: None,
        })
    }
    pub async fn start(&mut self, credentials: &Credentials) -> Result<()> {
        self.state = "connecting";
        self.uploaded_base = self.telemetry.uploaded.load(Ordering::Relaxed);
        self.downloaded_base = self.telemetry.downloaded.load(Ordering::Relaxed);
        let (device, preferences, directory) = {
            let mut settings = self.settings.lock().map_err(|_| Error("unsafe_settings"))?;
            (
                settings.device(&credentials.host, credentials.port, &credentials.username)?,
                settings.value.clone(),
                settings.directory.clone(),
            )
        };
        let runtime = recover_runtime(&directory)?;
        self.runtime = Some(runtime.clone());
        let mut validated = credentials.clone();
        if !credentials.ca_file.is_empty() {
            let ca = runtime.join("ca.pem");
            write_private(&ca, &read_ca(Path::new(&credentials.ca_file))?)?;
            validated.ca_file = ca.to_string_lossy().into();
        }
        // Freeze one complete numeric transport endpoint before any capture rules.
        if preferences.mode == Mode::Vpn {
            self.authorize_helper().await?;
        }
        let snapshot_key = serde_json::to_string(&(
            credentials.host.clone(),
            credentials.port,
            credentials.username.clone(),
            device.clone(),
            "vpn",
        ))
        .map_err(|_| Error("unsafe_settings"))?;
        let addresses = if self.guard_retained {
            vec![
                preferences
                    .transport_snapshots
                    .get(&snapshot_key)
                    .and_then(|ip| ip.parse().ok())
                    .ok_or(Error("guarded_transport_unknown"))?,
            ]
        } else {
            resolve_brokers(&credentials.host, credentials.port).await?
        };
        let (broker, enrollment) = tokio::time::timeout(Duration::from_secs(20), async {
            for broker in addresses {
                validated.dial_ip = Some(broker);
                match enroll(&validated, &device).await {
                    Ok(enrollment) => return Ok((broker, enrollment)),
                    Err(Error("server_unavailable" | "enrollment_failed"))
                        if !self.guard_retained => {}
                    Err(error) => return Err(error),
                }
            }
            Err(Error("server_unavailable"))
        })
        .await
        .map_err(|_| Error("server_unavailable"))??;
        if preferences.mode == Mode::Vpn {
            let mut settings = self.settings.lock().map_err(|_| Error("unsafe_settings"))?;
            let mut value = settings.value.clone();
            value
                .transport_snapshots
                .insert(snapshot_key, broker.to_string());
            settings.save(value)?;
        }
        self.peer_id = Some(enrollment.peer_id);
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            Command::new(&self.executable)
                .arg("--version")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| Error("version_mismatch"))??;
        if !output.status.success()
            || String::from_utf8_lossy(&output.stdout).trim()
                != format!("skvoz-network-runtime {RUNTIME_VERSION} network=3 api=1 core=3.1.0")
        {
            return Err(Error("version_mismatch"));
        }
        let endpoint = std::net::SocketAddr::new(broker, credentials.port);
        let config = StartupConfig {
            v: 1,
            role: Role::Client,
            core: CoreConfig {
                url: format!("tls://{endpoint}"),
                tls_server_name: credentials
                    .host
                    .parse::<IpAddr>()
                    .is_err()
                    .then(|| credentials.host.clone()),
                trust: if credentials.ca_file.is_empty() {
                    "system"
                } else {
                    "managed_ca"
                }
                .into(),
                ca_file: (!validated.ca_file.is_empty()).then(|| validated.ca_file.clone().into()),
                username: credentials.username.clone(),
                password: credentials.password.clone(),
                namespace: enrollment.namespace,
                peer_id: enrollment.peer_id.to_string(),
                membership: "allowlist".into(),
                allowed_peers: vec!["0".into()],
                initiate: vec!["0".into()],
            },
            network: NetworkConfig {
                families: preferences.runtime_families(),
                max_mtu: preferences.max_mtu,
                channels: 1,
                limits: Limits::canonical(Role::Client),
            },
            server: None,
        };
        config
            .validate()
            .map_err(|_| Error("invalid_network_settings"))?;
        let profile = runtime.join("profile.json");
        write_private(
            &profile,
            &serde_json::to_vec(&config).map_err(|_| Error("core_unavailable"))?,
        )?;
        let (local, remote) = std::os::unix::net::UnixStream::pair()?;
        local.set_nonblocking(true)?;
        remote.set_nonblocking(true)?;
        use std::os::fd::AsFd;
        let mut command = Command::new("setpriv");
        command
            .args(["--pdeathsig", "KILL"])
            .arg(std::env::current_exe()?)
            .arg("--child")
            .arg(std::process::id().to_string())
            .arg(&self.executable)
            .arg(&profile)
            .arg("3")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        skvoz_network_native::inherit_control(command.as_std_mut(), remote.as_fd(), 3)?;
        let child = command.spawn()?;
        self.telemetry.runtime_generation();
        drop(command);
        drop(remote);
        let child_id = child.id().ok_or(Error("core_unavailable"))?;
        self.child = Some(child);
        remember_child(&runtime, child_id)?;
        self.control = Some(Session::new(local.into(), false)?);
        let session = self.control.as_mut().ok_or(Error("ipc_failed"))?;
        let hello = session.call("HELLO", json!({"api":1,"network":3})).await?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Capabilities {
            profiles: Vec<String>,
            families: Vec<u8>,
            max_mtu: u16,
            max_channels: u8,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Hello {
            api: u8,
            network: u8,
            role: Role,
            capabilities: Capabilities,
        }
        let hello: Hello = serde_json::from_value(hello).map_err(|_| Error("version_mismatch"))?;
        if hello.api != 1
            || hello.network != 3
            || hello.role != Role::Client
            || hello.capabilities.profiles != ["tcp", "ip"]
            || hello.capabilities.families != preferences.runtime_families()
            || hello.capabilities.max_mtu != preferences.max_mtu
            || hello.capabilities.max_channels != 1
        {
            return Err(Error("version_mismatch"));
        }
        fs::remove_file(profile)?;
        loop {
            let event = session.wait_event("RUNTIME_STATE").await?;
            let state: skvoz_network::local_api::RuntimeStateEvent =
                skvoz_network::local_api::arguments(&event.data)
                    .map_err(|_| Error("ipc_failed"))?;
            if state.state == "ready" {
                break;
            }
            if state.state == "closed" || state.state == "closing" {
                return Err(crate::ipc::api_error(state.error));
            }
        }
        match preferences.mode {
            Mode::Proxy => {
                let http = bind(preferences.http_port);
                let socks = bind(preferences.socks_port);
                let started = session
                    .call("START_PROXY", json!({"http_bind":http,"socks_bind":socks}))
                    .await?;
                if started
                    != json!({
                        "http": http.map(|address| format!("http://{address}")),
                        "socks": socks.map(|address| format!("socks5://{address}"))
                    })
                {
                    return Err(Error("ipc_failed"));
                }
            }
            Mode::Vpn => {
                let result = session
                    .call(
                        "START_IP",
                        serde_json::to_value(preferences.ip_request())
                            .map_err(|_| Error("invalid_network_settings"))?,
                    )
                    .await?;
                let result: skvoz_network::local_api::HandleArgs =
                    skvoz_network::local_api::arguments(&result)
                        .map_err(|_| Error("ipc_failed"))?;
                let handle = result.handle;

                let event = session.wait_event("CONFIGURED").await?;
                let configured: skvoz_network::local_api::ConfiguredEvent =
                    skvoz_network::local_api::arguments(&event.data)
                        .map_err(|_| Error("ipc_failed"))?;
                if configured.handle != handle {
                    return Err(Error("ipc_failed"));
                }
                let config = configured.config;
                let families = config.families.clone();
                let args = PrepareClientArgs {
                    handle: handle.clone(),
                    config,
                    transport_endpoints: vec![TransportEndpoint {
                        ip: broker,
                        port: credentials.port,
                    }],
                };
                let helper = self.helper.as_ref().ok_or(Error("helper_failed"))?;
                let frame = helper
                    .helper_call(
                        "PREPARE_CLIENT",
                        serde_json::to_value(args).map_err(|_| Error("helper_failed"))?,
                    )
                    .await?;
                let response =
                    HelperResponse::parse_json(&frame.body).map_err(|_| Error("helper_failed"))?;
                let prepared: ClientPrepared =
                    serde_json::from_value(response.result.ok_or(Error("helper_failed"))?)
                        .map_err(|_| Error("helper_failed"))?;
                if prepared.handle != handle {
                    return Err(Error("helper_failed"));
                }
                self.guard_retained = true;
                self.ip_handle = Some(handle.clone());
                let fd = frame.fd.ok_or(Error("helper_failed"))?;
                let attached = session
                    .request(
                        "ATTACH_IP",
                        json!({"handle":handle,"interface":prepared.interface,"mtu":prepared.mtu}),
                        Some(fd),
                    )
                    .await?;
                let attached: skvoz_network::local_api::HandleArgs =
                    skvoz_network::local_api::arguments(&require_success(&attached.body)?)
                        .map_err(|_| Error("ipc_failed"))?;
                if attached.handle != handle {
                    return Err(Error("ipc_failed"));
                }
                helper
                    .helper_call("ACTIVATE_CLIENT", json!({"handle":handle}))
                    .await?;
                let ready = session
                    .call("LOCAL_READY", json!({"handle":handle}))
                    .await?;
                let ready: skvoz_network::local_api::HandleArgs =
                    skvoz_network::local_api::arguments(&ready).map_err(|_| Error("ipc_failed"))?;
                if ready.handle != handle {
                    return Err(Error("ipc_failed"));
                }
                let active = session.wait_event("ACTIVE").await?;
                if active.data["handle"] != json!(handle) {
                    return Err(Error("ipc_failed"));
                }
                self.families = families;
            }
        }
        self.state = "connected";
        Ok(())
    }
    async fn authorize_helper(&mut self) -> Result<()> {
        let uid = crate::settings::uid()?;
        let process = format!(
            "{},{},{}",
            std::process::id(),
            identity(std::process::id()).ok_or(Error("helper_failed"))?,
            uid
        );
        if self.helper.as_ref().is_some_and(|h| !h.healthy()) {
            self.helper = None;
            return Err(Error("helper_failed"));
        }
        if self.helper.is_none() {
            let status = tokio::time::timeout(
                Duration::from_secs(60),
                Command::new("pkcheck")
                    .args([
                        "--action-id",
                        "org.skvoz.network.manage",
                        "--process",
                        &process,
                        "--allow-user-interaction",
                    ])
                    .kill_on_drop(true)
                    .status(),
            )
            .await
            .map_err(|_| Error("helper_authorization_failed"))??;
            if !status.success() {
                return Err(Error("helper_authorization_failed"));
            }
        }
        if self.helper.is_none() {
            let socket = tokio::net::UnixStream::connect(format!(
                "/run/skvoz-network-helper/{uid}/control.sock"
            ))
            .await?;
            let socket = socket.into_std()?;
            let helper = Session::new(socket.into(), true)?;
            helper
                .helper_call("HELLO", json!({"api":1,"network":3}))
                .await?;
            self.helper = Some(helper);
        }
        let helper = self.helper.as_ref().ok_or(Error("helper_failed"))?;
        let recovery = helper.helper_call("RECOVER", json!({})).await?;
        let r = HelperResponse::parse_json(&recovery.body).map_err(|_| Error("helper_failed"))?;
        let recovered: skvoz_network::local_api::HelperState =
            serde_json::from_value(r.result.ok_or(Error("helper_failed"))?)
                .map_err(|_| Error("helper_failed"))?;
        match (recovered.state.as_str(), &recovered.handle) {
            ("guarded", Some(_)) => {
                self.guard_retained = true;
                self.ip_handle = recovered.handle;
            }
            ("idle", None) => {
                self.guard_retained = false;
                self.ip_handle = None;
            }
            _ => return Err(Error("helper_failed")),
        }
        Ok(())
    }
    pub async fn healthy(&mut self) -> bool {
        if self
            .child
            .as_mut()
            .and_then(|c| c.try_wait().ok())
            .flatten()
            .is_some()
        {
            return false;
        }
        let Some(control) = self.control.as_mut() else {
            return false;
        };
        if !control.healthy() {
            return false;
        }
        for event in control.drain() {
            if matches!(event.event.as_str(), "CLOSED" | "ERROR")
                || event.event == "RUNTIME_STATE" && event.data["state"] != "ready"
            {
                return false;
            }
            if event.event == "REQUEST" {
                self.telemetry.runtime_request(&event.data);
            }
            if event.event == "STATS" {
                let Ok(stats) = skvoz_network::local_api::arguments::<
                    skvoz_network::local_api::StatsEvent,
                >(&event.data) else {
                    return false;
                };
                let Ok(connections) = usize::try_from(stats.counters.tcp_open) else {
                    return false;
                };
                self.connections = connections;
                self.telemetry.uploaded.store(
                    self.uploaded_base.saturating_add(stats.counters.uploaded),
                    Ordering::Relaxed,
                );
                self.telemetry.downloaded.store(
                    self.downloaded_base
                        .saturating_add(stats.counters.downloaded),
                    Ordering::Relaxed,
                );
            }
        }
        true
    }
    pub async fn cleanup(&mut self) {
        self.explicit_stop("user_stop").await;
    }
    pub async fn shutdown(&mut self) {
        self.explicit_stop("shutdown").await;
    }
    async fn explicit_stop(&mut self, reason: &str) {
        let vpn = self
            .settings
            .lock()
            .map(|s| s.value.mode == Mode::Vpn)
            .unwrap_or(false);
        if self.helper.as_ref().is_some_and(|h| !h.healthy()) {
            self.helper = None;
        }
        let recovery_failed =
            vpn && self.helper.is_none() && self.authorize_helper().await.is_err();
        self.cleanup_reason(Some(reason)).await;
        if recovery_failed {
            self.state = "guarded";
        }
    }
    pub async fn abort(&mut self) {
        self.cleanup_reason(None).await;
    }
    async fn cleanup_reason(&mut self, reason: Option<&str>) {
        self.families.clear();
        let mut cleanup_failed = false;
        if reason.is_none()
            && let (Some(handle), Some(helper)) = (&self.ip_handle, &self.helper)
            && helper
                .helper_call("ABORT_CLIENT", json!({"handle":handle}))
                .await
                .is_err()
        {
            cleanup_failed = true;
        }
        if let Some(control) = self.control.take() {
            if let (Some(handle), Some(reason)) = (&self.ip_handle, reason) {
                // A failed STOP cannot authorize restoration while its process lives.
                let _ = control
                    .call("STOP_IP", json!({"handle":handle,"reason":reason}))
                    .await;
            }
            let _ = control.call("PREPARE_SHUTDOWN", json!({})).await;
        }
        let mut dead = true;
        if let Some(mut child) = self.child.take()
            && !matches!(
                tokio::time::timeout(Duration::from_secs(3), child.wait()).await,
                Ok(Ok(_))
            )
        {
            let _ = child.start_kill();
            dead = matches!(
                tokio::time::timeout(Duration::from_secs(3), child.wait()).await,
                Ok(Ok(_))
            );
            if !dead {
                self.child = Some(child);
                cleanup_failed = true;
            }
        }
        if let (Some(reason), Some(handle), Some(helper)) = (reason, &self.ip_handle, &self.helper)
        {
            if dead {
                if helper
                    .helper_call("RESTORE_CLIENT", json!({"handle":handle,"reason":reason}))
                    .await
                    .is_err()
                {
                    cleanup_failed = true;
                } else {
                    self.guard_retained = false;
                }
            } else {
                cleanup_failed = true;
            }
        }
        if dead && let Some(runtime) = self.runtime.take() {
            let _ = fs::remove_dir_all(runtime);
        }
        if reason.is_some() && !cleanup_failed && !self.guard_retained {
            self.ip_handle = None;
            self.helper = None;
        }
        self.peer_id = None;
        self.connections = 0;
        self.state = if cleanup_failed || self.guard_retained {
            "guarded"
        } else {
            "disconnected"
        };
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}
fn bind(port: u16) -> Option<String> {
    (port != 0).then(|| format!("127.0.0.1:{port}"))
}
fn require_success(body: &[u8]) -> Result<Value> {
    let r =
        skvoz_network::local_api::Response::parse_json(body).map_err(|_| Error("ipc_failed"))?;
    r.result.ok_or(Error("network_unavailable"))
}
async fn resolve_brokers(host: &str, port: u16) -> Result<Vec<IpAddr>> {
    let operation = async {
        let addresses = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| {
                #[cfg(feature = "qualification")]
                eprintln!("Broker address lookup failed: host={host}, port={port}, error={error}");
                Error::from(error)
            })?;
        let mut result = Vec::new();
        for a in addresses {
            let ip = a.ip();
            if ip.to_canonical() != ip || ip.is_unspecified() || ip.is_multicast() {
                #[cfg(feature = "qualification")]
                eprintln!("Broker address rejected: host={host}, address={ip}");
                return Err(Error("server_unavailable"));
            }
            if !result.contains(&ip) {
                if result.len() >= 32 {
                    return Err(Error("server_unavailable"));
                }
                result.push(ip);
            }
        }
        if result.is_empty() {
            return Err(Error("server_unavailable"));
        }
        #[cfg(feature = "qualification")]
        eprintln!("Broker addresses resolved: host={host}, addresses={result:?}");
        Ok(result)
    };
    tokio::time::timeout(Duration::from_secs(5), operation)
        .await
        .map_err(|_| {
            #[cfg(feature = "qualification")]
            eprintln!("Broker address lookup deadline exceeded: host={host}, port={port}");
            Error("server_unavailable")
        })?
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    parent: u32,
    parent_start: String,
    runtime_pid: Option<u32>,
    runtime_start: Option<String>,
}
pub fn identity(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, tail) = text.rsplit_once(") ")?;
    let fields: Vec<_> = tail.split_whitespace().collect();
    if fields.first() == Some(&"Z") {
        return None;
    }
    fields.get(19).map(|value| value.to_string())
}
pub fn parent_pid() -> Result<u32> {
    let text = fs::read_to_string("/proc/self/status")?;
    text.lines()
        .find_map(|line| {
            line.strip_prefix("PPid:")
                .and_then(|value| value.trim().parse().ok())
        })
        .ok_or(Error("core_unavailable"))
}
pub fn child_guard(args: &[String]) -> Result<()> {
    if args.len() != 5 || args[0] != "--child" || args[1].parse::<u32>().ok() != Some(parent_pid()?)
    {
        return Err(Error("core_unavailable"));
    }
    use std::os::unix::process::CommandExt;
    let _ = std::process::Command::new(&args[2])
        .arg("--config")
        .arg(&args[3])
        .arg("--control-fd")
        .arg(&args[4])
        .exec();
    Err(Error("core_unavailable"))
}
fn tag(directory: &Path) -> Result<String> {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    directory.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}
pub fn recover_runtime(directory: &Path) -> Result<PathBuf> {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("skvoz-{}", crate::settings::uid()?));
    private_dir(&base)?;
    let prefix = tag(directory)?;
    let mut count = 0;
    for entry in fs::read_dir(&base)? {
        let path = entry?.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(&format!("{prefix}-")))
        {
            continue;
        }
        count += 1;
        if count > 32 {
            return Err(Error("unsafe_settings"));
        }
        private_dir(&path)?;
        let record = path.join("owner.json");
        if !record.exists() {
            if fs::read_dir(&path)?.next().is_none() {
                fs::remove_dir(path)?;
                continue;
            }
            return Err(Error("unsafe_settings"));
        }
        let owner: Owner = serde_json::from_slice(&read_private(&record, 1024)?)
            .map_err(|_| Error("unsafe_settings"))?;
        let parent = identity(owner.parent);
        let runtime_pid = owner.runtime_pid.and_then(identity);
        if parent.is_some() && parent.as_ref() == Some(&owner.parent_start)
            || runtime_pid.is_some() && runtime_pid == owner.runtime_start
        {
            return Err(Error("already_running"));
        }
        fs::remove_dir_all(path)?;
    }
    let path = base.join(format!("{prefix}-{}", token()?));
    fs::create_dir(&path)?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    let owner = Owner {
        parent: std::process::id(),
        parent_start: identity(std::process::id()).ok_or(Error("core_unavailable"))?,
        runtime_pid: None,
        runtime_start: None,
    };
    write_private(
        &path.join("owner.json"),
        &serde_json::to_vec(&owner).map_err(|_| Error("core_unavailable"))?,
    )?;
    Ok(path)
}
fn remember_child(directory: &Path, pid: u32) -> Result<()> {
    let mut owner: Owner =
        serde_json::from_slice(&read_private(&directory.join("owner.json"), 1024)?)
            .map_err(|_| Error("unsafe_settings"))?;
    owner.runtime_pid = Some(pid);
    owner.runtime_start = identity(pid);
    write_private(
        &directory.join("owner.json"),
        &serde_json::to_vec(&owner).map_err(|_| Error("unsafe_settings"))?,
    )
}
pub fn bundled_runtime() -> Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let directory = executable.parent().ok_or(Error("core_unavailable"))?;
    Ok(directory.join("skvoz-network-runtime"))
}

#[derive(Clone)]
pub enum Control {
    Connect {
        host: String,
        port: u16,
        username: String,
        password: String,
    },
    Disconnect,
    Info,
    Resume,
    NetworkAvailable,
    #[cfg(feature = "qualification")]
    KillCore,
    Quit,
}
async fn attempt_start(
    engine: &mut Engine,
    input: &Credentials,
    commands: &mut tokio::sync::mpsc::Receiver<Control>,
    status: &std::sync::mpsc::SyncSender<Status>,
) -> (Option<Result<()>>, bool) {
    let mut interim = engine.status(None);
    interim.state = "connecting";
    let startup = engine.start(input);
    tokio::pin!(startup);
    loop {
        tokio::select! {
            result = &mut startup => return (Some(result), false),
            command = commands.recv() => match command {
                Some(Control::Info) => { let mut value = interim.clone(); value.info = true; let _ = status.try_send(value); },
                Some(Control::Resume | Control::NetworkAvailable) => {},
                Some(Control::Quit) | None => return (None, true),
                _ => return (None, false),
            }
        }
    }
}
pub async fn run(
    mut engine: Engine,
    mut commands: tokio::sync::mpsc::Receiver<Control>,
    status: std::sync::mpsc::SyncSender<Status>,
) {
    let mut credentials: Option<Credentials> = None;
    let mut retry = std::time::Instant::now();
    let mut delay = 1;
    let mut timer = tokio::time::interval(Duration::from_millis(500));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let emit = |engine: &Engine, error| {
        let _ = status.try_send(engine.status(error));
    };
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Control::Connect { host, port, username, password }) => {
                    credentials = None;
                    if engine.control.is_some() {engine.explicit_stop("mode_change").await;}
                    if engine.guard_retained || engine.state=="guarded" {emit(&engine,Some("helper_failed"));continue;}
                    match engine.prepare(&host, port, &username, password) {
                        Ok(input) => {
                            engine.state = "connecting";
                            emit(&engine, None);
                            let (attempt, quit) = attempt_start(&mut engine, &input, &mut commands, &status).await;
                            if quit { engine.shutdown().await; engine.state = "stopped"; emit(&engine, None); return; }
                            match attempt {
                                Some(Ok(())) => {
                                    credentials = Some(input);
                                    emit(&engine, None);
                                }
                                Some(Err(error)) => {
                                    engine.abort().await;
                                    if retryable(error.0) {
                                        credentials = Some(input);
                                        engine.state = "reconnecting";
                                        retry = std::time::Instant::now() + Duration::from_secs(1);
                                        delay = 1;
                                    } else { engine.state = if engine.guard_retained {"guarded"} else {"error"}; }
                                    emit(&engine, Some(error.0));
                                }
                                None => {
                                    engine.cleanup().await;
                                    emit(&engine, None);
                                }
                            }
                        }
                        Err(error) => {
                            engine.state = "error";
                            emit(&engine, Some(error.0));
                        }
                    }
                }
                Some(Control::Disconnect) => {
                    credentials = None;
                    engine.cleanup().await;
                    emit(&engine, None);
                }
                Some(Control::Info) => {
                    let mut value = engine.status(None);
                    value.info = true;
                    value.requests = engine.telemetry.history();
                    let _ = status.try_send(value);
                }
                Some(Control::NetworkAvailable) => { if credentials.is_some() && engine.state == "reconnecting" { retry = std::time::Instant::now(); } }
                Some(Control::Resume) => {
                    if credentials.is_some() {
                        engine.abort().await;
                        engine.state = "reconnecting";
                        retry = std::time::Instant::now();
                        delay = 1;
                        emit(&engine, None);
                    }
                }
                #[cfg(feature = "qualification")]
                Some(Control::KillCore) => {
                    if let Some(child) = &mut engine.child { let _ = child.start_kill(); }
                }
                Some(Control::Quit) | None => {
                    engine.shutdown().await;
                    engine.state = "stopped";
                    emit(&engine, None);
                    return;
                }
            },
            _ = timer.tick() => {
                if let Some(input) = &credentials {
                    if engine.state == "connected" && !engine.healthy().await {
                        engine.abort().await;
                        engine.state = "reconnecting";
                        retry = std::time::Instant::now() + Duration::from_secs(1);
                        delay = 1;
                        emit(&engine, None);
                    }
                    if engine.state == "reconnecting" && std::time::Instant::now() >= retry {
                        let (attempt, quit) = attempt_start(&mut engine, input, &mut commands, &status).await;
                        if quit { engine.shutdown().await; engine.state = "stopped"; emit(&engine, None); return; }
                        match attempt {
                            Some(Ok(())) => {
                                delay = 1;
                                emit(&engine, None);
                            }
                            Some(Err(error)) => {
                                engine.abort().await;
                                if !retryable(error.0) { credentials = None; engine.state = if engine.guard_retained {"guarded"} else {"error"}; emit(&engine, Some(error.0)); continue; }
                                engine.state = "reconnecting";
                                delay = (delay * 2).min(15);
                                retry = std::time::Instant::now() + Duration::from_secs(delay);
                                emit(&engine, Some("server_unavailable"));
                            }
                            None => {
                                credentials = None;
                                engine.cleanup().await;
                                emit(&engine, None);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn retryable(code: &str) -> bool {
    matches!(
        code,
        "server_unavailable"
            | "enrollment_failed"
            | "core_unavailable"
            | "io_failed"
            | "io_connection_reset"
            | "io_connection_aborted"
            | "io_broken_pipe"
            | "io_unexpected_eof"
            | "io_timeout"
            | "ipc_failed"
            | "ipc_timeout"
            | "network_unavailable"
            | "network_timeout"
    )
}

pub fn read_ca(path: &Path) -> Result<Vec<u8>> {
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| Error("invalid_ca"))?;
    let metadata = file.metadata().map_err(|_| Error("invalid_ca"))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 65536 {
        return Err(Error("invalid_ca"));
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| Error("invalid_ca"))?;
    if bytes.is_empty() || bytes.len() > 65536 {
        return Err(Error("invalid_ca"));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn accidental_loss_retains_guard_and_explicit_stop_requires_restore_proof() {
        let directory = std::env::temp_dir().join(format!("skvoz-guard-{}", token().unwrap()));
        let settings = Arc::new(Mutex::new(Settings::open(directory.clone()).unwrap()));
        let mut engine = Engine::new(settings.clone(), "/unused".into());
        engine.guard_retained = true;
        engine.ip_handle = Some(SessionId::random().unwrap());
        let original = engine.ip_handle.clone();
        engine.abort().await;
        assert_eq!(engine.state, "guarded");
        assert!(engine.guard_retained);
        assert_eq!(engine.ip_handle, original);
        // With no helper acknowledgment, a manual stop cannot claim restoration.
        engine.cleanup().await;
        assert_eq!(engine.state, "guarded");
        assert!(engine.guard_retained);
        assert_eq!(engine.ip_handle, original);
        engine.guard_retained = false;
        engine.ip_handle = None;
        engine.abort().await;
        assert_eq!(engine.state, "disconnected");
        drop(engine);
        drop(settings);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn invalid_login_does_not_corrupt_persistent_settings() {
        let directory = std::env::temp_dir().join(format!("skvoz-backend-{}", token().unwrap()));
        let settings = Arc::new(Mutex::new(Settings::open(directory.clone()).unwrap()));
        let mut engine = Engine::new(settings.clone(), "/unused".into());
        assert!(
            engine
                .prepare(
                    "localhost",
                    4222,
                    &"a".repeat(65),
                    "long-enough-password".into()
                )
                .is_err()
        );
        drop(engine);
        drop(settings);
        let settings = Settings::open(directory.clone()).unwrap();
        assert_eq!(settings.value.username, "");
        drop(settings);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn rejects_oversized_ca_and_recovers_dead_start_identity() {
        let directory = std::env::temp_dir().join(format!("skvoz-ca-{}", token().unwrap()));
        private_dir(&directory).unwrap();
        let path = directory.join("ca.pem");
        write_private(&path, &vec![0; 65537]).unwrap();
        assert_eq!(read_ca(&path).unwrap_err(), Error("invalid_ca"));
        let fifo = directory.join("ca-fifo");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        assert_eq!(read_ca(&fifo).unwrap_err(), Error("invalid_ca"));
        let settings_path = directory.join("config");
        private_dir(&settings_path).unwrap();
        let runtime = recover_runtime(&settings_path).unwrap();
        let owner = Owner {
            parent: u32::MAX,
            parent_start: "1".into(),
            runtime_pid: Some(u32::MAX),
            runtime_start: None,
        };
        write_private(
            &runtime.join("owner.json"),
            &serde_json::to_vec(&owner).unwrap(),
        )
        .unwrap();
        write_private(&runtime.join("profile.json"), b"transient-secret").unwrap();
        let replacement = recover_runtime(&settings_path).unwrap();
        assert!(!runtime.exists());
        fs::remove_dir_all(replacement).unwrap();
        fs::remove_dir_all(directory).unwrap();
    }
}
