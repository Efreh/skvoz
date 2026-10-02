use crate::{
    DAEMON_VERSION, Error, Result,
    enrollment::{Credentials, enroll},
    ipc::Session,
    proxy::{Budgets, Proxies},
    settings::{Settings, host_name, private_dir, read_private, token, write_private},
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::fs::MetadataExt,
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
}
pub type SharedSettings = Arc<Mutex<Settings>>;
pub struct Engine {
    pub settings: SharedSettings,
    daemon: PathBuf,
    pub child: Option<Child>,
    pub proxies: Option<Proxies>,
    pub runtime: Option<PathBuf>,
    pub peer_id: Option<u64>,
    pub state: &'static str,
    pub budgets: Budgets,
}
impl Engine {
    pub fn new(settings: SharedSettings, daemon: PathBuf, budgets: Budgets) -> Self {
        Self {
            settings,
            daemon,
            child: None,
            proxies: None,
            runtime: None,
            peer_id: None,
            state: "disconnected",
            budgets,
        }
    }
    pub fn status(&self, error: Option<&'static str>) -> Status {
        Status {
            state: self.state,
            error,
            peer_id: self.peer_id,
            pid: self.child.as_ref().and_then(Child::id),
            runtime: self.runtime.clone(),
            info: false,
            connections: self
                .proxies
                .as_ref()
                .map(|proxies| proxies.active.load(Ordering::Relaxed))
                .unwrap_or(0),
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
        preferences.username = user.to_owned();
        preferences.remember_password(&host, port, user, password.clone())?;
        settings.save(preferences)?;
        Ok(Credentials {
            host,
            port,
            username: user.to_owned(),
            password,
            ca_file: settings.value.ca_file.clone(),
        })
    }
    pub async fn start(&mut self, credentials: &Credentials) -> Result<()> {
        self.state = "connecting";
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
            let bytes = read_ca(Path::new(&credentials.ca_file))?;
            let ca = runtime.join("ca.pem");
            write_private(&ca, &bytes)?;
            validated.ca_file = ca.to_string_lossy().into_owned();
        }
        let enrollment = enroll(&validated, &device).await?;
        self.peer_id = Some(enrollment.peer_id);
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            Command::new(&self.daemon)
                .arg("--version")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| Error("version_mismatch"))??;
        if !output.status.success()
            || String::from_utf8_lossy(&output.stdout).trim()
                != format!("skvoz-core-daemon {DAEMON_VERSION} ipc=1")
        {
            return Err(Error("version_mismatch"));
        }
        let path = runtime.join("core.sock");
        let host = if credentials.host.contains(':') {
            format!("[{}]", credentials.host)
        } else {
            credentials.host.clone()
        };
        let mut profile = serde_json::json!({"ipc_path":path,"url":format!("tls://{host}:{}",credentials.port),"username":credentials.username,"password":credentials.password,"trust":if credentials.ca_file.is_empty(){"system"}else{"managed_ca"},"namespace":enrollment.namespace,"peer_id":enrollment.peer_id,"allowed_peers":[0],"initiate":[0],"limits":{"owners":64,"streams_per_owner":1,"streams":64,"streams_per_peer":64,"peers":1,"receive_window":8192,"max_frame":1024,"receive_bytes":524288,"receive_bytes_per_peer":524288,"send_bytes":524288,"send_bytes_per_peer":524288,"output_frames":32,"output_bytes":16384,"subscription_frames":768,"join_frames":128}});
        if !credentials.ca_file.is_empty() {
            profile["ca_file"] = serde_json::json!(validated.ca_file);
        }
        let profile_path = runtime.join("profile.json");
        write_private(
            &profile_path,
            &serde_json::to_vec(&profile).map_err(|_| Error("core_unavailable"))?,
        )?;
        let executable = std::env::current_exe()?;
        let child = Command::new("setpriv")
            .args(["--pdeathsig", "KILL"])
            .arg(executable)
            .arg("--child")
            .arg(std::process::id().to_string())
            .arg(&self.daemon)
            .arg(&profile_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()?;
        let child_id = child.id().ok_or(Error("core_unavailable"))?;
        self.child = Some(child);
        remember_child(&runtime, child_id)?;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if self
                    .child
                    .as_mut()
                    .ok_or(Error("core_unavailable"))?
                    .try_wait()?
                    .is_some()
                {
                    return Err(Error("core_unavailable"));
                }
                if path.exists() {
                    let mut session = Session::connect(&path, 32, 16384).await?;
                    let ready = session.request(11, 0, &0u64.to_be_bytes(), &[0]).await?;
                    if ready.extra == [1] {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<(), Error>(())
        })
        .await
        .map_err(|_| Error("core_unavailable"))??;
        fs::remove_file(profile_path)?;
        self.proxies = Some(
            Proxies::start(
                path,
                preferences.http_port,
                preferences.socks_port,
                self.budgets,
            )
            .await?,
        );
        self.state = "connected";
        Ok(())
    }
    pub async fn healthy(&mut self) -> bool {
        if self
            .child
            .as_mut()
            .and_then(|child| child.try_wait().ok())
            .flatten()
            .is_some()
        {
            return false;
        }
        let Some(runtime) = &self.runtime else {
            return false;
        };
        let operation = async {
            let mut session = Session::connect(&runtime.join("core.sock"), 32, 16384).await?;
            let status = session.request(9, 0, &[], &[0]).await?;
            let ready = session.request(11, 0, &0u64.to_be_bytes(), &[0]).await?;
            Ok::<bool, Error>(
                status.extra.len() == 97 && status.extra[0] == 1 && ready.extra == [1],
            )
        };
        matches!(
            tokio::time::timeout(Duration::from_secs(2), operation).await,
            Ok(Ok(true))
        )
    }
    pub async fn cleanup(&mut self) {
        if let Some(proxies) = self.proxies.take() {
            proxies.close().await;
        }
        if let Some(mut child) = self.child.take()
            && child.try_wait().ok().flatten().is_none()
        {
            if let Some(pid) = child.id() {
                let _ = Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .await;
            }
            if tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .is_err()
            {
                let _ = child.kill().await;
            }
            let _ = child.wait().await;
        }
        if let Some(directory) = self.runtime.take() {
            let _ = fs::remove_dir_all(directory);
        }
        self.peer_id = None;
        self.state = "disconnected";
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(proxies) = &self.proxies {
            proxies.abort();
        }
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    parent: u32,
    parent_start: String,
    daemon: Option<u32>,
    daemon_start: Option<String>,
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
    if args.len() != 4 || args[0] != "--child" || args[1].parse::<u32>().ok() != Some(parent_pid()?)
    {
        return Err(Error("core_unavailable"));
    }
    use std::os::unix::process::CommandExt;
    let _ = std::process::Command::new(&args[2])
        .arg("--config")
        .arg(&args[3])
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
        let daemon = owner.daemon.and_then(identity);
        if parent.is_some() && parent.as_ref() == Some(&owner.parent_start)
            || daemon.is_some() && daemon == owner.daemon_start
        {
            return Err(Error("already_running"));
        }
        let socket = path.join("core.sock");
        if socket.exists() || socket.is_symlink() {
            let meta = fs::symlink_metadata(socket)?;
            use std::os::unix::fs::FileTypeExt;
            if !meta.file_type().is_socket() || meta.uid() != crate::settings::uid()? {
                return Err(Error("unsafe_settings"));
            }
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
        daemon: None,
        daemon_start: None,
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
    owner.daemon = Some(pid);
    owner.daemon_start = identity(pid);
    write_private(
        &directory.join("owner.json"),
        &serde_json::to_vec(&owner).map_err(|_| Error("unsafe_settings"))?,
    )
}
pub fn bundled_daemon() -> Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let directory = executable.parent().ok_or(Error("core_unavailable"))?;
    Ok(directory.join("skvoz-core-daemon"))
}

pub enum Control {
    Connect {
        host: String,
        port: u16,
        username: String,
        password: String,
    },
    Disconnect,
    Info,
    #[cfg(feature = "qualification")]
    KillCore,
    Quit,
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
    let emit = |engine: &Engine, error| {
        let _ = status.try_send(engine.status(error));
    };
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(Control::Connect { host, port, username, password }) => {
                    credentials = None;
                    engine.cleanup().await;
                    match engine.prepare(&host, port, &username, password) {
                        Ok(input) => {
                            engine.state = "connecting";
                            emit(&engine, None);
                            let attempt = tokio::select! {
                                result = engine.start(&input) => Some(result),
                                interrupt = commands.recv() => {
                                    match interrupt {
                                        Some(Control::Quit) | None => {
                                            engine.cleanup().await;
                                            engine.state = "stopped";
                                            emit(&engine, None);
                                            return;
                                        }
                                        _ => None,
                                    }
                                }
                            };
                            match attempt {
                                Some(Ok(())) => {
                                    credentials = Some(input);
                                    emit(&engine, None);
                                }
                                Some(Err(error)) => {
                                    engine.cleanup().await;
                                    engine.state = "error";
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
                    let _ = status.try_send(value);
                }
                #[cfg(feature = "qualification")]
                Some(Control::KillCore) => {
                    if let Some(child) = &mut engine.child { let _ = child.start_kill(); }
                }
                Some(Control::Quit) | None => {
                    engine.cleanup().await;
                    engine.state = "stopped";
                    emit(&engine, None);
                    return;
                }
            },
            _ = timer.tick() => {
                if let Some(input) = &credentials {
                    if engine.state == "connected" && !engine.healthy().await {
                        engine.cleanup().await;
                        engine.state = "reconnecting";
                        retry = std::time::Instant::now() + Duration::from_secs(1);
                        delay = 1;
                        emit(&engine, None);
                    }
                    if engine.state == "reconnecting" && std::time::Instant::now() >= retry {
                        let attempt = tokio::select! {
                            result = engine.start(input) => Some(result),
                            interrupt = commands.recv() => {
                                match interrupt {
                                    Some(Control::Quit) | None => {
                                        engine.cleanup().await;
                                        engine.state = "stopped";
                                        emit(&engine, None);
                                        return;
                                    }
                                    _ => None,
                                }
                            }
                        };
                        match attempt {
                            Some(Ok(())) => {
                                delay = 1;
                                emit(&engine, None);
                            }
                            Some(Err(_)) => {
                                engine.cleanup().await;
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
    #[test]
    fn invalid_login_does_not_corrupt_persistent_settings() {
        let directory = std::env::temp_dir().join(format!("skvoz-backend-{}", token().unwrap()));
        let settings = Arc::new(Mutex::new(Settings::open(directory.clone()).unwrap()));
        let mut engine = Engine::new(settings.clone(), "/unused".into(), Budgets::default());
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
            daemon: Some(u32::MAX),
            daemon_start: None,
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
