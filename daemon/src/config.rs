//! Strict provisioned profile. Secrets are never formatted into errors or Debug.
use crate::endpoint;
use serde_json::{Map, Value};
use skvoz_core::{
    Config, ManagerConfig, PeerId,
    runtime::{Authentication, Membership, RuntimeConfig, Trust},
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

pub struct Profile {
    pub ipc_path: PathBuf,
    pub runtime: RuntimeConfig,
    pub manager: ManagerConfig,
    pub owners: usize,
    pub streams_per_owner: usize,
    pub output_frames: usize,
    pub output_bytes: usize,
    pub timeout: Duration,
    pub shutdown_timeout: Duration,
}
fn string(m: &mut Map<String, Value>, key: &str) -> Result<String, &'static str> {
    m.remove(key)
        .and_then(|v| v.as_str().map(str::to_owned))
        .filter(|s| !s.is_empty())
        .ok_or("invalid profile")
}
fn list(m: &mut Map<String, Value>, key: &str) -> Result<Vec<PeerId>, &'static str> {
    match m.remove(key) {
        None => Ok(vec![]),
        Some(Value::Array(v)) if v.len() <= 512 => v
            .into_iter()
            .map(|x| x.as_u64().map(PeerId).ok_or("invalid profile"))
            .collect(),
        _ => Err("invalid profile"),
    }
}
fn limit(
    m: &mut Map<String, Value>,
    key: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, &'static str> {
    let n = match m.remove(key) {
        None => default,
        Some(v) => {
            usize::try_from(v.as_u64().ok_or("invalid limit")?).map_err(|_| "invalid limit")?
        }
    };
    if (min..=max).contains(&n) {
        Ok(n)
    } else {
        Err("invalid limit")
    }
}
impl Profile {
    pub fn load(path: &Path, uid: u32) -> Result<Self, &'static str> {
        let bytes = endpoint::secret_file(path, uid, 16384).map_err(|_| "unsafe profile file")?;
        let mut m = serde_json::from_slice::<Value>(&bytes)
            .map_err(|_| "invalid profile")?
            .as_object()
            .cloned()
            .ok_or("invalid profile")?;
        let ipc_path = PathBuf::from(string(&mut m, "ipc_path")?);
        endpoint::validate_endpoint_path(&ipc_path, uid).map_err(|_| "unsafe IPC path")?;
        let url = string(&mut m, "url")?;
        let tls_server_name = match m.remove("tls_server_name") {
            None => None,
            Some(Value::String(name)) => Some(name),
            _ => return Err("invalid TLS identity"),
        };
        let username = string(&mut m, "username")?;
        let password = string(&mut m, "password")?;
        let namespace = string(&mut m, "namespace")?;
        let id = PeerId(
            m.remove("peer_id")
                .and_then(|v| v.as_u64())
                .ok_or("invalid profile")?,
        );
        let trust = match string(&mut m, "trust")?.as_str() {
            "system" => Trust::System,
            "managed_ca" => {
                let path = PathBuf::from(string(&mut m, "ca_file")?);
                endpoint::secret_file(&path, uid, 1024 * 1024).map_err(|_| "unsafe CA file")?;
                Trust::ManagedCa(path)
            }
            _ => return Err("invalid trust profile"),
        };
        let allowed = list(&mut m, "allowed_peers")?;
        let broker_authorized = match m.remove("broker_authorized") {
            None => false,
            Some(Value::Bool(b)) => b,
            _ => return Err("invalid profile"),
        };
        if broker_authorized && !allowed.is_empty() {
            return Err("invalid membership");
        }
        let membership = if broker_authorized {
            Membership::BrokerAuthorized
        } else {
            Membership::Allowlist(BTreeSet::from_iter(allowed))
        };
        let initiate = list(&mut m, "initiate")?;
        let mut l = match m.remove("limits") {
            None => Map::new(),
            Some(Value::Object(l)) => l,
            _ => return Err("invalid limits"),
        };
        if !m.is_empty() {
            return Err("unknown profile field");
        }
        let owners = limit(&mut l, "owners", 16, 1, 64)?;
        let streams_per_owner = limit(&mut l, "streams_per_owner", 64, 1, 1024)?;
        let output_frames = limit(&mut l, "output_frames", 256, 1, 4096)?;
        let output_bytes = limit(&mut l, "output_bytes", 256 * 1024, 143, 8 * 1024 * 1024)?;
        let timeout =
            Duration::from_millis(limit(&mut l, "ipc_timeout_ms", 5000, 50, 60000)? as u64);
        let shutdown_timeout =
            Duration::from_millis(limit(&mut l, "shutdown_timeout_ms", 5000, 100, 60000)? as u64);
        let manager = ManagerConfig {
            stream: Config {
                receive_window: limit(
                    &mut l,
                    "receive_window",
                    8192,
                    1,
                    skvoz_core::MAX_RECEIVE_WINDOW as usize,
                )? as u32,
                max_frame: limit(&mut l, "max_frame", 1024, 1, 32768)? as u32,
                max_pending_frames: limit(&mut l, "pending_frames", 8, 1, 64)?,
                max_metadata: 512,
                open_timeout_ms: limit(&mut l, "open_timeout_ms", 5000, 100, 60000)? as u64,
            },
            max_peers: limit(&mut l, "peers", 128, 1, 512)?,
            max_streams: limit(&mut l, "streams", 1024, 1, 8192)?,
            max_streams_per_peer: limit(&mut l, "streams_per_peer", 128, 1, 8192)?,
            receive_budget: limit(
                &mut l,
                "receive_bytes",
                8 * 1024 * 1024,
                1,
                512 * 1024 * 1024,
            )?,
            receive_budget_per_peer: limit(
                &mut l,
                "receive_bytes_per_peer",
                1024 * 1024,
                1,
                64 * 1024 * 1024,
            )?,
            send_budget: limit(&mut l, "send_bytes", 2 * 1024 * 1024, 1, 64 * 1024 * 1024)?,
            send_budget_per_peer: limit(
                &mut l,
                "send_bytes_per_peer",
                256 * 1024,
                1,
                8 * 1024 * 1024,
            )?,
        };
        skvoz_core::Manager::new(manager).map_err(|_| "invalid manager limits")?;
        let mut runtime = RuntimeConfig::new(
            url,
            trust,
            Authentication { username, password },
            namespace,
            id,
            membership,
        );
        runtime.tls_server_name = tls_server_name;
        runtime.initiate = initiate;
        runtime.shards = limit(&mut l, "shards", 8, 1, 32)?;
        runtime.subscription_capacity = limit(&mut l, "subscription_frames", 128, 1, 65536)?;
        runtime.join_capacity = limit(&mut l, "join_frames", 128, 1, 65536)?;
        runtime.client_capacity = limit(&mut l, "nats_commands", 16, 1, 65536)?;
        if !l.is_empty() {
            return Err("unknown limit field");
        }
        runtime
            .validate_profile(manager)
            .map_err(|_| "invalid runtime profile")?;
        Ok(Self {
            ipc_path,
            runtime,
            manager,
            owners,
            streams_per_owner,
            output_frames,
            output_bytes,
            timeout,
            shutdown_timeout,
        })
    }
}
