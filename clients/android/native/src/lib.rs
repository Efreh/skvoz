//! Bounded control JNI; all traffic stays in the shared Rust network/Core actor.
#![deny(unsafe_op_in_unsafe_fn)]
use jni::{
    Env, EnvUnowned,
    objects::{JObject, JString},
    sys::{jboolean, jint, jlong},
};
use serde::Deserialize;
use skvoz_network::{
    PollError, RuntimeFailure, RuntimeHandle,
    config::{CoreConfig, Limits, NetworkConfig, Role, StartupConfig},
    enrollment,
};
use std::{
    net::{IpAddr, SocketAddr},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Debug)]
struct Failure(&'static str);
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Failure {}
impl From<jni::errors::Error> for Failure {
    fn from(_: jni::errors::Error) -> Self {
        Self("jni_failed")
    }
}
type Result<T> = std::result::Result<T, Failure>;
fn request_failure(error: RuntimeFailure) -> Failure {
    Failure(match error {
        RuntimeFailure::Closed => "runtime_lost",
        RuntimeFailure::InvalidArgument => "invalid_native_request",
        RuntimeFailure::Overloaded => "native_overloaded",
        RuntimeFailure::Internal => "native_internal",
    })
}
fn poll_failure(error: PollError) -> Failure {
    Failure(match error {
        PollError::Closed => "runtime_lost",
        PollError::InsufficientBuffer { .. } => "native_budget_exceeded",
        PollError::Internal => "native_internal",
        PollError::Timeout => "timeout",
    })
}

struct Slot {
    generation: u32,
    runtime: Option<RuntimeHandle>,
    stopping: bool,
}
static ENROLLMENT_LOCK: Mutex<()> = Mutex::new(());
static ENROLLMENT_EPOCH: AtomicU64 = AtomicU64::new(1);
static SLOT: OnceLock<Mutex<Slot>> = OnceLock::new();
fn slot() -> &'static Mutex<Slot> {
    SLOT.get_or_init(|| {
        Mutex::new(Slot {
            generation: 1,
            runtime: None,
            stopping: false,
        })
    })
}
fn with_runtime<T>(handle: jlong, f: impl FnOnce(&mut RuntimeHandle) -> Result<T>) -> Result<T> {
    let mut slot = slot().lock().map_err(|_| Failure("native_internal"))?;
    if handle <= 0 || handle as u64 != u64::from(slot.generation) {
        return Err(Failure("stale_native_handle"));
    }
    f(slot
        .runtime
        .as_mut()
        .ok_or(Failure("stale_native_handle"))?)
}
fn guarded<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(_) => {
            let runtime = {
                let mut slot = slot().lock().unwrap_or_else(|e| e.into_inner());
                slot.generation = slot.generation.saturating_add(1);
                slot.stopping = true;
                slot.runtime.take()
            };
            if let Some(runtime) = runtime {
                let _ = runtime.shutdown();
            }
            slot().lock().unwrap_or_else(|e| e.into_inner()).stopping = false;
            slot().clear_poison();
            ENROLLMENT_LOCK.clear_poison();
            Err(Failure("native_panic"))
        }
    }
}
fn bounded_string(input: &JString, env: &mut Env) -> Result<String> {
    let length = input.as_char_sequence().length(env)?;
    if length <= 0 || length as usize > skvoz_network::local_api::BODY_MAX {
        return Err(Failure("invalid_native_request"));
    }
    let input = input.try_to_string(env)?;
    if input.is_empty() || input.len() > skvoz_network::local_api::BODY_MAX {
        return Err(Failure("invalid_native_request"));
    }
    Ok(input)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Profile {
    host: String,
    port: u16,
    username: String,
    password: String,
    ca_file: String,
    device: String,
}
async fn configuration(profile: Profile) -> Result<StartupConfig> {
    if profile.host.is_empty()
        || profile.host.len() > 253
        || profile.port == 0
        || profile.host.contains(['/', '@', '#', '?', '%'])
    {
        return Err(Failure("invalid_address"));
    }
    if profile.ca_file.is_empty() {
        return Err(Failure("trusted_ca_unavailable"));
    }
    let addresses = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::lookup_host((profile.host.as_str(), profile.port)),
    )
    .await
    .map_err(|_| Failure("server_unavailable"))?
    .map_err(|_| Failure("server_unavailable"))?;
    let mut ips = Vec::new();
    for address in addresses {
        let ip = address.ip();
        if ip.is_unspecified() || ip.is_multicast() || ip.to_canonical() != ip {
            return Err(Failure("server_unavailable"));
        }
        if !ips.contains(&ip) {
            if ips.len() == 32 {
                return Err(Failure("server_unavailable"));
            }
            ips.push(ip)
        }
    }
    let mut credentials = enrollment::Credentials {
        host: profile.host.clone(),
        port: profile.port,
        username: profile.username.clone(),
        password: profile.password.clone(),
        ca_file: profile.ca_file.clone(),
        dial_ip: None,
    };
    let (ip, enrollment) = tokio::time::timeout(Duration::from_secs(20), async {
        for ip in ips {
            credentials.dial_ip = Some(ip);
            match enrollment::enroll(&credentials, &profile.device).await {
                Ok(e) => return Ok((ip, e)),
                Err(enrollment::Error("server_unavailable" | "enrollment_failed")) => {}
                Err(e) => return Err(Failure(e.0)),
            }
        }
        Err(Failure("server_unavailable"))
    })
    .await
    .map_err(|_| Failure("server_unavailable"))??;
    let config = StartupConfig {
        v: 1,
        role: Role::Client,
        core: CoreConfig {
            url: format!("tls://{}", SocketAddr::new(ip, profile.port)),
            tls_server_name: profile
                .host
                .parse::<IpAddr>()
                .is_err()
                .then_some(profile.host),
            trust: "managed_ca".into(),
            ca_file: Some(profile.ca_file.into()),
            username: profile.username,
            password: profile.password,
            namespace: enrollment.namespace,
            peer_id: enrollment.peer_id.to_string(),
            membership: "allowlist".into(),
            allowed_peers: vec!["0".into()],
            initiate: vec!["0".into()],
        },
        network: NetworkConfig {
            families: vec![4, 6],
            max_mtu: 1500,
            channels: 1,
            limits: Limits::canonical(Role::Client),
        },
        server: None,
    };
    config
        .validate()
        .map_err(|_| Failure("invalid_configuration"))?;
    Ok(config)
}
async fn cancellation(token: u64) {
    loop {
        if ENROLLMENT_EPOCH.load(Ordering::Acquire) != token {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_cancellationToken(
    _env: EnvUnowned<'_>,
    _: JObject<'_>,
) -> jlong {
    ENROLLMENT_EPOCH
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            (value < jlong::MAX as u64).then_some(value + 1)
        })
        .map(|old| (old + 1) as jlong)
        .unwrap_or(0)
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_cancelEnrollment(
    _env: EnvUnowned<'_>,
    _: JObject<'_>,
    token: jlong,
) {
    if token > 0 {
        let _ = ENROLLMENT_EPOCH.compare_exchange(
            token as u64,
            token as u64 + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_enroll<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    input: JString<'a>,
    token: jlong,
) -> JString<'a> {
    unowned
        .with_env(|env| -> Result<_> {
            guarded(|| {
                let _operation = ENROLLMENT_LOCK
                    .try_lock()
                    .map_err(|_| Failure("enrollment_already_active"))?;
                if token <= 0 {
                    return Err(Failure("invalid_enrollment_token"));
                }
                let input = bounded_string(&input, env)?;
                let value = skvoz_network::local_api::parse_strict_json(input.as_bytes())
                    .map_err(|_| Failure("invalid_native_request"))?;
                let profile =
                    serde_json::from_value(value).map_err(|_| Failure("invalid_native_request"))?;
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| Failure("native_internal"))?;
                let config = rt.block_on(async {
                    tokio::select! {
                        biased;
                        _ = cancellation(token as u64) => Err(Failure("enrollment_cancelled")),
                        result = configuration(profile) => result,
                    }
                })?;
                let output =
                    serde_json::to_string(&config).map_err(|_| Failure("native_internal"))?;
                Ok(JString::from_str(env, output)?)
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_liveHandles<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
) -> jint {
    unowned
        .with_env(|_| -> Result<jint> {
            guarded(|| {
                Ok(i32::from(
                    slot()
                        .lock()
                        .map_err(|_| Failure("native_internal"))?
                        .runtime
                        .is_some(),
                ))
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_start<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    input: JString<'a>,
) -> jlong {
    unowned
        .with_env(|env| -> Result<_> {
            guarded(|| {
                let input = bounded_string(&input, env)?;
                let config = StartupConfig::parse_json(input.as_bytes())
                    .map_err(|_| Failure("invalid_configuration"))?;
                if config.role != Role::Client {
                    return Err(Failure("invalid_configuration"));
                }
                let mut slot = slot().lock().map_err(|_| Failure("native_internal"))?;
                if slot.runtime.is_some() || slot.stopping || slot.generation == u32::MAX {
                    return Err(Failure("native_already_active"));
                }
                let rt = RuntimeHandle::start(config, None)
                    .map_err(|_| Failure("native_start_failed"))?;
                slot.runtime = Some(rt);
                Ok(jlong::from(slot.generation))
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_request<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    handle: jlong,
    input: JString<'a>,
    fd: jint,
) {
    unowned
        .with_env(|env| -> Result<()> {
            guarded(|| {
                let input = bounded_string(&input, env)?;
                let owned = if fd == -1 {
                    None
                } else if fd >= 0 {
                    Some(
                        skvoz_network_native::duplicate_inherited(fd)
                            .map_err(|_| Failure("invalid_descriptor"))?,
                    )
                } else {
                    return Err(Failure("invalid_descriptor"));
                };
                with_runtime(handle, |rt| {
                    rt.request_json(input.as_bytes(), owned)
                        .map(|_| ())
                        .map_err(request_failure)
                })
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_poll<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    handle: jlong,
) -> JString<'a> {
    unowned
        .with_env(|env| -> Result<_> {
            guarded(|| {
                let message = with_runtime(handle, |rt| {
                    match rt.next_message(
                        skvoz_network::local_api::BODY_MAX,
                        Duration::from_millis(100),
                    ) {
                        Ok(message) => {
                            if message.fd.is_some() {
                                return Err(Failure("unexpected_descriptor"));
                            }
                            Ok(Some(message.json))
                        }
                        Err(PollError::Timeout) => Ok(None),
                        Err(error) => Err(poll_failure(error)),
                    }
                })?;
                match message {
                    Some(bytes) => {
                        let text =
                            String::from_utf8(bytes).map_err(|_| Failure("native_internal"))?;
                        Ok(JString::from_str(env, text)?)
                    }
                    None => Ok(JString::default()),
                }
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_diagnostics<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    handle: jlong,
    enabled: jboolean,
) -> JString<'a> {
    unowned
        .with_env(|env| -> Result<_> {
            guarded(|| {
                let snapshot = with_runtime(handle, |rt| {
                    rt.diagnostics(enabled)
                        .map_err(|_| Failure("diagnostics_unavailable"))
                })?;
                let text = serde_json::to_string(&snapshot)
                    .map_err(|_| Failure("diagnostics_unavailable"))?;
                if text.len() > 4096 {
                    return Err(Failure("diagnostics_unavailable"));
                }
                Ok(JString::from_str(env, text)?)
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_stop<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    handle: jlong,
) {
    unowned
        .with_env(|_| -> Result<()> {
            guarded(|| {
                let runtime = {
                    let mut slot = slot().lock().map_err(|_| Failure("native_internal"))?;
                    if handle <= 0 || handle as u64 != u64::from(slot.generation) {
                        return Err(Failure("stale_native_handle"));
                    }
                    let runtime = slot.runtime.take().ok_or(Failure("stale_native_handle"))?;
                    slot.generation = slot.generation.saturating_add(1);
                    slot.stopping = true;
                    runtime
                };
                let result = runtime
                    .shutdown()
                    .map_err(|_| Failure("native_shutdown_failed"));
                slot()
                    .lock()
                    .map_err(|_| Failure("native_internal"))?
                    .stopping = false;
                result
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_skvoz_android_NativeBridge_tunName<'a>(
    mut unowned: EnvUnowned<'a>,
    _: JObject<'a>,
    fd: jint,
    mtu: jint,
) -> JString<'a> {
    unowned
        .with_env(|env| -> Result<_> {
            guarded(|| {
                let fd = skvoz_network_native::duplicate_inherited(fd)
                    .map_err(|_| Failure("invalid_descriptor"))?;
                let tun = skvoz_network_native::TunDevice::from_owned_fd(
                    fd,
                    mtu.try_into().map_err(|_| Failure("invalid_mtu"))?,
                )
                .map_err(|_| Failure("invalid_tun"))?;
                Ok(JString::from_str(env, tun.name())?)
            })
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_transport_closure_is_recoverable() {
        assert_eq!(request_failure(RuntimeFailure::Closed).0, "runtime_lost");
        assert_eq!(poll_failure(PollError::Closed).0, "runtime_lost");
        assert_eq!(
            request_failure(RuntimeFailure::InvalidArgument).0,
            "invalid_native_request"
        );
        assert_eq!(
            request_failure(RuntimeFailure::Overloaded).0,
            "native_overloaded"
        );
        assert_eq!(
            request_failure(RuntimeFailure::Internal).0,
            "native_internal"
        );
        assert_eq!(poll_failure(PollError::Internal).0, "native_internal");
        assert_eq!(
            poll_failure(PollError::InsufficientBuffer { required: 32769 }).0,
            "native_budget_exceeded"
        );
    }
    #[test]
    fn invalid_handles_and_panic_retire_generation() {
        for h in [-1, 0, 2, jlong::MAX] {
            assert!(with_runtime(h, |_| Ok(())).is_err());
        }
        let old = slot().lock().unwrap().generation;
        assert_eq!(
            guarded::<()>(|| {
                let _enrollment = ENROLLMENT_LOCK.lock().unwrap();
                panic!("test native panic")
            })
            .unwrap_err()
            .0,
            "native_panic"
        );
        assert!(ENROLLMENT_LOCK.try_lock().is_ok());
        let current = slot().lock().unwrap();
        assert!(current.runtime.is_none());
        assert!(!current.stopping);
        assert_ne!(current.generation, old);
    }
    #[test]
    fn descriptor_rejection_keeps_borrowed_owner_alive() {
        use std::{
            io::{Read, Write},
            os::fd::AsRawFd,
            os::unix::net::UnixStream,
        };
        let (mut source, mut peer) = UnixStream::pair().unwrap();
        source.set_nonblocking(true).unwrap();
        let duplicate = skvoz_network_native::duplicate_inherited(source.as_raw_fd()).unwrap();
        assert!(skvoz_network_native::TunDevice::from_owned_fd(duplicate, 1500).is_err());
        source.write_all(b"x").unwrap();
        let mut bytes = [0];
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, *b"x");
        assert!(skvoz_network_native::duplicate_inherited(-1).is_err());
    }
}
