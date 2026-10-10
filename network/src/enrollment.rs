//! Shared enrollment3 transport and strict current-component negotiation.
use futures_util::StreamExt;
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};
#[derive(serde::Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Enrollment {
    pub v: u8,
    pub namespace: String,
    pub peer_id: u64,
    pub network_runtime: RuntimeVersion,
}
#[derive(serde::Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RuntimeVersion {
    pub version: String,
    pub api: u8,
    pub network: u8,
    pub core: String,
}
#[derive(Clone)]
pub struct Credentials {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub ca_file: String,
    pub dial_ip: Option<std::net::IpAddr>,
}
impl Drop for Credentials {
    fn drop(&mut self) {
        self.password.clear();
    }
}
pub async fn enroll(credentials: &Credentials, device: &str) -> Result<Enrollment, Error> {
    if !login(&credentials.username) {
        return Err(Error("invalid_login"));
    }
    if !(12..=72).contains(&credentials.password.len()) {
        return Err(Error("invalid_password"));
    }
    if !valid_token(device) {
        return Err(Error("enrollment_failed"));
    }
    let operation = async {
        let mut options = async_nats::ConnectOptions::with_user_and_password(
            credentials.username.clone(),
            credentials.password.clone(),
        )
        .require_tls(true)
        .connection_timeout(Duration::from_secs(5))
        .max_reconnects(0)
        .ignore_discovered_servers()
        .client_capacity(8)
        .subscription_capacity(8)
        .raw_message_limit(512)
        .custom_inbox_prefix(format!("skvoz.enroll.reply.{}", credentials.username));
        if !credentials.ca_file.is_empty() {
            options = options.add_root_certificates(PathBuf::from(&credentials.ca_file));
        }
        if credentials.dial_ip.is_some() {
            let trust = if credentials.ca_file.is_empty() {
                skvoz_core::runtime::Trust::System
            } else {
                skvoz_core::runtime::Trust::ManagedCa(PathBuf::from(&credentials.ca_file))
            };
            options = options.tls_client_config(
                skvoz_core::runtime::verified_tls_config(&trust, &credentials.host)
                    .map_err(|_| Error("certificate_failed"))?,
            );
        }
        let dial_host = credentials
            .dial_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| credentials.host.clone());
        let host = if dial_host.contains(':') {
            format!("[{}]", dial_host)
        } else {
            dial_host
        };
        let client = options
            .connect(format!("tls://{host}:{}", credentials.port))
            .await
            .map_err(|error| {
                let mut text = error.to_string().to_ascii_lowercase();
                use std::error::Error as _;
                let mut source = error.source();
                while let Some(error) = source {
                    text.push_str(&error.to_string().to_ascii_lowercase());
                    source = error.source();
                }
                #[cfg(feature = "enrollment-diagnostics")]
                {
                    let details = text
                        .replace(&credentials.password.to_ascii_lowercase(), "[redacted]")
                        .replace(&credentials.username.to_ascii_lowercase(), "[redacted]");
                    eprintln!(
                        "Enrollment connection failed: address={host}:{}, kind={:?}, details={details}",
                        credentials.port,
                        error.kind()
                    );
                }
                if text.contains("authorization") || text.contains("authentication") {
                    Error("authentication_failed")
                } else if text.contains("certificate")
                    || text.contains("unknownissuer")
                    || text.contains("invalid peer")
                {
                    Error("certificate_failed")
                } else {
                    Error("server_unavailable")
                }
            })?;
        #[cfg(feature = "enrollment-diagnostics")]
        eprintln!(
            "Enrollment transport connected: address={host}:{}",
            credentials.port
        );
        let reply = format!(
            "skvoz.enroll.reply.{}.{}",
            credentials.username,
            token().map_err(|_| Error("random_failed"))?
        );
        let mut subscription = client
            .subscribe(reply.clone())
            .await
            .map_err(|_| Error("enrollment_failed"))?;
        client
            .flush()
            .await
            .map_err(|_| Error("enrollment_failed"))?;
        let body = serde_json::to_vec(&serde_json::json!({"v":3,"device":device}))
            .map_err(|_| Error("enrollment_failed"))?;
        client
            .publish_with_reply(
                format!("skvoz.enroll.v3.{}", credentials.username),
                reply,
                body.into(),
            )
            .await
            .map_err(|_| Error("enrollment_failed"))?;
        let message = subscription
            .next()
            .await
            .ok_or(Error("enrollment_failed"))?;
        #[cfg(feature = "enrollment-diagnostics")]
        eprintln!("Enrollment reply received: bytes={}", message.payload.len());
        if message.payload.len() > 512 {
            return Err(Error("enrollment_failed"));
        }
        let value: serde_json::Value =
            serde_json::from_slice(&message.payload).map_err(|_| Error("enrollment_failed"))?;
        if let Some(error) = value.get("error") {
            return Err(Error(if error == "device_limit" {
                "device_limit"
            } else {
                "enrollment_failed"
            }));
        }
        let result: Enrollment =
            serde_json::from_value(value).map_err(|_| Error("version_mismatch"))?;
        if result.v != 3
            || result.network_runtime.version != env!("CARGO_PKG_VERSION")
            || result.network_runtime.api != 1
            || result.network_runtime.network != crate::NETWORK_VERSION
            || result.network_runtime.core != "4.1.0"
        {
            return Err(Error("version_mismatch"));
        }
        if !crate::routing::device_id(result.peer_id)
            || result.namespace.len() > 256
            || result.namespace.split('.').any(|part| {
                part.is_empty()
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            })
        {
            return Err(Error("enrollment_failed"));
        }
        let _ = client.drain().await;
        Ok(result)
    };
    tokio::time::timeout(Duration::from_secs(20), operation)
        .await
        .map_err(|_| {
            #[cfg(feature = "enrollment-diagnostics")]
            eprintln!("Enrollment deadline exceeded");
            Error("server_unavailable")
        })?
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
pub fn login(input: &str) -> bool {
    !input.is_empty()
        && input.len() <= 64
        && input != "__skvoz_server"
        && input
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
pub fn valid_token(input: &str) -> bool {
    input.len() == 32
        && input
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn token() -> Result<String, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
