//! Compileable embedding profile; live setup is provided by the host/provisioner.
use skvoz_core::runtime::{Authentication, Membership, NatsRuntime, RuntimeConfig, Trust};
use skvoz_core::{Config, ManagerConfig, PeerId};
use std::collections::BTreeSet;

pub async fn connect() -> Result<NatsRuntime, Box<dyn std::error::Error>> {
    let auth = Authentication {
        username: std::env::var("NATS_USERNAME")?,
        password: std::env::var("NATS_PASSWORD")?,
    };
    let mut config = RuntimeConfig::new(
        "tls://nats.example.org:4222",
        Trust::System,
        auth,
        "skvoz.application",
        PeerId(1),
        Membership::Allowlist(BTreeSet::from([PeerId(0)])),
    );
    config.initiate = vec![PeerId(0)];
    let limits = ManagerConfig {
        stream: Config {
            max_metadata: 512,
            ..ManagerConfig::default().stream
        },
        ..ManagerConfig::default()
    };
    Ok(NatsRuntime::connect(config, limits).await?)
}
fn main() {
    println!(
        "Use connect() in an embedding Tokio host with its provisioned endpoint, identity and ACL profile."
    );
}
