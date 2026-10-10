#![cfg(feature = "real-nats")]
//! Safe orchestration of an actual C ABI consumer and isolated private target.
use skvoz_network::config::{
    AuthorityConfig, CoreConfig, Limits, Role, ServerConfig, StartupConfig,
};
use skvoz_network::local_api::parse_strict_json;
use skvoz_network::routing::Registry;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn docker(arguments: &[&str]) -> Output {
    let output = Command::new("docker").args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "Docker fixture operation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn capture(arguments: &[&str]) -> String {
    String::from_utf8(docker(arguments).stdout)
        .unwrap()
        .trim()
        .to_owned()
}

struct Target {
    container: String,
    network: String,
}
impl Drop for Target {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.container])
            .output();
        let _ = Command::new("docker")
            .args(["network", "rm", &self.network])
            .output();
    }
}

fn configured(role: Role, target: &str, directory: &Path) -> StartupConfig {
    let mut config = StartupConfig::parse_json(include_bytes!(
        "../../network/tests/fixtures/client-startup.json"
    ))
    .unwrap();
    let credential = if role == Role::Server { 0 } else { 1 };
    let node_id = std::env::var("SKVOZ_NATS_FFI_NODE_ID")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let id = if role == Role::Server { node_id } else { 1 };
    config.role = role;
    config.core = CoreConfig {
        url: std::env::var("SKVOZ_NATS_URL").unwrap(),
        tls_server_name: None,
        trust: "managed_ca".into(),
        ca_file: Some(std::env::var("SKVOZ_NATS_CA").unwrap().into()),
        username: format!("ffi-p{credential}"),
        password: std::env::var(format!("SKVOZ_NATS_FFI_P{credential}_PASSWORD")).unwrap(),
        namespace: format!(
            "skvoz.ffi.{}",
            std::env::var("SKVOZ_NATS_RUN_TOKEN").unwrap()
        ),
        peer_id: id.to_string(),
        membership: "allowlist".into(),
        allowed_peers: vec![],
        initiate: vec![],
    };
    config.network.limits = Limits::canonical(role);
    if role == Role::Server {
        let mut authority = config.core.clone();
        authority.peer_id = "0".into();
        authority.username = "ffi-authority".into();
        authority.password = std::env::var("SKVOZ_NATS_FFI_AUTHORITY_PASSWORD").unwrap();
        config.routing.egress = true;
        config.routing.authority = Some(AuthorityConfig {
            core: authority,
            registry: Registry {
                revision: 1,
                devices: vec![1],
                nodes: vec![node_id],
            },
        });
        config.network.families.clear();
        let value = parse_strict_json(format!(
            "{{\"ipv4\":null,\"ipv6\":null,\"dns_servers\":[],\"allow\":[{{\"cidr\":\"{target}/32\",\"protocols\":[6],\"ports\":[4444]}}],\"deny\":[],\"service_prefixes\":[],\"lease_store\":\"{}\",\"server_addresses\":[],\"management_endpoints\":[]}}",
            directory.join("leases.json").display()).as_bytes()).unwrap();
        config.server = Some(serde_json::from_value::<ServerConfig>(value).unwrap());
    }
    config.validate().unwrap();
    config
}

fn private_json(path: &Path, config: &StartupConfig) {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(&serde_json::to_vec(config).unwrap())
        .unwrap();
}

fn shared_library() -> PathBuf {
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    for directory in [&deps, deps.parent().unwrap()] {
        if directory.join("libskvoz_network_ffi.so").is_file() {
            return directory.to_owned();
        }
    }
    panic!("FFI cdylib missing; build skvoz-network-ffi in the current Cargo profile");
}

#[test]
fn actual_c_abi_binary_tcp_and_fd_ownership_over_verified_tls_nats() {
    let token = std::env::var("SKVOZ_NATS_RUN_TOKEN").unwrap();
    let target = Target {
        container: format!("skvoz-ffi-{token}-target"),
        network: format!("skvoz-ffi-{token}-private"),
    };
    docker(&["network", "create", "--internal", &target.network]);
    let broker = std::env::var("SKVOZ_NATS_CONTAINER").unwrap();
    let image = capture(&["inspect", "--format", "{{.Image}}", &broker]);
    docker(&[
        "run",
        "-d",
        "--name",
        &target.container,
        "--network",
        &target.network,
        "--pull=never",
        "--cap-drop=ALL",
        "--security-opt",
        "no-new-privileges",
        "--read-only",
        "--memory=128m",
        "--cpus=1",
        "--pids-limit=32",
        "--entrypoint",
        "/bin/sh",
        &image,
        "-c",
        "while nc -l -p 4444 -e /bin/cat; do printf 'TARGET_EOF\\n'; done",
    ]);
    let address = capture(&[
        "inspect",
        "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        &target.container,
    ]);
    let _: std::net::Ipv4Addr = address.parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // BusyBox may bind the dual-stack wildcard through an IPv6 socket.
        let listening = capture(&[
            "exec",
            &target.container,
            "cat",
            "/proc/net/tcp",
            "/proc/net/tcp6",
        ]);
        if listening
            .lines()
            .any(|line| line.contains(":115C") && line.contains(" 0A "))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "isolated target did not start listening"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let directory =
        PathBuf::from(std::env::var("SKVOZ_NATS_FIXTURE_DIR").unwrap()).join("network-ffi");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let server = directory.join("server.json");
    let client = directory.join("client.json");
    private_json(&server, &configured(Role::Server, &address, &directory));
    private_json(&client, &configured(Role::Client, &address, &directory));
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let library = shared_library();
    let consumer = directory.join("consumer");
    let compiled = Command::new("cc")
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror"])
        .arg(format!("-I{}", root.join("network/ffi/include").display()))
        .arg(root.join("network/ffi/tests/real_nats.c"))
        .arg(format!("-L{}", library.display()))
        .arg("-lskvoz_network_ffi")
        .arg(format!("-Wl,-rpath,{}", library.display()))
        .arg("-o")
        .arg(&consumer)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "C ABI consumer failed to compile: {}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let result = Command::new(&consumer)
        .arg(&server)
        .arg(&client)
        .arg(&address)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "C ABI consumer failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("FFI REAL PASS"));
    println!("{}", String::from_utf8_lossy(&result.stdout).trim());
}
