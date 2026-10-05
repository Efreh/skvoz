//! Narrow root helper launcher. No shell, caller paths or arbitrary operations.
#![forbid(unsafe_code)]
use skvoz_network::config::{HelperConfig, Role};
use skvoz_network_helper::{
    HelperError, Result, control_loop, kernel::LinuxKernel, service::Service,
};
use skvoz_network_native::{
    IncrementalUnix, SecureStateDir, adopt_inherited, read_root_config, restrict_helper_caps,
    set_nonblocking,
};
use std::{
    os::fd::AsFd,
    path::PathBuf,
    time::{Duration, Instant},
};
fn main() {
    if let Err(error) = run() {
        eprintln!("network helper failed: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args == ["--version"] {
        println!("skvoz-network-helper 0.2.0 api=1 network=2");
        return Ok(());
    }
    if args == ["--help"] {
        println!(
            "skvoz-network-helper --config ROOT_JSON --control-fd FD | --listen-fd FD\nskvoz-network-helper --check-removal --config ROOT_JSON"
        );
        return Ok(());
    }
    if args.len() == 3 && args[0] == "--check-removal" && args[1] == "--config" {
        let config = HelperConfig::parse_json(&read_root_config(&PathBuf::from(&args[2]), 32768)?)
            .map_err(|_| HelperError::InvalidRequest)?;
        if config.role != Role::Client {
            return Err(HelperError::InvalidRequest);
        }
        let uid = config
            .state_dir
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.parse::<u32>().ok())
            .ok_or(HelperError::InvalidRequest)?;
        if uid == 0
            || config.state_dir
                != std::path::Path::new(&format!("/var/lib/skvoz-network-helper/{uid}"))
        {
            return Err(HelperError::InvalidRequest);
        }
        if let Some(state) = SecureStateDir::open_existing(&config.state_dir)? {
            if let Some(journal) = skvoz_network_helper::journal::Journal::load(&state)? {
                if journal.guarded
                    || journal.prepared
                    || journal.operation.is_some()
                    || journal.client.is_some()
                    || !journal.peers.is_empty()
                {
                    return Err(HelperError::InvalidState);
                }
            } else {
                return Err(HelperError::LeaseStoreInvalid);
            }
        }
        return Ok(());
    }
    if args.len() != 4
        || args[0] != "--config"
        || !["--control-fd", "--listen-fd"].contains(&args[2].as_str())
    {
        return Err(HelperError::InvalidRequest);
    }
    let path = PathBuf::from(&args[1]);
    let number = args[3]
        .parse::<i32>()
        .map_err(|_| HelperError::InvalidRequest)?;
    if !(3..=63).contains(&number) {
        return Err(HelperError::InvalidRequest);
    }
    let config = HelperConfig::parse_json(&read_root_config(&path, 32768)?)
        .map_err(|_| HelperError::InvalidRequest)?;
    restrict_helper_caps()?;
    if args[2] == "--control-fd" {
        if config.role != Role::Server {
            return Err(HelperError::InvalidRequest);
        }
        let fd = adopt_inherited(number)?;
        set_nonblocking(fd.as_fd())?;
        let channel = IncrementalUnix::from_owned_fd(fd)?;
        let hello_deadline = Instant::now() + Duration::from_secs(5);
        let store = SecureStateDir::open(&config.state_dir)?;
        let mut service = Service::new(config, store, LinuxKernel::default())?;
        control_loop::serve(&mut service, channel, Role::Server, None, hello_deadline)
    } else {
        if config.role != Role::Client
            || std::env::var("LISTEN_PID").ok() != Some(std::process::id().to_string())
            || std::env::var("LISTEN_FDS").ok().as_deref() != Some("1")
            || number != 3
        {
            return Err(HelperError::InvalidRequest);
        }
        if std::env::var("LISTEN_FDNAMES").is_ok_and(|s| s != "control") {
            return Err(HelperError::InvalidRequest);
        }
        let (listener, uid) =
            skvoz_network_native::activated_client_listener(adopt_inherited(number)?)?;
        // A config cannot direct one UID to another UID's persistent journal.
        if config.state_dir != std::path::Path::new(&format!("/var/lib/skvoz-network-helper/{uid}"))
        {
            return Err(HelperError::Forbidden);
        }
        loop {
            match listener.accept() {
                Ok((socket, _)) => {
                    socket.set_nonblocking(true)?;
                    let channel = IncrementalUnix::from_owned_fd(socket.into())?;
                    let hello_deadline = Instant::now() + Duration::from_secs(5);
                    let store = SecureStateDir::open(&config.state_dir)?;
                    let mut service = Service::new(config.clone(), store, LinuxKernel::default())?;
                    // A failed operation can leave uncertain privileged state;
                    // terminate for supervisor recovery instead of admitting anew.
                    return control_loop::serve(
                        &mut service,
                        channel,
                        Role::Client,
                        Some(uid),
                        hello_deadline,
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    match skvoz_network_native::wait_interest(
                        listener.as_fd(),
                        true,
                        false,
                        Instant::now() + Duration::from_secs(1),
                    ) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
