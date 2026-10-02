use skvoz_daemon::{config::Profile, driver, endpoint};
use std::{path::Path, process::ExitCode};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).take(4).collect();
    if args == ["--help"] {
        println!(
            "skvoz-core-daemon --config <private-profile.json>\n  --check-config <private-profile.json>\n  --version\nLinux IPC v1; provisioned TLS NATS profile required."
        );
        return ExitCode::SUCCESS;
    }
    if args == ["--version"] {
        println!("skvoz-core-daemon {} ipc=1", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args.len() != 2 || !["--config", "--check-config"].contains(&args[0].as_str()) {
        eprintln!("invalid arguments; use --help");
        return ExitCode::from(2);
    }
    let uid = match endpoint::effective_uid() {
        Ok(uid) => uid,
        Err(_) => {
            eprintln!("local credential check failed");
            return ExitCode::from(2);
        }
    };
    let profile = match Profile::load(Path::new(&args[1]), uid) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    if args[0] == "--check-config" {
        println!("PROFILE valid ipc=1; broker compatibility requires connection");
        return ExitCode::SUCCESS;
    }
    match driver::run(profile, uid).await {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("{message}");
            ExitCode::from(code)
        }
    }
}
