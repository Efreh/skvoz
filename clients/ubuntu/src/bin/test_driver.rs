//! Test-only independent backend process; never installed in the deb package.
use skvoz_ubuntu_client::{
    backend::{Control, Engine, child_guard, run},
    proxy::Budgets,
    settings::Settings,
};
use std::{
    io::{self, BufRead, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};
fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|arg| arg == "--child") {
        let _ = child_guard(&args);
        std::process::exit(1);
    }
    let stdin = io::stdin();
    let mut first = String::new();
    stdin.read_line(&mut first).unwrap();
    let config: serde_json::Value = serde_json::from_str(&first).unwrap();
    let directory = PathBuf::from(config["directory"].as_str().unwrap());
    let mut settings = Settings::open(directory).unwrap();
    let mut preferences = settings.value.clone();
    preferences.http_port = config["http_port"].as_u64().unwrap() as u16;
    preferences.socks_port = config["socks_port"].as_u64().unwrap() as u16;
    preferences.ca_file = config["ca_file"].as_str().unwrap_or("").to_owned();
    settings.save(preferences).unwrap();
    let budgets = Budgets {
        connections: config["budgets"]["max_connections"].as_u64().unwrap_or(62) as usize,
        frames: config["budgets"]["event_frames"].as_u64().unwrap_or(32) as usize,
        bytes: config["budgets"]["event_bytes"].as_u64().unwrap_or(16384) as usize,
    };
    let engine = Engine::new(
        Arc::new(Mutex::new(settings)),
        PathBuf::from(config["daemon"].as_str().unwrap()),
        budgets,
    );
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (status_tx, status_rx) =
        std::sync::mpsc::sync_channel::<skvoz_ubuntu_client::backend::Status>(32);
    let control = tx.clone();
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            let Ok(command) = serde_json::from_str::<serde_json::Value>(&line) else {
                break;
            };
            let input = match command["command"].as_str() {
                Some("quit") => Control::Quit,
                Some("info") => Control::Info,
                Some("kill-core") => Control::KillCore,
                _ => continue,
            };
            if control.blocking_send(input).is_err() {
                return;
            }
        }
        let _ = control.blocking_send(Control::Quit);
    });
    let host = config["host"].as_str().unwrap().to_owned();
    let port = config["port"].as_u64().unwrap() as u16;
    let user = config["username"].as_str().unwrap().to_owned();
    let password = config["password"].as_str().unwrap().to_owned();
    tx.blocking_send(Control::Connect {
        host,
        port,
        username: user,
        password,
    })
    .unwrap();
    drop(tx);
    std::thread::spawn(move || {
        let mut first_ready = true;
        while let Ok(status) = status_rx.recv() {
            println!("{}", serde_json::to_string(&status).unwrap());
            if status.state == "connected" && first_ready {
                println!(
                    "{}",
                    serde_json::json!({"ready":true,"peer_id":status.peer_id,"pid":status.pid,"runtime":status.runtime})
                );
                first_ready = false;
            }
            if status.state == "error" {
                println!("{}", serde_json::json!({"failed":status.error}));
            }
            if status.info {
                println!(
                    "{}",
                    serde_json::json!({"info":true,"peer_id":status.peer_id,"pid":status.pid,"runtime":status.runtime,"connections":status.connections})
                );
            }
            io::stdout().flush().unwrap();
        }
    });
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(run(engine, rx, status_tx));
}
