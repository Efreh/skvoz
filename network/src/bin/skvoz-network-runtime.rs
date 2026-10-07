//! Executable control adapter for the same library-owned actor.
use skvoz_network::{PollError, RuntimeHandle, config::StartupConfig};
use skvoz_network_native::{IncrementalUnix, adopt_inherited, read_private_config};
use std::{
    io,
    os::fd::AsFd,
    path::PathBuf,
    time::{Duration, Instant},
};
fn main() {
    if let Err(error) = run() {
        eprintln!("network runtime failed: {error}");
        std::process::exit(1)
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (mut config, mut control, mut helper) = (None, None, None);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--version" => {
                if config.is_some()
                    || control.is_some()
                    || helper.is_some()
                    || args.next().is_some()
                {
                    return Err("invalid arguments".into());
                }
                println!("skvoz-network-runtime 0.4.2 network=4 api=1 core=4.0.1");
                return Ok(());
            }
            "--help" => {
                println!(
                    "Usage: skvoz-network-runtime --config FILE --control-fd FD [--helper-fd FD]"
                );
                return Ok(());
            }
            "--config" if config.is_none() => {
                config = Some(PathBuf::from(args.next().ok_or("missing config")?))
            }
            "--control-fd" if control.is_none() => {
                control = Some(args.next().ok_or("missing control fd")?.parse::<i32>()?)
            }
            "--helper-fd" if helper.is_none() => {
                helper = Some(args.next().ok_or("missing helper fd")?.parse::<i32>()?)
            }
            _ => return Err("invalid arguments".into()),
        }
    }
    let bytes = read_private_config(&config.ok_or("missing config")?, 32768)?;
    let config = StartupConfig::parse_json(&bytes)?;
    let control = adopt_inherited(control.ok_or("missing control fd")?)?;
    let helper = helper.map(adopt_inherited).transpose()?;
    let mut channel = IncrementalUnix::from_owned_fd(control)?;
    let mut runtime =
        RuntimeHandle::start(config, helper).map_err(|e| format!("startup failed: {e:?}"))?;
    let until = Instant::now() + Duration::from_secs(5);
    let mut hello = false;
    'owner: loop {
        let mut progress = false;
        channel.check_deadlines()?;
        if !hello && Instant::now() >= until {
            return Err("HELLO timeout".into());
        }
        match channel.try_receive_frame() {
            Ok(frame) => {
                progress = true;
                runtime
                    .request_json(&frame.body, frame.fd)
                    .map_err(|e| format!("invalid owner request: {e:?}"))?;
                hello = true;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        // Drain a bounded batch before sleeping. One record per 2ms artificially
        // stalled a reading owner during ordinary TCP opening/closing bursts.
        for _ in 0..32 {
            if !channel.write_pending() {
                match runtime.next_message(32768, Duration::ZERO) {
                    Ok(message) => {
                        channel.queue_frame(message.json, message.fd)?;
                        progress = true;
                    }
                    Err(PollError::Timeout) => break,
                    Err(PollError::Closed) => break 'owner,
                    Err(e) => return Err(format!("owner output failed: {e:?}").into()),
                }
            }
            match channel.try_flush() {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        if skvoz_network_native::owner_closed(channel.as_fd())? {
            break;
        }
        if !progress {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    runtime
        .shutdown()
        .map_err(|e| format!("shutdown failed: {e:?}"))?;
    Ok(())
}
