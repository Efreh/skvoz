//! One private connected owner, exact API1 messages and bounded shutdown.
use crate::{
    HelperError, Result,
    command::{self, Tool},
    kernel::Kernel,
    registry::Store,
    service::Service,
};
use skvoz_network::{
    config::Role,
    local_api::{HelperOperation, HelperRequest, HelperResponse},
};
use skvoz_network_native::{IncrementalUnix, PeerCredentials};
use std::{
    os::fd::AsFd,
    time::{Duration, Instant},
};

/// Credentials on an inherited socketpair identify its root creator; they do
/// not track later UID changes. A client connection instead identifies the
/// actual app process and is reauthorized independently from JSON arguments.
pub fn serve<S: Store, K: Kernel>(
    service: &mut Service<S, K>,
    mut channel: IncrementalUnix,
    role: Role,
    expected_uid: Option<u32>,
    hello_deadline: Instant,
) -> Result<()> {
    let peer = channel.peer_credentials()?;
    if role == Role::Server && peer.uid != 0 || expected_uid.is_some_and(|uid| peer.uid != uid) {
        return Err(HelperError::Forbidden);
    }
    let mut hello = false;
    let mut stopping = false;
    let result = (|| {
        loop {
            let now = Instant::now();
            if !hello && now >= hello_deadline {
                return Err(HelperError::InvalidState);
            }
            channel.check_deadlines()?;
            channel.try_flush()?;
            if stopping && !channel.write_pending() {
                return Ok(());
            }
            if !channel.write_pending() {
                let frame = match channel.try_receive_frame() {
                    Ok(frame) => Some(frame),
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) =>
                    {
                        None
                    }
                    Err(e) => return Err(e.into()),
                };
                if let Some(frame) = frame {
                    if frame.fd.is_some() {
                        return Err(HelperError::InvalidRequest);
                    }
                    let request = HelperRequest::parse_json(&frame.body)
                        .map_err(|_| HelperError::InvalidRequest)?;
                    if !hello {
                        if request.op != HelperOperation::Hello {
                            return Err(HelperError::InvalidRequest);
                        }
                        if role == Role::Client {
                            authorize(&peer, hello_deadline, channel.as_fd())?;
                        }
                    }
                    let reply = service.handle(&request, Some(channel.as_fd()));
                    let (response, fd) = match reply {
                        Ok(reply) => (
                            HelperResponse::success(request.id, reply.result, reply.fd.is_some()),
                            reply.fd,
                        ),
                        Err(error) => {
                            eprintln!(
                                "helper operation rejected: {:?} ({})",
                                request.op,
                                error_code(&error)
                            );
                            stopping = service.terminal();
                            (
                                HelperResponse::failure(request.id, error_code(&error)),
                                None,
                            )
                        }
                    };
                    if request.op == HelperOperation::Hello && response.error.is_none() {
                        hello = true;
                    }
                    if request.op == HelperOperation::StopServer && response.error.is_none() {
                        stopping = true;
                    }
                    let body =
                        serde_json::to_vec(&response).map_err(|_| HelperError::InvalidState)?;
                    channel.queue_frame(body, fd)?;
                }
            }
            let deadline = channel
                .next_deadline()
                .unwrap_or(now + Duration::from_millis(100))
                .min(if hello {
                    now + Duration::from_millis(100)
                } else {
                    hello_deadline
                });
            match skvoz_network_native::wait_interest(
                channel.as_fd(),
                !channel.write_pending(),
                channel.write_pending(),
                deadline,
            ) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => return Err(e.into()),
            }
        }
    })();
    // Fatal parsing, extra FD, deadline or owner EOF revokes live access. Cleanup
    // failures are surfaced; no successful acknowledgment is invented.
    let cleanup = service.owner_eof();
    match (result, cleanup) {
        (_, Err(e)) => Err(e),
        (Err(HelperError::Io(e)), Ok(())) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Ok(())
        }
        (value, Ok(())) => value,
    }
}
fn error_code(error: &HelperError) -> &'static str {
    match error {
        HelperError::InvalidRequest => "invalid_request",
        HelperError::Forbidden => "forbidden",
        HelperError::InvalidState => "invalid_state",
        HelperError::LeaseStoreInvalid => "lease_store_invalid",
        HelperError::Overloaded => "overloaded",
        HelperError::Io(_) => "helper_unavailable",
    }
}
fn authorize(
    peer: &PeerCredentials,
    deadline: Instant,
    owner: std::os::fd::BorrowedFd<'_>,
) -> Result<()> {
    if peer.pid <= 0 || peer.uid == 0 {
        return Err(HelperError::Forbidden);
    }
    let subject = process_subject(peer.pid, peer.uid)?;
    let output = command::run(
        Tool::Pkcheck,
        &[
            "--action-id".into(),
            "org.skvoz.network.manage".into(),
            "--process".into(),
            subject.clone(),
        ],
        &[],
        Some(owner),
        deadline,
    )?;
    if !output.success || process_subject(peer.pid, peer.uid)? != subject {
        return Err(HelperError::Forbidden);
    }
    Ok(())
}
fn process_subject(pid: i32, uid: u32) -> Result<String> {
    use std::io::Read;
    let mut stat = String::new();
    std::fs::File::open(format!("/proc/{pid}/stat"))?
        .take(4097)
        .read_to_string(&mut stat)?;
    if stat.len() > 4096 {
        return Err(HelperError::Forbidden);
    }
    let (_, tail) = stat.rsplit_once(')').ok_or(HelperError::Forbidden)?;
    let ticks = tail
        .split_whitespace()
        .nth(19)
        .ok_or(HelperError::Forbidden)?
        .parse::<u64>()
        .map_err(|_| HelperError::Forbidden)?;
    if ticks == 0 {
        return Err(HelperError::Forbidden);
    }
    Ok(format!("{pid},{ticks},{uid}"))
}
