//! Bounded API1 control owner. Payload never crosses this channel.
use crate::{Error, Result};
use serde_json::{Value, json};
use skvoz_network::local_api::{Event, HelperResponse, Response, parse_strict_json};
use skvoz_network_native::{ControlFrame, IncrementalUnix};
use std::{
    collections::VecDeque,
    os::fd::OwnedFd,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};
struct Command {
    op: String,
    args: Value,
    fd: Option<OwnedFd>,
    reply: oneshot::Sender<Result<ControlFrame>>,
}
struct AccountedEvent {
    event: Event,
    bytes: usize,
    usage: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}
impl Drop for AccountedEvent {
    fn drop(&mut self) {
        self.usage.fetch_sub(self.bytes, Ordering::Relaxed);
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}
pub struct Session {
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<AccountedEvent>,
    failed: watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
    queued: VecDeque<AccountedEvent>,
}
impl Session {
    pub fn new(fd: OwnedFd, helper: bool) -> Result<Self> {
        let mut socket = IncrementalUnix::from_owned_fd(fd)?;
        if helper && socket.peer_credentials()?.uid != 0 {
            return Err(Error("helper_untrusted"));
        }
        let (commands, mut input) = mpsc::channel::<Command>(1);
        let (events, receiver) = mpsc::channel(128);
        let (failure, failed) = watch::channel(false);
        let usage = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(async move {
            let mut id = 0u32;
            let mut seq = 0;
            let mut pending: Option<(
                u32,
                std::time::Instant,
                oneshot::Sender<Result<ControlFrame>>,
            )> = None;
            let mut timer = tokio::time::interval(Duration::from_millis(5));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let run = async {
                loop {
                    tokio::select! {
                        command = input.recv(), if pending.is_none() => {
                            let Some(command) = command else { return Ok::<(),Error>(()); };
                            id = id.checked_add(1).filter(|n| *n <= 2147483647).ok_or(Error("ipc_failed"))?;
                            let body = serde_json::to_vec(&json!({"v":1,"id":id,"op":command.op,"args":command.args,"fd_count":u8::from(command.fd.is_some())})).map_err(|_|Error("ipc_failed"))?;
                            socket.queue_frame(body, command.fd)?;
                            pending = Some((id, std::time::Instant::now()+Duration::from_secs(5), command.reply));
                        },
                        _ = timer.tick() => {},
                    }
                    if pending
                        .as_ref()
                        .is_some_and(|(_, deadline, _)| std::time::Instant::now() >= *deadline)
                    {
                        return Err(Error("ipc_timeout"));
                    }
                    socket.check_deadlines()?;
                    if socket.write_pending() {
                        match socket.try_flush() {
                            Ok(()) => {}
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(e) => return Err(e.into()),
                        }
                    }
                    loop {
                        let frame = match socket.try_receive_frame() {
                            Ok(frame) => frame,
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => return Err(e.into()),
                        };
                        let value =
                            parse_strict_json(&frame.body).map_err(|_| Error("ipc_failed"))?;
                        if value.get("event").is_some() && !helper {
                            let event =
                                Event::parse_json(&frame.body).map_err(|_| Error("ipc_failed"))?;
                            if event.v != 1
                                || !matches!(
                                    event.event.as_str(),
                                    "RUNTIME_STATE"
                                        | "CONFIGURED"
                                        | "ACTIVE"
                                        | "CLOSED"
                                        | "STATS"
                                        | "REQUEST"
                                )
                                || event.seq <= seq
                                || event.seq > i64::MAX as u64
                                || event.fd_count != 0
                                || frame.fd.is_some()
                            {
                                return Err(Error("ipc_failed"));
                            }
                            seq = event.seq;
                            let bytes = frame.body.len();
                            if count.load(Ordering::Relaxed) >= 128
                                || usage.load(Ordering::Relaxed) + bytes > 131072
                            {
                                return Err(Error("ipc_overflow"));
                            }
                            usage.fetch_add(bytes, Ordering::Relaxed);
                            count.fetch_add(1, Ordering::Relaxed);
                            events
                                .try_send(AccountedEvent {
                                    event,
                                    bytes,
                                    usage: usage.clone(),
                                    count: count.clone(),
                                })
                                .map_err(|_| Error("ipc_overflow"))?;
                        } else {
                            let (expected, _, reply) = pending.take().ok_or(Error("ipc_failed"))?;
                            let (v, received, count, valid) = if helper {
                                let r = HelperResponse::parse_json(&frame.body)
                                    .map_err(|_| Error("ipc_failed"))?;
                                (
                                    r.v,
                                    r.id,
                                    r.fd_count,
                                    r.result.is_some() != r.error.is_some(),
                                )
                            } else {
                                let r = Response::parse_json(&frame.body)
                                    .map_err(|_| Error("ipc_failed"))?;
                                (
                                    r.v,
                                    r.id,
                                    r.fd_count,
                                    r.result.is_some() != r.error.is_some(),
                                )
                            };
                            if v != 1
                                || received != expected
                                || count != u8::from(frame.fd.is_some())
                                || !valid
                            {
                                return Err(Error("ipc_failed"));
                            }
                            // A cancelled caller still leaves the actor draining its response.
                            let _ = reply.send(Ok(frame));
                        }
                    }
                }
            };
            let _ = run.await;
            let _ = failure.send(true);
        });
        Ok(Self {
            commands,
            events: receiver,
            failed,
            task,
            queued: VecDeque::new(),
        })
    }
    pub async fn request(
        &self,
        op: &str,
        args: Value,
        fd: Option<OwnedFd>,
    ) -> Result<ControlFrame> {
        let (reply, answer) = oneshot::channel();
        let command = Command {
            op: op.into(),
            args,
            fd,
            reply,
        };
        tokio::time::timeout(Duration::from_secs(6), async {
            self.commands
                .send(command)
                .await
                .map_err(|_| Error("ipc_failed"))?;
            answer.await.map_err(|_| Error("ipc_failed"))?
        })
        .await
        .map_err(|_| Error("ipc_timeout"))?
    }
    pub async fn call(&self, op: &str, args: Value) -> Result<Value> {
        let frame = self.request(op, args, None).await?;
        if frame.fd.is_some() {
            return Err(Error("ipc_failed"));
        }
        let r = Response::parse_json(&frame.body).map_err(|_| Error("ipc_failed"))?;
        let result = r.result.ok_or_else(|| api_error(r.error))?;
        if op == "STATUS" {
            let status: skvoz_network::local_api::RuntimeStatus =
                skvoz_network::local_api::arguments(&result).map_err(|_| Error("ipc_failed"))?;
            status.validate().map_err(|_| Error("ipc_failed"))?;
        }
        Ok(result)
    }
    pub async fn helper_call(&self, op: &str, args: Value) -> Result<ControlFrame> {
        let expected_handle = args.get("handle").cloned();
        let frame = self.request(op, args, None).await?;
        let r = HelperResponse::parse_json(&frame.body).map_err(|_| Error("ipc_failed"))?;
        if r.error.is_some() {
            return Err(Error("helper_failed"));
        }
        if r.fd_count != u8::from(op == "PREPARE_CLIENT") {
            return Err(Error("helper_failed"));
        }
        if op == "HELLO" && r.result != Some(json!({"api":1,"network":5,"role":"client"})) {
            return Err(Error("version_mismatch"));
        }
        if matches!(op, "ACTIVATE_CLIENT" | "ABORT_CLIENT" | "RESTORE_CLIENT")
            && r.result != Some(json!({"handle":expected_handle.ok_or(Error("helper_failed"))?}))
        {
            return Err(Error("helper_failed"));
        }
        Ok(frame)
    }
    pub async fn wait_event(&mut self, name: &str) -> Result<Event> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let event = tokio::time::timeout_at(deadline, self.events.recv())
                .await
                .map_err(|_| Error("network_timeout"))?
                .ok_or(Error("ipc_failed"))?;
            if event.event.event == name {
                return Ok(event.event.clone());
            }
            if event.event.event == "CLOSED" {
                let closed: skvoz_network::local_api::ClosedEvent =
                    skvoz_network::local_api::arguments(&event.event.data)
                        .map_err(|_| Error("ipc_failed"))?;
                return Err(api_error(closed.error));
            }
            if self.queued.len() >= 128 {
                return Err(Error("ipc_overflow"));
            }
            self.queued.push_back(event);
        }
    }
    pub fn drain(&mut self) -> Vec<Event> {
        let mut output: Vec<_> = self.queued.drain(..).map(|e| e.event.clone()).collect();
        while let Ok(event) = self.events.try_recv() {
            output.push(event.event.clone());
        }
        output
    }
    pub fn healthy(&self) -> bool {
        !*self.failed.borrow()
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn api_error(error: Option<skvoz_network::local_api::ApiError>) -> Error {
    use skvoz_network::local_api::ApiError;
    Error(match error {
        Some(ApiError::UnsupportedVersion) => "version_mismatch",
        Some(ApiError::UnsupportedFamily) => "unsupported_family",
        Some(ApiError::InvalidRequest) => "invalid_request",
        Some(ApiError::InvalidState) => "invalid_state",
        Some(ApiError::UnknownHandle) => "unknown_handle",
        Some(ApiError::Forbidden) => "forbidden",
        Some(ApiError::Overloaded) => "overloaded",
        Some(ApiError::LocalSetupFailed) => "local_setup_failed",
        Some(ApiError::Timeout) => "network_timeout",
        _ => "network_unavailable",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use skvoz_network_native::FramedUnix;
    #[tokio::test]
    async fn owner_reads_events_while_waiting_for_correlated_reply() {
        let (local, remote) = std::os::unix::net::UnixStream::pair().unwrap();
        local.set_nonblocking(true).unwrap();
        remote.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let mut channel = FramedUnix::from_owned_fd(remote.into()).unwrap();
            let frame = channel
                .receive_frame_until(std::time::Instant::now() + Duration::from_secs(5))
                .unwrap();
            let request: Value = serde_json::from_slice(&frame.body).unwrap();
            assert_eq!(request["op"], "HELLO");
            channel.send_frame(&serde_json::to_vec(&json!({"v":1,"seq":3,"event":"RUNTIME_STATE","data":{"state":"ready","error":null},"fd_count":0})).unwrap(),None).unwrap();
            channel.send_frame(&serde_json::to_vec(&json!({"v":1,"seq":5,"event":"STATS","data":{"counters":skvoz_network::local_api::Counters::default()},"fd_count":0})).unwrap(),None).unwrap();
            channel.send_frame(&serde_json::to_vec(&json!({"v":1,"id":request["id"],"result":{"api":1,"network":5},"error":null,"fd_count":0})).unwrap(),None).unwrap();
            channel
                .receive_frame_until(std::time::Instant::now() + Duration::from_secs(5))
                .unwrap();
            channel
                .send_frame(
                    &serde_json::to_vec(
                        &json!({"v":1,"id":2,"result":{"lifecycle":"ready","mode":"proxy","session":null,"routing":{"control_ready":true,"eligible_exits":0},"counters":skvoz_network::local_api::Counters::default()},"error":null,"fd_count":0}),
                    )
                    .unwrap(),
                    None,
                )
                .unwrap();
        });
        let mut session = Session::new(local.into(), false).unwrap();
        assert_eq!(
            session
                .call("HELLO", json!({"api":1,"network":5}))
                .await
                .unwrap()["api"],
            1
        );
        assert_eq!(session.wait_event("RUNTIME_STATE").await.unwrap().seq, 3);
        session.call("STATUS", json!({})).await.unwrap();
        let events = session.drain();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "STATS");
        assert_eq!(events[0].seq, 5);
        server.join().unwrap();
    }
    #[tokio::test]
    async fn missing_counter_fields_are_terminal_instead_of_zero_defaults() {
        let (local, remote) = std::os::unix::net::UnixStream::pair().unwrap();
        local.set_nonblocking(true).unwrap();
        remote.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let mut channel = FramedUnix::from_owned_fd(remote.into()).unwrap();
            channel
                .receive_frame_until(std::time::Instant::now() + Duration::from_secs(5))
                .unwrap();
            channel.send_frame(&serde_json::to_vec(&json!({"v":1,"seq":1,"event":"STATS","data":{"counters":{"tcp_open":1}},"fd_count":0})).unwrap(),None).unwrap();
        });
        let session = Session::new(local.into(), false).unwrap();
        assert!(
            session
                .call("HELLO", json!({"api":1,"network":5}))
                .await
                .is_err()
        );
        server.join().unwrap();
        assert!(!session.healthy());
    }
    #[tokio::test]
    async fn wrong_response_id_terminates_owner() {
        let (local, remote) = std::os::unix::net::UnixStream::pair().unwrap();
        local.set_nonblocking(true).unwrap();
        remote.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            let mut channel = FramedUnix::from_owned_fd(remote.into()).unwrap();
            channel
                .receive_frame_until(std::time::Instant::now() + Duration::from_secs(5))
                .unwrap();
            channel
                .send_frame(
                    &serde_json::to_vec(
                        &json!({"v":1,"id":2,"result":{},"error":null,"fd_count":0}),
                    )
                    .unwrap(),
                    None,
                )
                .unwrap();
        });
        let session = Session::new(local.into(), false).unwrap();
        assert!(
            session
                .call("HELLO", json!({"api":1,"network":5}))
                .await
                .is_err()
        );
        server.join().unwrap();
        assert!(!session.healthy());
    }
    #[tokio::test]
    async fn unexpected_received_rights_are_rejected_and_closed() {
        let (local, remote) = std::os::unix::net::UnixStream::pair().unwrap();
        local.set_nonblocking(true).unwrap();
        remote.set_nonblocking(true).unwrap();
        let server = std::thread::spawn(move || {
            use std::os::fd::AsFd;
            let mut channel = FramedUnix::from_owned_fd(remote.into()).unwrap();
            let file = std::fs::File::open("/dev/null").unwrap();
            channel
                .receive_frame_until(std::time::Instant::now() + Duration::from_secs(5))
                .unwrap();
            channel
                .send_frame(
                    &serde_json::to_vec(
                        &json!({"v":1,"id":1,"result":{},"error":null,"fd_count":0}),
                    )
                    .unwrap(),
                    Some(file.as_fd()),
                )
                .unwrap();
        });
        let session = Session::new(local.into(), false).unwrap();
        assert!(
            session
                .call("HELLO", json!({"api":1,"network":5}))
                .await
                .is_err()
        );
        server.join().unwrap();
    }
}
