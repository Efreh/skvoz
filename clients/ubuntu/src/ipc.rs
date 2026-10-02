use crate::{Error, Result};
use std::{collections::VecDeque, path::Path, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};
pub const OPENED: u16 = 0x9002;
pub const REJECTED: u16 = 0x9003;
pub const DATA: u16 = 0x9004;
pub const WRITABLE: u16 = 0x9005;
pub const REMOTE_FINISHED: u16 = 0x9006;
pub const CLOSED: u16 = 0x9007;
#[derive(Debug)]
pub struct Event {
    pub kind: u16,
    pub handle: u128,
    pub payload: Vec<u8>,
}
pub struct Reply {
    pub code: u16,
    pub value: u64,
    pub handle: u128,
    pub extra: Vec<u8>,
}
pub struct Session {
    socket: UnixStream,
    sequence: u64,
    events: VecDeque<Event>,
    bytes: usize,
    count: usize,
    frames: usize,
    byte_limit: usize,
}
impl Session {
    pub async fn connect(path: &Path, frames: usize, bytes: usize) -> Result<Self> {
        let socket = UnixStream::connect(path).await?;
        let mut session = Self {
            socket,
            sequence: 0,
            events: VecDeque::new(),
            bytes: 0,
            count: 0,
            frames,
            byte_limit: bytes,
        };
        let reply = session.request(1, 0, &[0, 1, 0, 1, 0], &[0]).await?;
        if reply.extra.len() != 46 || reply.extra[..2] != [0, 1] {
            return Err(Error("version_mismatch"));
        }
        Ok(session)
    }
    async fn frame(&mut self) -> Result<(u16, u64, u128, Vec<u8>)> {
        read_frame(&mut self.socket).await
    }
    pub async fn request(
        &mut self,
        kind: u16,
        handle: u128,
        payload: &[u8],
        allowed: &[u16],
    ) -> Result<Reply> {
        if payload.len() > 65536 {
            return Err(Error("ipc_failed"));
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error("ipc_failed"))?;
        let operation = async {
            let mut body = Vec::with_capacity(payload.len() + 36);
            body.extend_from_slice(&((32 + payload.len()) as u32).to_be_bytes());
            body.extend_from_slice(b"SKI1");
            body.extend_from_slice(&1u16.to_be_bytes());
            body.extend_from_slice(&kind.to_be_bytes());
            body.extend_from_slice(&self.sequence.to_be_bytes());
            body.extend_from_slice(&handle.to_be_bytes());
            body.extend_from_slice(payload);
            self.socket.write_all(&body).await?;
            loop {
                let (kind, request, handle, mut payload) = self.frame().await?;
                if kind == 0x8000 {
                    if request != self.sequence || payload.len() < 10 {
                        return Err(Error("ipc_failed"));
                    }
                    let code = u16::from_be_bytes(
                        payload[..2].try_into().map_err(|_| Error("ipc_failed"))?,
                    );
                    let value = u64::from_be_bytes(
                        payload[2..10].try_into().map_err(|_| Error("ipc_failed"))?,
                    );
                    if !allowed.contains(&code) {
                        return Err(Error("ipc_failed"));
                    }
                    return Ok(Reply {
                        code,
                        value,
                        handle,
                        extra: payload.split_off(10),
                    });
                }
                self.enqueue(kind, request, handle, payload)?;
            }
        };
        tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .map_err(|_| Error("ipc_failed"))?
    }
    fn enqueue(&mut self, kind: u16, request: u64, handle: u128, payload: Vec<u8>) -> Result<()> {
        if !(OPENED..=CLOSED).contains(&kind)
            || request != 0
            || handle == 0
            || self.count >= self.frames
            || self.bytes + payload.len() > self.byte_limit
        {
            return Err(Error("ipc_overflow"));
        }
        self.count += 1;
        self.bytes += payload.len();
        self.events.push_back(Event {
            kind,
            handle,
            payload,
        });
        Ok(())
    }
    pub async fn event(&mut self) -> Result<Event> {
        if let Some(event) = self.events.pop_front() {
            return Ok(event);
        }
        let (kind, request, handle, payload) = self.frame().await?;
        self.enqueue(kind, request, handle, payload)?;
        self.events.pop_front().ok_or(Error("ipc_failed"))
    }
    pub async fn close(&mut self) {
        let _ = self.socket.shutdown().await;
    }
}

async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<(u16, u64, u128, Vec<u8>)> {
    let size = reader.read_u32().await? as usize;
    if !(32..=65568).contains(&size) {
        return Err(Error("ipc_failed"));
    }
    let mut body = vec![0; size];
    reader.read_exact(&mut body).await?;
    if &body[..4] != b"SKI1" || body[4..6] != [0, 1] {
        return Err(Error("ipc_failed"));
    }
    let kind = u16::from_be_bytes(body[6..8].try_into().map_err(|_| Error("ipc_failed"))?);
    let request = u64::from_be_bytes(body[8..16].try_into().map_err(|_| Error("ipc_failed"))?);
    let handle = u128::from_be_bytes(body[16..32].try_into().map_err(|_| Error("ipc_failed"))?);
    Ok((kind, request, handle, body.split_off(32)))
}

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
type Pending = Arc<Mutex<Option<(u64, oneshot::Sender<Reply>)>>>;
struct Command {
    kind: u16,
    handle: u128,
    payload: Vec<u8>,
    allowed: Vec<u16>,
    reply: oneshot::Sender<Result<Reply>>,
}
#[derive(Clone)]
pub struct Commands(mpsc::Sender<Command>);
pub struct AccountedEvent {
    pub event: Event,
    bytes: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}
impl Drop for AccountedEvent {
    fn drop(&mut self) {
        self.bytes
            .fetch_sub(self.event.payload.len(), Ordering::Relaxed);
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}
pub struct Owner {
    pub commands: Commands,
    pub events: mpsc::Receiver<AccountedEvent>,
    pub failed: watch::Receiver<bool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Commands {
    pub async fn request(
        &self,
        kind: u16,
        handle: u128,
        payload: &[u8],
        allowed: &[u16],
    ) -> Result<Reply> {
        if payload.len() > 1024 {
            return Err(Error("ipc_failed"));
        }
        let (reply, answer) = oneshot::channel();
        let operation = async {
            self.0
                .send(Command {
                    kind,
                    handle,
                    payload: payload.to_vec(),
                    allowed: allowed.to_vec(),
                    reply,
                })
                .await
                .map_err(|_| Error("ipc_failed"))?;
            answer.await.map_err(|_| Error("ipc_failed"))?
        };
        tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .map_err(|_| Error("ipc_failed"))?
    }
}
impl Owner {
    pub async fn connect(path: &Path, frames: usize, byte_limit: usize) -> Result<Self> {
        let session = Session::connect(path, frames, byte_limit).await?;
        let (mut reader, mut writer) = session.socket.into_split();
        let mut sequence = session.sequence;
        let pending: Pending = Arc::new(Mutex::new(None));
        let (tx, mut commands) = mpsc::channel::<Command>(8);
        let (events, receiver) = mpsc::channel(frames);
        let bytes = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let (failure, failed) = watch::channel(false);
        let task = tokio::spawn(async move {
            // The read future owns partial frames for its entire lifetime. It remains
            // polled while the writer awaits replies, so SEND and DATA cannot deadlock.
            let read = async {
                loop {
                    let (kind, request, handle, mut payload) = read_frame(&mut reader).await?;
                    if kind == 0x8000 {
                        if payload.len() < 10 {
                            return Err(Error("ipc_failed"));
                        }
                        let (expected, reply) =
                            pending.lock().await.take().ok_or(Error("ipc_failed"))?;
                        if request != expected {
                            return Err(Error("ipc_failed"));
                        }
                        let code = u16::from_be_bytes(
                            payload[..2].try_into().map_err(|_| Error("ipc_failed"))?,
                        );
                        let value = u64::from_be_bytes(
                            payload[2..10].try_into().map_err(|_| Error("ipc_failed"))?,
                        );
                        reply
                            .send(Reply {
                                code,
                                value,
                                handle,
                                extra: payload.split_off(10),
                            })
                            .map_err(|_| Error("ipc_failed"))?;
                    } else {
                        if !(OPENED..=CLOSED).contains(&kind)
                            || request != 0
                            || handle == 0
                            || count.load(Ordering::Relaxed) >= frames
                            || bytes.load(Ordering::Relaxed) + payload.len() > byte_limit
                        {
                            return Err(Error("ipc_overflow"));
                        }
                        bytes.fetch_add(payload.len(), Ordering::Relaxed);
                        count.fetch_add(1, Ordering::Relaxed);
                        events
                            .try_send(AccountedEvent {
                                event: Event {
                                    kind,
                                    handle,
                                    payload,
                                },
                                bytes: bytes.clone(),
                                count: count.clone(),
                            })
                            .map_err(|_| Error("ipc_overflow"))?;
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            };
            let write = async {
                while let Some(command) = commands.recv().await {
                    if command.reply.is_closed() {
                        continue;
                    }
                    sequence = sequence.checked_add(1).ok_or(Error("ipc_failed"))?;
                    let (reply, answer) = oneshot::channel();
                    *pending.lock().await = Some((sequence, reply));
                    let mut frame = Vec::with_capacity(command.payload.len() + 36);
                    frame.extend_from_slice(&((32 + command.payload.len()) as u32).to_be_bytes());
                    frame.extend_from_slice(b"SKI1");
                    frame.extend_from_slice(&1u16.to_be_bytes());
                    frame.extend_from_slice(&command.kind.to_be_bytes());
                    frame.extend_from_slice(&sequence.to_be_bytes());
                    frame.extend_from_slice(&command.handle.to_be_bytes());
                    frame.extend_from_slice(&command.payload);
                    writer.write_all(&frame).await?;
                    let reply = tokio::time::timeout(Duration::from_secs(5), answer)
                        .await
                        .map_err(|_| Error("ipc_failed"))?
                        .map_err(|_| Error("ipc_failed"))?;
                    if !command.allowed.contains(&reply.code) {
                        return Err(Error("ipc_failed"));
                    }
                    let _ = command.reply.send(Ok(reply));
                }
                Ok::<(), Error>(())
            };
            tokio::select! {
                _ = read => {},
                _ = write => {},
            }
            let _ = failure.send(true);
        });
        Ok(Self {
            commands: Commands(tx),
            events: receiver,
            failed,
            task,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn idle_reader_does_not_block_commands() {
        let path =
            std::env::temp_dir().join(format!("skvoz-ipc-{}", crate::settings::token().unwrap()));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for expected in [1, 5] {
                let (kind, sequence, handle, payload) = read_frame(&mut socket).await.unwrap();
                assert_eq!(kind, expected);
                let mut extra = Vec::new();
                let value = if kind == 1 {
                    extra.resize(46, 0);
                    extra[1] = 1;
                    0u64
                } else {
                    assert_eq!(payload, b"hello");
                    5u64
                };
                let mut frame = Vec::new();
                frame.extend_from_slice(&((42 + extra.len()) as u32).to_be_bytes());
                frame.extend_from_slice(b"SKI1");
                frame.extend_from_slice(&1u16.to_be_bytes());
                frame.extend_from_slice(&0x8000u16.to_be_bytes());
                frame.extend_from_slice(&sequence.to_be_bytes());
                frame.extend_from_slice(&handle.to_be_bytes());
                frame.extend_from_slice(&0u16.to_be_bytes());
                frame.extend_from_slice(&value.to_be_bytes());
                frame.extend_from_slice(&extra);
                socket.write_all(&frame).await.unwrap();
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let owner = Owner::connect(&path, 32, 16384).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let reply = tokio::time::timeout(
            Duration::from_millis(500),
            owner.commands.request(5, 1, b"hello", &[0]),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(reply.value, 5);
        drop(owner);
        server.abort();
        std::fs::remove_file(path).unwrap();
    }
}
