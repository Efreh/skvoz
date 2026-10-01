//! A bounded single-socket example connector. The host establishes one Core
//! stream and owns the NATS node; this connector only owns TCP I/O buffers.
use skvoz_core::{
    CloseReason, Event, SendOutcome, StreamKey,
    nats::{NatsError, NatsNode},
};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream};
#[derive(Debug, Default)]
pub struct RelayStats {
    pub sent: u64,
    pub received: u64,
}
fn error(message: &str) -> NatsError {
    std::io::Error::other(message).into()
}
/// Relay one established stream. The node must be dedicated to this socket;
/// its configured stream max_frame must be >= 1. Application-controlled overall
/// timeout is a demo limit, not a Core idle timeout or production liveness API.
pub async fn relay_one(
    node: &mut NatsNode,
    key: StreamKey,
    socket: TcpStream,
    timeout: Duration,
) -> Result<RelayStats, NatsError> {
    let result = tokio::time::timeout(timeout, relay_established(node, key, socket))
        .await
        .unwrap_or_else(|_| Err(error("TCP relay exceeded its configured deadline")));
    if result.is_err() {
        // The relay future has dropped all socket buffers before cancellation.
        // Give the CANCEL a bounded chance to reach the peer and reclaim this
        // node's terminal frame/event. Preserve the original socket/timeout error.
        let _ = node.close(key);
        let _ = tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                node.poll_events(256);
                if node.snapshot(key).is_none() {
                    break;
                }
                if node.turn(Duration::from_millis(1)).await.is_err() {
                    node.poll_events(256);
                    break;
                }
            }
        })
        .await;
        // A bounded transmit attempt may be cancelled or fail. This dedicated
        // node must still return without retaining local stream reservations.
        if node.snapshot(key).is_some() {
            node.peer_lost(key.peer);
            node.poll_events(256);
        }
    }
    result
}
async fn relay_established(
    node: &mut NatsNode,
    key: StreamKey,
    mut socket: TcpStream,
) -> Result<RelayStats, NatsError> {
    let mut stats = RelayStats::default();
    let mut outgoing = Vec::new();
    let mut out_position = 0;
    let mut incoming: Option<(u64, Box<[u8]>, usize)> = None;
    let mut local_eof = false;
    let mut remote_eof = false;
    loop {
        if node.failure_kind().is_some() {
            node.poll_events(1);
            return Err(error("TCP relay transport lost"));
        }
        if incoming.is_none() {
            for e in node.poll_events(1) {
                if e.key != key {
                    return Err(error("single-socket connector received another stream"));
                }
                match e.event {
                    Event::Data { offset, bytes } => {
                        if offset != stats.received {
                            return Err(error("TCP relay received an unexpected offset"));
                        }
                        incoming = Some((offset, bytes, 0));
                    }
                    Event::RemoteFinished => {
                        if !remote_eof {
                            socket.shutdown().await?;
                            remote_eof = true;
                        }
                    }
                    Event::Closed {
                        reason: CloseReason::Finished,
                    } => {
                        if !local_eof || !remote_eof || !outgoing.is_empty() {
                            return Err(error(
                                "TCP relay closed before both socket directions finished",
                            ));
                        }
                        return Ok(stats);
                    }
                    Event::Closed { .. } => return Err(error("TCP relay stream aborted")),
                    Event::Writable | Event::Opened { .. } => {}
                    Event::IncomingOpen { .. } | Event::Rejected { .. } => {
                        return Err(error("TCP relay stream was not established"));
                    }
                }
            }
        }
        if let Some((offset, bytes, position)) = &mut incoming {
            match socket.try_write(&bytes[*position..]) {
                Ok(0) => return Err(error("TCP socket write returned zero")),
                Ok(n) => {
                    *position += n;
                    stats.received = *offset + *position as u64;
                    if *position == bytes.len() {
                        // Release the entire delivered allocation before ACK
                        // permits the peer to refill the receive window.
                        incoming = None;
                        node.consume_through(key, stats.received)?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.into()),
            }
        }
        if outgoing.is_empty() && !local_eof {
            let mut buffer = [0u8; 1024];
            match socket.try_read(&mut buffer) {
                Ok(0) => {
                    node.finish(key)?;
                    local_eof = true;
                }
                Ok(n) => {
                    outgoing.extend_from_slice(&buffer[..n]);
                    out_position = 0;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.into()),
            }
        }
        if !outgoing.is_empty()
            && let SendOutcome::Accepted(n) = node.send(key, &outgoing[out_position..])?
        {
            out_position += n;
            stats.sent += n as u64;
            if out_position == outgoing.len() {
                outgoing.clear();
                out_position = 0;
            }
        }
        node.turn(Duration::from_millis(1)).await?;
    }
}
