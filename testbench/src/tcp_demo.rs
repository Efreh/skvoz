//! Real TCP request/response through two copies of the same embedded Core.
use crate::{BenchError, mesh};
use skvoz_core::{Event, PeerId};
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
pub async fn run() -> Result<(), BenchError> {
    let options = mesh::LoadOptions {
        clients: 1,
        streams_per_client: 1,
        active_per_client: 1,
        bytes: 0,
    };
    let (mut server, mut clients) = mesh::nodes("tcp", options).await?;
    let mut client = clients.pop().unwrap();
    let local_listener = TcpListener::bind("127.0.0.1:0").await?;
    let target_listener = TcpListener::bind("127.0.0.1:0").await?;
    let requester = TcpStream::connect(local_listener.local_addr()?).await?;
    let (local_socket, _) = local_listener.accept().await?;
    let target_socket = TcpStream::connect(target_listener.local_addr()?).await?;
    let (mut target, _) = target_listener.accept().await?;
    let client_key = client.open(PeerId(0), b"tcp").unwrap();
    let mut server_key = None;
    let mut opened = false;
    let start = Instant::now();
    while !opened {
        assert!(start.elapsed() < Duration::from_secs(3));
        client.turn(Duration::ZERO).await?;
        server.turn(Duration::from_millis(1)).await?;
        for e in server.poll_events(256) {
            if matches!(e.event, Event::IncomingOpen { .. }) {
                server_key = Some(e.key);
                server.accept(e.key, b"tcp")?;
            }
        }
        for e in client.poll_events(256) {
            assert!(matches!(e.event, Event::Opened { .. }));
            opened = true;
        }
    }
    let request: Vec<_> = (0..32768).map(|i| (i * 37) as u8).collect();
    let response: Vec<_> = (0..65536).map(|i| (i * 13 + 127) as u8).collect();
    let mut requester = requester;
    let requester_exchange = async {
        requester.write_all(&request).await?;
        requester.shutdown().await?;
        let mut actual = Vec::new();
        requester.read_to_end(&mut actual).await?;
        assert_eq!(actual, response);
        Ok::<_, BenchError>(())
    };
    let target_exchange = async {
        let mut actual = Vec::new();
        target.read_to_end(&mut actual).await?;
        assert_eq!(actual, request);
        target.write_all(&response).await?;
        target.shutdown().await?;
        Ok::<_, BenchError>(())
    };
    let (a, b, _, _) = tokio::try_join!(
        skvoz_tcp::relay_one(
            &mut client,
            client_key,
            local_socket,
            Duration::from_secs(15)
        ),
        skvoz_tcp::relay_one(
            &mut server,
            server_key.unwrap(),
            target_socket,
            Duration::from_secs(15)
        ),
        requester_exchange,
        target_exchange
    )?;
    assert_eq!(a.sent, request.len() as u64);
    assert_eq!(a.received, response.len() as u64);
    assert_eq!(b.received, request.len() as u64);
    assert_eq!(b.sent, response.len() as u64);
    assert_eq!(client.resources().streams, 0);
    assert_eq!(server.resources().streams, 0);
    println!(
        "PASS real TCP: requester FIN, target response after EOF, request={} response={} bytes, embedded Core on both sides, live slots=0",
        request.len(),
        response.len()
    );
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

/// Real socket write failure or overall deadline must cancel the live Core key.
pub async fn failure(write_error: bool) -> Result<(), BenchError> {
    let options = mesh::LoadOptions {
        clients: 1,
        streams_per_client: 1,
        active_per_client: 0,
        bytes: 0,
    };
    let case = if write_error {
        "tcp_write_error"
    } else {
        "tcp_deadline"
    };
    let (mut server, mut clients) = mesh::nodes(case, options).await?;
    let mut client = clients.pop().unwrap();
    let initial_receive_backing = [
        client.resources().reserved_receive_bytes,
        server.resources().reserved_receive_bytes,
    ];
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let _requester = TcpStream::connect(listener.local_addr()?).await?;
    let (mut socket, _) = listener.accept().await?;
    let key = client.open(PeerId(0), b"")?;
    let start = Instant::now();
    let mut remote = None;
    let mut opened = false;
    while !opened {
        assert!(start.elapsed() < Duration::from_secs(3));
        client.turn(Duration::ZERO).await?;
        server.turn(Duration::from_millis(1)).await?;
        for event in server.poll_events(256) {
            if matches!(event.event, Event::IncomingOpen { .. }) {
                remote = Some(event.key);
                server.accept(event.key, b"")?;
            }
        }
        opened = client
            .poll_events(256)
            .iter()
            .any(|e| matches!(e.event, Event::Opened { .. }));
    }
    if write_error {
        // A real TCP socket with its write direction shut down produces an I/O
        // error when the relay writes DATA received through the broker.
        socket.shutdown().await?;
        assert!(matches!(
            server.send(remote.unwrap(), b"response")?,
            skvoz_core::SendOutcome::Accepted(8)
        ));
    }
    let timeout = if write_error {
        Duration::from_secs(2)
    } else {
        Duration::from_millis(20)
    };
    let started = Instant::now();
    let relay = skvoz_tcp::relay_one(&mut client, key, socket, timeout);
    let remote_cancel = async {
        let start = Instant::now();
        let mut cancelled = false;
        while !cancelled || server.resources().streams != 0 {
            assert!(start.elapsed() < Duration::from_secs(3));
            server.turn(Duration::from_millis(1)).await?;
            for event in server.poll_events(256) {
                match event.event {
                    Event::Closed { reason } => {
                        assert_eq!(reason, skvoz_core::CloseReason::Cancelled);
                        cancelled = true;
                    }
                    Event::Writable => {}
                    other => panic!("unexpected TCP failure event: {other:?}"),
                }
            }
        }
        Ok::<_, BenchError>(())
    };
    let (result, cancellation) = tokio::join!(relay, remote_cancel);
    cancellation?;
    let err = result.unwrap_err();
    if write_error {
        assert_eq!(
            err.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::BrokenPipe
        );
    } else {
        assert_eq!(
            err.to_string(),
            "TCP relay exceeded its configured deadline"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }
    for (node, initial) in [&client, &server].into_iter().zip(initial_receive_backing) {
        let resources = node.resources();
        assert_eq!(resources.streams, 0);
        assert_eq!(resources.reserved_receive_bytes, initial);
        assert_eq!(resources.buffered_receive_bytes, 0);
        assert_eq!(resources.receive_unconsumed_bytes, 0);
        assert_eq!(resources.receive_capacity_bytes, 0);
        assert_eq!(resources.pending_send_bytes, 0);
    }
    println!(
        "PASS real TCP failure: case={case}, original error preserved, remote CANCEL received, stream payload resources released, idle peer credit retained"
    );
    client.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}
