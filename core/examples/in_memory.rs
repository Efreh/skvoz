use skvoz_core::{CloseReason, Config, Event, SendOutcome, Stream};

fn deliver(from: &mut Stream, to: &mut Stream) -> Result<(), Box<dyn std::error::Error>> {
    for frame in from.poll_frames(64) {
        to.receive(&frame, 0)?;
    }
    Ok(())
}

fn consume(stream: &mut Stream, expected: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let mut received = Vec::new();
    let mut end = 0;
    for event in stream.poll_events(64) {
        match event {
            Event::Data { offset, bytes } => {
                assert_eq!(offset, end);
                end += bytes.len() as u64;
                received.extend_from_slice(&bytes);
            }
            Event::RemoteFinished => println!("Remote sending direction finished"),
            _ => {}
        }
    }
    assert_eq!(received, expected);
    stream.consume_through(end)?;
    println!("Consumed {} bytes: {received:?}", received.len());
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut a = Stream::new(Config::default())?;
    let mut b = Stream::new(Config::default())?;
    a.open(b"example byte stream", 0)?;
    deliver(&mut a, &mut b)?;
    assert!(matches!(b.poll_events(1)[0], Event::IncomingOpen { .. }));
    b.accept(b"accepted")?;
    deliver(&mut b, &mut a)?;
    a.poll_events(1);
    b.poll_events(1);

    let request = [b'h', b'i', 0, 0xff];
    assert_eq!(a.send(&request)?, SendOutcome::Accepted(request.len()));
    a.finish()?;
    deliver(&mut a, &mut b)?;
    consume(&mut b, &request)?;

    let response = b"response after request EOF";
    assert_eq!(b.send(response)?, SendOutcome::Accepted(response.len()));
    b.finish()?;
    deliver(&mut b, &mut a)?;
    consume(&mut a, response)?;
    for stream in [&mut a, &mut b] {
        assert_eq!(
            stream.poll_events(64),
            [Event::Closed {
                reason: CloseReason::Finished,
            }]
        );
    }
    println!("Both streams finished; internal byte queues released");
    Ok(())
}
