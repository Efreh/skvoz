use skvoz_core::{Config, Event, Frame, SendOutcome, Stream};

fn hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "-".into();
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn bytes(value: &str) -> Vec<u8> {
    if value == "-" {
        return Vec::new();
    }
    assert_eq!(value.len() % 2, 0);
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
        .collect()
}

fn frame_text(frame: &Frame) -> String {
    match frame {
        Frame::Open {
            receive_window,
            max_frame,
            metadata,
        } => {
            format!("open({receive_window},{max_frame},{})", hex(metadata))
        }
        Frame::Accept {
            receive_window,
            max_frame,
            metadata,
        } => {
            format!("accept({receive_window},{max_frame},{})", hex(metadata))
        }
        Frame::Reject { reason } => format!("reject({})", hex(reason)),
        Frame::Data { offset, bytes } => format!("data({offset},{})", hex(bytes)),
        Frame::WindowUpdate { consumed } => format!("window({consumed})"),
        Frame::Fin { final_offset } => format!("fin({final_offset})"),
        Frame::Close { reason } => format!("close({reason:?})"),
    }
}

fn event_text(event: &Event) -> String {
    match event {
        Event::IncomingOpen { metadata } => format!("incoming({})", hex(metadata)),
        Event::Opened { metadata } => format!("opened({})", hex(metadata)),
        Event::Rejected { reason } => format!("rejected({})", hex(reason)),
        Event::Data { offset, bytes } => format!("data({offset},{})", hex(bytes)),
        Event::Writable => "writable".into(),
        Event::RemoteFinished => "remote_finished".into(),
        Event::Closed { reason } => format!("closed({reason:?})"),
    }
}

fn list(items: impl Iterator<Item = String>) -> String {
    let result = items.collect::<Vec<_>>().join(",");
    if result.is_empty() {
        "-".into()
    } else {
        result
    }
}

fn index(value: &str) -> usize {
    match value {
        "A" => 0,
        "B" => 1,
        _ => panic!("unknown fixture endpoint: {value}"),
    }
}

fn run(fixture: &str) {
    let config = Config {
        receive_window: 8,
        max_frame: 4,
        max_pending_frames: 2,
        max_metadata: 8,
        open_timeout_ms: 10,
    };
    let mut streams = [Stream::new(config).unwrap(), Stream::new(config).unwrap()];
    for (line_number, line) in fixture.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (command, expected) = line.split_once(" => ").unwrap();
        let args: Vec<_> = command.split_whitespace().collect();
        let i = index(args[1]);
        let actual = match args[0] {
            "OPEN" => {
                streams[i].open(&bytes(args[2]), 0).unwrap();
                "ok".into()
            }
            "ACCEPT" => {
                streams[i].accept(&bytes(args[2])).unwrap();
                "ok".into()
            }
            "REJECT" => {
                streams[i].reject(&bytes(args[2])).unwrap();
                "ok".into()
            }
            "SEND" => match streams[i].send(&bytes(args[2])).unwrap() {
                SendOutcome::Accepted(count) => format!("accepted({count})"),
                SendOutcome::WouldBlock => "would_block".into(),
            },
            "FINISH" => {
                streams[i].finish().unwrap();
                "ok".into()
            }
            "CONSUME" => {
                streams[i]
                    .consume_through(args[2].parse().unwrap())
                    .unwrap();
                "ok".into()
            }
            "TRANSFER" => {
                let j = index(args[2]);
                let frames = streams[i].poll_frames(64);
                for frame in &frames {
                    streams[j].receive(frame, 0).unwrap();
                }
                list(frames.iter().map(frame_text))
            }
            "EVENTS" => list(
                streams[i]
                    .poll_events(args[2].parse().unwrap())
                    .iter()
                    .map(event_text),
            ),
            "STATE" => format!("{:?}", streams[i].state()),
            "TICK" => {
                streams[i].tick(args[2].parse().unwrap()).unwrap();
                "ok".into()
            }
            _ => panic!("unknown fixture command: {command}"),
        };
        assert_eq!(
            actual,
            expected,
            "fixture line {}: {command}",
            line_number + 1
        );
    }
}

#[test]
fn duplex_golden_scenario() {
    run(include_str!("fixtures/duplex.txt"));
}

#[test]
fn rejection_golden_scenario() {
    run(include_str!("fixtures/reject.txt"));
}

#[test]
fn timeout_golden_scenario() {
    run(include_str!("fixtures/timeout.txt"));
}
