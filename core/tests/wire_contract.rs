use skvoz_core::{
    CloseReason, Frame,
    wire::{self, WireError},
};

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn language_neutral_vectors_match_all_frames() {
    for line in include_str!("fixtures/wire-v2.tsv").lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once('\t').unwrap();
        let frame = match name {
            "open" => Frame::Open {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([0, 255]),
            },
            "accept" => Frame::Accept {
                receive_window: 8,
                max_frame: 4,
                metadata: Box::new([0, 255]),
            },
            "reject" => Frame::Reject {
                reason: Box::new([0, 255]),
            },
            "data" => Frame::Data {
                offset: 0,
                bytes: Box::new([0, 255]),
            },
            "window" => Frame::WindowUpdate { consumed: 2 },
            "fin" => Frame::Fin { final_offset: 2 },
            "close" => Frame::Close {
                reason: CloseReason::Cancelled,
            },
            "window_grant" => Frame::WindowGrant {
                consumed: 2,
                limit: 16,
                probe: 9,
            },
            "peer_grant" => Frame::PeerGrant {
                epoch: 1,
                consumed_bytes: 2,
                limit_bytes: 65536,
                consumed_records: 1,
                limit_records: 64,
                probe: 7,
            },
            "peer_request" => Frame::PeerRequest {
                bytes: 1048576,
                records: 128,
                probe: 9,
                requester_stream_id: 2,
                blocked: 3,
            },
            "peer_freeze" => Frame::PeerFreeze { epoch: 1 },
            "peer_frozen" => Frame::PeerFrozen {
                epoch: 1,
                bytes: 1024,
                records: 1,
            },
            _ => panic!("unknown vector"),
        };
        let stream_id = if name.starts_with("peer_") { 0 } else { 2 };
        let bytes = unhex(value);
        assert_eq!(wire::encode(stream_id, &frame).unwrap(), bytes, "{name}");
        assert_eq!(
            wire::decode(&bytes).unwrap(),
            wire::Packet { stream_id, frame },
            "{name}"
        );
    }
}

#[test]
fn malformed_lengths_headers_and_versions_are_rejected() {
    let valid = wire::encode(
        2,
        &Frame::Data {
            offset: 0,
            bytes: Box::new([0, 255]),
        },
    )
    .unwrap();
    for prefix in 0..valid.len() {
        assert!(wire::decode(&valid[..prefix]).is_err());
    }
    for (position, value, error) in [
        (0, 0, WireError::InvalidHeader),
        (4, 1, WireError::UnsupportedVersion),
        (5, 255, WireError::UnknownKind),
        (6, 1, WireError::InvalidHeader),
    ] {
        let mut bytes = valid.clone();
        bytes[position] = value;
        assert_eq!(wire::decode(&bytes), Err(error));
    }
    let mut oversized = valid.clone();
    oversized[24..28].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(wire::decode(&oversized), Err(WireError::TooLarge));
    let mut trailing = valid;
    trailing.push(0);
    assert_eq!(wire::decode(&trailing), Err(WireError::InvalidLength));
    assert_eq!(
        wire::decode(&vec![0; wire::MAX_PACKET_BYTES + 1]),
        Err(WireError::TooLarge)
    );
}

#[test]
fn bounded_decoder_mutation_smoke_test() {
    let frames = [
        Frame::Open {
            receive_window: 8,
            max_frame: 4,
            metadata: Box::new([0, 255]),
        },
        Frame::Data {
            offset: 0,
            bytes: Box::new([0, 255]),
        },
        Frame::Fin { final_offset: 2 },
    ];
    for frame in frames {
        let bytes = wire::encode(2, &frame).unwrap();
        for i in 0..bytes.len() {
            for value in [0, 1, 127, 255] {
                let mut mutated = bytes.clone();
                mutated[i] = value;
                if let Ok(packet) = wire::decode(&mutated) {
                    assert_eq!(
                        wire::encode(packet.stream_id, &packet.frame).unwrap(),
                        mutated
                    );
                }
            }
        }
    }
}

#[test]
fn aggregate_controls_have_a_separate_zero_stream_namespace() {
    let controls = [
        Frame::PeerGrant {
            epoch: 0,
            consumed_bytes: 0,
            limit_bytes: 65536,
            consumed_records: 0,
            limit_records: 64,
            probe: 7,
        },
        Frame::PeerRequest {
            bytes: 1048576,
            records: 128,
            probe: 9,
            requester_stream_id: 2,
            blocked: 3,
        },
        Frame::PeerFreeze { epoch: 1 },
        Frame::PeerFrozen {
            epoch: 1,
            bytes: 1024,
            records: 1,
        },
    ];
    for frame in controls {
        assert_eq!(wire::encode(2, &frame), Err(WireError::InvalidValue));
        let bytes = wire::encode(0, &frame).unwrap();
        assert_eq!(
            wire::decode(&bytes).unwrap(),
            wire::Packet {
                stream_id: 0,
                frame
            }
        );
        for prefix in 0..bytes.len() {
            assert!(wire::decode(&bytes[..prefix]).is_err());
        }
        let mut wrong_namespace = bytes;
        wrong_namespace[15] = 2;
        assert_eq!(wire::decode(&wrong_namespace), Err(WireError::InvalidValue));
    }
    assert_eq!(
        wire::encode(
            0,
            &Frame::Data {
                offset: 0,
                bytes: Box::new([1])
            }
        ),
        Err(WireError::InvalidValue)
    );
}
