use skvoz_daemon::protocol::{self, Decoder, Frame};
use std::time::Duration;
use tokio::net::UnixStream;

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
#[test]
fn public_vectors_preserve_binary_bytes() {
    for line in include_str!("fixtures/ipc-v1.tsv")
        .lines()
        .filter(|l| !l.starts_with('#'))
    {
        let (_, s) = line.split_once('\t').unwrap();
        let bytes = hex(s);
        assert_eq!(
            u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );
        let frame = Frame::decode(bytes[4..].to_vec()).unwrap();
        assert_eq!(frame.encode(), bytes);
    }
}
#[test]
fn version_magic_and_extreme_size_are_rejected() {
    assert!(Frame::decode(vec![0; 31]).is_err());
    assert!(Frame::decode(vec![0; protocol::MAX_BODY + 1]).is_err());
    let frame = Frame {
        kind: 5,
        request: 1,
        handle: 1,
        payload: vec![255; protocol::MAX_PAYLOAD],
    };
    let full = frame.encode();
    assert_eq!(Frame::decode(full[4..].to_vec()).unwrap(), frame);
    let mut body = full[4..].to_vec();
    body[5] = 2;
    assert_eq!(Frame::decode(body).unwrap_err(), "unsupported IPC version");
}
#[tokio::test]
async fn fragmented_prefix_and_body_survive_partial_io() {
    let (writer, reader) = UnixStream::pair().unwrap();
    let f = Frame {
        kind: 5,
        request: 17,
        handle: 123,
        payload: (0..=255).collect(),
    };
    let mut decoder = Decoder::default();
    let bytes = f.encode();
    for byte in bytes {
        writer.writable().await.unwrap();
        assert_eq!(writer.try_write(&[byte]).unwrap(), 1);
        reader.readable().await.unwrap();
        let read = decoder.read(&reader).unwrap();
        if let Some(actual) = read {
            assert_eq!(actual, f);
            return;
        }
    }
    panic!("frame was not completed");
}
#[tokio::test]
async fn oversize_prefix_rejected_without_body() {
    let (writer, reader) = UnixStream::pair().unwrap();
    writer.writable().await.unwrap();
    writer.try_write(&u32::MAX.to_be_bytes()).unwrap();
    reader.readable().await.unwrap();
    let mut decoder = Decoder::default();
    let e = tokio::time::timeout(Duration::from_secs(1), async { decoder.read(&reader) })
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
}
