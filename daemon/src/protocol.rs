//! Fixed IPC v1 framing. All integer fields use network byte order.
use std::io;
use tokio::net::UnixStream;

pub const VERSION: u16 = 1;
pub const HEADER: usize = 32;
pub const MAX_PAYLOAD: usize = 65536;
pub const MAX_BODY: usize = HEADER + MAX_PAYLOAD;
pub const RESPONSE: u16 = 0x8000;
pub const INCOMING: u16 = 0x9001;
pub const OPENED: u16 = 0x9002;
pub const REJECTED: u16 = 0x9003;
pub const DATA: u16 = 0x9004;
pub const WRITABLE: u16 = 0x9005;
pub const REMOTE_FINISHED: u16 = 0x9006;
pub const CLOSED: u16 = 0x9007;

#[derive(Debug, Eq, PartialEq)]
pub struct Frame {
    pub kind: u16,
    pub request: u64,
    pub handle: u128,
    pub payload: Vec<u8>,
}
impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        assert!(self.payload.len() <= MAX_PAYLOAD);
        let mut b = Vec::with_capacity(4 + HEADER + self.payload.len());
        b.extend_from_slice(&((HEADER + self.payload.len()) as u32).to_be_bytes());
        b.extend_from_slice(b"SKI1");
        b.extend_from_slice(&VERSION.to_be_bytes());
        b.extend_from_slice(&self.kind.to_be_bytes());
        b.extend_from_slice(&self.request.to_be_bytes());
        b.extend_from_slice(&self.handle.to_be_bytes());
        b.extend_from_slice(&self.payload);
        b
    }
    pub fn decode(mut body: Vec<u8>) -> Result<Self, &'static str> {
        if !(HEADER..=MAX_BODY).contains(&body.len()) || &body[..4] != b"SKI1" {
            return Err("invalid IPC frame");
        }
        if u16::from_be_bytes(body[4..6].try_into().unwrap()) != VERSION {
            return Err("unsupported IPC version");
        }
        let kind = u16::from_be_bytes(body[6..8].try_into().unwrap());
        let request = u64::from_be_bytes(body[8..16].try_into().unwrap());
        let handle = u128::from_be_bytes(body[16..32].try_into().unwrap());
        body.drain(..HEADER);
        Ok(Self {
            kind,
            request,
            handle,
            payload: body,
        })
    }
}

/// At most one allocated input frame per session. Prefix checked before allocation.
#[derive(Default)]
pub struct Decoder {
    prefix: [u8; 4],
    prefix_used: usize,
    body: Vec<u8>,
    body_used: usize,
}
impl Decoder {
    pub fn partial(&self) -> bool {
        self.prefix_used != 0
    }
    pub fn read(&mut self, socket: &UnixStream) -> io::Result<Option<Frame>> {
        if self.prefix_used < 4 {
            match socket.try_read(&mut self.prefix[self.prefix_used..]) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => self.prefix_used += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e),
            }
            if self.prefix_used < 4 {
                return Ok(None);
            }
            let size = u32::from_be_bytes(self.prefix) as usize;
            if !(HEADER..=MAX_BODY).contains(&size) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid IPC size",
                ));
            }
            self.body.resize(size, 0);
        }
        match socket.try_read(&mut self.body[self.body_used..]) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => self.body_used += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e),
        }
        if self.body_used != self.body.len() {
            return Ok(None);
        }
        self.prefix_used = 0;
        self.body_used = 0;
        Frame::decode(std::mem::take(&mut self.body))
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

pub fn response(request: u64, handle: u128, code: u16, value: u64, extra: &[u8]) -> Frame {
    let mut payload = Vec::with_capacity(10 + extra.len());
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(&value.to_be_bytes());
    payload.extend_from_slice(extra);
    Frame {
        kind: RESPONSE,
        request,
        handle,
        payload,
    }
}
pub fn number(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}
