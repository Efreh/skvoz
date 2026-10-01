//! Experimental v1 NATS-message encoding. All integers use network byte order.

use std::fmt;

use crate::{CloseReason, Frame, MAX_FRAME_BYTES, MAX_METADATA_BYTES, MAX_RECEIVE_WINDOW};

pub const MAX_PACKET_BYTES: usize = 16 + 12 + MAX_FRAME_BYTES as usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Truncated,
    InvalidHeader,
    UnsupportedVersion,
    UnknownKind,
    InvalidLength,
    TooLarge,
    InvalidValue,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid experimental SKVOZ packet: {self:?}")
    }
}

impl std::error::Error for WireError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Packet {
    pub stream_id: u64,
    pub frame: Frame,
}

pub fn encode(stream_id: u64, frame: &Frame) -> Result<Vec<u8>, WireError> {
    if stream_id == 0 {
        return Err(WireError::InvalidValue);
    }
    let kind = match frame {
        Frame::Open { .. } => 1,
        Frame::Accept { .. } => 2,
        Frame::Reject { .. } => 3,
        Frame::Data { .. } => 4,
        Frame::WindowUpdate { .. } => 5,
        Frame::Fin { .. } => 6,
        Frame::Close { .. } => 7,
    };
    let mut result = Vec::new();
    result.extend_from_slice(b"SKVZ");
    result.extend_from_slice(&[1, kind, 0, 0]);
    result.extend_from_slice(&stream_id.to_be_bytes());
    match frame {
        Frame::Open {
            receive_window,
            max_frame,
            metadata,
        }
        | Frame::Accept {
            receive_window,
            max_frame,
            metadata,
        } => {
            limits(*receive_window, *max_frame)?;
            result.extend_from_slice(&receive_window.to_be_bytes());
            result.extend_from_slice(&max_frame.to_be_bytes());
            put_bytes(&mut result, metadata, MAX_METADATA_BYTES)?;
        }
        Frame::Reject { reason } => put_bytes(&mut result, reason, MAX_METADATA_BYTES)?,
        Frame::Data { offset, bytes } => {
            if bytes.is_empty() {
                return Err(WireError::InvalidValue);
            }
            result.extend_from_slice(&offset.to_be_bytes());
            put_bytes(&mut result, bytes, MAX_FRAME_BYTES as usize)?;
        }
        Frame::WindowUpdate { consumed } => result.extend_from_slice(&consumed.to_be_bytes()),
        Frame::Fin { final_offset } => result.extend_from_slice(&final_offset.to_be_bytes()),
        Frame::Close { reason } => result.push(match reason {
            CloseReason::Cancelled => 1,
            CloseReason::TransportLost => 2,
            CloseReason::ProtocolError => 3,
            CloseReason::OpenTimeout => 4,
            _ => return Err(WireError::InvalidValue),
        }),
    }
    Ok(result)
}

pub fn decode(bytes: &[u8]) -> Result<Packet, WireError> {
    if bytes.len() > MAX_PACKET_BYTES {
        return Err(WireError::TooLarge);
    }
    let mut reader = Reader { bytes, cursor: 0 };
    if reader.take(4)? != b"SKVZ" {
        return Err(WireError::InvalidHeader);
    }
    if reader.take(1)?[0] != 1 {
        return Err(WireError::UnsupportedVersion);
    }
    let kind = reader.take(1)?[0];
    if reader.take(2)? != [0, 0] {
        return Err(WireError::InvalidHeader);
    }
    let stream_id = reader.u64()?;
    if stream_id == 0 {
        return Err(WireError::InvalidValue);
    }
    let frame = match kind {
        1 | 2 => {
            let receive_window = reader.u32()?;
            let max_frame = reader.u32()?;
            limits(receive_window, max_frame)?;
            let metadata = reader.blob(MAX_METADATA_BYTES)?;
            if kind == 1 {
                Frame::Open {
                    receive_window,
                    max_frame,
                    metadata,
                }
            } else {
                Frame::Accept {
                    receive_window,
                    max_frame,
                    metadata,
                }
            }
        }
        3 => Frame::Reject {
            reason: reader.blob(MAX_METADATA_BYTES)?,
        },
        4 => {
            let offset = reader.u64()?;
            let bytes = reader.blob(MAX_FRAME_BYTES as usize)?;
            if bytes.is_empty() {
                return Err(WireError::InvalidValue);
            }
            Frame::Data { offset, bytes }
        }
        5 => Frame::WindowUpdate {
            consumed: reader.u64()?,
        },
        6 => Frame::Fin {
            final_offset: reader.u64()?,
        },
        7 => Frame::Close {
            reason: match reader.take(1)?[0] {
                1 => CloseReason::Cancelled,
                2 => CloseReason::TransportLost,
                3 => CloseReason::ProtocolError,
                4 => CloseReason::OpenTimeout,
                _ => return Err(WireError::InvalidValue),
            },
        },
        _ => return Err(WireError::UnknownKind),
    };
    if reader.cursor != bytes.len() {
        return Err(WireError::InvalidLength);
    }
    Ok(Packet { stream_id, frame })
}

fn limits(window: u32, frame: u32) -> Result<(), WireError> {
    if window == 0
        || window > MAX_RECEIVE_WINDOW
        || frame == 0
        || frame > MAX_FRAME_BYTES
        || frame > window
    {
        Err(WireError::InvalidValue)
    } else {
        Ok(())
    }
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<(), WireError> {
    if bytes.len() > limit {
        return Err(WireError::TooLarge);
    }
    output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or(WireError::InvalidLength)?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or(WireError::Truncated)?;
        self.cursor = end;
        Ok(bytes)
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| WireError::Truncated)?,
        ))
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| WireError::Truncated)?,
        ))
    }
    fn blob(&mut self, limit: usize) -> Result<Box<[u8]>, WireError> {
        let count = self.u32()? as usize;
        if count > limit {
            return Err(WireError::TooLarge);
        }
        let bytes = self.take(count)?;
        if self.cursor != self.bytes.len() {
            return Err(WireError::InvalidLength);
        }
        Ok(bytes.into())
    }
}
