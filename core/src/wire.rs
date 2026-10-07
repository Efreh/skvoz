//! Experimental v2 NATS-message encoding. All integers use network byte order.

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

/// Encode one validated packet with an exact initial allocation.
pub fn encode(stream_id: u64, frame: &Frame) -> Result<Vec<u8>, WireError> {
    let mut result = Vec::with_capacity(encoded_size(stream_id, frame)?);
    encode_into(&mut result, stream_id, frame)?;
    Ok(result)
}

// Validate all fields before a caller-visible buffer can be appended to.
pub(crate) fn encoded_size(stream_id: u64, frame: &Frame) -> Result<usize, WireError> {
    if (stream_id == 0) != frame.is_peer_control() {
        return Err(WireError::InvalidValue);
    }
    let body = match frame {
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
            blob_size(metadata, MAX_METADATA_BYTES)? + 8
        }
        Frame::Reject { reason } => blob_size(reason, MAX_METADATA_BYTES)?,
        Frame::Data { bytes, .. } => {
            if bytes.is_empty() {
                return Err(WireError::InvalidValue);
            }
            blob_size(bytes, MAX_FRAME_BYTES as usize)? + 8
        }
        Frame::WindowUpdate { .. } | Frame::Fin { .. } | Frame::PeerFreeze { .. } => 8,
        Frame::WindowGrant { .. } | Frame::PeerFrozen { .. } => 24,
        Frame::PeerGrant { .. } => 48,
        Frame::PeerRequest {
            bytes,
            records,
            requester_stream_id,
            blocked,
            ..
        } => {
            if *bytes == 0
                || *bytes > MAX_RECEIVE_WINDOW
                || *records == 0
                || *requester_stream_id < 2
                || *blocked > 3
            {
                return Err(WireError::InvalidValue);
            }
            25
        }
        Frame::Close { reason } => {
            if !reason.is_abort() {
                return Err(WireError::InvalidValue);
            }
            1
        }
    };
    Ok(16 + body)
}

fn blob_size(bytes: &[u8], limit: usize) -> Result<usize, WireError> {
    if bytes.len() > limit {
        Err(WireError::TooLarge)
    } else {
        Ok(4 + bytes.len())
    }
}

pub(crate) fn encode_into(
    result: &mut Vec<u8>,
    stream_id: u64,
    frame: &Frame,
) -> Result<(), WireError> {
    let size = encoded_size(stream_id, frame)?;
    result.reserve(size);
    let kind = match frame {
        Frame::Open { .. } => 1,
        Frame::Accept { .. } => 2,
        Frame::Reject { .. } => 3,
        Frame::Data { .. } => 4,
        Frame::WindowUpdate { .. } => 5,
        Frame::Fin { .. } => 6,
        Frame::Close { .. } => 7,
        Frame::WindowGrant { .. } => 8,
        Frame::PeerGrant { .. } => 9,
        Frame::PeerRequest { .. } => 10,
        Frame::PeerFreeze { .. } => 11,
        Frame::PeerFrozen { .. } => 12,
    };
    result.extend_from_slice(b"SKVZ");
    result.extend_from_slice(&[2, kind, 0, 0]);
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
            result.extend_from_slice(&receive_window.to_be_bytes());
            result.extend_from_slice(&max_frame.to_be_bytes());
            put_bytes(result, metadata);
        }
        Frame::Reject { reason } => put_bytes(result, reason),
        Frame::Data { offset, bytes } => {
            result.extend_from_slice(&offset.to_be_bytes());
            put_bytes(result, bytes);
        }
        Frame::WindowUpdate { consumed } => result.extend_from_slice(&consumed.to_be_bytes()),
        Frame::WindowGrant {
            consumed,
            limit,
            probe,
        } => {
            for value in [consumed, limit, probe] {
                result.extend_from_slice(&value.to_be_bytes());
            }
        }
        Frame::PeerGrant {
            epoch,
            consumed_bytes,
            limit_bytes,
            consumed_records,
            limit_records,
            probe,
        } => {
            for value in [
                epoch,
                consumed_bytes,
                limit_bytes,
                consumed_records,
                limit_records,
                probe,
            ] {
                result.extend_from_slice(&value.to_be_bytes());
            }
        }
        Frame::PeerRequest {
            bytes,
            records,
            probe,
            requester_stream_id,
            blocked,
        } => {
            result.extend_from_slice(&bytes.to_be_bytes());
            result.extend_from_slice(&records.to_be_bytes());
            result.extend_from_slice(&probe.to_be_bytes());
            result.extend_from_slice(&requester_stream_id.to_be_bytes());
            result.push(*blocked);
        }
        Frame::PeerFreeze { epoch } => result.extend_from_slice(&epoch.to_be_bytes()),
        Frame::PeerFrozen {
            epoch,
            bytes,
            records,
        } => {
            for value in [epoch, bytes, records] {
                result.extend_from_slice(&value.to_be_bytes());
            }
        }
        Frame::Fin { final_offset } => result.extend_from_slice(&final_offset.to_be_bytes()),
        Frame::Close { reason } => result.push(match reason {
            CloseReason::Cancelled => 1,
            CloseReason::TransportLost => 2,
            CloseReason::ProtocolError => 3,
            CloseReason::OpenTimeout => 4,
            _ => unreachable!("validated abort reason"),
        }),
    }
    Ok(())
}

pub fn decode(bytes: &[u8]) -> Result<Packet, WireError> {
    if bytes.len() > MAX_PACKET_BYTES {
        return Err(WireError::TooLarge);
    }
    let mut reader = Reader { bytes, cursor: 0 };
    if reader.take(4)? != b"SKVZ" {
        return Err(WireError::InvalidHeader);
    }
    if reader.take(1)?[0] != 2 {
        return Err(WireError::UnsupportedVersion);
    }
    let kind = reader.take(1)?[0];
    if reader.take(2)? != [0, 0] {
        return Err(WireError::InvalidHeader);
    }
    let stream_id = reader.u64()?;
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
        8 => Frame::WindowGrant {
            consumed: reader.u64()?,
            limit: reader.u64()?,
            probe: reader.u64()?,
        },
        9 => Frame::PeerGrant {
            epoch: reader.u64()?,
            consumed_bytes: reader.u64()?,
            limit_bytes: reader.u64()?,
            consumed_records: reader.u64()?,
            limit_records: reader.u64()?,
            probe: reader.u64()?,
        },
        10 => Frame::PeerRequest {
            bytes: reader.u32()?,
            records: reader.u32()?,
            probe: reader.u64()?,
            requester_stream_id: reader.u64()?,
            blocked: reader.take(1)?[0],
        },
        11 => Frame::PeerFreeze {
            epoch: reader.u64()?,
        },
        12 => Frame::PeerFrozen {
            epoch: reader.u64()?,
            bytes: reader.u64()?,
            records: reader.u64()?,
        },
        _ => return Err(WireError::UnknownKind),
    };
    if reader.cursor != bytes.len() {
        return Err(WireError::InvalidLength);
    }
    encoded_size(stream_id, &frame)?;
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

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    output.extend_from_slice(bytes);
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

#[cfg(test)]
mod encoding_tests {
    use super::*;

    #[test]
    fn invalid_append_keeps_existing_prefix_bytes_and_capacity() {
        let invalid = [
            Frame::Data {
                offset: 0,
                bytes: Box::new([]),
            },
            Frame::Data {
                offset: 0,
                bytes: vec![0; MAX_FRAME_BYTES as usize + 1].into(),
            },
            Frame::Open {
                receive_window: 0,
                max_frame: 1,
                metadata: Box::new([]),
            },
            Frame::Reject {
                reason: vec![0; MAX_METADATA_BYTES + 1].into(),
            },
            Frame::Close {
                reason: CloseReason::Finished,
            },
        ];
        for frame in invalid {
            let mut buffer = b"existing envelope prefix".to_vec();
            let original = buffer.clone();
            let capacity = buffer.capacity();
            assert!(encode_into(&mut buffer, 1, &frame).is_err());
            assert_eq!(buffer, original);
            assert_eq!(buffer.capacity(), capacity);
        }
    }

    #[test]
    fn exact_size_append_preserves_prefix_and_maximum_payload() {
        for size in [1, 1508, 16384, MAX_FRAME_BYTES as usize] {
            let frame = Frame::Data {
                offset: 123,
                bytes: vec![42; size].into(),
            };
            let encoded = encode(7, &frame).unwrap();
            assert_eq!(encoded_size(7, &frame).unwrap(), encoded.len());
            let mut envelope = Vec::with_capacity(24 + encoded.len());
            envelope.extend_from_slice(&[9; 24]);
            let capacity = envelope.capacity();
            encode_into(&mut envelope, 7, &frame).unwrap();
            assert_eq!(envelope.capacity(), capacity);
            assert_eq!(&envelope[..24], &[9; 24]);
            assert_eq!(&envelope[24..], encoded);
        }
    }
}
