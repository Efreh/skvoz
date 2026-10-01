use std::fmt;

pub const MAX_RECEIVE_WINDOW: u32 = 16 * 1024 * 1024;
pub const MAX_FRAME_BYTES: u32 = 64 * 1024;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_PENDING_FRAMES: usize = 1024;
pub const MAX_BATCH_EVENTS: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub receive_window: u32,
    pub max_frame: u32,
    pub max_pending_frames: usize,
    pub max_metadata: usize,
    pub open_timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            receive_window: 64 * 1024,
            max_frame: 16 * 1024,
            max_pending_frames: 64,
            max_metadata: 4096,
            open_timeout_ms: 5000,
        }
    }
}

impl Config {
    pub fn validate(self) -> Result<(), Error> {
        if self.receive_window == 0 || self.receive_window > MAX_RECEIVE_WINDOW {
            return Err(Error::InvalidConfig("receive window out of range"));
        }
        if self.max_frame == 0
            || self.max_frame > MAX_FRAME_BYTES
            || self.max_frame > self.receive_window
        {
            return Err(Error::InvalidConfig("frame size out of range"));
        }
        if self.max_pending_frames == 0 || self.max_pending_frames > MAX_PENDING_FRAMES {
            return Err(Error::InvalidConfig("pending frame count out of range"));
        }
        if self.max_metadata > MAX_METADATA_BYTES {
            return Err(Error::InvalidConfig("metadata limit out of range"));
        }
        if self.open_timeout_ms == 0 {
            return Err(Error::InvalidConfig("opening timeout must be positive"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Incoming,
    Outgoing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    Opening(Direction),
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Draining,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseReason {
    Finished,
    Rejected,
    Cancelled,
    TransportLost,
    ProtocolError,
    OpenTimeout,
}

impl CloseReason {
    pub(crate) fn is_abort(self) -> bool {
        !matches!(self, Self::Finished | Self::Rejected)
    }
}

/// Typed internal frames. The driver must authenticate and route the session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Open {
        receive_window: u32,
        max_frame: u32,
        metadata: Box<[u8]>,
    },
    Accept {
        receive_window: u32,
        max_frame: u32,
        metadata: Box<[u8]>,
    },
    Reject {
        reason: Box<[u8]>,
    },
    Data {
        offset: u64,
        bytes: Box<[u8]>,
    },
    WindowUpdate {
        consumed: u64,
    },
    Fin {
        final_offset: u64,
    },
    Close {
        reason: CloseReason,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    IncomingOpen { metadata: Box<[u8]> },
    Opened { metadata: Box<[u8]> },
    Rejected { reason: Box<[u8]> },
    Data { offset: u64, bytes: Box<[u8]> },
    Writable,
    RemoteFinished,
    Closed { reason: CloseReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendOutcome {
    Accepted(usize),
    WouldBlock,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    UnexpectedFrame,
    InvalidLimits,
    MetadataTooLarge,
    InvalidDataSize,
    IncorrectOffset,
    ReceiveWindowExceeded,
    InvalidCredit,
    DataAfterFin,
    InvalidCloseReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidConfig(&'static str),
    InvalidState,
    MetadataTooLarge,
    InvalidTime,
    InvalidConsumption,
    InvalidCloseReason,
    OffsetExhausted,
    Protocol(ProtocolError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(reason) => write!(f, "invalid stream configuration: {reason}"),
            Self::InvalidState => f.write_str("operation is invalid in the current stream state"),
            Self::MetadataTooLarge => f.write_str("metadata exceeds the configured limit"),
            Self::InvalidTime => f.write_str("time regressed or the opening deadline overflowed"),
            Self::InvalidConsumption => f.write_str("consumption exceeds delivered data"),
            Self::InvalidCloseReason => f.write_str("close requires an abort reason"),
            Self::OffsetExhausted => f.write_str("byte offset exhausted"),
            Self::Protocol(reason) => write!(f, "stream protocol violation: {reason:?}"),
        }
    }
}

impl std::error::Error for Error {}

/// Occupancy is local to this stream, excluding buffers transferred to callers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub state: State,
    pub pending_data_frames: usize,
    pub pending_send_bytes: usize,
    pub buffered_receive_bytes: usize,
    pub receive_capacity_bytes: usize,
    pub receive_unconsumed_bytes: u64,
    pub send_unacknowledged_bytes: u64,
    pub retained_metadata_bytes: usize,
}
