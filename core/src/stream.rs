use std::collections::VecDeque;

use crate::{
    CloseReason, Config, Direction, Error, Event, Frame, MAX_BATCH_EVENTS, MAX_FRAME_BYTES,
    MAX_RECEIVE_WINDOW, PeerLimits, ProtocolError, SendOutcome, Snapshot, State,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Opening(Direction),
    Established,
    Closed,
}

/// Deterministic state for one stream; methods require exclusive ownership.
#[derive(Debug)]
pub struct Stream {
    config: Config,
    phase: Phase,
    now_ms: u64,
    deadline_ms: Option<u64>,
    peer_window: u32,
    peer_max_frame: u32,
    sent: u64,
    dispatched: u64,
    peer_consumed: u64,
    received: u64,
    delivered: u64,
    consumed: u64,
    local_finished: bool,
    remote_finished: bool,
    blocked: bool,
    writable_event: bool,
    remote_finished_event: bool,
    closed_event: Option<CloseReason>,
    lifecycle_events: VecDeque<Event>,
    received_bytes: VecDeque<u8>,
    outgoing_data: VecDeque<Frame>,
    handshake_frame: Option<Frame>,
    credit_frame: Option<u64>,
    fin_frame: Option<u64>,
    terminal_frame: Option<Frame>,
}

impl Stream {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config,
            phase: Phase::Idle,
            now_ms: 0,
            deadline_ms: None,
            peer_window: 0,
            peer_max_frame: 0,
            sent: 0,
            dispatched: 0,
            peer_consumed: 0,
            received: 0,
            delivered: 0,
            consumed: 0,
            local_finished: false,
            remote_finished: false,
            blocked: false,
            writable_event: false,
            remote_finished_event: false,
            closed_event: None,
            lifecycle_events: VecDeque::new(),
            received_bytes: VecDeque::new(),
            outgoing_data: VecDeque::new(),
            handshake_frame: None,
            credit_frame: None,
            fin_frame: None,
            terminal_frame: None,
        })
    }

    pub fn state(&self) -> State {
        match self.phase {
            Phase::Idle => State::Idle,
            Phase::Opening(direction) => State::Opening(direction),
            Phase::Closed => State::Closed,
            Phase::Established => match (self.local_finished, self.remote_finished) {
                (false, false) => State::Open,
                (true, false) => State::HalfClosedLocal,
                (false, true) => State::HalfClosedRemote,
                (true, true) => State::Draining,
            },
        }
    }

    /// Peer limits are unknown until a valid OPEN or ACCEPT has been received.
    /// Reading them does not extract frames, consume bytes, or change credit.
    pub fn peer_limits(&self) -> Option<PeerLimits> {
        (self.peer_window != 0).then_some(PeerLimits {
            receive_window: self.peer_window,
            max_frame: self.peer_max_frame,
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        let frame_metadata = match &self.handshake_frame {
            Some(Frame::Open { metadata, .. } | Frame::Accept { metadata, .. }) => metadata.len(),
            _ => 0,
        };
        let terminal_metadata = match &self.terminal_frame {
            Some(Frame::Reject { reason }) => reason.len(),
            _ => 0,
        };
        let event_metadata: usize = self
            .lifecycle_events
            .iter()
            .map(|event| match event {
                Event::IncomingOpen { metadata } | Event::Opened { metadata } => metadata.len(),
                Event::Rejected { reason } => reason.len(),
                _ => 0,
            })
            .sum();
        Snapshot {
            state: self.state(),
            pending_data_frames: self.outgoing_data.len(),
            pending_send_bytes: self
                .outgoing_data
                .iter()
                .map(|frame| match frame {
                    Frame::Data { bytes, .. } => bytes.len(),
                    _ => 0,
                })
                .sum(),
            buffered_receive_bytes: self.received_bytes.len(),
            receive_capacity_bytes: self.received_bytes.capacity(),
            receive_unconsumed_bytes: self.received - self.consumed,
            send_unacknowledged_bytes: self.sent - self.peer_consumed,
            retained_metadata_bytes: frame_metadata + terminal_metadata + event_metadata,
        }
    }

    pub fn open(&mut self, metadata: &[u8], now_ms: u64) -> Result<(), Error> {
        if self.phase != Phase::Idle {
            return Err(Error::InvalidState);
        }
        self.check_metadata(metadata)?;
        let deadline = self.opening_deadline(now_ms)?;
        self.now_ms = now_ms;
        self.deadline_ms = Some(deadline);
        self.phase = Phase::Opening(Direction::Outgoing);
        self.handshake_frame = Some(Frame::Open {
            receive_window: self.config.receive_window,
            max_frame: self.config.max_frame,
            metadata: metadata.into(),
        });
        Ok(())
    }

    pub fn accept(&mut self, metadata: &[u8]) -> Result<(), Error> {
        if self.phase != Phase::Opening(Direction::Incoming) {
            return Err(Error::InvalidState);
        }
        self.check_metadata(metadata)?;
        self.phase = Phase::Established;
        self.deadline_ms = None;
        self.handshake_frame = Some(Frame::Accept {
            receive_window: self.config.receive_window,
            max_frame: self.config.max_frame,
            metadata: metadata.into(),
        });
        self.lifecycle_events.push_back(Event::Opened {
            metadata: metadata.into(),
        });
        Ok(())
    }

    pub fn reject(&mut self, reason: &[u8]) -> Result<(), Error> {
        if self.phase != Phase::Opening(Direction::Incoming) {
            return Err(Error::InvalidState);
        }
        self.check_metadata(reason)?;
        self.abort(
            CloseReason::Rejected,
            Some(Frame::Reject {
                reason: reason.into(),
            }),
        );
        Ok(())
    }

    /// Accept at most one frame. A partial acceptance leaves the suffix to the caller.
    pub fn send(&mut self, bytes: &[u8]) -> Result<SendOutcome, Error> {
        if self.phase != Phase::Established || self.local_finished {
            return Err(Error::InvalidState);
        }
        if bytes.is_empty() {
            return Ok(SendOutcome::Accepted(0));
        }
        if !self.can_send() {
            self.blocked = true;
            self.writable_event = false;
            return Ok(SendOutcome::WouldBlock);
        }
        let count = bytes
            .len()
            .min(self.config.max_frame as usize)
            .min(self.peer_max_frame as usize)
            .min(self.available_credit() as usize);
        let next = self
            .sent
            .checked_add(count as u64)
            .ok_or(Error::OffsetExhausted)?;
        self.outgoing_data.push_back(Frame::Data {
            offset: self.sent,
            bytes: bytes[..count].into(),
        });
        self.sent = next;
        Ok(SendOutcome::Accepted(count))
    }

    pub fn finish(&mut self) -> Result<(), Error> {
        if self.phase == Phase::Closed && self.local_finished {
            return Ok(());
        }
        if self.phase != Phase::Established {
            return Err(Error::InvalidState);
        }
        if !self.local_finished {
            self.local_finished = true;
            self.fin_frame = Some(self.sent);
            self.blocked = false;
            self.writable_event = false;
        }
        Ok(())
    }

    /// Acknowledge a contiguous prefix only after the connector consumed it.
    pub fn consume_through(&mut self, offset: u64) -> Result<(), Error> {
        if self.phase != Phase::Established {
            return Err(Error::InvalidState);
        }
        if offset > self.delivered {
            return Err(Error::InvalidConsumption);
        }
        if offset > self.consumed {
            self.consumed = offset;
            self.credit_frame = Some(offset);
            self.maybe_complete();
        }
        Ok(())
    }

    pub fn receive(&mut self, frame: &Frame, now_ms: u64) -> Result<(), Error> {
        if self.phase == Phase::Idle && matches!(frame, Frame::Open { .. }) {
            self.opening_deadline(now_ms)?;
        }
        self.tick(now_ms)?;
        if self.phase == Phase::Closed {
            return Ok(());
        }
        if let Err(reason) = self.apply_frame(frame) {
            self.abort(
                CloseReason::ProtocolError,
                Some(Frame::Close {
                    reason: CloseReason::ProtocolError,
                }),
            );
            return Err(Error::Protocol(reason));
        }
        Ok(())
    }

    pub fn tick(&mut self, now_ms: u64) -> Result<(), Error> {
        if now_ms < self.now_ms {
            return Err(Error::InvalidTime);
        }
        self.now_ms = now_ms;
        if self.deadline_ms.is_some_and(|deadline| now_ms >= deadline) {
            self.abort(
                CloseReason::OpenTimeout,
                Some(Frame::Close {
                    reason: CloseReason::OpenTimeout,
                }),
            );
        }
        Ok(())
    }

    pub fn close(&mut self, reason: CloseReason) -> Result<(), Error> {
        if !reason.is_abort() {
            return Err(Error::InvalidCloseReason);
        }
        if self.phase != Phase::Closed {
            self.abort(reason, Some(Frame::Close { reason }));
        }
        Ok(())
    }

    pub fn transport_lost(&mut self) {
        if self.phase != Phase::Closed {
            self.abort(CloseReason::TransportLost, None);
        } else {
            self.terminal_frame = None;
        }
    }

    /// The driver must transmit returned frames in this order without replay.
    pub fn poll_frames(&mut self, max_count: usize) -> Vec<Frame> {
        let mut frames = Vec::new();
        while frames.len() < max_count {
            let Some(frame) = self.next_frame() else {
                break;
            };
            frames.push(frame);
        }
        frames
    }

    /// Data buffers transfer to the connector; credit remains reserved.
    pub fn poll_events(&mut self, max_count: usize) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < max_count.min(MAX_BATCH_EVENTS) {
            let event = if let Some(event) = self.lifecycle_events.pop_front() {
                event
            } else if self.writable_event {
                self.writable_event = false;
                Event::Writable
            } else if !self.received_bytes.is_empty() {
                let count = self
                    .received_bytes
                    .len()
                    .min(self.config.max_frame as usize);
                let bytes = self.received_bytes.drain(..count).collect();
                let offset = self.delivered;
                self.delivered += count as u64;
                Event::Data { offset, bytes }
            } else if self.remote_finished_event {
                self.remote_finished_event = false;
                Event::RemoteFinished
            } else if let Some(reason) = self.closed_event.take() {
                Event::Closed { reason }
            } else {
                break;
            };
            events.push(event);
        }
        events
    }

    pub(crate) fn has_frames(&self) -> bool {
        self.terminal_frame.is_some()
            || self.handshake_frame.is_some()
            || self.credit_frame.is_some()
            || !self.outgoing_data.is_empty()
            || self.fin_frame.is_some()
    }

    pub(crate) fn has_events(&self) -> bool {
        !self.lifecycle_events.is_empty()
            || self.writable_event
            || !self.received_bytes.is_empty()
            || self.remote_finished_event
            || self.closed_event.is_some()
    }

    pub(crate) fn budget_blocked(&mut self) {
        self.blocked = true;
        self.writable_event = false;
    }

    pub(crate) fn budget_writable(&mut self) {
        if self.can_send() {
            self.writable_event = true;
        }
    }

    fn apply_frame(&mut self, frame: &Frame) -> Result<(), ProtocolError> {
        match frame {
            Frame::Open {
                receive_window,
                max_frame,
                metadata,
            } => {
                if self.phase != Phase::Idle {
                    return Err(ProtocolError::UnexpectedFrame);
                }
                self.check_peer_limits(*receive_window, *max_frame, metadata)?;
                let deadline = self
                    .opening_deadline(self.now_ms)
                    .map_err(|_| ProtocolError::InvalidLimits)?;
                self.peer_window = *receive_window;
                self.peer_max_frame = *max_frame;
                self.phase = Phase::Opening(Direction::Incoming);
                self.deadline_ms = Some(deadline);
                self.lifecycle_events.push_back(Event::IncomingOpen {
                    metadata: metadata.clone(),
                });
            }
            Frame::Accept {
                receive_window,
                max_frame,
                metadata,
            } => {
                if self.phase != Phase::Opening(Direction::Outgoing)
                    || self.handshake_frame.is_some()
                {
                    return Err(ProtocolError::UnexpectedFrame);
                }
                self.check_peer_limits(*receive_window, *max_frame, metadata)?;
                self.peer_window = *receive_window;
                self.peer_max_frame = *max_frame;
                self.phase = Phase::Established;
                self.deadline_ms = None;
                self.lifecycle_events.push_back(Event::Opened {
                    metadata: metadata.clone(),
                });
            }
            Frame::Reject { reason } => {
                if self.phase != Phase::Opening(Direction::Outgoing)
                    || self.handshake_frame.is_some()
                {
                    return Err(ProtocolError::UnexpectedFrame);
                }
                if reason.len() > self.config.max_metadata {
                    return Err(ProtocolError::MetadataTooLarge);
                }
                self.abort(CloseReason::Rejected, None);
                self.lifecycle_events.push_back(Event::Rejected {
                    reason: reason.clone(),
                });
            }
            Frame::Data { offset, bytes } => {
                self.require_established()?;
                if self.remote_finished {
                    return Err(ProtocolError::DataAfterFin);
                }
                if bytes.is_empty() || bytes.len() > self.config.max_frame as usize {
                    return Err(ProtocolError::InvalidDataSize);
                }
                if *offset != self.received {
                    return Err(ProtocolError::IncorrectOffset);
                }
                let next = self
                    .received
                    .checked_add(bytes.len() as u64)
                    .ok_or(ProtocolError::IncorrectOffset)?;
                if next - self.consumed > self.config.receive_window as u64 {
                    return Err(ProtocolError::ReceiveWindowExceeded);
                }
                let required = self.received_bytes.len() + bytes.len();
                if required > self.received_bytes.capacity() {
                    let target = required
                        .max(self.received_bytes.capacity().saturating_mul(2))
                        .min(self.config.receive_window as usize);
                    self.received_bytes
                        .reserve_exact(target - self.received_bytes.len());
                }
                self.received_bytes.extend(bytes.iter().copied());
                self.received = next;
            }
            Frame::WindowUpdate { consumed } => {
                self.require_established()?;
                if *consumed > self.dispatched {
                    return Err(ProtocolError::InvalidCredit);
                }
                self.peer_consumed = self.peer_consumed.max(*consumed);
                self.notify_writable();
            }
            Frame::Fin { final_offset } => {
                self.require_established()?;
                if *final_offset != self.received {
                    return Err(ProtocolError::IncorrectOffset);
                }
                if !self.remote_finished {
                    self.remote_finished = true;
                    self.remote_finished_event = true;
                    self.maybe_complete();
                }
            }
            Frame::Close { reason } => {
                if self.phase == Phase::Idle {
                    return Err(ProtocolError::UnexpectedFrame);
                }
                if !reason.is_abort() {
                    return Err(ProtocolError::InvalidCloseReason);
                }
                self.abort(*reason, None);
            }
        }
        Ok(())
    }

    fn next_frame(&mut self) -> Option<Frame> {
        if let Some(frame) = self.terminal_frame.take() {
            return Some(frame);
        }
        if let Some(frame) = self.handshake_frame.take() {
            return Some(frame);
        }
        if let Some(consumed) = self.credit_frame.take() {
            return Some(Frame::WindowUpdate { consumed });
        }
        if let Some(frame) = self.outgoing_data.pop_front() {
            if let Frame::Data { offset, bytes } = &frame {
                self.dispatched = offset + bytes.len() as u64;
            }
            self.notify_writable();
            return Some(frame);
        }
        if let Some(final_offset) = self.fin_frame.take() {
            self.maybe_complete();
            return Some(Frame::Fin { final_offset });
        }
        None
    }

    fn available_credit(&self) -> u64 {
        self.peer_window as u64 - (self.sent - self.peer_consumed)
    }

    fn can_send(&self) -> bool {
        self.phase == Phase::Established
            && !self.local_finished
            && self.outgoing_data.len() < self.config.max_pending_frames
            && self.available_credit() > 0
    }

    fn notify_writable(&mut self) {
        if self.blocked && self.can_send() {
            self.blocked = false;
            self.writable_event = true;
        }
    }

    fn maybe_complete(&mut self) {
        if self.phase == Phase::Established
            && self.local_finished
            && self.remote_finished
            && self.fin_frame.is_none()
            && self.outgoing_data.is_empty()
            && self.consumed == self.received
        {
            self.phase = Phase::Closed;
            self.credit_frame = None;
            self.writable_event = false;
            self.closed_event = Some(CloseReason::Finished);
            self.received_bytes = VecDeque::new();
            self.outgoing_data = VecDeque::new();
        }
    }

    fn abort(&mut self, reason: CloseReason, frame: Option<Frame>) {
        self.phase = Phase::Closed;
        self.deadline_ms = None;
        self.received_bytes = VecDeque::new();
        self.outgoing_data = VecDeque::new();
        self.lifecycle_events = VecDeque::new();
        self.handshake_frame = None;
        self.credit_frame = None;
        self.fin_frame = None;
        self.terminal_frame = frame;
        self.blocked = false;
        self.writable_event = false;
        self.remote_finished_event = false;
        self.peer_consumed = self.sent;
        self.consumed = self.received;
        self.delivered = self.received;
        self.closed_event = Some(reason);
    }

    fn require_established(&self) -> Result<(), ProtocolError> {
        if self.phase == Phase::Established {
            Ok(())
        } else {
            Err(ProtocolError::UnexpectedFrame)
        }
    }

    fn check_metadata(&self, metadata: &[u8]) -> Result<(), Error> {
        if metadata.len() > self.config.max_metadata {
            Err(Error::MetadataTooLarge)
        } else {
            Ok(())
        }
    }

    fn check_peer_limits(
        &self,
        window: u32,
        max_frame: u32,
        metadata: &[u8],
    ) -> Result<(), ProtocolError> {
        if window == 0
            || window > MAX_RECEIVE_WINDOW
            || max_frame == 0
            || max_frame > MAX_FRAME_BYTES
            || max_frame > window
        {
            return Err(ProtocolError::InvalidLimits);
        }
        if metadata.len() > self.config.max_metadata {
            return Err(ProtocolError::MetadataTooLarge);
        }
        Ok(())
    }

    fn opening_deadline(&self, now_ms: u64) -> Result<u64, Error> {
        if now_ms < self.now_ms {
            return Err(Error::InvalidTime);
        }
        now_ms
            .checked_add(self.config.open_timeout_ms)
            .ok_or(Error::InvalidTime)
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    fn near_offset_limit() -> Stream {
        let mut stream = Stream::new(Config::default()).unwrap();
        stream.phase = Phase::Established;
        stream.peer_window = stream.config.receive_window;
        stream.peer_max_frame = stream.config.max_frame;
        stream.sent = u64::MAX - 1;
        stream.dispatched = u64::MAX - 1;
        stream.peer_consumed = u64::MAX - 1;
        stream.received = u64::MAX - 1;
        stream.delivered = u64::MAX - 1;
        stream.consumed = u64::MAX - 1;
        stream
    }

    #[test]
    fn send_offset_exhaustion_is_atomic() {
        let mut stream = near_offset_limit();
        let before = stream.snapshot();
        assert_eq!(stream.send(b"ab"), Err(Error::OffsetExhausted));
        assert_eq!(stream.snapshot(), before);
        assert!(stream.poll_frames(1).is_empty());
        assert_eq!(stream.send(b"a"), Ok(SendOutcome::Accepted(1)));
        stream.finish().unwrap();
        assert_eq!(
            stream.poll_frames(8),
            [
                Frame::Data {
                    offset: u64::MAX - 1,
                    bytes: Box::new([b'a'])
                },
                Frame::Fin {
                    final_offset: u64::MAX
                },
            ]
        );
    }

    #[test]
    fn receive_offset_exhaustion_fails_closed_without_wrapping() {
        let mut stream = near_offset_limit();
        assert_eq!(
            stream.receive(
                &Frame::Data {
                    offset: u64::MAX - 1,
                    bytes: Box::new([0, 1])
                },
                0,
            ),
            Err(Error::Protocol(ProtocolError::IncorrectOffset))
        );
        assert_eq!(stream.state(), State::Closed);
        assert_eq!(stream.snapshot().buffered_receive_bytes, 0);
    }
}
