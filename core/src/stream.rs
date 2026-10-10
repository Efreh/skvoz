use std::collections::{LinkedList, VecDeque};

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
    peer_granted_window: u32,
    peer_limit: u64,
    peer_record_window: usize,
    sent_ends: LinkedList<u64>,
    received_ends: LinkedList<u64>,
    local_window: u32,
    receive_limit: u64,
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
    received_bytes: LinkedList<Box<[u8]>>,
    outgoing_data: LinkedList<Frame>,
    handshake_frame: Option<Frame>,
    credit_frame: Option<u64>,
    grant_frame: Option<(u64, u64)>,
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
            peer_granted_window: 0,
            peer_limit: 0,
            peer_record_window: 0,
            sent_ends: LinkedList::new(),
            received_ends: LinkedList::new(),
            local_window: config.receive_window,
            receive_limit: config.receive_window as u64,
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
            received_bytes: LinkedList::new(),
            outgoing_data: LinkedList::new(),
            handshake_frame: None,
            credit_frame: None,
            grant_frame: None,
            fin_frame: None,
            terminal_frame: None,
        })
    }

    fn record_window(window: u32, frame: u32) -> usize {
        (window as usize / frame as usize)
            .saturating_add(64)
            .min(65536)
    }
    pub(crate) fn receive_window(&self) -> u32 {
        self.local_window
    }
    pub(crate) fn receive_record_window(&self) -> usize {
        Self::record_window(self.local_window, self.config.max_frame)
    }
    pub(crate) fn send_unacknowledged_bytes(&self) -> u64 {
        self.sent - self.peer_consumed
    }
    pub(crate) fn credit_blocked(&self) -> bool {
        self.sent == self.peer_limit || self.sent_ends.len() >= self.peer_record_window
    }
    fn acknowledge_peer(&mut self, consumed: u64) {
        self.peer_consumed = self.peer_consumed.max(consumed);
        while self
            .sent_ends
            .front()
            .is_some_and(|end| *end <= self.peer_consumed)
        {
            self.sent_ends.pop_front();
        }
    }

    pub(crate) fn initial_receive_window(&mut self, window: u32) {
        assert_eq!(self.phase, Phase::Idle);
        self.local_window = window.min(self.config.receive_window);
        self.receive_limit = self.local_window as u64;
    }

    pub(crate) fn grant_receive_window(&mut self, window: u32, probe: u64) -> Result<(), Error> {
        if window > self.config.receive_window {
            return Err(Error::InvalidConfig("invalid receive grant"));
        }
        self.local_window = self.local_window.max(window);
        let limit = self
            .consumed
            .checked_add(self.local_window as u64)
            .ok_or(Error::OffsetExhausted)?;
        self.receive_limit = self.receive_limit.max(limit);
        self.grant_frame = Some((self.receive_limit, probe));
        Ok(())
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
            buffered_receive_bytes: (self.received - self.delivered) as usize,
            receive_capacity_bytes: (self.received - self.delivered) as usize,
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
            receive_window: self.local_window,
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
            receive_window: self.local_window,
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
        self.sent_ends.push_back(next);
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
            while self.received_ends.front().is_some_and(|end| *end <= offset) {
                self.received_ends.pop_front();
            }
            self.receive_limit = self
                .receive_limit
                .max(offset.saturating_add(self.local_window as u64));
            if let Some((limit, _)) = &mut self.grant_frame {
                *limit = self.receive_limit;
            }
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
                let bytes = self.received_bytes.pop_front().unwrap();
                let offset = self.delivered;
                self.delivered += bytes.len() as u64;
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

    pub(crate) fn retained_send_records(&self) -> usize {
        self.sent_ends.len()
    }
    pub(crate) fn next_event_data_bytes(&self) -> Option<usize> {
        if self.lifecycle_events.is_empty() && !self.writable_event {
            self.received_bytes.front().map(|bytes| bytes.len())
        } else {
            None
        }
    }

    pub(crate) fn has_frames(&self) -> bool {
        self.terminal_frame.is_some()
            || self.handshake_frame.is_some()
            || self.credit_frame.is_some()
            || self.grant_frame.is_some()
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
                self.peer_limit = *receive_window as u64;
                self.peer_record_window = Self::record_window(*receive_window, *max_frame);
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
                self.peer_limit = *receive_window as u64;
                self.peer_record_window = Self::record_window(*receive_window, *max_frame);
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
                if next > self.receive_limit
                    || self.received_ends.len()
                        >= Self::record_window(self.local_window, self.config.max_frame)
                {
                    return Err(ProtocolError::ReceiveWindowExceeded);
                }
                self.received_bytes.push_back(bytes.clone());
                self.received_ends.push_back(next);
                self.received = next;
            }
            Frame::WindowUpdate { consumed } => {
                self.require_established()?;
                if *consumed > self.dispatched {
                    return Err(ProtocolError::InvalidCredit);
                }
                self.acknowledge_peer(*consumed);
                self.peer_limit = self.peer_limit.max(
                    consumed.saturating_add(self.peer_window.max(self.peer_granted_window) as u64),
                );
                self.notify_writable();
            }
            Frame::WindowGrant {
                consumed, limit, ..
            } => {
                self.require_established()?;
                if *consumed > self.dispatched
                    || *limit < *consumed
                    || limit - consumed > MAX_RECEIVE_WINDOW as u64
                    || *limit < self.sent
                {
                    return Err(ProtocolError::InvalidCredit);
                }
                self.acknowledge_peer(*consumed);
                self.peer_limit = self.peer_limit.max(*limit);
                // Consumption-only updates must also slide the grown window.
                self.peer_granted_window = self.peer_granted_window.max((limit - consumed) as u32);
                self.peer_record_window = self.peer_record_window.max(Self::record_window(
                    (limit - consumed) as u32,
                    self.peer_max_frame,
                ));
                self.notify_writable();
            }
            Frame::PeerGrant { .. }
            | Frame::PeerRequest { .. }
            | Frame::PeerFreeze { .. }
            | Frame::PeerFrozen { .. } => {
                return Err(ProtocolError::UnexpectedFrame);
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

    pub(crate) fn has_pending_open(&self) -> bool {
        self.terminal_frame.is_none() && matches!(self.handshake_frame, Some(Frame::Open { .. }))
    }

    pub(crate) fn next_frame_size_for(&self, stream_id: u64) -> Option<(usize, bool)> {
        let describe = |frame: &Frame| {
            crate::wire::encoded_size(stream_id, frame)
                .ok()
                .map(|size| (size, matches!(frame, Frame::Data { .. })))
        };
        if let Some(frame) = self
            .terminal_frame
            .as_ref()
            .or(self.handshake_frame.as_ref())
        {
            return describe(frame);
        }
        if let Some((limit, probe)) = self.grant_frame {
            return describe(&Frame::WindowGrant {
                consumed: self.consumed,
                limit,
                probe,
            });
        }
        if let Some(consumed) = self.credit_frame {
            return describe(&Frame::WindowUpdate { consumed });
        }
        if let Some(frame) = self.outgoing_data.front() {
            return describe(frame);
        }
        self.fin_frame
            .and_then(|final_offset| describe(&Frame::Fin { final_offset }))
    }

    pub(crate) fn transport_demand(
        &self,
        stream_id: u64,
        overhead: usize,
        maximum: usize,
    ) -> usize {
        let weight = |frame: &Frame| {
            crate::wire::encoded_size(stream_id, frame)
                .unwrap_or(maximum)
                .saturating_add(overhead)
        };
        if let Some(frame) = self.terminal_frame.as_ref() {
            return weight(frame).min(maximum);
        }
        let mut bytes = self.handshake_frame.as_ref().map_or(0, weight);
        if let Some((limit, probe)) = self.grant_frame {
            bytes = bytes.saturating_add(weight(&Frame::WindowGrant {
                consumed: self.consumed,
                limit,
                probe,
            }));
        } else if let Some(consumed) = self.credit_frame {
            bytes = bytes.saturating_add(weight(&Frame::WindowUpdate { consumed }));
        }
        for frame in &self.outgoing_data {
            if bytes >= maximum {
                break;
            }
            bytes = bytes.saturating_add(weight(frame));
        }
        if let Some(final_offset) = self.fin_frame {
            bytes = bytes.saturating_add(weight(&Frame::Fin { final_offset }));
        }
        bytes.min(maximum)
    }
    fn next_frame(&mut self) -> Option<Frame> {
        if let Some(frame) = self.terminal_frame.take() {
            return Some(frame);
        }
        if let Some(frame) = self.handshake_frame.take() {
            return Some(frame);
        }
        if let Some((limit, probe)) = self.grant_frame.take() {
            self.credit_frame = None;
            self.grant_frame = None;
            return Some(Frame::WindowGrant {
                consumed: self.consumed,
                limit,
                probe,
            });
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
        self.peer_limit.saturating_sub(self.sent)
    }

    fn can_send(&self) -> bool {
        self.phase == Phase::Established
            && !self.local_finished
            && self.outgoing_data.len() < self.config.max_pending_frames
            && self.sent_ends.len() < self.peer_record_window
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
            self.grant_frame = None;
            self.writable_event = false;
            self.closed_event = Some(CloseReason::Finished);
            self.received_bytes = LinkedList::new();
            self.received_ends.clear();
            self.sent_ends.clear();
            self.outgoing_data = LinkedList::new();
        }
    }

    fn abort(&mut self, reason: CloseReason, frame: Option<Frame>) {
        self.phase = Phase::Closed;
        self.deadline_ms = None;
        self.received_bytes = LinkedList::new();
        self.received_ends.clear();
        self.sent_ends.clear();
        self.outgoing_data = LinkedList::new();
        self.lifecycle_events = VecDeque::new();
        self.handshake_frame = None;
        self.credit_frame = None;
        self.grant_frame = None;
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

    fn adaptive_pair() -> (Stream, Stream) {
        let config = Config {
            receive_window: 1 << 20,
            max_frame: 16384,
            ..Config::default()
        };
        let mut sender = Stream::new(config).unwrap();
        let mut receiver = Stream::new(config).unwrap();
        sender.initial_receive_window(65536);
        receiver.initial_receive_window(65536);
        sender.open(b"", 0).unwrap();
        receiver
            .receive(&sender.poll_frames(1).pop().unwrap(), 0)
            .unwrap();
        receiver.accept(b"").unwrap();
        sender
            .receive(&receiver.poll_frames(1).pop().unwrap(), 0)
            .unwrap();
        sender.poll_events(8);
        receiver.poll_events(8);
        (sender, receiver)
    }

    #[test]
    fn window_updates_preserve_a_grown_window_across_repeated_transfers() {
        let (mut sender, mut receiver) = adaptive_pair();
        let grown_window = 262144;
        receiver.grant_receive_window(grown_window, 1).unwrap();
        sender
            .receive(&receiver.poll_frames(1).pop().unwrap(), 1)
            .unwrap();
        let mut consumed = 0;
        for round in 0..12 {
            let mut accepted = 0;
            while let SendOutcome::Accepted(count) = sender.send(&[7; 16384]).unwrap() {
                accepted += count;
                if accepted == grown_window as usize {
                    break;
                }
            }
            assert_eq!(
                accepted, grown_window as usize,
                "window fell back in round {round}"
            );
            assert_eq!(sender.send(b"x"), Ok(SendOutcome::WouldBlock));
            for frame in sender.poll_frames(256) {
                receiver.receive(&frame, round + 2).unwrap();
            }
            for event in receiver.poll_events(256) {
                if let Event::Data { offset, bytes } = event {
                    assert_eq!(offset, consumed);
                    assert!(bytes.iter().all(|byte| *byte == 7));
                    consumed += bytes.len() as u64;
                }
            }
            receiver.consume_through(consumed).unwrap();
            let updates = receiver.poll_frames(256);
            assert!(matches!(updates.as_slice(), [Frame::WindowUpdate { .. }]));
            for frame in updates {
                sender.receive(&frame, round + 2).unwrap();
            }
            assert_eq!(sender.snapshot().send_unacknowledged_bytes, 0);
        }
        assert_eq!(consumed, 12 * grown_window as u64);
    }

    #[test]
    fn queued_window_grant_tracks_consumption_before_dispatch() {
        let (mut sender, mut receiver) = adaptive_pair();
        for _ in 0..2 {
            assert_eq!(sender.send(&[7; 16384]), Ok(SendOutcome::Accepted(16384)));
        }
        for frame in sender.poll_frames(8) {
            receiver.receive(&frame, 1).unwrap();
        }
        assert_eq!(receiver.poll_events(8).len(), 2);
        receiver.consume_through(16384).unwrap();
        receiver.grant_receive_window(262144, 7).unwrap();
        receiver.consume_through(32768).unwrap();
        let frames = receiver.poll_frames(8);
        assert_eq!(
            frames,
            [Frame::WindowGrant {
                consumed: 32768,
                limit: 32768 + 262144,
                probe: 7,
            }]
        );
        sender.receive(&frames[0], 2).unwrap();
        assert_eq!(sender.peer_limits().unwrap().receive_window, 65536);
    }

    #[test]
    fn standalone_tiny_data_records_remain_bounded_after_event_transfer() {
        let config = Config {
            receive_window: MAX_RECEIVE_WINDOW,
            ..Config::default()
        };
        let mut a = Stream::new(config).unwrap();
        let mut b = Stream::new(config).unwrap();
        a.open(b"", 0).unwrap();
        b.receive(&a.poll_frames(1).pop().unwrap(), 0).unwrap();
        b.accept(b"").unwrap();
        a.receive(&b.poll_frames(1).pop().unwrap(), 0).unwrap();
        a.poll_events(8);
        b.poll_events(8);
        let records = Stream::record_window(config.receive_window, config.max_frame);
        for _ in 0..records {
            assert_eq!(a.send(b"x"), Ok(SendOutcome::Accepted(1)));
            b.receive(&a.poll_frames(1).pop().unwrap(), 0).unwrap();
        }
        assert_eq!(a.send(b"x"), Ok(SendOutcome::WouldBlock));
        assert_eq!(a.sent_ends.len(), records);
        assert_eq!(b.received_ends.len(), records);
        let mut delivered = 0;
        while delivered < records {
            for event in b.poll_events(256) {
                if let Event::Data { offset, bytes } = event {
                    assert_eq!(offset, delivered as u64);
                    assert_eq!(&*bytes, b"x");
                    delivered += bytes.len();
                }
            }
        }
        assert_eq!(b.snapshot().receive_capacity_bytes, 0);
        assert_eq!(b.received_ends.len(), records);
        assert_eq!(a.send(b"x"), Ok(SendOutcome::WouldBlock));
        b.consume_through(delivered as u64).unwrap();
        a.receive(&b.poll_frames(1).pop().unwrap(), 0).unwrap();
        assert!(a.sent_ends.is_empty());
        assert!(b.received_ends.is_empty());
        assert_eq!(a.send(b"x"), Ok(SendOutcome::Accepted(1)));
    }

    fn near_offset_limit() -> Stream {
        let mut stream = Stream::new(Config::default()).unwrap();
        stream.phase = Phase::Established;
        stream.peer_window = stream.config.receive_window;
        stream.peer_limit = u64::MAX;
        stream.peer_record_window =
            Stream::record_window(stream.config.receive_window, stream.config.max_frame);
        stream.receive_limit = u64::MAX;
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
    fn send_offset_boundary_accepts_only_the_credit_limited_prefix() {
        let mut stream = near_offset_limit();
        assert_eq!(stream.send(b"ab"), Ok(SendOutcome::Accepted(1)));
        let before = stream.snapshot();
        assert_eq!(stream.send(b"b"), Ok(SendOutcome::WouldBlock));
        assert_eq!(stream.snapshot(), before);
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
