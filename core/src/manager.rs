//! Multi-peer ownership, admission and active-work scheduling without I/O.
use crate::{
    CloseReason, Config, Error, Event, Frame, MAX_BATCH_EVENTS, SendOutcome, Snapshot, State,
    Stream,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PeerId(pub u64);
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StreamKey {
    pub peer: PeerId,
    pub stream_id: u64,
}
#[derive(Debug)]
pub struct ManagedEvent {
    pub key: StreamKey,
    pub event: Event,
}
#[derive(Debug)]
pub struct RoutedFrame {
    pub key: StreamKey,
    pub frame: Frame,
}

#[derive(Clone, Copy, Debug)]
pub struct ManagerConfig {
    pub stream: Config,
    pub max_peers: usize,
    pub max_streams: usize,
    pub max_streams_per_peer: usize,
    pub receive_budget: usize,
    pub receive_budget_per_peer: usize,
    pub send_budget: usize,
    pub send_budget_per_peer: usize,
}
impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            stream: Config {
                receive_window: 8192,
                max_frame: 1024,
                max_pending_frames: 8,
                max_metadata: 256,
                open_timeout_ms: 5000,
            },
            max_peers: 128,
            max_streams: 8192,
            max_streams_per_peer: 128,
            receive_budget: 64 * 1024 * 1024,
            receive_budget_per_peer: 2 * 1024 * 1024,
            send_budget: 8 * 1024 * 1024,
            send_budget_per_peer: 256 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagerError {
    Stream(Error),
    InvalidConfig,
    UnknownPeer,
    UnknownStream,
    Admission,
    IdentityExhausted,
    InvalidOrigin,
    TransportLost,
    PeerBusy,
}
impl fmt::Display for ManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Core manager error: {self:?}")
    }
}
impl std::error::Error for ManagerError {}
impl From<Error> for ManagerError {
    fn from(e: Error) -> Self {
        Self::Stream(e)
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Resources {
    pub peers: usize,
    pub streams: usize,
    pub reserved_receive_bytes: usize,
    pub pending_send_bytes: usize,
    pub buffered_receive_bytes: usize,
    pub receive_capacity_bytes: usize,
    pub receive_unconsumed_bytes: u64,
    pub ready_output_peers: usize,
    pub ready_event_peers: usize,
    pub pending_rejections: usize,
}
struct Entry {
    stream: Stream,
    deadline: Option<u64>,
}
struct Peer {
    local_bit: u64,
    local_sequence: u64,
    remote_sequence: u64,
    count: usize,
    pending_send: usize,
    output: VecDeque<u64>,
    output_set: BTreeSet<u64>,
    events: VecDeque<u64>,
    event_set: BTreeSet<u64>,
    rejection: Option<RoutedFrame>,
    reject_next: bool,
    failed: bool,
}
/// A single owner drives this value. Returned buffers transfer to its caller.
pub struct Manager {
    config: ManagerConfig,
    peers: BTreeMap<PeerId, Peer>,
    streams: BTreeMap<StreamKey, Entry>,
    output: VecDeque<PeerId>,
    output_set: BTreeSet<PeerId>,
    events: VecDeque<PeerId>,
    event_set: BTreeSet<PeerId>,
    deadlines: BTreeSet<(u64, StreamKey)>,
    budget_waiters: BTreeSet<StreamKey>,
    pending_send: usize,
    wake_cursor: Option<StreamKey>,
    now: u64,
    failed: bool,
}
impl Manager {
    pub fn new(config: ManagerConfig) -> Result<Self, ManagerError> {
        config.stream.validate()?;
        if config.max_peers == 0
            || config.max_streams == 0
            || config.max_streams_per_peer == 0
            || config.receive_budget < config.stream.receive_window as usize
            || config.receive_budget_per_peer < config.stream.receive_window as usize
            || config.send_budget == 0
            || config.send_budget_per_peer == 0
            || config
                .max_streams
                .checked_mul(config.stream.receive_window as usize)
                .is_none()
            || config
                .max_streams_per_peer
                .checked_mul(config.stream.receive_window as usize)
                .is_none()
        {
            return Err(ManagerError::InvalidConfig);
        }
        Ok(Self {
            config,
            peers: BTreeMap::new(),
            streams: BTreeMap::new(),
            output: VecDeque::new(),
            output_set: BTreeSet::new(),
            events: VecDeque::new(),
            event_set: BTreeSet::new(),
            deadlines: BTreeSet::new(),
            budget_waiters: BTreeSet::new(),
            pending_send: 0,
            wake_cursor: None,
            now: 0,
            failed: false,
        })
    }
    pub fn config(&self) -> ManagerConfig {
        self.config
    }
    /// The host must bind this registration to an authenticated peer session.
    pub fn register_peer(
        &mut self,
        id: PeerId,
        local_origin_bit: bool,
    ) -> Result<(), ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        if self.peers.contains_key(&id) || self.peers.len() >= self.config.max_peers {
            return Err(ManagerError::Admission);
        }
        self.peers.insert(
            id,
            Peer {
                local_bit: u64::from(local_origin_bit),
                local_sequence: 0,
                remote_sequence: 0,
                count: 0,
                pending_send: 0,
                output: VecDeque::new(),
                output_set: BTreeSet::new(),
                events: VecDeque::new(),
                event_set: BTreeSet::new(),
                rejection: None,
                reject_next: true,
                failed: false,
            },
        );
        Ok(())
    }
    pub fn remove_peer(&mut self, id: PeerId) -> Result<(), ManagerError> {
        let peer = self.peers.get(&id).ok_or(ManagerError::UnknownPeer)?;
        if peer.count != 0 || peer.rejection.is_some() || !peer.failed {
            return Err(ManagerError::PeerBusy);
        }
        self.peers.remove(&id);
        self.output.retain(|p| *p != id);
        self.output_set.remove(&id);
        self.events.retain(|p| *p != id);
        self.event_set.remove(&id);
        Ok(())
    }
    fn available_admission(&self, id: PeerId) -> Result<(), ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&id).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        let window = self.config.stream.receive_window as usize;
        if self.streams.len() >= self.config.max_streams
            || p.count >= self.config.max_streams_per_peer
            || self.streams.len() >= self.config.receive_budget / window
            || p.count >= self.config.receive_budget_per_peer / window
        {
            return Err(ManagerError::Admission);
        }
        Ok(())
    }
    pub fn open(
        &mut self,
        peer: PeerId,
        metadata: &[u8],
        now: u64,
    ) -> Result<StreamKey, ManagerError> {
        self.tick(now)?;
        self.available_admission(peer)?;
        let p = self.peers.get(&peer).unwrap();
        let seq = p
            .local_sequence
            .checked_add(1)
            .ok_or(ManagerError::IdentityExhausted)?;
        let id = seq
            .checked_mul(2)
            .and_then(|n| n.checked_add(p.local_bit))
            .ok_or(ManagerError::IdentityExhausted)?;
        let mut stream = Stream::new(self.config.stream)?;
        stream.open(metadata, now)?;
        let key = StreamKey {
            peer,
            stream_id: id,
        };
        self.insert(key, stream, now)?;
        self.peers.get_mut(&peer).unwrap().local_sequence = seq;
        Ok(key)
    }
    fn insert(&mut self, key: StreamKey, stream: Stream, now: u64) -> Result<(), ManagerError> {
        let deadline = now
            .checked_add(self.config.stream.open_timeout_ms)
            .ok_or(Error::InvalidTime)?;
        self.deadlines.insert((deadline, key));
        self.streams.insert(
            key,
            Entry {
                stream,
                deadline: Some(deadline),
            },
        );
        self.peers.get_mut(&key.peer).unwrap().count += 1;
        self.refresh(key, 0);
        Ok(())
    }
    /// Unknown non-OPEN packets are ignored. Excess OPEN receives at most one
    /// pending REJECT per peer; additional overload requests time out remotely.
    pub fn receive(&mut self, key: StreamKey, frame: &Frame, now: u64) -> Result<(), ManagerError> {
        self.tick(now)?;
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&key.peer).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        if !self.streams.contains_key(&key) {
            if !matches!(frame, Frame::Open { .. }) {
                return Ok(());
            }
            let seq = key.stream_id >> 1;
            if seq == 0 || key.stream_id & 1 == p.local_bit {
                return Err(ManagerError::InvalidOrigin);
            }
            if seq <= p.remote_sequence {
                return Ok(());
            }
            self.peers.get_mut(&key.peer).unwrap().remote_sequence = seq;
            if self.available_admission(key.peer).is_err() {
                let p = self.peers.get_mut(&key.peer).unwrap();
                if p.rejection.is_none() {
                    p.rejection = Some(RoutedFrame {
                        key,
                        frame: Frame::Reject {
                            reason: Box::new([]),
                        },
                    });
                }
                self.ready_peer(key.peer, true);
                return Ok(());
            }
            self.insert(key, Stream::new(self.config.stream)?, now)?;
        }
        let before = self.streams[&key].stream.snapshot().pending_send_bytes;
        let result = self
            .streams
            .get_mut(&key)
            .unwrap()
            .stream
            .receive(frame, now);
        self.refresh(key, before);
        result.map_err(Into::into)
    }
    fn operation(
        &mut self,
        key: StreamKey,
        f: impl FnOnce(&mut Stream) -> Result<(), Error>,
    ) -> Result<(), ManagerError> {
        let e = self
            .streams
            .get_mut(&key)
            .ok_or(ManagerError::UnknownStream)?;
        let before = e.stream.snapshot().pending_send_bytes;
        let result = f(&mut e.stream);
        self.refresh(key, before);
        result.map_err(Into::into)
    }
    pub fn accept(&mut self, key: StreamKey, metadata: &[u8]) -> Result<(), ManagerError> {
        self.operation(key, |s| s.accept(metadata))
    }
    pub fn reject(&mut self, key: StreamKey, reason: &[u8]) -> Result<(), ManagerError> {
        self.operation(key, |s| s.reject(reason))
    }
    pub fn finish(&mut self, key: StreamKey) -> Result<(), ManagerError> {
        self.operation(key, Stream::finish)
    }
    pub fn consume_through(&mut self, key: StreamKey, offset: u64) -> Result<(), ManagerError> {
        self.operation(key, |s| s.consume_through(offset))
    }
    pub fn close(&mut self, key: StreamKey, reason: CloseReason) -> Result<(), ManagerError> {
        self.operation(key, |s| s.close(reason))
    }
    pub fn send(&mut self, key: StreamKey, bytes: &[u8]) -> Result<SendOutcome, ManagerError> {
        if self.failed {
            return Err(ManagerError::TransportLost);
        }
        let p = self.peers.get(&key.peer).ok_or(ManagerError::UnknownPeer)?;
        if p.failed {
            return Err(ManagerError::TransportLost);
        }
        let e = self
            .streams
            .get_mut(&key)
            .ok_or(ManagerError::UnknownStream)?;
        let before = e.stream.snapshot().pending_send_bytes;
        let room = (self.config.send_budget - self.pending_send)
            .min(self.config.send_budget_per_peer - p.pending_send);
        if room == 0
            && !bytes.is_empty()
            && matches!(e.stream.state(), State::Open | State::HalfClosedRemote)
        {
            e.stream.budget_blocked();
            self.budget_waiters.insert(key);
            return Ok(SendOutcome::WouldBlock);
        }
        let result = e.stream.send(&bytes[..bytes.len().min(room)]);
        self.refresh(key, before);
        result.map_err(Into::into)
    }
    fn ready_peer(&mut self, id: PeerId, output: bool) {
        let (q, set) = if output {
            (&mut self.output, &mut self.output_set)
        } else {
            (&mut self.events, &mut self.event_set)
        };
        if set.insert(id) {
            q.push_back(id);
        }
    }
    fn refresh(&mut self, key: StreamKey, before: usize) {
        let e = self.streams.get_mut(&key).unwrap();
        let after = e.stream.snapshot().pending_send_bytes;
        let opening = matches!(e.stream.state(), State::Idle | State::Opening(_));
        if !opening && let Some(d) = e.deadline.take() {
            self.deadlines.remove(&(d, key));
        }
        let output = e.stream.has_frames();
        let events = e.stream.has_events();
        let p = self.peers.get_mut(&key.peer).unwrap();
        p.pending_send = p.pending_send - before + after;
        self.pending_send = self.pending_send - before + after;
        if output && p.output_set.insert(key.stream_id) {
            p.output.push_back(key.stream_id);
        }
        if events && p.event_set.insert(key.stream_id) {
            p.events.push_back(key.stream_id);
        }
        if output {
            self.ready_peer(key.peer, true);
        }
        if events {
            self.ready_peer(key.peer, false);
        }
        if after < before {
            self.wake_budget_waiters();
        }
    }
    fn wake_budget_waiters(&mut self) {
        if self.pending_send >= self.config.send_budget {
            return;
        }
        let mut keys: Vec<_> = match self.wake_cursor {
            Some(cursor) => self
                .budget_waiters
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(32)
                .copied()
                .collect(),
            None => Vec::new(),
        };
        if keys.len() < 32 {
            keys.extend(self.budget_waiters.iter().take(32 - keys.len()).copied());
        }
        self.wake_cursor = keys.last().copied();
        for key in keys {
            if self.peers[&key.peer].pending_send >= self.config.send_budget_per_peer {
                continue;
            }
            self.budget_waiters.remove(&key);
            if let Some(e) = self.streams.get_mut(&key) {
                e.stream.budget_writable();
                let p = self.peers.get_mut(&key.peer).unwrap();
                if e.stream.has_events() && p.event_set.insert(key.stream_id) {
                    p.events.push_back(key.stream_id);
                }
                if e.stream.has_events() {
                    self.ready_peer(key.peer, false);
                }
            }
        }
    }
    fn reap(&mut self, key: StreamKey) {
        if self.streams.get(&key).is_some_and(|e| {
            e.stream.state() == State::Closed && !e.stream.has_frames() && !e.stream.has_events()
        }) {
            let e = self.streams.remove(&key).unwrap();
            if let Some(d) = e.deadline {
                self.deadlines.remove(&(d, key));
            }
            let p = self.peers.get_mut(&key.peer).unwrap();
            p.count -= 1;
            p.output.retain(|id| *id != key.stream_id);
            p.output_set.remove(&key.stream_id);
            p.events.retain(|id| *id != key.stream_id);
            p.event_set.remove(&key.stream_id);
            self.budget_waiters.remove(&key);
        }
    }
    /// One frame per ready peer, rotating its ready streams. Caller must
    /// transmit in returned order and bound its own retained frame batches.
    pub fn poll_frames(&mut self, max: usize) -> Vec<RoutedFrame> {
        let mut result = Vec::new();
        while result.len() < max.min(MAX_BATCH_EVENTS) {
            let Some(peer) = self.output.pop_front() else {
                break;
            };
            self.output_set.remove(&peer);
            if self.peers[&peer].rejection.is_some()
                && (self.peers[&peer].output.is_empty() || self.peers[&peer].reject_next)
            {
                let p = self.peers.get_mut(&peer).unwrap();
                result.push(p.rejection.take().unwrap());
                p.reject_next = false;
            } else {
                let p = self.peers.get_mut(&peer).unwrap();
                p.reject_next = true;
                if let Some(id) = p.output.pop_front() {
                    p.output_set.remove(&id);
                    let key = StreamKey {
                        peer,
                        stream_id: id,
                    };
                    if let Some(e) = self.streams.get_mut(&key) {
                        let before = e.stream.snapshot().pending_send_bytes;
                        if let Some(frame) = e.stream.poll_frames(1).pop() {
                            result.push(RoutedFrame { key, frame });
                        }
                        self.refresh(key, before);
                        self.reap(key);
                    }
                }
            }
            let p = &self.peers[&peer];
            if !p.output.is_empty() || p.rejection.is_some() {
                self.ready_peer(peer, true);
            }
        }
        result
    }
    pub fn poll_events(&mut self, max: usize) -> Vec<ManagedEvent> {
        self.wake_budget_waiters();
        let mut result = Vec::new();
        while result.len() < max.min(MAX_BATCH_EVENTS) {
            let Some(peer) = self.events.pop_front() else {
                break;
            };
            self.event_set.remove(&peer);
            let p = self.peers.get_mut(&peer).unwrap();
            if let Some(id) = p.events.pop_front() {
                p.event_set.remove(&id);
                let key = StreamKey {
                    peer,
                    stream_id: id,
                };
                if let Some(e) = self.streams.get_mut(&key) {
                    let before = e.stream.snapshot().pending_send_bytes;
                    if let Some(event) = e.stream.poll_events(1).pop() {
                        result.push(ManagedEvent { key, event });
                    }
                    self.refresh(key, before);
                    self.reap(key);
                }
            }
            if !self.peers[&peer].events.is_empty() {
                self.ready_peer(peer, false);
            }
        }
        result
    }
    pub fn tick(&mut self, now: u64) -> Result<(), ManagerError> {
        if now < self.now {
            return Err(Error::InvalidTime.into());
        }
        self.now = now;
        while let Some(&(deadline, key)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            self.deadlines.remove(&(deadline, key));
            if let Some(e) = self.streams.get_mut(&key) {
                let before = e.stream.snapshot().pending_send_bytes;
                e.deadline = None;
                e.stream.tick(now)?;
                self.refresh(key, before);
            }
        }
        Ok(())
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|(d, _)| *d)
    }
    pub fn transport_lost(&mut self) {
        if self.failed {
            return;
        }
        self.failed = true;
        let peers: Vec<_> = self.peers.keys().copied().collect();
        for peer in peers {
            self.peer_lost(peer);
        }
    }
    pub fn protocol_error(&mut self, peer: PeerId) {
        let keys: Vec<_> = self
            .streams
            .range(
                StreamKey { peer, stream_id: 0 }..=StreamKey {
                    peer,
                    stream_id: u64::MAX,
                },
            )
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            let _ = self.close(key, CloseReason::ProtocolError);
        }
    }
    pub fn peer_lost(&mut self, peer: PeerId) {
        if let Some(p) = self.peers.get_mut(&peer) {
            p.failed = true;
            p.rejection = None;
        }
        let keys: Vec<_> = self
            .streams
            .range(
                StreamKey { peer, stream_id: 0 }..=StreamKey {
                    peer,
                    stream_id: u64::MAX,
                },
            )
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            let e = self.streams.get_mut(&key).unwrap();
            let before = e.stream.snapshot().pending_send_bytes;
            e.stream.transport_lost();
            self.refresh(key, before);
        }
    }
    pub fn snapshot(&self, key: StreamKey) -> Option<Snapshot> {
        self.streams.get(&key).map(|e| e.stream.snapshot())
    }
    /// Resource inspection is explicit and O(stream count), never a driver turn.
    pub fn resources(&self) -> Resources {
        let mut r = Resources {
            peers: self.peers.len(),
            streams: self.streams.len(),
            reserved_receive_bytes: self.streams.len() * self.config.stream.receive_window as usize,
            pending_send_bytes: self.pending_send,
            ready_output_peers: self.output_set.len(),
            ready_event_peers: self.event_set.len(),
            pending_rejections: self
                .peers
                .values()
                .filter(|p| p.rejection.is_some())
                .count(),
            ..Resources::default()
        };
        for e in self.streams.values() {
            let s = e.stream.snapshot();
            r.buffered_receive_bytes += s.buffered_receive_bytes;
            r.receive_capacity_bytes += s.receive_capacity_bytes;
            r.receive_unconsumed_bytes += s.receive_unconsumed_bytes;
        }
        r
    }
}
