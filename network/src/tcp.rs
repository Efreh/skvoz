//! Native stream ownership: partial SEND and write prefixes advance independently.
use crate::{
    NetworkEngine, NetworkError,
    budget::{Budget, Reservation},
};
use skvoz_core::runtime::RuntimeKey;
use skvoz_core::{Event, SendOutcome};
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    net::{Shutdown, TcpStream},
    os::fd::{AsFd, OwnedFd},
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};

trait CorePort {
    fn send(&mut self, key: RuntimeKey, bytes: &[u8]) -> Result<SendOutcome, NetworkError>;
    fn consume(&mut self, key: RuntimeKey, offset: u64) -> Result<(), NetworkError>;
    fn finish(&mut self, key: RuntimeKey) -> Result<bool, NetworkError>;
    fn allowance(&self, peer: skvoz_core::PeerId) -> (usize, usize, usize);
    fn account(&mut self, peer: skvoz_core::PeerId, send: usize, receive: usize, records: usize);
}
impl CorePort for NetworkEngine {
    fn send(&mut self, key: RuntimeKey, bytes: &[u8]) -> Result<SendOutcome, NetworkError> {
        self.runtime.send(key, bytes).map_err(Into::into)
    }
    fn consume(&mut self, key: RuntimeKey, offset: u64) -> Result<(), NetworkError> {
        self.runtime
            .consume_through(key, offset)
            .map_err(Into::into)
    }
    fn finish(&mut self, key: RuntimeKey) -> Result<bool, NetworkError> {
        self.finish_native_tcp(key)
    }
    fn allowance(&self, peer: skvoz_core::PeerId) -> (usize, usize, usize) {
        self.tcp_allowance(peer)
    }
    fn account(&mut self, peer: skvoz_core::PeerId, send: usize, receive: usize, records: usize) {
        self.account_native(peer, send, receive, records)
    }
}
pub(crate) enum Socket {
    Tcp(TcpStream),
    Unix(UnixStream),
}
impl Socket {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(s) => s.read(b),
            Self::Unix(s) => s.read(b),
        }
    }
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(s) => s.write(b),
            Self::Unix(s) => s.write(b),
        }
    }
    fn shutdown_write(&self) -> io::Result<()> {
        match self {
            Self::Tcp(s) => s.shutdown(Shutdown::Write),
            Self::Unix(s) => s.shutdown(Shutdown::Write),
        }
    }
}
struct WritePending {
    _reservation: Reservation,
    bytes: Box<[u8]>,
    cursor: usize,
    offset: u64,
}
pub(crate) struct SetupFailure {
    pub error: NetworkError,
    pub socket: Socket,
    pub reservation: Reservation,
}
pub(crate) struct TcpConnection {
    pub key: RuntimeKey,
    socket: Socket,
    send: Vec<u8>,
    send_cursor: usize,
    receive: VecDeque<WritePending>,
    receive_bytes: usize,
    receive_limit: usize,
    record_limit: usize,
    local_eof: bool,
    finished: bool,
    remote_eof: bool,
    write_shutdown: bool,
    write_deadline: Option<Instant>,
    pub opened: bool,
    pub reply: Option<(u32, crate::SessionId, OwnedFd)>,
    pub success: Vec<u8>,
    pub failure: Vec<u8>,
    pub uploaded: u64,
    pub downloaded: u64,
    core_closed: bool,
    rejected: bool,
    pub rejection: Option<crate::local_api::ApiError>,
    prefix: Vec<u8>,
    prefix_cursor: usize,
    budget: Budget,
    _reservation: Reservation,
}
pub(crate) struct DataFailure {
    pub event: Event,
    pub error: Option<NetworkError>,
}
impl TcpConnection {
    pub fn new(
        key: RuntimeKey,
        socket: Socket,
        budget: &Budget,
        initial: Vec<u8>,
        limits: skvoz_core::ManagerConfig,
    ) -> Result<Self, NetworkError> {
        let reservation = budget.reserve(4096 + initial.capacity(), 4)?;
        Self::with_reservation(key, socket, budget, initial, limits, reservation)
            .map_err(|failure| failure.error)
    }
    pub fn with_reservation(
        key: RuntimeKey,
        socket: Socket,
        budget: &Budget,
        initial: Vec<u8>,
        limits: skvoz_core::ManagerConfig,
        mut reservation: Reservation,
    ) -> Result<Self, SetupFailure> {
        // Explicit TCP buffer sizes disable Linux autotuning. Local Unix
        // sockets retain a bounded queue; TCP uses the kernel's active policy.
        if let Socket::Unix(stream) = &socket
            && skvoz_network_native::configure_socket_buffers(stream.as_fd(), 131072).is_err()
        {
            return Err(SetupFailure {
                error: NetworkError::InvalidState,
                socket,
                reservation,
            });
        }
        // The setup parser has been dropped; its owned reservation transfers once.
        if let Err(error) = reservation.resize(4096 + initial.capacity(), 4) {
            return Err(SetupFailure {
                error,
                socket,
                reservation,
            });
        }
        if initial.len() > 65536 {
            return Err(SetupFailure {
                error: NetworkError::Overloaded,
                socket,
                reservation,
            });
        }
        Ok(Self {
            key,
            socket,
            send: initial,
            send_cursor: 0,
            receive: VecDeque::new(),
            receive_bytes: 0,
            receive_limit: limits
                .receive_budget_per_peer
                .min(limits.stream.receive_window as usize),
            record_limit: limits.receive_records_per_peer(),
            local_eof: false,
            finished: false,
            remote_eof: false,
            write_shutdown: false,
            write_deadline: None,
            opened: false,
            reply: None,
            success: Vec::new(),
            failure: Vec::new(),
            uploaded: 0,
            downloaded: 0,
            core_closed: false,
            rejected: false,
            rejection: None,
            prefix: Vec::new(),
            prefix_cursor: 0,
            budget: budget.clone(),
            _reservation: reservation,
        })
    }
    pub fn core_finished(&self) -> bool {
        self.core_closed || self.rejected
    }
    pub fn gracefully_finished(&self) -> bool {
        self.core_closed && self.receive.is_empty() && self.write_shutdown
    }
    pub fn event(&mut self, event: Event) -> Result<bool, NetworkError> {
        match event {
            Event::Opened { .. } => {
                self.opened = true;
                self.prefix = std::mem::take(&mut self.success);
                if !self.prefix.is_empty() {
                    self.write_deadline = Some(Instant::now() + Duration::from_secs(30));
                }
            }
            event @ Event::Data { .. } => {
                self.try_data(event)
                    .map_err(|failure| failure.error.unwrap_or(NetworkError::Overloaded))?;
            }
            Event::RemoteFinished => self.remote_eof = true,
            Event::Closed {
                reason: skvoz_core::CloseReason::Finished,
            } => {
                self.core_closed = true;
                self.remote_eof = true;
            }
            Event::Rejected { reason } => {
                self.rejection = Some(reject_error(&reason));
                if self.failure.is_empty() {
                    return Ok(false);
                }
                self.opened = true;
                self.rejected = true;
                self.prefix = if self.rejection == Some(crate::local_api::ApiError::Overloaded)
                    && self.failure.starts_with(b"HTTP/")
                {
                    self.failure = Vec::new();
                    crate::proxy::HTTP_OVERLOADED.to_vec()
                } else {
                    std::mem::take(&mut self.failure)
                };
                self.write_deadline = Some(Instant::now() + Duration::from_secs(1));
            }
            Event::Closed { .. } => return Ok(false),
            Event::Writable => {}
            Event::IncomingOpen { .. } => return Err(NetworkError::InvalidState),
        }
        Ok(true)
    }
    /// Shared-budget exhaustion retains the original event for an ordered retry.
    /// Every reservation is atomic; an availability snapshot is not sufficient.
    pub(crate) fn try_data(&mut self, event: Event) -> Result<(), DataFailure> {
        let Event::Data { offset, bytes } = event else {
            return Err(DataFailure {
                event,
                error: Some(NetworkError::InvalidState),
            });
        };
        let failure = |bytes, error| DataFailure {
            event: Event::Data { offset, bytes },
            error,
        };
        if self.remote_eof
            || self.receive.len() >= self.record_limit
            || self
                .receive_bytes
                .checked_add(bytes.len())
                .is_none_or(|n| n > self.receive_limit)
        {
            return Err(failure(bytes, Some(NetworkError::Overloaded)));
        }
        let reservation = match self.budget.reserve(bytes.len(), 1) {
            Ok(reservation) => reservation,
            Err(_) => return Err(failure(bytes, None)),
        };
        if self.receive.len() == self.receive.capacity() {
            let target = (self.receive.capacity().max(2) * 2).min(self.record_limit);
            let old_capacity = self.receive.capacity();
            let base = 4096 + self.send.capacity();
            let node = std::mem::size_of::<WritePending>();
            // Charge both old and replacement arrays during ownership transfer.
            if self
                ._reservation
                .resize(base + (old_capacity + target) * node, 4)
                .is_err()
            {
                return Err(failure(bytes, None));
            }
            let mut replacement = VecDeque::with_capacity(target);
            if replacement.capacity() > target
                && self
                    ._reservation
                    .resize(base + (old_capacity + replacement.capacity()) * node, 4)
                    .is_err()
            {
                drop(replacement);
                // Keeping a conservative reservation is safe and retryable.
                return Err(failure(bytes, None));
            }
            replacement.append(&mut self.receive);
            self.receive = replacement;
            if let Err(error) = self.account_capacity() {
                return Err(failure(bytes, Some(error)));
            }
        }
        self.receive_bytes += bytes.len();
        self.receive.push_back(WritePending {
            _reservation: reservation,
            bytes,
            cursor: 0,
            offset,
        });
        self.write_deadline
            .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
        Ok(())
    }
    pub fn turn(&mut self, engine: &mut NetworkEngine) -> Result<(), NetworkError> {
        self.turn_port(engine)
    }
    fn turn_port(&mut self, engine: &mut impl CorePort) -> Result<(), NetworkError> {
        if !self.opened {
            return Ok(());
        }
        if self.write_deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(NetworkError::Timeout);
        }
        // Native preamble is written before any remote payload, and creates no Core credit.
        if self.prefix_cursor < self.prefix.len() {
            match self.socket.write(&self.prefix[self.prefix_cursor..]) {
                Ok(0) => return Err(NetworkError::InvalidState),
                Ok(n) => {
                    self.prefix_cursor += n;
                    if !self.rejected {
                        self.write_deadline = Some(Instant::now() + Duration::from_secs(30));
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(_) => return Err(NetworkError::InvalidState),
            }
            if self.prefix_cursor < self.prefix.len() {
                return Ok(());
            }
            self.prefix = Vec::new();
            self.write_deadline = None;
        }
        if self.rejected {
            return Err(NetworkError::InvalidState);
        }
        let (_, mut receive_limit, record_limit) = engine.allowance(self.key.stream.peer);
        for _ in 0..record_limit {
            if receive_limit == 0 {
                break;
            }
            let Some(front) = self.receive.front_mut() else {
                break;
            };
            match self.socket.write(
                &front.bytes[front.cursor..front.bytes.len().min(front.cursor + receive_limit)],
            ) {
                Ok(0) => return Err(NetworkError::InvalidState),
                Ok(n) => {
                    front.cursor += n;
                    self.receive_bytes -= n;
                    receive_limit -= n;
                    self.downloaded = self.downloaded.saturating_add(n as u64);
                    engine.account(self.key.stream.peer, 0, n, 1);
                    self.write_deadline = Some(Instant::now() + Duration::from_secs(30));
                    if front.cursor == front.bytes.len() {
                        let consumed = front
                            .offset
                            .checked_add(front.bytes.len() as u64)
                            .ok_or(NetworkError::InvalidState)?;
                        // Retain byte and record promises until the entire owned chunk is released.
                        if !self.core_closed {
                            engine.consume(self.key, consumed)?;
                        }
                        self.receive.pop_front();
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return Err(NetworkError::InvalidState),
            }
        }
        if self.receive.is_empty() {
            self.receive = VecDeque::new();
            self.account_capacity()?;
            self.write_deadline = None;
        }
        if self.remote_eof && self.receive.is_empty() && !self.write_shutdown {
            self.socket
                .shutdown_write()
                .map_err(|_| NetworkError::InvalidState)?;
            self.write_shutdown = true;
        }
        if self.core_closed {
            if self.receive.is_empty() && self.write_shutdown {
                return Err(NetworkError::InvalidState);
            }
            return Ok(());
        }
        if self.send_cursor == self.send.len() {
            self.send.clear();
            self.send_cursor = 0;
            if !self.local_eof {
                // Do not remove bytes from the kernel until their worst-case
                // read capacity is backed. Exhaustion applies backpressure.
                if self
                    ._reservation
                    .resize(
                        4096 + self.send.capacity().max(16384)
                            + self.receive.capacity() * std::mem::size_of::<WritePending>(),
                        4,
                    )
                    .is_err()
                {
                    return Ok(());
                }
                let mut buffer = [0u8; 16384];
                match self.socket.read(&mut buffer) {
                    Ok(0) => {
                        self.local_eof = true;
                        self.send = Vec::new();
                        self.account_capacity()?;
                    }
                    Ok(n) => {
                        // Reserve before growth; reuse already charged capacity during active I/O.
                        let target = self.send.capacity().max(n);
                        self._reservation.resize(
                            4096 + target
                                + self.receive.capacity() * std::mem::size_of::<WritePending>(),
                            4,
                        )?;
                        self.send.reserve_exact(n.saturating_sub(self.send.len()));
                        self.send.extend_from_slice(&buffer[..n]);
                        self.account_capacity()?;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.send = Vec::new();
                        self.account_capacity()?;
                    }
                    Err(_) => return Err(NetworkError::InvalidState),
                }
            }
        }
        let (send_limit, _, record_limit) = engine.allowance(self.key.stream.peer);
        if self.send_cursor < self.send.len() && send_limit > 0 && record_limit > 0 {
            match engine.send(
                self.key,
                &self.send[self.send_cursor..self.send.len().min(self.send_cursor + send_limit)],
            )? {
                SendOutcome::Accepted(n) => {
                    self.send_cursor += n;
                    self.uploaded = self.uploaded.saturating_add(n as u64);
                    engine.account(self.key.stream.peer, n, 0, 1);
                }
                SendOutcome::WouldBlock => {}
            }
        }
        if self.local_eof
            && self.send_cursor == self.send.len()
            && !self.finished
            && engine.allowance(self.key.stream.peer).2 > 0
            && engine.finish(self.key)?
        {
            engine.account(self.key.stream.peer, 0, 0, 1);
            self.finished = true;
        }
        Ok(())
    }
    fn account_capacity(&mut self) -> Result<(), NetworkError> {
        self._reservation.resize(
            4096 + self.send.capacity()
                + self.receive.capacity() * std::mem::size_of::<WritePending>(),
            4,
        )
    }
}

pub(crate) fn reject_error(bytes: &[u8]) -> crate::local_api::ApiError {
    use crate::local_api::ApiError;
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Rejection {
        v: u8,
        #[serde(rename = "type")]
        kind: String,
        error: String,
    }
    let Ok(value) = crate::local_api::parse_strict_json_bounded(bytes, 512) else {
        return ApiError::InvalidRequest;
    };
    let Ok(rejection) = serde_json::from_value::<Rejection>(value) else {
        return ApiError::InvalidRequest;
    };
    if rejection.v != crate::NETWORK_VERSION {
        return ApiError::UnsupportedVersion;
    }
    if rejection.kind != "tcp" {
        return ApiError::InvalidRequest;
    }
    match rejection.error.as_str() {
        "forbidden" => ApiError::Forbidden,
        "overloaded" => ApiError::Overloaded,
        "timeout" => ApiError::Timeout,
        "unsupported_version" => ApiError::UnsupportedVersion,
        "network_unavailable" => ApiError::NetworkUnavailable,
        _ => ApiError::InvalidRequest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skvoz_core::{PeerId, StreamKey};
    struct Port {
        script: VecDeque<usize>,
        sent: Vec<u8>,
        consumed: Vec<u64>,
        finished: bool,
        terminal: usize,
        used: (usize, usize),
    }
    impl CorePort for Port {
        fn send(&mut self, _key: RuntimeKey, bytes: &[u8]) -> Result<SendOutcome, NetworkError> {
            let n = self
                .script
                .pop_front()
                .unwrap_or(bytes.len())
                .min(bytes.len());
            if n == 0 {
                return Ok(SendOutcome::WouldBlock);
            }
            self.sent.extend_from_slice(&bytes[..n]);
            Ok(SendOutcome::Accepted(n))
        }
        fn consume(&mut self, _key: RuntimeKey, offset: u64) -> Result<(), NetworkError> {
            self.consumed.push(offset);
            Ok(())
        }
        fn finish(&mut self, _key: RuntimeKey) -> Result<bool, NetworkError> {
            if self.terminal >= 4 {
                return Ok(false);
            }
            self.terminal += 1;
            self.finished = true;
            Ok(true)
        }
        fn allowance(&self, _peer: PeerId) -> (usize, usize, usize) {
            let bytes = 32768usize.saturating_sub(self.used.0);
            (bytes, bytes, 16usize.saturating_sub(self.used.1))
        }
        fn account(&mut self, _peer: PeerId, send: usize, receive: usize, records: usize) {
            self.used.0 += send + receive;
            self.used.1 += records;
        }
    }
    fn port() -> Port {
        Port {
            script: VecDeque::new(),
            sent: Vec::new(),
            consumed: Vec::new(),
            finished: false,
            terminal: 0,
            used: (0, 0),
        }
    }
    fn key() -> RuntimeKey {
        RuntimeKey {
            epoch: 1,
            incarnation: 1,
            stream: StreamKey {
                peer: PeerId(0),
                stream_id: 1,
            },
        }
    }
    fn pair(initial: Vec<u8>) -> (TcpConnection, UnixStream) {
        let (a, b) = UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.set_nonblocking(true).unwrap();
        let mut c = TcpConnection::new(
            key(),
            Socket::Unix(a),
            &Budget::new(1_000_000, 256),
            initial,
            crate::config::Limits::canonical(crate::config::Role::Client)
                .manager(crate::config::Role::Client),
        )
        .unwrap();
        c.opened = true;
        (c, b)
    }
    #[test]
    fn fin_waits_for_and_consumes_the_shared_peer_record_quantum() {
        let (mut connection, host) = pair(Vec::new());
        host.shutdown(std::net::Shutdown::Write).unwrap();
        let mut port = port();
        port.used.1 = 16;
        connection.turn_port(&mut port).unwrap();
        assert!(connection.local_eof);
        assert!(!port.finished);
        port.used = (0, 15);
        connection.turn_port(&mut port).unwrap();
        assert!(port.finished);
        assert_eq!(port.used.1, 16);
        connection.turn_port(&mut port).unwrap();
        assert_eq!(port.used.1, 16);
    }
    #[test]
    fn tiny_global_send_budget_backpressures_without_reading_or_canceling_other_socket() {
        let budget = Budget::new(8192 + 16384, 32);
        let make = || {
            let (a, b) = UnixStream::pair().unwrap();
            a.set_nonblocking(true).unwrap();
            b.set_nonblocking(true).unwrap();
            let mut c = TcpConnection::new(
                key(),
                Socket::Unix(a),
                &budget,
                Vec::new(),
                crate::config::Limits::canonical(crate::config::Role::Client)
                    .manager(crate::config::Role::Client),
            )
            .unwrap();
            c.opened = true;
            (c, b)
        };
        let (mut first, mut a) = make();
        let (mut second, mut b) = make();
        a.write_all(&vec![1; 16384]).unwrap();
        b.write_all(b"other connection").unwrap();
        let mut one = port();
        one.script.push_back(0);
        first.turn_port(&mut one).unwrap();
        assert_eq!(budget.usage().bytes, 8192 + 16384);
        let mut two = port();
        second.turn_port(&mut two).unwrap();
        assert!(two.sent.is_empty());
        first.turn_port(&mut one).unwrap();
        one.used = (0, 0);
        first.turn_port(&mut one).unwrap();
        assert_eq!(budget.usage().bytes, 8192);
        second.turn_port(&mut two).unwrap();
        assert_eq!(two.sent, b"other connection");
        two.used = (0, 0);
        second.turn_port(&mut two).unwrap();
        assert_eq!(budget.usage().bytes, 8192);
        drop((first, second));
        assert_eq!(budget.usage(), crate::budget::Usage::default());
    }
    #[test]
    fn larger_read_after_small_retained_send_is_charged_and_released_when_idle() {
        let (mut c, mut socket) = pair(vec![7; 8]);
        let mut port = port();
        c.turn_port(&mut port).unwrap();
        socket.write_all(&vec![9; 16000]).unwrap();
        port.used = (0, 0);
        c.turn_port(&mut port).unwrap();
        assert_eq!(port.sent.len(), 16008);
        assert_eq!(c._reservation.usage().bytes, 4096 + c.send.capacity());
        port.used = (0, 0);
        c.turn_port(&mut port).unwrap();
        assert_eq!(c.send.capacity(), 0);
        assert_eq!(c._reservation.usage().bytes, 4096);
    }
    #[test]
    fn rejected_stream_drains_native_failure_after_core_retirement() {
        let (mut connection, mut socket) = pair(Vec::new());
        connection.opened = false;
        let failure = vec![5, 1, 0, 1, 0, 0, 0, 0, 0, 0];
        connection.failure = failure.clone();
        assert!(
            connection
                .event(Event::Rejected {
                    reason: br#"{"v":4,"type":"tcp","error":"forbidden"}"#
                        .to_vec()
                        .into_boxed_slice(),
                })
                .unwrap()
        );
        assert!(connection.core_finished());
        let mut port = port();
        assert!(connection.turn_port(&mut port).is_err());
        let mut received = [0; 10];
        socket.read_exact(&mut received).unwrap();
        assert_eq!(received.as_slice(), failure);
        assert!(port.sent.is_empty());
        assert!(port.consumed.is_empty());
        assert!(!port.finished);
    }
    #[test]
    fn partial_core_acceptance_retains_exact_suffix_and_fin_waits() {
        let initial: Vec<u8> = (0..241).map(|n| n as u8).collect();
        let (mut c, socket) = pair(initial.clone());
        socket.shutdown(Shutdown::Write).unwrap();
        let mut port = port();
        port.script = VecDeque::from([1, 0, 3, 0, 17, 2, 0, 218]);
        for _ in 0..16 {
            port.used = (0, 0);
            c.turn_port(&mut port).unwrap();
            if port.finished {
                break;
            }
            assert!(port.sent.len() <= initial.len());
        }
        assert_eq!(port.sent, initial);
        assert_eq!(c.uploaded, 241);
        assert!(port.finished);
    }
    #[test]
    fn actual_shared_budget_shortage_preserves_box_for_exact_retry_and_fin() {
        let (mut old, _stalled) = pair(Vec::new());
        let budget = old.budget.clone();
        old.event(Event::Data {
            offset: 0,
            bytes: vec![7; 65536].into(),
        })
        .unwrap();
        let mut old_port = port();
        old.turn_port(&mut old_port).unwrap();
        assert!(old.downloaded > 0 && old.downloaded < 65536);
        let (socket, mut owner) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        owner.set_nonblocking(true).unwrap();
        let mut next = TcpConnection::new(
            key(),
            Socket::Unix(socket),
            &budget,
            Vec::new(),
            crate::config::Limits::canonical(crate::config::Role::Client)
                .manager(crate::config::Role::Client),
        )
        .unwrap();
        next.opened = true;
        let expected = (0..4093).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let payload: Box<[u8]> = expected.clone().into();
        let pointer = payload.as_ptr();
        let mut event = Event::Data {
            offset: 0,
            bytes: payload,
        };
        // An observed free budget can be occupied before the atomic reserve.
        assert!(budget.available().bytes > expected.len());
        let blocker = budget.reserve(budget.available().bytes, 1).unwrap();
        for _ in 0..2 {
            let failure = next
                .try_data(event)
                .expect_err("actual reservation must fail");
            assert!(failure.error.is_none());
            event = failure.event;
            let Event::Data { bytes, .. } = &event else {
                panic!("original DATA required")
            };
            assert_eq!(bytes.as_ptr(), pointer);
            assert_eq!(bytes.as_ref(), expected);
            assert!(next.receive.is_empty());
        }
        assert!(
            !old.event(Event::Closed {
                reason: skvoz_core::CloseReason::Cancelled
            })
            .unwrap()
        );
        drop(old);
        drop(blocker);
        assert!(next.try_data(event).is_ok());
        next.event(Event::RemoteFinished).unwrap();
        owner.shutdown(Shutdown::Write).unwrap();
        let mut out = Vec::new();
        let mut p = port();
        for _ in 0..8 {
            p.used = (0, 0);
            next.turn_port(&mut p).unwrap();
            let mut buffer = [0; 8192];
            match owner.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&buffer[..n]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("{error}"),
            }
        }
        assert_eq!(out, expected);
        assert_eq!(p.consumed, [4093]);
        assert!(next.write_shutdown);
        assert!(p.finished);
        drop(next);
        assert_eq!(budget.usage(), crate::budget::Usage::default());
    }
    #[test]
    fn partial_write_and_terminal_host_retention_keep_the_full_payload_charged() {
        let (mut connection, _owner) = pair(Vec::new());
        let budget = connection.budget.clone();
        let baseline = budget.usage();
        connection
            .event(Event::Data {
                offset: 0,
                bytes: vec![7; 65536].into(),
            })
            .unwrap();
        assert_eq!(
            connection
                .receive
                .front()
                .unwrap()
                ._reservation
                .usage()
                .bytes,
            65536
        );
        let mut port = port();
        connection.turn_port(&mut port).unwrap();
        assert!(connection.downloaded > 0 && connection.downloaded < 65536);
        assert!(port.consumed.is_empty());
        assert_eq!(
            connection
                .receive
                .front()
                .unwrap()
                ._reservation
                .usage()
                .bytes,
            65536
        );
        assert!(
            !connection
                .event(Event::Closed {
                    reason: skvoz_core::CloseReason::Cancelled
                })
                .unwrap()
        );
        let retained = budget.usage();
        assert!(retained.bytes >= baseline.bytes + 65536);
        let (socket, _new_owner) = UnixStream::pair().unwrap();
        socket.set_nonblocking(true).unwrap();
        let mut next = TcpConnection::new(
            key(),
            Socket::Unix(socket),
            &budget,
            Vec::new(),
            crate::config::Limits::canonical(crate::config::Role::Client)
                .manager(crate::config::Role::Client),
        )
        .unwrap();
        next.event(Event::Data {
            offset: 0,
            bytes: vec![9; 65536].into(),
        })
        .unwrap();
        assert!(budget.usage().bytes >= retained.bytes + 65536);
        drop(connection);
        assert!(budget.usage().bytes >= 65536);
        drop(next);
        assert_eq!(budget.usage(), crate::budget::Usage::default());
    }
    #[test]
    fn copying_remote_data_never_consumes_native_write_grants_exact_prefix() {
        let (mut c, mut socket) = pair(Vec::new());
        let mut port = port();
        let bytes: Vec<u8> = (0..65536).map(|n| (n % 251) as u8).collect();
        c.event(Event::Data {
            offset: 0,
            bytes: bytes.clone().into_boxed_slice(),
        })
        .unwrap();
        assert!(port.consumed.is_empty());
        assert_eq!(c.downloaded, 0);
        let mut received = Vec::new();
        for _ in 0..8 {
            port.used = (0, 0);
            c.turn_port(&mut port).unwrap();
            let mut chunk = [0u8; 32768];
            loop {
                match socket.read(&mut chunk) {
                    Ok(n) if n > 0 => received.extend_from_slice(&chunk[..n]),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    other => panic!("unexpected native read: {other:?}"),
                }
            }
            if received.len() == bytes.len() {
                break;
            }
        }
        assert_eq!(received, bytes);
        assert_eq!(port.consumed.last(), Some(&65536));
        assert_eq!(c.downloaded, 65536);
        assert!(port.consumed.windows(2).all(|p| p[0] < p[1]));
    }
    #[test]
    fn remote_halfclose_waits_for_native_payload_then_preserves_reverse_direction() {
        let (mut c, mut socket) = pair(Vec::new());
        let mut port = port();
        c.event(Event::Data {
            offset: 0,
            bytes: b"payload".to_vec().into_boxed_slice(),
        })
        .unwrap();
        c.event(Event::RemoteFinished).unwrap();
        let mut byte = [0];
        assert_eq!(
            socket.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        c.turn_port(&mut port).unwrap();
        let mut data = [0; 7];
        socket.read_exact(&mut data).unwrap();
        assert_eq!(&data, b"payload");
        assert_eq!(socket.read(&mut byte).unwrap(), 0);
        socket.write_all(b"reply").unwrap();
        c.turn_port(&mut port).unwrap();
        assert_eq!(&port.sent, b"reply");
        assert!(!port.finished);
    }
    #[test]
    fn graceful_core_closed_retains_already_received_native_bytes() {
        let (mut c, mut socket) = pair(Vec::new());
        let mut port = port();
        c.event(Event::Data {
            offset: 0,
            bytes: b"tail".to_vec().into_boxed_slice(),
        })
        .unwrap();
        assert!(
            c.event(Event::Closed {
                reason: skvoz_core::CloseReason::Finished
            })
            .unwrap()
        );
        assert_eq!(c.turn_port(&mut port), Err(NetworkError::InvalidState));
        let mut tail = [0; 4];
        socket.read_exact(&mut tail).unwrap();
        assert_eq!(&tail, b"tail");
        assert!(c.gracefully_finished());
        assert!(port.consumed.is_empty());
    }
    #[test]
    fn repeated_or_early_remote_fin_data_is_terminal() {
        let (mut c, _) = pair(Vec::new());
        c.event(Event::RemoteFinished).unwrap();
        assert!(
            c.event(Event::Data {
                offset: 0,
                bytes: b"late".to_vec().into_boxed_slice()
            })
            .is_err()
        );
    }
    #[test]
    fn exhausted_receive_record_quantum_prevents_an_extra_send() {
        let (mut c, _socket) = pair(b"send-after-writes".to_vec());
        let mut port = port();
        for offset in 0..16 {
            c.event(Event::Data {
                offset,
                bytes: vec![1].into_boxed_slice(),
            })
            .unwrap();
        }
        c.turn_port(&mut port).unwrap();
        assert_eq!(port.used.1, 16);
        assert!(port.sent.is_empty());
        assert_eq!(c.send_cursor, 0);
        port.used = (0, 0);
        c.turn_port(&mut port).unwrap();
        assert_eq!(port.sent, b"send-after-writes");
    }
    #[test]
    fn terminal_quantum_defers_fin_without_losing_native_eof() {
        let mut connections = Vec::new();
        let mut owners = Vec::new();
        let mut port = port();
        for _ in 0..5 {
            let (connection, owner) = pair(Vec::new());
            owner.shutdown(Shutdown::Write).unwrap();
            connections.push(connection);
            owners.push(owner);
        }
        for connection in &mut connections {
            connection.turn_port(&mut port).unwrap();
        }
        assert_eq!(port.terminal, 4);
        assert!(connections[..4].iter().all(|c| c.finished));
        assert!(connections[4].local_eof && !connections[4].finished);
        port.terminal = 0;
        port.used = (0, 0);
        connections[4].turn_port(&mut port).unwrap();
        assert!(connections[4].finished);
        assert_eq!(port.terminal, 1);
    }
}
