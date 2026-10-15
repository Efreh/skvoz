//! Continuous delivery allocator shared by all transport lanes.
use super::*;

const DOMAIN_RECORDS: u64 = 2048;
const ACTIVE_BYTES: u64 = 256 * 1024;
const ACTIVE_RECORDS: u64 = 32;

impl NatsRuntime {
    pub(super) fn domain_limit(&self) -> Result<Amount, RuntimeError> {
        Ok(Amount {
            bytes: lane_credit_limit(self.limits.max_peers)?,
            records: DOMAIN_RECORDS,
        })
    }

    /// Shared native-domain accounting across every peer and lane.
    pub fn delivery_status(&self) -> Result<DeliveryStatus, RuntimeError> {
        let limit = self.domain_limit()?;
        let receive = self.domain_obligation()?;
        let send = self.send_flight()?;
        Ok(DeliveryStatus {
            limit_bytes: limit.bytes,
            limit_records: limit.records,
            receive_promised_bytes: receive.bytes,
            receive_promised_records: receive.records,
            send_flight_bytes: send.bytes,
            send_flight_records: send.records,
            quarantined_send_bytes: self.retired_send.values().map(|a| a.bytes).sum(),
            quarantined_receive_bytes: self
                .lanes
                .iter()
                .flatten()
                .flat_map(|c| c.retired.values())
                .map(|s| s.outstanding.bytes)
                .sum(),
        })
    }
    pub(super) fn lane_obligation(&self, lane: usize) -> Result<Amount, RuntimeError> {
        let mut total = Amount::default();
        if let Some(connection) = &self.lanes[lane] {
            for source in connection.retired.values() {
                total = total.add(source.outstanding)?;
            }
        }
        for (&id, peer) in &self.peers {
            if shard(id, self.config.shards) == lane
                && let Some(session) = &peer.session
            {
                total = total.add(session.recipient_credit.obligation())?;
            }
        }
        Ok(total)
    }

    pub(super) fn domain_obligation(&self) -> Result<Amount, RuntimeError> {
        let mut total = Amount::default();
        for lane in 0..self.lanes.len() {
            total = total.add(self.lane_obligation(lane)?)?;
        }
        Ok(total)
    }

    pub(super) fn send_flight(&self) -> Result<Amount, RuntimeError> {
        let mut total = Amount::default();
        for peer in self.peers.values() {
            if let Some(session) = &peer.session {
                total = total.add(session.sender_credit.outstanding())?;
            }
        }
        for &retired in self.retired_send.values() {
            total = total.add(retired)?;
        }
        Ok(total)
    }

    pub(super) fn apply_control(&mut self, id: PeerId, c: Control) -> Result<(), RuntimeError> {
        let s = self
            .peers
            .get_mut(&id)
            .and_then(|p| p.session.as_mut())
            .ok_or(RuntimeError::PeerUnavailable)?;
        s.control.acknowledge(c.ack)?;
        if c.kind == 11 {
            if c.serial != 0 {
                return Err(RuntimeError::Protocol);
            }
            return Ok(());
        }
        if !s.control.fresh(c.serial)? {
            return Ok(());
        }
        if let Some(pending) = s.pending_control
            && pending != c
        {
            return Err(RuntimeError::Protocol);
        }
        if c.body_len != 0 && c.kind != 8 {
            return Err(RuntimeError::Protocol);
        }
        if c.body_len != 0 && c.hello > s.rx as u128 {
            s.pending_control = Some(c);
            return Ok(());
        }
        match c.kind {
            7 => {
                if c.watermark > DATA_DELIVERY_BYTES || c.records > DOMAIN_RECORDS {
                    return Err(RuntimeError::Protocol);
                }
                s.remote_demand = c.watermark;
                if c.watermark > 0 {
                    s.target.bytes = s.target.bytes.max(ACTIVE_BYTES);
                    s.target.records = s.target.records.max(ACTIVE_RECORDS);
                }
            }
            8 => {
                let old_epoch = s.sender_credit.epoch;
                s.sender_credit.grant(delivery::Limit {
                    epoch: c.epoch,
                    allowed: Amount {
                        bytes: c.watermark,
                        records: c.records,
                    },
                    applied: Amount {
                        bytes: c.applied_bytes,
                        records: c.applied_records,
                    },
                })?;
                if s.sender_credit.epoch != old_epoch {
                    s.announced_demand = 0;
                }
            }
            9 => {
                s.sender_credit.freeze(c.epoch)?;
                s.frozen_sent = false;
            }
            10 => {
                let prefix = Amount {
                    bytes: c.watermark,
                    records: c.records,
                };
                s.recipient_credit.observe_frozen(c.epoch, prefix)?;
                let sequence = u64::try_from(c.hello).map_err(|_| RuntimeError::Protocol)?;
                if sequence < s.rx {
                    return Err(RuntimeError::Protocol);
                }
                if sequence > s.rx {
                    s.pending_control = Some(c);
                    return Ok(());
                }
                if s.recipient_credit.reclaim()?.is_none() {
                    return Err(RuntimeError::Protocol);
                }
                s.fence_deadline = None;
                s.fence_sent = false;
                s.remote_demand = 0;
                s.growth_applied = s.recipient_credit.applied.bytes;
                s.growth_records = s.recipient_credit.applied.records;
                s.target = Amount {
                    bytes: 4096,
                    records: 2,
                };
            }
            _ => return Err(RuntimeError::Protocol),
        }
        if c.body_len != 0 {
            if c.kind != 8 {
                return Err(RuntimeError::Protocol);
            }
            let prefix = u64::try_from(c.hello).map_err(|_| RuntimeError::Protocol)?;
            if prefix > s.rx {
                s.pending_control = Some(c);
                return Ok(());
            }
            let packet =
                wire::decode(&c.body[..c.body_len as usize]).map_err(|_| RuntimeError::Protocol)?;
            if !(packet.frame.is_peer_control()
                || matches!(
                    packet.frame,
                    crate::Frame::Close { .. }
                        | crate::Frame::WindowUpdate { .. }
                        | crate::Frame::WindowGrant { .. }
                ))
            {
                return Err(RuntimeError::Protocol);
            }
            let key = StreamKey {
                peer: id,
                stream_id: packet.stream_id,
            };
            self.manager.validate_control_object(key)?;
            self.manager.receive(key, &packet.frame, self.now()?)?;
        }
        self.peers
            .get_mut(&id)
            .unwrap()
            .session
            .as_mut()
            .unwrap()
            .control
            .applied(c.serial)?;
        Ok(())
    }

    pub(super) async fn credit_work(&mut self) -> Result<usize, RuntimeError> {
        let now = Instant::now();
        let limit = self.domain_limit()?;
        let mut free = limit.sub(self.domain_obligation()?)?;
        let sources: usize = self
            .lanes
            .iter()
            .flatten()
            .map(|c| c.sources.len() + c.retired.len())
            .sum();
        let future = self.limits.max_peers.saturating_sub(sources) as u64;
        let protected = Amount {
            bytes: future * 4096,
            records: future * 2,
        };
        let mut ids: Vec<_> = self.peers.keys().copied().collect();
        if let Some(cursor) = self.credit_cursor {
            let at = ids.partition_point(|id| *id <= cursor);
            ids.rotate_left(at);
        }
        let mut pressure = false;
        for &id in &ids {
            let Some(s) = self
                .peers
                .get_mut(&id)
                .and_then(|p| p.session.as_mut())
                .filter(|s| !s.retiring)
            else {
                continue;
            };
            if s.recipient_credit.freezing.is_some() {
                continue;
            }
            let rx = &mut s.recipient_credit;
            if s.remote_demand > 0
                && rx.applied.bytes.saturating_sub(s.growth_applied) >= s.target.bytes / 2
            {
                s.target.bytes = s.target.bytes.saturating_mul(2).min(limit.bytes);
                s.growth_applied = rx.applied.bytes;
            }
            if s.remote_demand > 0
                && rx.applied.records.saturating_sub(s.growth_records)
                    >= (s.target.records / 2).max(1)
            {
                s.target.records = s.target.records.saturating_mul(2).min(limit.records);
                s.growth_records = rx.applied.records;
            }
            let held = rx.obligation();
            let need = Amount {
                bytes: s.target.bytes.saturating_sub(held.bytes),
                records: s.target.records.saturating_sub(held.records),
            };
            // Retain small releases until a meaningful update. Applied receipts
            // are still sent independently to release the global send-flight.
            if need.bytes < (s.target.bytes / 4).max(1)
                && held.bytes >= ENVELOPE_WEIGHT
                && held.records > 1
            {
                continue;
            }
            let floor = Amount {
                bytes: 4096u64
                    .saturating_sub(held.bytes)
                    .min(need.bytes)
                    .min(free.bytes),
                records: 2u64
                    .saturating_sub(held.records)
                    .min(need.records)
                    .min(free.records),
            };
            let after_floor = free.sub(floor)?;
            let grant = floor.add(Amount {
                bytes: need
                    .bytes
                    .saturating_sub(floor.bytes)
                    .min(after_floor.bytes.saturating_sub(protected.bytes)),
                records: need
                    .records
                    .saturating_sub(floor.records)
                    .min(after_floor.records.saturating_sub(protected.records)),
            })?;
            if s.remote_demand > 0
                && (held.bytes + grant.bytes < ACTIVE_BYTES / 2 || held.records + grant.records < 2)
            {
                pressure = true;
            }
            if grant != Amount::default() {
                rx.extend(grant)?;
                free = free.sub(grant)?;
                self.credit_cursor = Some(id);
            }
        }
        // Rare resource pressure reclaims an unused promise. No idle timeout
        // shrinks a healthy window after every burst.
        if pressure
            && !self.peers.values().any(|p| {
                p.session
                    .as_ref()
                    .is_some_and(|s| s.recipient_credit.freezing.is_some())
            })
        {
            let victim = ids
                .iter()
                .filter_map(|id| {
                    let s = self.peers[id].session.as_ref()?;
                    if s.retiring
                        || s.recipient_credit.freezing.is_some()
                        || (self.manager.peer_streams(*id).is_some_and(|n| n != 0)
                            && now.duration_since(s.last_data) < Duration::from_secs(1))
                    {
                        return None;
                    }
                    Some((*id, s.recipient_credit.obligation().bytes))
                })
                .max_by_key(|(_, bytes)| *bytes)
                .filter(|(_, bytes)| *bytes > 4096);
            if let Some((id, _)) = victim {
                let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
                s.recipient_credit.freeze()?;
                s.fence_deadline = Some(now + self.config.peer_timeout);
            }
        }
        let mut count = 0;
        for id in peer_batch(
            &self.peers,
            &mut self.credit_visit_cursor,
            self.config.max_outgoing_per_turn,
        ) {
            let can_extract = self
                .peers
                .get(&id)
                .and_then(|p| p.session.as_ref())
                .is_some_and(|s| {
                    !s.retiring
                        && s.proven
                        && s.control.deadline.is_none()
                        && s.recipient_credit.freezing.is_none()
                        && s.sender_credit.freezing.is_none()
                });
            let attached = if can_extract {
                self.manager.poll_priority_control(id)
            } else {
                None
            };
            let Some(s) = self
                .peers
                .get_mut(&id)
                .and_then(|p| p.session.as_mut())
                .filter(|s| !s.retiring && s.proven)
            else {
                continue;
            };
            if s.control.deadline.is_some_and(|d| d <= now)
                || s.fence_deadline.is_some_and(|d| d <= now)
            {
                eprintln!(
                    "Peer control timeout: peer={} sent={} acknowledged={} received={} pending={:?} tx={} rx={} grant={:?} applied={:?} target={:?} demand={}/{} remote={}",
                    id.0,
                    s.control.sent,
                    s.control.acknowledged,
                    s.control.received,
                    s.pending_control.map(|c| (c.kind, c.serial, c.hello)),
                    s.tx,
                    s.rx,
                    s.sender_credit.allowed,
                    s.sender_credit.applied,
                    s.target,
                    s.demand,
                    s.announced_demand,
                    s.remote_demand
                );
                self.retire(id);
                self.counters.peer_timeouts += 1;
                continue;
            }
            let mut message = Control {
                sender: self.epoch,
                recipient: s.generation,
                token: s.token,
                ..Control::default()
            };
            let snapshot = s.recipient_credit.snapshot();
            if let Some(epoch) = s.recipient_credit.freezing.filter(|_| !s.fence_sent) {
                message.kind = 9;
                message.epoch = epoch;
            } else if let Some(prefix) = s.sender_credit.frozen_prefix().filter(|_| !s.frozen_sent)
            {
                message.kind = 10;
                message.epoch = s.sender_credit.freezing.unwrap();
                message.hello = s.tx as u128;
                message.watermark = prefix.bytes;
                message.records = prefix.records;
            } else if snapshot != s.advertised || attached.is_some() {
                message.kind = 8;
                message.epoch = snapshot.epoch;
                message.watermark = snapshot.allowed.bytes;
                message.records = snapshot.allowed.records;
                message.applied_bytes = snapshot.applied.bytes;
                message.applied_records = snapshot.applied.records;
                if let Some(frame) = attached {
                    let encoded = wire::encode(frame.key.stream_id, &frame.frame)
                        .map_err(|_| RuntimeError::Protocol)?;
                    if encoded.len() > message.body.len() {
                        return Err(RuntimeError::Protocol);
                    }
                    message.body_len = encoded.len() as u8;
                    message.body[..encoded.len()].copy_from_slice(&encoded);
                    message.hello = s.tx as u128;
                }
            } else if s.demand > s.announced_demand {
                message.kind = 7;
                message.watermark = s.demand.min(DATA_DELIVERY_BYTES);
                message.records = DOMAIN_RECORDS;
            }
            if message.kind != 0
                && let Some(serial) = s.control.begin(now, self.config.peer_timeout)?
            {
                message.serial = serial;
                match message.kind {
                    7 => s.announced_demand = s.demand,
                    8 => s.advertised = snapshot,
                    9 => s.fence_sent = true,
                    10 => s.frozen_sent = true,
                    _ => unreachable!(),
                }
            } else if s.control.receipt_dirty() {
                message.kind = 11;
                message.serial = 0;
            } else {
                continue;
            }
            self.send_control(id, message, true).await?;
            count += 1;
        }
        Ok(count)
    }
}
