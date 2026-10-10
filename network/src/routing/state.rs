use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

fn after(now: Instant, millis: u64) -> Instant {
    now + Duration::from_millis(millis)
}
fn vpn(target: &Metadata) -> bool {
    matches!(target, Metadata::IpSession { .. })
}
#[derive(Clone)]
struct Lease {
    epoch: SessionId,
    received: Instant,
    request: u64,
    membership: u64,
}
impl Lease {
    fn fresh(&self, now: Instant) -> bool {
        now < after(self.received, LEASE_MS)
    }
    fn admit(&mut self, epoch: &SessionId, request: u64, now: Instant) -> bool {
        if &self.epoch != epoch && now < after(self.received, FENCE_MS) {
            return false;
        }
        if &self.epoch == epoch && request <= self.request {
            return false;
        }
        self.epoch = epoch.clone();
        self.received = now;
        self.request = request;
        true
    }
}
struct Node {
    lease: Lease,
    families: Vec<u8>,
    tcp: usize,
    vpn: usize,
    revision: u64,
    watermark: u64,
    membership: u64,
    command: u64,
    dispatched: BTreeMap<u64, bool>,
}
#[derive(Clone)]
struct Record {
    assignment: Assignment,
    target: Metadata,
    command: u64,
    created: Instant,
    state: RecordState,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum RecordState {
    Pending,
    Reserved,
    Cancelled,
    Refused,
}
pub struct Authority {
    pub epoch: SessionId,
    pub registry: Registry,
    clients: BTreeMap<u64, Lease>,
    nodes: BTreeMap<u64, Node>,
    high_water: BTreeMap<u64, (SessionId, u64)>,
    records: BTreeMap<Attempt, Record>,
    vpn_guard: BTreeMap<u64, Record>,
    revision: u64,
    tie: u64,
    query_cursor: u64,
}
impl Authority {
    /// Fully confirmed exits for the current configured membership snapshot.
    /// Admission itself uses the requesting device's required revision.
    pub fn ready_exits(&self, now: Instant) -> usize {
        self.nodes
            .values()
            .filter(|node| node.lease.fresh(now) && node.membership >= self.revision)
            .count()
    }
    /// Static queue/parser/registry headroom plus every retained table row.
    /// The 1024-byte row charge includes BTree nodes, duplicated attempt keys,
    /// maximum 512-byte target backing, tokens and string allocations.
    pub fn control_records(&self) -> usize {
        256 + self.clients.len()
            + self.nodes.len()
            + self.high_water.len()
            + self.records.len()
            + self.vpn_guard.len()
            + self
                .nodes
                .values()
                .map(|n| n.dispatched.len())
                .sum::<usize>()
    }
    pub fn control_bytes(&self) -> usize {
        1048576 + self.control_records() * 1024
    }
    fn make_room(&mut self, extra: usize) -> Result<(), NetworkError> {
        while self.control_records() + extra > 4096
            || self.control_bytes() + extra * 1024 > 8 * 1048576
        {
            let key = self
                .records
                .iter()
                .filter(|(_, r)| r.state != RecordState::Pending)
                .min_by_key(|(_, r)| r.created)
                .map(|(key, _)| key.clone())
                .ok_or(NetworkError::Overloaded)?;
            self.records.remove(&key);
        }
        Ok(())
    }
    pub fn new(registry: Registry) -> Result<Self, NetworkError> {
        registry.validate()?;
        Ok(Self {
            epoch: SessionId::random()?,
            registry,
            clients: BTreeMap::new(),
            nodes: BTreeMap::new(),
            high_water: BTreeMap::new(),
            records: BTreeMap::new(),
            vpn_guard: BTreeMap::new(),
            revision: 1,
            tie: 0,
            query_cursor: 0,
        })
    }
    pub fn apply_registry(&mut self, registry: Registry) -> Result<(), NetworkError> {
        registry.validate()?;
        if registry.revision <= self.registry.revision {
            return Err(NetworkError::InvalidState);
        }
        self.clients.retain(|id, _| registry.devices.contains(id));
        self.nodes.retain(|id, _| registry.nodes.contains(id));
        self.high_water
            .retain(|id, _| registry.devices.contains(id));
        self.registry = registry;
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(NetworkError::InvalidState)?;
        Ok(())
    }
    pub fn client_lease(
        &mut self,
        id: u64,
        epoch: &SessionId,
        request: u64,
        now: Instant,
    ) -> Result<(), NetworkError> {
        if !self.registry.devices.contains(&id) || epoch_number(epoch) == 0 {
            return Err(NetworkError::Forbidden);
        }
        self.make_room(
            usize::from(!self.clients.contains_key(&id))
                + usize::from(!self.high_water.contains_key(&id)),
        )?;
        let changed = self
            .clients
            .get(&id)
            .is_none_or(|l| &l.epoch != epoch || !l.fresh(now));
        if let Some(lease) = self.clients.get_mut(&id) {
            if !lease.admit(epoch, request, now) {
                return Err(NetworkError::Forbidden);
            }
        } else {
            self.clients.insert(
                id,
                Lease {
                    epoch: epoch.clone(),
                    received: now,
                    request,
                    membership: 0,
                },
            );
        }
        if changed {
            self.revision = self
                .revision
                .checked_add(1)
                .ok_or(NetworkError::InvalidState)?;
            self.clients.get_mut(&id).unwrap().membership = self.revision;
            if self.high_water.get(&id).is_none_or(|(old, _)| old != epoch) {
                self.high_water.insert(id, (epoch.clone(), 0));
            }
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub fn node_lease(
        &mut self,
        id: u64,
        epoch: &SessionId,
        request: u64,
        families: Vec<u8>,
        tcp: usize,
        ip: usize,
        revision: u64,
        watermark: u64,
        membership: u64,
        now: Instant,
    ) -> Result<(), NetworkError> {
        if !self.registry.nodes.contains(&id)
            || !matches!(families.as_slice(), [] | [4] | [6] | [4, 6])
            || tcp > 2048
            || ip > 128
        {
            return Err(NetworkError::Forbidden);
        }
        if self
            .nodes
            .get(&id)
            .is_some_and(|n| n.lease.epoch != *epoch && now < after(n.lease.received, FENCE_MS))
        {
            return Err(NetworkError::Forbidden);
        }
        let replacing = self
            .nodes
            .get(&id)
            .is_none_or(|n| n.lease.epoch != *epoch || now >= after(n.lease.received, FENCE_MS));
        if self.nodes.get(&id).is_some_and(|n| {
            n.lease.epoch == *epoch
                && (request <= n.lease.request
                    || watermark > n.command
                    || (revision > n.revision && watermark < n.watermark))
        }) || replacing && watermark != 0
        {
            return Err(NetworkError::InvalidState);
        }
        // Collect the old owner/epoch before replacing its Node row. Otherwise
        // a later expiry sees only the new incarnation and strands old guards.
        self.expire(now);
        self.make_room(usize::from(!self.nodes.contains_key(&id)))?;
        if self.nodes.get(&id).is_none_or(|n| n.lease.epoch != *epoch) {
            self.nodes.insert(
                id,
                Node {
                    lease: Lease {
                        epoch: epoch.clone(),
                        received: now,
                        request,
                        membership: 0,
                    },
                    families: families.clone(),
                    tcp: 0,
                    vpn: 0,
                    revision: 0,
                    watermark: 0,
                    membership: 0,
                    command: 0,
                    dispatched: BTreeMap::new(),
                },
            );
        }
        let node = self.nodes.get_mut(&id).unwrap();
        if revision > node.revision {
            if watermark < node.watermark || watermark > node.command {
                return Err(NetworkError::InvalidState);
            }
            node.revision = revision;
            node.watermark = watermark;
            node.tcp = tcp;
            node.vpn = ip;
            node.families = families;
            node.membership = membership;
            node.dispatched.retain(|command, _| *command > watermark);
        }
        node.lease.received = now;
        node.lease.request = request;
        Ok(())
    }
    pub fn snapshots(&self, node: u64, now: Instant) -> Result<Vec<Body>, NetworkError> {
        let owner = self.nodes.get(&node).ok_or(NetworkError::Forbidden)?;
        // Include configured but unleased devices with zero remaining time.
        // Their anti-replay row must survive a same-incarnation lease lapse.
        let members: Vec<_> = self
            .registry
            .devices
            .iter()
            .map(|id| {
                self.clients
                    .get(id)
                    .map(|l| {
                        Member(
                            *id,
                            l.epoch.clone(),
                            after(l.received, LEASE_MS)
                                .saturating_duration_since(now)
                                .as_millis() as u64,
                        )
                    })
                    .unwrap_or_else(|| Member(*id, epoch(0), 0))
            })
            .collect();
        let parts = members.len().div_ceil(MEMBERS_PER_CHUNK).max(1) as u8;
        let chunks = if members.is_empty() {
            vec![Vec::new()]
        } else {
            members
                .chunks(MEMBERS_PER_CHUNK)
                .map(|c| c.to_vec())
                .collect()
        };
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(part, members)| Body::Snapshot {
                authority: self.epoch.clone(),
                node_epoch: owner.lease.epoch.clone(),
                revision: self.revision,
                part: part as u8,
                parts,
                members,
            })
            .collect())
    }
    fn pressure(node: &Node) -> (usize, usize) {
        (
            node.tcp + node.dispatched.values().filter(|v| !**v).count(),
            node.vpn + node.dispatched.values().filter(|v| **v).count(),
        )
    }
    pub fn reserve(
        &mut self,
        attempt: Attempt,
        target: Metadata,
        now: Instant,
    ) -> Result<(Assignment, u64), NetworkError> {
        self.expire(now);
        if attempt.authority != self.epoch
            || attempt.sequence == 0
            || !self
                .clients
                .get(&attempt.device)
                .is_some_and(|l| l.epoch == attempt.epoch && l.fresh(now))
        {
            return Err(NetworkError::Forbidden);
        }
        target.encode()?;
        if !matches!(target, Metadata::Tcp { .. } | Metadata::IpSession { .. }) {
            return Err(NetworkError::InvalidMetadata);
        }
        if let Some(record) = self.records.get(&attempt) {
            if record.target != target
                || matches!(record.state, RecordState::Cancelled | RecordState::Refused)
            {
                return Err(NetworkError::InvalidState);
            }
            return Ok((record.assignment.clone(), record.command));
        }
        self.make_room(2 + usize::from(vpn(&target)))?;
        let high = self
            .high_water
            .get_mut(&attempt.device)
            .ok_or(NetworkError::Forbidden)?;
        if high.0 != attempt.epoch || attempt.sequence <= high.1 {
            return Err(NetworkError::InvalidState);
        }
        high.1 = attempt.sequence;
        if let Metadata::IpSession {
            families,
            family_policy,
            ..
        } = &target
        {
            let ready: Vec<_> = self
                .nodes
                .values()
                .filter(|node| {
                    node.lease.fresh(now)
                        && node.membership >= self.clients[&attempt.device].membership
                })
                .collect();
            if !ready.is_empty()
                && ready
                    .iter()
                    .all(|node| family_policy.negotiate(families, &node.families).is_err())
            {
                return Err(NetworkError::UnsupportedFamily);
            }
        }
        if self
            .records
            .values()
            .filter(|r| r.state == RecordState::Pending)
            .count()
            >= 256
            || self
                .records
                .iter()
                .filter(|(a, r)| a.device == attempt.device && r.state == RecordState::Pending)
                .count()
                >= 32
            || vpn(&target)
                && (self.vpn_guard.contains_key(&attempt.device)
                    || self.vpn_guard.len() >= DEVICE_MAX)
        {
            return Err(NetworkError::Overloaded);
        }
        let total = self
            .nodes
            .values()
            .map(Self::pressure)
            .fold((0, 0), |(t, v), (a, b)| (t + a, v + b));
        if (!vpn(&target) && total.0 >= 2048) || (vpn(&target) && total.1 >= 128) {
            return Err(NetworkError::Overloaded);
        }
        let mut eligible: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, n)| {
                n.lease.fresh(now)
                    && n.membership >= self.clients[&attempt.device].membership
                    && match &target {
                        Metadata::Tcp { .. } => Self::pressure(n).0 < 2048,
                        Metadata::IpSession {
                            families,
                            family_policy,
                            ..
                        } => {
                            Self::pressure(n).1 < 128
                                && family_policy.negotiate(families, &n.families).is_ok()
                        }
                        _ => false,
                    }
            })
            .map(|(id, n)| (*id, Self::pressure(n)))
            .collect();
        eligible.sort_by_key(|(id, (tcp, ip))| (tcp + ip, id.wrapping_sub(self.tie)));
        let owner = eligible
            .first()
            .map(|(id, _)| *id)
            .ok_or(NetworkError::Overloaded)?;
        self.tie = owner.wrapping_add(1);
        let node = self.nodes.get_mut(&owner).unwrap();
        node.command = node
            .command
            .checked_add(1)
            .ok_or(NetworkError::InvalidState)?;
        node.dispatched.insert(node.command, vpn(&target));
        let assignment = Assignment {
            attempt: attempt.clone(),
            owner,
            owner_epoch: node.lease.epoch.clone(),
            token: SessionId::random()?,
        };
        let record = Record {
            assignment: assignment.clone(),
            target: target.clone(),
            command: node.command,
            created: now,
            state: RecordState::Pending,
        };
        if vpn(&target) {
            self.vpn_guard.insert(attempt.device, record.clone());
        }
        self.records.insert(attempt, record);
        Ok((assignment, node.command))
    }
    pub fn reserved(
        &mut self,
        owner: u64,
        epoch: &SessionId,
        assignment: &Assignment,
        command: u64,
        accepted: bool,
        now: Instant,
    ) -> Result<(), NetworkError> {
        if assignment.owner != owner
            || assignment.owner_epoch != *epoch
            || !self
                .nodes
                .get(&owner)
                .is_some_and(|n| n.lease.epoch == *epoch && n.lease.fresh(now))
        {
            return Err(NetworkError::Forbidden);
        }
        let r = self
            .records
            .get_mut(&assignment.attempt)
            .ok_or(NetworkError::InvalidState)?;
        if r.assignment != *assignment
            || r.command != command
            || r.state != RecordState::Pending
            || now >= after(r.created, 2000)
        {
            return Err(NetworkError::InvalidState);
        }
        r.state = if accepted {
            RecordState::Reserved
        } else {
            RecordState::Refused
        };
        if !accepted {
            self.vpn_guard.remove(&assignment.attempt.device);
        }
        Ok(())
    }
    pub fn assigned(&self, attempt: &Attempt) -> bool {
        self.records
            .get(attempt)
            .is_some_and(|r| r.state == RecordState::Reserved)
    }
    pub fn cancel(&mut self, attempt: &Attempt) -> Result<(u64, SessionId, u64), NetworkError> {
        let record = if let Some(record) = self.records.get_mut(attempt) {
            record
        } else {
            self.vpn_guard
                .get_mut(&attempt.device)
                .filter(|r| r.assignment.attempt == *attempt)
                .ok_or(NetworkError::InvalidState)?
        };
        record.state = RecordState::Cancelled;
        let node = self
            .nodes
            .get_mut(&record.assignment.owner)
            .ok_or(NetworkError::InvalidState)?;
        node.command = node
            .command
            .checked_add(1)
            .ok_or(NetworkError::InvalidState)?;
        Ok((
            record.assignment.owner,
            record.assignment.owner_epoch.clone(),
            node.command,
        ))
    }
    pub fn queries(&mut self) -> Vec<(u64, SessionId, u64, Attempt)> {
        let mut out = Vec::new();
        let mut ids: Vec<_> = self
            .vpn_guard
            .keys()
            .filter(|id| **id > self.query_cursor)
            .take(8)
            .copied()
            .collect();
        if ids.len() < 8 {
            ids.extend(
                self.vpn_guard
                    .keys()
                    .filter(|id| **id <= self.query_cursor)
                    .take(8 - ids.len())
                    .copied(),
            );
        }
        for id in ids {
            self.query_cursor = id;
            let record = &self.vpn_guard[&id];
            if let Some(n) = self.nodes.get_mut(&record.assignment.owner)
                && let Some(command) = n.command.checked_add(1)
            {
                n.command = command;
                out.push((
                    record.assignment.owner,
                    n.lease.epoch.clone(),
                    command,
                    record.assignment.attempt.clone(),
                ));
            }
        }
        out
    }
    pub fn terminal(
        &mut self,
        node: u64,
        epoch: &SessionId,
        attempt: &Attempt,
        watermark: u64,
        absent: bool,
    ) {
        if absent
            && self.vpn_guard.get(&attempt.device).is_some_and(|r| {
                r.assignment.attempt == *attempt
                    && r.assignment.owner == node
                    && r.assignment.owner_epoch == *epoch
                    && watermark >= r.command
                    && attempt.authority == self.epoch
            })
        {
            self.vpn_guard.remove(&attempt.device);
        }
    }
    pub fn expire(&mut self, now: Instant) {
        let fenced: BTreeSet<_> = self
            .nodes
            .iter()
            .filter(|(_, n)| now >= after(n.lease.received, FENCE_MS))
            .map(|(id, n)| (*id, n.lease.epoch.clone()))
            .collect();
        self.nodes
            .retain(|id, n| !fenced.contains(&(*id, n.lease.epoch.clone())));
        self.vpn_guard.retain(|_, r| {
            !fenced.contains(&(r.assignment.owner, r.assignment.owner_epoch.clone()))
        });
        self.records.retain(|_, r| now < after(r.created, 30000));
        for record in self.records.values_mut() {
            if record.state == RecordState::Pending && now >= after(record.created, 2000) {
                record.state = RecordState::Cancelled;
            }
        }
        while self.records.len() >= 4096 {
            if let Some(key) = self
                .records
                .iter()
                .min_by_key(|(_, r)| r.created)
                .map(|(a, _)| a.clone())
            {
                self.records.remove(&key);
            }
        }
        let mut counts = BTreeMap::new();
        let keys: Vec<_> = self
            .records
            .iter()
            .rev()
            .filter_map(|(a, _)| {
                let n = counts.entry(a.device).or_insert(0);
                *n += 1;
                (*n > 64).then_some(a.clone())
            })
            .collect();
        for key in keys {
            self.records.remove(&key);
        }
    }
}

/// A lease acknowledgement cannot add its transit delay to the local lease.
#[derive(Default, Debug)]
pub struct LocalLease {
    sent: BTreeMap<u64, Instant>,
    pub authority: Option<SessionId>,
    deadline: Option<Instant>,
    accepted: u64,
}
impl LocalLease {
    pub fn sent(&mut self, request: u64, now: Instant) {
        self.sent.insert(request, now);
        while self.sent.len() > 4 {
            let key = *self.sent.first_key_value().unwrap().0;
            self.sent.remove(&key);
        }
    }
    pub fn acknowledge(
        &mut self,
        request: u64,
        authority: SessionId,
        remaining_ms: u64,
        now: Instant,
    ) -> bool {
        let Some(sent) = self.sent.get(&request).copied() else {
            return false;
        };
        let deadline = after(sent, remaining_ms.min(LEASE_MS));
        if request <= self.accepted || now >= deadline {
            return false;
        }
        self.authority = Some(authority);
        self.deadline = Some(deadline);
        self.accepted = request;
        self.sent.retain(|id, _| *id >= request);
        true
    }
    pub fn send_time(&self, request: u64) -> Option<Instant> {
        self.sent.get(&request).copied()
    }
    pub fn ready(&self, now: Instant) -> bool {
        self.deadline.is_some_and(|d| now < d)
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub fn clear(&mut self) {
        self.deadline = None;
        self.authority = None;
        self.sent.clear();
        self.accepted = 0;
    }
}
struct Ticket {
    assignment: Assignment,
    target: Metadata,
    expires: Instant,
    key: Option<skvoz_core::runtime::RuntimeKey>,
    cancelled: bool,
}
type PendingSnapshot = (SessionId, u64, u8, Vec<Option<Vec<Member>>>, Instant);

pub struct Egress {
    pub epoch: SessionId,
    pub authority: Option<SessionId>,
    pub members: BTreeMap<u64, (SessionId, Instant)>,
    pub revision: u64,
    pub watermark: u64,
    high_water: BTreeMap<u64, (SessionId, u64)>,
    tickets: BTreeMap<Attempt, Ticket>,
    snapshot: Option<PendingSnapshot>,
}
impl Egress {
    pub fn control_records(&self) -> usize {
        256 + self.members.len() + self.high_water.len() + self.tickets.len()
    }
    pub fn control_bytes(&self) -> usize {
        524288 + self.control_records() * 1024
    }
    pub fn new(epoch: SessionId) -> Self {
        Self {
            epoch,
            authority: None,
            members: BTreeMap::new(),
            revision: 0,
            watermark: 0,
            high_water: BTreeMap::new(),
            tickets: BTreeMap::new(),
            snapshot: None,
        }
    }
    pub fn fence(&mut self) {
        self.members.clear();
        self.snapshot = None;
        for ticket in self.tickets.values_mut() {
            ticket.cancelled = true;
        }
    }
    #[allow(clippy::too_many_arguments)]
    pub fn snapshot(
        &mut self,
        authority: SessionId,
        node_epoch: &SessionId,
        revision: u64,
        part: u8,
        parts: u8,
        members: Vec<Member>,
        sent: Instant,
        now: Instant,
    ) -> Result<bool, NetworkError> {
        if *node_epoch != self.epoch
            || parts == 0
            || parts > 3
            || part >= parts
            || members.len() > MEMBERS_PER_CHUNK
            || now >= after(sent, LEASE_MS)
            || (self.authority.as_ref() == Some(&authority) && revision < self.revision)
        {
            return Err(NetworkError::InvalidState);
        }
        if self.authority.as_ref().is_some_and(|a| *a != authority) {
            self.fence();
            self.revision = 0;
            self.watermark = 0;
            self.high_water.clear();
        }
        self.authority = Some(authority.clone());
        if self
            .snapshot
            .as_ref()
            .is_none_or(|(a, r, _, _, s)| *a != authority || *r != revision || *s != sent)
        {
            self.snapshot = Some((authority, revision, parts, vec![None; parts as usize], sent));
        }
        let (_, _, count, chunks, _) = self.snapshot.as_mut().unwrap();
        if *count != parts {
            return Err(NetworkError::InvalidRecord);
        }
        chunks[part as usize] = Some(members);
        if chunks.iter().any(Option::is_none) {
            return Ok(false);
        }
        let mut next = BTreeMap::new();
        for Member(id, epoch, remaining) in chunks.iter().flatten().flatten() {
            if !device_id(*id)
                || (epoch_number(epoch) == 0 && *remaining != 0)
                || *remaining > LEASE_MS
                || next.len() >= DEVICE_MAX
                || next
                    .insert(*id, (epoch.clone(), after(sent, *remaining)))
                    .is_some()
            {
                return Err(NetworkError::InvalidRecord);
            }
        }
        let retained_high = self
            .high_water
            .keys()
            .filter(|id| next.contains_key(id))
            .count();
        let configured: BTreeSet<_> = next.keys().copied().collect();
        next.retain(|_, (_, deadline)| now < *deadline);
        if 256 + next.len() + retained_high + self.tickets.len() > 2048 {
            return Err(NetworkError::Overloaded);
        }
        if next.iter().any(|(id, (epoch, _))| {
            self.tickets.values().any(|t| {
                t.key.is_some()
                    && t.assignment.attempt.device == *id
                    && t.assignment.attempt.epoch != *epoch
            })
        }) {
            return Err(NetworkError::InvalidState);
        }
        self.high_water.retain(|id, _| configured.contains(id));
        self.members = next;
        self.revision = revision;
        self.snapshot = None;
        Ok(true)
    }
    pub fn reserve(
        &mut self,
        command: u64,
        assignment: Assignment,
        target: Metadata,
        now: Instant,
    ) -> Result<(), NetworkError> {
        if let Some(ticket) = self.tickets.get(&assignment.attempt) {
            if ticket.assignment == assignment
                && ticket.target == target
                && !ticket.cancelled
                && now < ticket.expires
            {
                return Ok(());
            }
            return Err(NetworkError::InvalidState);
        }
        if command <= self.watermark {
            return Err(NetworkError::InvalidState);
        }
        self.watermark = command;
        let attempt = &assignment.attempt;
        if self.authority.as_ref() != Some(&attempt.authority)
            || assignment.owner_epoch != self.epoch
            || !self
                .members
                .get(&attempt.device)
                .is_some_and(|(epoch, deadline)| *epoch == attempt.epoch && now < *deadline)
        {
            return Err(NetworkError::Forbidden);
        }
        if self.control_records() + 1 + usize::from(!self.high_water.contains_key(&attempt.device))
            > 2048
        {
            return Err(NetworkError::Overloaded);
        }
        let high = self
            .high_water
            .entry(attempt.device)
            .or_insert((attempt.epoch.clone(), 0));
        if high.0 != attempt.epoch {
            *high = (attempt.epoch.clone(), 0);
        }
        if attempt.sequence <= high.1 {
            return Err(NetworkError::InvalidState);
        }
        high.1 = attempt.sequence;
        let counts = self.counts();
        if !matches!(target, Metadata::Tcp { .. } | Metadata::IpSession { .. })
            || (!vpn(&target) && counts.0 >= 2048)
            || (vpn(&target)
                && (counts.1 >= 128
                    || self
                        .tickets
                        .values()
                        .any(|t| vpn(&t.target) && t.assignment.attempt.device == attempt.device)))
        {
            return Err(NetworkError::Overloaded);
        }
        self.tickets.insert(
            attempt.clone(),
            Ticket {
                assignment,
                target,
                expires: after(now, RESERVATION_MS),
                key: None,
                cancelled: false,
            },
        );
        Ok(())
    }
    pub fn consume(
        &mut self,
        key: skvoz_core::runtime::RuntimeKey,
        client_epoch: &SessionId,
        admission: &Admission,
        target: &Metadata,
        now: Instant,
    ) -> Result<(), NetworkError> {
        let attempt = Attempt {
            authority: admission.authority.clone(),
            device: key.stream.peer.0,
            epoch: client_epoch.clone(),
            sequence: admission.sequence,
        };
        let ticket = self
            .tickets
            .get_mut(&attempt)
            .ok_or(NetworkError::Forbidden)?;
        if self.authority.as_ref() != Some(&attempt.authority)
            || ticket.assignment.owner_epoch != self.epoch
            || ticket.assignment.token != admission.token
            || ticket.target != *target
            || ticket.cancelled
            || ticket.key.is_some()
            || now >= ticket.expires
            || !self
                .members
                .get(&attempt.device)
                .is_some_and(|(e, deadline)| *e == attempt.epoch && now < *deadline)
        {
            return Err(NetworkError::Forbidden);
        }
        ticket.key = Some(key);
        Ok(())
    }
    pub fn cancel(&mut self, command: u64, attempt: &Attempt) {
        if command <= self.watermark {
            return;
        }
        self.watermark = command;
        if let Some(ticket) = self.tickets.get_mut(attempt) {
            ticket.cancelled = true;
        }
    }
    pub fn query(&mut self, command: u64, attempt: &Attempt) -> bool {
        if command > self.watermark {
            self.watermark = command;
        }
        !self.tickets.contains_key(attempt)
    }
    pub fn counts(&self) -> (usize, usize) {
        self.tickets.values().fold((0, 0), |(t, v), ticket| {
            if vpn(&ticket.target) {
                (t, v + 1)
            } else {
                (t + 1, v)
            }
        })
    }
    pub fn cleanup(
        &mut self,
        now: Instant,
        live: impl Fn(skvoz_core::runtime::RuntimeKey, bool) -> bool,
    ) -> Vec<skvoz_core::runtime::RuntimeKey> {
        self.members.retain(|_, (_, deadline)| now < *deadline);
        let mut closing = Vec::new();
        self.tickets.retain(|attempt, ticket| {
            if self
                .members
                .get(&attempt.device)
                .is_none_or(|(epoch, _)| *epoch != attempt.epoch)
                || (ticket.key.is_none() && now >= ticket.expires)
            {
                ticket.cancelled = true;
            }
            match ticket.key {
                Some(key) if live(key, vpn(&ticket.target)) => {
                    if ticket.cancelled {
                        closing.push(key);
                    }
                    true
                }
                Some(_) => false,
                None => !ticket.cancelled,
            }
        });
        closing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u128) -> SessionId {
        epoch(n)
    }
    fn tcp() -> Metadata {
        Metadata::Tcp {
            v: crate::NETWORK_VERSION,
            host: "example.org".into(),
            port: 443,
        }
    }
    fn authority(devices: usize, now: Instant) -> Authority {
        let mut state = Authority::new(Registry {
            revision: 1,
            devices: (1..=devices as u64).collect(),
            nodes: vec![(1 << 63) + 8, (1 << 63) + 16],
        })
        .unwrap();
        for device in 1..=devices as u64 {
            state
                .client_lease(device, &id(device as u128), 1, now)
                .unwrap();
        }
        for (node, epoch) in [((1 << 63) + 8, id(500)), ((1 << 63) + 16, id(501))] {
            state
                .node_lease(node, &epoch, 1, vec![4, 6], 0, 0, 1, 0, state.revision, now)
                .unwrap();
        }
        state
    }
    fn attempt(state: &Authority, device: u64, sequence: u64) -> Attempt {
        Attempt {
            authority: state.epoch.clone(),
            device,
            epoch: id(device as u128),
            sequence,
        }
    }
    #[test]
    fn same_epoch_lapse_and_history_expiry_never_reset_sequence() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let old = attempt(&state, 1, 1);
        let (assignment, command) = state.reserve(old.clone(), tcp(), now).unwrap();
        state
            .reserved(
                assignment.owner,
                &assignment.owner_epoch,
                &assignment,
                command,
                true,
                now,
            )
            .unwrap();
        let later = after(now, 31000);
        state.expire(later);
        assert!(state.records.is_empty());
        state.client_lease(1, &id(1), 2, later).unwrap();
        let owner = (1 << 63) + 8;
        state
            .node_lease(
                owner,
                &id(500),
                2,
                vec![4],
                0,
                0,
                2,
                0,
                state.revision,
                later,
            )
            .unwrap();
        assert!(matches!(
            state.reserve(old, tcp(), later),
            Err(NetworkError::InvalidState)
        ));
        state.reserve(attempt(&state, 1, 2), tcp(), later).unwrap();
    }
    #[test]
    fn sixty_four_duplicate_lost_reply_requests_keep_one_owner_and_conservative_capacity() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let original = attempt(&state, 1, 1);
        let first = state.reserve(original.clone(), tcp(), now).unwrap();
        let retained = state.control_records();
        for _ in 0..64 {
            // The requester has not observed the reply. Repeating the same
            // authenticated attempt cannot create another owner/reservation.
            assert_eq!(state.reserve(original.clone(), tcp(), now).unwrap(), first);
            assert_eq!(state.control_records(), retained);
        }
        let mut other_target = tcp();
        if let Metadata::Tcp { port, .. } = &mut other_target {
            *port = 8443;
        }
        assert_eq!(
            state.reserve(original.clone(), other_target, now),
            Err(NetworkError::InvalidState)
        );
        let owner = first.0.owner;
        assert_eq!(Authority::pressure(&state.nodes[&owner]), (1, 0));
        let (_, epoch, cancel) = state.cancel(&original).unwrap();
        assert_eq!(Authority::pressure(&state.nodes[&owner]), (1, 0));
        assert_eq!(
            state.reserve(original, tcp(), now),
            Err(NetworkError::InvalidState)
        );
        // A confirmed processed watermark/count snapshot, not missing replies
        // or cancellation dispatch alone, releases the conservative count.
        state
            .node_lease(
                owner,
                &epoch,
                2,
                vec![4, 6],
                0,
                0,
                2,
                cancel,
                state.revision,
                now,
            )
            .unwrap();
        assert_eq!(Authority::pressure(&state.nodes[&owner]), (0, 0));
        assert!(
            state
                .reserved(owner, &epoch, &first.0, first.1, true, now)
                .is_err()
        );
        let newer = state.reserve(attempt(&state, 1, 2), tcp(), now).unwrap();
        assert_ne!(newer.0.token, first.0.token);
    }
    #[test]
    fn invalid_watermark_and_wrong_authenticated_owner_do_not_renew_or_commit() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let owner = (1 << 63) + 8;
        assert!(
            state
                .node_lease(
                    owner,
                    &id(500),
                    2,
                    vec![4],
                    0,
                    0,
                    2,
                    1,
                    state.revision,
                    after(now, 5000)
                )
                .is_err()
        );
        assert!(!state.nodes[&owner].lease.fresh(after(now, 6000)));
        let (assignment, command) = state.reserve(attempt(&state, 1, 1), tcp(), now).unwrap();
        let impostor = if assignment.owner == owner {
            owner + 8
        } else {
            owner
        };
        let epoch = state.nodes[&impostor].lease.epoch.clone();
        assert!(
            state
                .reserved(impostor, &epoch, &assignment, command, true, now)
                .is_err()
        );
        assert!(!state.assigned(&assignment.attempt));
    }
    #[test]
    fn unrelated_membership_churn_does_not_gate_confirmed_device_admissions() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let confirmed = state.revision;
        state
            .apply_registry(Registry {
                revision: 2,
                devices: (1..=128).collect(),
                nodes: state.registry.nodes.clone(),
            })
            .unwrap();
        for device in 2..=128 {
            state
                .client_lease(device, &id(device as u128), 1, now)
                .unwrap();
            let (assignment, command) = state
                .reserve(attempt(&state, 1, device), tcp(), now)
                .unwrap();
            assert!(state.nodes[&assignment.owner].membership == confirmed);
            state
                .reserved(
                    assignment.owner,
                    &assignment.owner_epoch,
                    &assignment,
                    command,
                    true,
                    now,
                )
                .unwrap();
            assert!(matches!(
                state.reserve(attempt(&state, device, 1), tcp(), now),
                Err(NetworkError::Overloaded)
            ));
        }
        assert_eq!(state.ready_exits(now), 0); // conservative whole-snapshot health
    }
    #[test]
    fn all_128_vpn_guards_receive_query_proofs_in_four_seconds() {
        let now = Instant::now();
        let mut state = authority(128, now);
        let target = Metadata::IpSession {
            v: crate::NETWORK_VERSION,
            families: vec![4],
            family_policy: crate::FamilyPolicy::RequireAll,
            max_mtu: 1400,
            channels: 1,
        };
        for device in 1..=128 {
            let (assignment, command) = state
                .reserve(attempt(&state, device, 1), target.clone(), now)
                .unwrap();
            state
                .reserved(
                    assignment.owner,
                    &assignment.owner_epoch,
                    &assignment,
                    command,
                    true,
                    now,
                )
                .unwrap();
        }
        let mut seen = BTreeSet::new();
        for _ in 0..16 {
            // product query interval 250ms, eight bounded commands
            let queries = state.queries();
            assert_eq!(queries.len(), 8);
            for (owner, epoch, command, attempt) in queries {
                assert!(seen.insert(attempt.device));
                state.terminal(owner, &epoch, &attempt, command, true);
            }
        }
        assert_eq!(seen.len(), 128);
        assert!(state.vpn_guard.is_empty());
    }
    #[test]
    fn aggregate_history_eviction_preserves_dispatch_counts_and_high_water() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let mut admitted = 0;
        for sequence in 1..=2048 {
            match state.reserve(attempt(&state, 1, sequence), tcp(), now) {
                Ok((assignment, command)) => {
                    state
                        .reserved(
                            assignment.owner,
                            &assignment.owner_epoch,
                            &assignment,
                            command,
                            true,
                            now,
                        )
                        .unwrap();
                    admitted += 1;
                }
                Err(NetworkError::Overloaded) => break,
                Err(error) => panic!("unexpected admission: {error:?}"),
            }
            assert!(state.control_records() <= 4096);
            assert!(state.control_bytes() <= 8 * 1048576);
        }
        assert_eq!(admitted, 2048);
        assert!(state.records.len() < admitted); // finite history gives active counts headroom
        assert_eq!(
            state
                .nodes
                .values()
                .map(|node| node.dispatched.len())
                .sum::<usize>(),
            admitted
        );
        assert!(matches!(
            state.reserve(attempt(&state, 1, 1), tcp(), now),
            Err(NetworkError::InvalidState)
        ));
    }
    #[test]
    fn node_epoch_replacement_collects_old_guard_without_prior_expiry_tick() {
        let now = Instant::now();
        let mut state = authority(1, now);
        let target = Metadata::IpSession {
            v: crate::NETWORK_VERSION,
            families: vec![4],
            family_policy: crate::FamilyPolicy::RequireAll,
            max_mtu: 1400,
            channels: 1,
        };
        let (old, command) = state
            .reserve(attempt(&state, 1, 1), target.clone(), now)
            .unwrap();
        state
            .reserved(old.owner, &old.owner_epoch, &old, command, true, now)
            .unwrap();
        assert!(
            state
                .node_lease(
                    old.owner,
                    &id(900),
                    1,
                    vec![4],
                    0,
                    0,
                    1,
                    0,
                    state.revision,
                    after(now, 8999)
                )
                .is_err()
        );
        assert_eq!(state.vpn_guard.len(), 1);
        let fenced = after(now, 9000);
        state.client_lease(1, &id(1), 2, fenced).unwrap();
        state
            .node_lease(
                old.owner,
                &id(900),
                1,
                vec![4],
                0,
                0,
                1,
                0,
                state.revision,
                fenced,
            )
            .unwrap();
        assert!(state.vpn_guard.is_empty());
        state
            .node_lease(
                old.owner,
                &id(900),
                2,
                vec![4],
                0,
                0,
                2,
                0,
                state.revision,
                after(now, 10000),
            )
            .unwrap();
        let (new, _) = state
            .reserve(attempt(&state, 1, 2), target, after(now, 10000))
            .unwrap();
        assert_eq!(new.owner, old.owner);
        assert_eq!(new.owner_epoch, id(900));
    }
    #[test]
    fn confirmed_family_incompatibility_is_terminal_while_capacity_is_retryable() {
        let now = Instant::now();
        let mut state = authority(1, now);
        for node in state.registry.nodes.clone() {
            let epoch = state.nodes[&node].lease.epoch.clone();
            state
                .node_lease(node, &epoch, 2, vec![4], 0, 0, 2, 0, state.revision, now)
                .unwrap();
        }
        let target = |families, family_policy| Metadata::IpSession {
            v: crate::NETWORK_VERSION,
            families,
            family_policy,
            max_mtu: 1400,
            channels: 1,
        };
        assert!(matches!(
            state.reserve(
                attempt(&state, 1, 1),
                target(vec![4, 6], crate::FamilyPolicy::RequireAll),
                now
            ),
            Err(NetworkError::UnsupportedFamily)
        ));
        let (auto, command) = state
            .reserve(
                attempt(&state, 1, 2),
                target(vec![4, 6], crate::FamilyPolicy::Auto),
                now,
            )
            .unwrap();
        assert_eq!(
            crate::FamilyPolicy::Auto
                .negotiate(&[4, 6], &state.nodes[&auto.owner].families)
                .unwrap(),
            vec![4]
        );
        state.terminal(auto.owner, &auto.owner_epoch, &auto.attempt, command, true);
        for node in state.registry.nodes.clone() {
            let n = &state.nodes[&node];
            let epoch = n.lease.epoch.clone();
            let watermark = n.command;
            state
                .node_lease(
                    node,
                    &epoch,
                    3,
                    vec![4],
                    0,
                    64,
                    3,
                    watermark,
                    state.revision,
                    now,
                )
                .unwrap();
        }
        assert!(matches!(
            state.reserve(
                attempt(&state, 1, 3),
                target(vec![4], crate::FamilyPolicy::RequireAll),
                now
            ),
            Err(NetworkError::Overloaded)
        ));
        assert!(matches!(
            state.reserve(
                attempt(&state, 1, 4),
                target(vec![6], crate::FamilyPolicy::Auto),
                now
            ),
            Err(NetworkError::UnsupportedFamily)
        ));
        state.nodes.clear();
        assert!(matches!(
            state.reserve(
                attempt(&state, 1, 5),
                target(vec![4, 6], crate::FamilyPolicy::RequireAll),
                now
            ),
            Err(NetworkError::Overloaded)
        ));
    }
    #[test]
    fn revoked_registry_churn_keeps_replay_and_lease_rows_bounded() {
        let now = Instant::now();
        let mut state = authority(1, now);
        for device in 2..=512 {
            state
                .apply_registry(Registry {
                    revision: device,
                    devices: vec![device],
                    nodes: state.registry.nodes.clone(),
                })
                .unwrap();
            state
                .client_lease(device, &id(device as u128), 1, now)
                .unwrap();
            assert_eq!(state.clients.len(), 1);
            assert_eq!(state.high_water.len(), 1);
            assert!(state.control_records() < 512);
        }
    }
    #[test]
    fn delayed_and_reordered_ack_do_not_extend_send_based_self_fence() {
        let now = Instant::now();
        let mut lease = LocalLease::default();
        lease.sent(1, now);
        assert!(lease.acknowledge(1, id(9), 6000, after(now, 5900)));
        assert!(!lease.ready(after(now, 6000)));
        lease.sent(2, after(now, 2000));
        lease.sent(3, after(now, 4000));
        assert!(lease.acknowledge(3, id(9), 6000, after(now, 5000)));
        assert!(!lease.acknowledge(2, id(9), 6000, after(now, 5001)));
        assert!(!lease.ready(after(now, 10000)));
        lease.sent(4, after(now, 6000));
        assert!(!lease.acknowledge(4, id(9), 6000, after(now, 12000)));
    }
    #[test]
    fn compact_full_membership_is_atomic_bounded_and_epoch_bound() {
        let now = Instant::now();
        let mut exit = Egress::new(id(2));
        let rows: Vec<_> = (1..=128).map(|i| Member(i, id(i as u128), 6000)).collect();
        for (part, chunk) in rows.chunks(48).enumerate() {
            let body = Body::Snapshot {
                authority: id(9),
                node_epoch: id(2),
                revision: 1,
                part: part as u8,
                parts: 3,
                members: chunk.to_vec(),
            };
            let envelope = Envelope {
                v: 1,
                from: 0,
                epoch: id(9),
                request: 1,
                body,
            };
            assert!(envelope.encode().unwrap().len() <= MESSAGE_MAX);
            assert_eq!(
                exit.snapshot(id(9), &id(2), 1, part as u8, 3, chunk.to_vec(), now, now)
                    .unwrap(),
                part == 2
            );
            assert_eq!(exit.members.len(), if part == 2 { 128 } else { 0 });
        }
        assert!(
            exit.snapshot(id(9), &id(3), 2, 0, 1, vec![], now, now)
                .is_err()
        );
        exit.cleanup(after(now, 6000), |_, _| false);
        assert!(exit.members.is_empty());
    }
    #[test]
    fn lost_command_newer_query_fences_delayed_reserve_and_guard_proof() {
        let now = Instant::now();
        let owner = (1 << 63) + 8;
        let mut exit = Egress::new(id(2));
        exit.snapshot(
            id(9),
            &id(2),
            1,
            0,
            1,
            vec![Member(1, id(1), 6000)],
            now,
            now,
        )
        .unwrap();
        let attempt = Attempt {
            authority: id(9),
            device: 1,
            epoch: id(1),
            sequence: 1,
        };
        let assignment = Assignment {
            attempt: attempt.clone(),
            owner,
            owner_epoch: id(2),
            token: id(3),
        };
        let target = Metadata::Tcp {
            v: crate::NETWORK_VERSION,
            host: "example.org".into(),
            port: 443,
        };
        assert!(exit.query(2, &attempt));
        assert!(
            exit.reserve(1, assignment.clone(), target.clone(), now)
                .is_err()
        );
        let mut newer = assignment;
        newer.attempt.sequence = 2;
        exit.reserve(3, newer, target, now).unwrap();
        assert_eq!(exit.counts(), (1, 0));
    }
}
