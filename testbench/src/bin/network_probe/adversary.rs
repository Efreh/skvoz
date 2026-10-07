//! Raw Core actor deliberately bypassing all client NetworkEngine packet policy.
use super::{ProbeResult, field};
use skvoz_core::{
    Event, PeerId, SendOutcome,
    runtime::{NatsRuntime, RuntimeKey},
};
use skvoz_network::{
    Accept, Control, Metadata, Record, RecordParser, SessionId, SessionSignal, encode_record,
};
use std::{
    collections::{BTreeMap, VecDeque},
    net::Ipv4Addr,
    time::{Duration, Instant},
};

struct Actor {
    runtime: NatsRuntime,
    deadline: Instant,
    parsers: BTreeMap<RuntimeKey, RecordParser>,
    records: BTreeMap<RuntimeKey, VecDeque<Record>>,
    events: BTreeMap<RuntimeKey, VecDeque<Event>>,
}
impl Actor {
    async fn turn(&mut self) -> ProbeResult<()> {
        if Instant::now() >= self.deadline {
            return Err(format!("Adversary deadline; core={:?}", self.runtime.status()).into());
        }
        self.runtime.turn(Duration::from_millis(1)).await?;
        for observed in self.runtime.poll_events(32) {
            match observed.event {
                Event::Data { offset, bytes } => {
                    let parser = self
                        .parsers
                        .get_mut(&observed.key)
                        .ok_or("Unexpected adversary DATA")?;
                    parser.push(offset, &bytes)?;
                    while let Some(record) = parser.next_record()? {
                        let queue = self.records.entry(observed.key).or_default();
                        if queue.len() >= 8 {
                            return Err("Adversary record queue exceeded".into());
                        }
                        queue.push_back(record);
                    }
                }
                Event::Writable => {}
                Event::IncomingOpen { .. } => return Err("Unexpected server OPEN".into()),
                Event::Closed { .. } | Event::RemoteFinished
                    if self.parsers.contains_key(&observed.key) =>
                {
                    return Err(
                        format!("Adversary live stream terminated: {:?}", observed.event).into(),
                    );
                }
                event => {
                    let queue = self.events.entry(observed.key).or_default();
                    if queue.len() >= 8 {
                        return Err("Adversary event queue exceeded".into());
                    }
                    queue.push_back(event);
                }
            }
        }
        Ok(())
    }
    async fn opened(&mut self, key: RuntimeKey) -> ProbeResult<Accept> {
        loop {
            if let Some(event) = self.events.entry(key).or_default().pop_front() {
                return match event {
                    Event::Opened { metadata } => Ok(Accept::decode(&metadata)?),
                    other => Err(format!("Expected ACCEPT, got {other:?}").into()),
                };
            }
            self.turn().await?;
        }
    }
    async fn rejected(&mut self, session: &SessionId, channel: u8) -> ProbeResult<()> {
        let key = self.runtime.open(
            PeerId(0),
            &Metadata::IpData {
                v: 4,
                session: session.clone(),
                channel,
            }
            .encode()?,
        )?;
        loop {
            if let Some(event) = self.events.entry(key).or_default().pop_front() {
                match event {
                    Event::Rejected { reason } => {
                        let value: serde_json::Value = serde_json::from_slice(&reason)?;
                        if value != serde_json::json!({"v":4,"type":"ip-data","error":"forbidden"})
                        {
                            return Err("Wrong forbidden OPEN response".into());
                        }
                        println!("ADVERSARY REJECTED channel={channel}");
                        return Ok(());
                    }
                    other => return Err(format!("Forbidden OPEN did not reject: {other:?}").into()),
                }
            }
            self.turn().await?;
        }
    }
    async fn record(&mut self, key: RuntimeKey) -> ProbeResult<Record> {
        loop {
            if let Some(record) = self.records.entry(key).or_default().pop_front() {
                return Ok(record);
            }
            self.turn().await?;
        }
    }
    async fn send(&mut self, key: RuntimeKey, bytes: &[u8]) -> ProbeResult<()> {
        let mut cursor = 0;
        while cursor < bytes.len() {
            match self.runtime.send(key, &bytes[cursor..])? {
                SendOutcome::Accepted(count) => cursor += count,
                SendOutcome::WouldBlock => {}
            }
            self.turn().await?;
        }
        // At most one small record in flight; wait for server's actual consume.
        loop {
            let snapshot = self
                .runtime
                .snapshot(key)
                .ok_or("Adversary stream disappeared")?;
            if snapshot.pending_send_bytes == 0 && snapshot.send_unacknowledged_bytes == 0 {
                return Ok(());
            }
            self.turn().await?;
        }
    }
}

fn packet(source: Ipv4Addr, target: Ipv4Addr, marker: &str) -> Vec<u8> {
    let body = format!("SKVOZ-REQUEST:{marker}").into_bytes();
    let mut packet = vec![0; 20];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&((20 + body.len()) as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 143;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&target.octets());
    let mut sum: u32 = packet
        .chunks_exact(2)
        .map(|b| u32::from(u16::from_be_bytes([b[0], b[1]])))
        .sum();
    while sum > 65535 {
        sum = (sum & 65535) + (sum >> 16);
    }
    packet[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    packet.extend(body);
    packet
}

pub(super) async fn run(runtime: NatsRuntime, value: &serde_json::Value) -> ProbeResult<()> {
    let mut actor = Actor {
        runtime,
        deadline: Instant::now() + Duration::from_secs(30),
        parsers: BTreeMap::new(),
        records: BTreeMap::new(),
        events: BTreeMap::new(),
    };
    while !actor.runtime.peer_ready(PeerId(0)) {
        actor.turn().await?;
    }
    let control = actor.runtime.open(
        PeerId(0),
        &Metadata::IpSession {
            v: 4,
            families: vec![4],
            family_policy: skvoz_network::FamilyPolicy::RequireAll,
            max_mtu: 1500,
            channels: 1,
        }
        .encode()?,
    )?;
    actor
        .parsers
        .insert(control, RecordParser::new(true, 16384, 65536)?);
    let Accept::IpSession { session, .. } = actor.opened(control).await? else {
        return Err("Wrong session ACCEPT".into());
    };
    let record = actor.record(control).await?;
    let Control::Config(config) = Control::decode(&record)? else {
        return Err("Missing CONFIG".into());
    };
    if config.session != session
        || config.families != [4]
        || config.channels != 1
        || !config
            .source_grants
            .iter()
            .any(|g| g.contains("192.0.2.12".parse().unwrap()))
    {
        return Err("Unexpected adversary grant".into());
    }
    actor.runtime.consume_through(control, record.end_offset)?;
    let foreign = SessionId::try_from(field(value, "foreign_session")?.to_owned())?;
    actor.rejected(&foreign, 0).await?;
    actor.rejected(&session, 1).await?;
    let data = actor.runtime.open(
        PeerId(0),
        &Metadata::IpData {
            v: 4,
            session: session.clone(),
            channel: 0,
        }
        .encode()?,
    )?;
    actor
        .parsers
        .insert(data, RecordParser::new(false, 1500, 65536)?);
    if actor.opened(data).await?
        != (Accept::IpData {
            v: 4,
            session: session.clone(),
            channel: 0,
        })
    {
        return Err("Wrong data ACCEPT".into());
    }
    actor.rejected(&session, 0).await?;
    actor
        .send(
            control,
            &Control::Ready(SessionSignal {
                session: session.clone(),
            })
            .encode()?,
        )
        .await?;
    let active = actor.record(control).await?;
    if Control::decode(&active)?
        != Control::Active(SessionSignal {
            session: session.clone(),
        })
    {
        return Err("Missing ACTIVE".into());
    }
    actor.runtime.consume_through(control, active.end_offset)?;
    println!("ADVERSARY ACTIVE");
    let target: Ipv4Addr = field(value, "target")?.parse()?;
    let source: Ipv4Addr = "192.0.2.12".parse()?;
    let prefix = field(value, "marker")?;
    let mut attacks = vec![(
        "foreign-source",
        packet("192.0.2.11".parse()?, target, &format!("{prefix}-foreign")),
    )];
    let mut checksum = packet(source, target, &format!("{prefix}-checksum"));
    checksum[10] ^= 1;
    attacks.push(("bad-checksum", checksum));
    let mut length = packet(source, target, &format!("{prefix}-length"));
    length.push(0);
    attacks.push(("wrong-ip-length", length));
    attacks.push(("truncated-ip", vec![0x45; 19]));
    for (name, bytes) in attacks {
        // encode_record validates framing only, deliberately bypassing validate_packet.
        actor.send(data, &encode_record(16, &bytes)?).await?;
        println!("ADVERSARY SENT {name}");
    }
    let good = packet(source, target, &format!("{prefix}-good"));
    actor.send(data, &encode_record(16, &good)?).await?;
    let reply = actor.record(data).await?;
    let info = skvoz_network::validate_packet(&reply.payload, 1500, &[4])?;
    if info.source != target
        || info.destination != source
        || reply.payload[20..] != format!("SKVOZ-RESPONSE:{prefix}-good").as_bytes()[..]
    {
        return Err("Own good packet failed after attacks".into());
    }
    actor.runtime.consume_through(data, reply.end_offset)?;
    println!("ADVERSARY GOOD");
    actor.runtime.close(control)?;
    actor.runtime.close(data)?;
    actor.parsers.clear();
    actor.turn().await?;
    actor.runtime.shutdown().await?;
    for _ in 0..4 {
        if actor.runtime.poll_events(32).is_empty() {
            break;
        }
    }
    if actor.runtime.status().resources.streams != 0 {
        return Err("Adversary streams retained".into());
    }
    println!("ADVERSARY STOPPED core={:?}", actor.runtime.status());
    Ok(())
}
