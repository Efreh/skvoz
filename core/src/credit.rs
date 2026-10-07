//! Conserved aggregate DATA permissions; callers own authenticated wire order.
use crate::{Frame, ProtocolError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Amount {
    pub bytes: u64,
    pub records: u64,
}
impl Amount {
    pub fn data(bytes: usize) -> Self {
        Self {
            bytes: bytes as u64,
            records: 1,
        }
    }
    pub fn contains(self, other: Self) -> bool {
        self.bytes >= other.bytes && self.records >= other.records
    }
    pub fn add(self, other: Self) -> Result<Self, ProtocolError> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .ok_or(ProtocolError::InvalidCredit)?,
            records: self
                .records
                .checked_add(other.records)
                .ok_or(ProtocolError::InvalidCredit)?,
        })
    }
    pub fn sub(self, other: Self) -> Result<Self, ProtocolError> {
        Ok(Self {
            bytes: self
                .bytes
                .checked_sub(other.bytes)
                .ok_or(ProtocolError::InvalidCredit)?,
            records: self
                .records
                .checked_sub(other.records)
                .ok_or(ProtocolError::InvalidCredit)?,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct SendCredit {
    pub epoch: u64,
    pub dispatched: Amount,
    pub pending: Amount,
    pub consumed: Amount,
    pub allowed: Amount,
    pub freezing: Option<u64>,
    pub frozen: bool,
}
impl SendCredit {
    pub fn room(&self) -> Amount {
        if self.freezing.is_some() {
            return Amount::default();
        }
        self.allowed
            .sub(self.dispatched)
            .and_then(|a| a.sub(self.pending))
            .unwrap_or_default()
    }
    pub fn reserve(&mut self, amount: Amount) -> Result<(), ProtocolError> {
        if !self.room().contains(amount) {
            return Err(ProtocolError::InvalidCredit);
        }
        self.pending = self.pending.add(amount)?;
        Ok(())
    }
    pub fn release_pending(&mut self, amount: Amount) -> Result<(), ProtocolError> {
        self.pending = self.pending.sub(amount)?;
        Ok(())
    }
    pub fn dispatch(&mut self, amount: Amount) -> Result<(), ProtocolError> {
        let pending = self.pending.sub(amount)?;
        let dispatched = self.dispatched.add(amount)?;
        if !self.allowed.contains(dispatched.add(pending)?) {
            return Err(ProtocolError::InvalidCredit);
        }
        self.pending = pending;
        self.dispatched = dispatched;
        Ok(())
    }
    pub fn grant(
        &mut self,
        epoch: u64,
        consumed: Amount,
        allowed: Amount,
    ) -> Result<(), ProtocolError> {
        if epoch < self.epoch {
            return Ok(());
        }
        if !consumed.contains(self.consumed)
            || !self.dispatched.contains(consumed)
            || !allowed.contains(consumed)
            || !allowed.contains(self.dispatched.add(self.pending)?)
        {
            return Err(ProtocolError::InvalidCredit);
        }
        if epoch == self.epoch {
            if !consumed.contains(self.consumed) || !allowed.contains(self.allowed) {
                // Ordered transport makes a partial regression a protocol error.
                if self.consumed.contains(consumed) && self.allowed.contains(allowed) {
                    return Ok(());
                }
                return Err(ProtocolError::InvalidCredit);
            }
            if self.frozen {
                return Err(ProtocolError::InvalidCredit);
            }
        } else if self.freezing != Some(epoch) || !self.frozen {
            return Err(ProtocolError::InvalidCredit);
        }
        self.epoch = epoch;
        self.consumed = consumed;
        self.allowed = allowed;
        self.freezing = None;
        self.frozen = false;
        Ok(())
    }
    pub fn freeze(&mut self, epoch: u64) -> Result<(), ProtocolError> {
        if epoch <= self.epoch {
            return Ok(());
        }
        if self.epoch.checked_add(1) != Some(epoch) || self.freezing.is_some_and(|old| old != epoch)
        {
            return Err(ProtocolError::InvalidCredit);
        }
        self.freezing = Some(epoch);
        Ok(())
    }
    /// Extract only after every pending DATA frame was extracted in wire order.
    pub fn frozen_frame(&mut self) -> Option<Frame> {
        let frame = self.peek_frozen_frame()?;
        self.frozen = true;
        Some(frame)
    }
    pub fn peek_frozen_frame(&self) -> Option<Frame> {
        let epoch = self.freezing?;
        if self.pending != Amount::default() || self.frozen {
            return None;
        }
        Some(Frame::PeerFrozen {
            epoch,
            bytes: self.dispatched.bytes,
            records: self.dispatched.records,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReceiveCredit {
    pub epoch: u64,
    pub received: Amount,
    pub consumed: Amount,
    pub allowed: Amount,
    pub freezing: Option<u64>,
}
impl ReceiveCredit {
    pub fn promise(&self) -> Amount {
        self.allowed
            .sub(self.consumed)
            .expect("receive credit conservation")
    }
    pub fn outstanding(&self) -> Amount {
        self.received
            .sub(self.consumed)
            .expect("receive consumption conservation")
    }
    pub fn receive(&mut self, amount: Amount) -> Result<(), ProtocolError> {
        let next = self.received.add(amount)?;
        if !self.allowed.contains(next) {
            return Err(ProtocolError::ReceiveWindowExceeded);
        }
        self.received = next;
        Ok(())
    }
    pub fn consume(&mut self, amount: Amount) -> Result<(), ProtocolError> {
        let next = self.consumed.add(amount)?;
        if !self.received.contains(next) {
            return Err(ProtocolError::InvalidCredit);
        }
        self.consumed = next;
        Ok(())
    }
    pub fn grant(&mut self, headroom: Amount) -> Result<(), ProtocolError> {
        if self.freezing.is_some() {
            return Err(ProtocolError::InvalidCredit);
        }
        let next = self.consumed.add(headroom)?;
        self.allowed.bytes = self.allowed.bytes.max(next.bytes);
        self.allowed.records = self.allowed.records.max(next.records);
        Ok(())
    }
    pub fn frame(&self, probe: u64) -> Frame {
        Frame::PeerGrant {
            epoch: self.epoch,
            consumed_bytes: self.consumed.bytes,
            limit_bytes: self.allowed.bytes,
            consumed_records: self.consumed.records,
            limit_records: self.allowed.records,
            probe,
        }
    }
    pub fn freeze(&mut self) -> Result<Frame, ProtocolError> {
        if self.freezing.is_some() {
            return Err(ProtocolError::InvalidCredit);
        }
        let epoch = self
            .epoch
            .checked_add(1)
            .ok_or(ProtocolError::InvalidCredit)?;
        self.freezing = Some(epoch);
        Ok(Frame::PeerFreeze { epoch })
    }
    /// The authenticated lane must have delivered every DATA before this ACK.
    pub fn frozen(
        &mut self,
        epoch: u64,
        dispatched: Amount,
        headroom: Amount,
    ) -> Result<(), ProtocolError> {
        if self.freezing != Some(epoch) || dispatched != self.received {
            return Err(ProtocolError::InvalidCredit);
        }
        let allowed = self.received.add(headroom)?;
        self.epoch = epoch;
        self.allowed = allowed;
        self.freezing = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const POOL: Amount = Amount {
        bytes: 1024,
        records: 4,
    };
    fn pair() -> (SendCredit, ReceiveCredit) {
        let mut receive = ReceiveCredit::default();
        receive.grant(POOL).unwrap();
        let mut send = SendCredit::default();
        send.grant(0, receive.consumed, receive.allowed).unwrap();
        (send, receive)
    }
    #[test]
    fn bytes_and_records_gate_pending_dispatched_and_actual_consumption() {
        let (mut tx, mut rx) = pair();
        for _ in 0..4 {
            tx.reserve(Amount::data(1)).unwrap();
        }
        assert_eq!(tx.room().records, 0);
        assert!(tx.reserve(Amount::data(1)).is_err());
        tx.dispatch(Amount::data(1)).unwrap();
        assert_eq!(tx.room().records, 0);
        assert!(rx.consume(Amount::data(1)).is_err());
        rx.receive(Amount::data(1)).unwrap();
        assert_eq!(rx.promise(), POOL);
        rx.consume(Amount::data(1)).unwrap();
        rx.grant(POOL).unwrap();
        tx.grant(0, rx.consumed, rx.allowed).unwrap();
        assert_eq!(tx.room().records, 1);
        assert_eq!(rx.promise(), POOL);
    }
    #[test]
    fn cancelled_unsent_reservations_do_not_consume_unarrived_promises() {
        let (mut tx, mut rx) = pair();
        tx.reserve(Amount::data(100)).unwrap();
        tx.release_pending(Amount::data(100)).unwrap();
        assert_eq!(tx.room(), POOL);
        assert_eq!(rx.promise(), POOL);
        assert!(rx.consume(Amount::data(100)).is_err());
        // Late DATA remains constrained even when its stream has been reaped.
        rx.receive(Amount::data(100)).unwrap();
        rx.consume(Amount::data(100)).unwrap();
        assert_eq!(
            rx.promise(),
            Amount {
                bytes: 924,
                records: 3
            }
        );
    }
    #[test]
    fn barrier_cannot_overtake_pending_data_and_only_ack_reclaims_unused_credit() {
        let (mut tx, mut rx) = pair();
        tx.reserve(Amount::data(100)).unwrap();
        let Frame::PeerFreeze { epoch } = rx.freeze().unwrap() else {
            unreachable!()
        };
        tx.freeze(epoch).unwrap();
        assert_eq!(tx.room(), Amount::default());
        assert!(tx.frozen_frame().is_none());
        assert_eq!(rx.promise(), POOL);
        assert!(
            rx.frozen(epoch, Amount::data(100), Amount::default())
                .is_err()
        );
        tx.dispatch(Amount::data(100)).unwrap();
        rx.receive(Amount::data(100)).unwrap();
        let Frame::PeerFrozen { bytes, records, .. } = tx.frozen_frame().unwrap() else {
            unreachable!()
        };
        rx.frozen(
            epoch,
            Amount { bytes, records },
            Amount {
                bytes: 10,
                records: 1,
            },
        )
        .unwrap();
        assert_eq!(
            rx.promise(),
            Amount {
                bytes: 110,
                records: 2
            }
        );
        tx.grant(epoch, rx.consumed, rx.allowed).unwrap();
        assert_eq!(
            tx.room(),
            Amount {
                bytes: 10,
                records: 1
            }
        );
        assert!(tx.grant(epoch + 1, rx.consumed, rx.allowed).is_err());
    }
    #[test]
    fn a_new_epoch_cannot_regress_actual_consumption() {
        let (mut tx, mut rx) = pair();
        tx.reserve(Amount::data(100)).unwrap();
        tx.dispatch(Amount::data(100)).unwrap();
        rx.receive(Amount::data(100)).unwrap();
        rx.consume(Amount::data(100)).unwrap();
        rx.grant(POOL).unwrap();
        tx.grant(0, rx.consumed, rx.allowed).unwrap();
        rx.freeze().unwrap();
        tx.freeze(1).unwrap();
        tx.frozen_frame().unwrap();
        rx.frozen(1, tx.dispatched, POOL).unwrap();
        assert!(tx.grant(1, Amount::default(), rx.allowed).is_err());
        assert_eq!(tx.room(), Amount::default());
        tx.grant(1, rx.consumed, rx.allowed).unwrap();
        assert_eq!(tx.room(), POOL);
    }
    #[test]
    fn malformed_duplicate_stale_and_overflow_counters_do_not_mint_credit() {
        let (mut tx, mut rx) = pair();
        tx.grant(0, rx.consumed, rx.allowed).unwrap();
        assert_eq!(tx.room(), POOL);
        assert!(tx.grant(0, Amount::data(1), rx.allowed).is_err());
        assert!(
            rx.receive(Amount {
                bytes: 1025,
                records: 1
            })
            .is_err()
        );
        assert_eq!(rx.received, Amount::default());
        rx.allowed = Amount {
            bytes: u64::MAX,
            records: u64::MAX,
        };
        rx.received = rx.allowed;
        assert!(rx.receive(Amount::data(1)).is_err());
        assert_eq!(rx.received, rx.allowed);
    }
}
