//! Continuous, conserved delivery permissions. No payload is retained here.

use super::RuntimeError;
use crate::credit::Amount;
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

/// One outstanding sequenced control. Receipts themselves are not sequenced.
#[derive(Debug, Default)]
pub(super) struct ControlWindow {
    pub sent: u64,
    pub acknowledged: u64,
    pub received: u64,
    pub receipt_sent: u64,
    pub deadline: Option<Instant>,
}

impl ControlWindow {
    pub fn begin(&mut self, now: Instant, timeout: Duration) -> Result<Option<u64>, RuntimeError> {
        if self.deadline.is_some() {
            return Ok(None);
        }
        self.sent = self
            .sent
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        self.deadline = Some(now + timeout);
        Ok(Some(self.sent))
    }

    pub fn acknowledge(&mut self, sequence: u64) -> Result<(), RuntimeError> {
        if sequence > self.sent {
            return Err(RuntimeError::Protocol);
        }
        if sequence > self.acknowledged {
            self.acknowledged = sequence;
            self.deadline = None;
        }
        Ok(())
    }

    pub fn fresh(&self, sequence: u64) -> Result<bool, RuntimeError> {
        if sequence == 0
            || sequence
                > self
                    .received
                    .checked_add(1)
                    .ok_or(RuntimeError::IdentityExhausted)?
        {
            return Err(RuntimeError::Protocol);
        }
        Ok(sequence > self.received)
    }

    /// Invoke only after the complete action, including any DATA prefix barrier.
    pub fn applied(&mut self, sequence: u64) -> Result<(), RuntimeError> {
        if self.fresh(sequence)? {
            self.received = sequence;
        }
        Ok(())
    }

    pub fn receipt_dirty(&self) -> bool {
        self.received > self.receipt_sent
    }

    pub fn mark_receipt_sent(&mut self) {
        self.receipt_sent = self.received;
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Limit {
    pub epoch: u64,
    pub allowed: Amount,
    pub applied: Amount,
}

/// Local reservations and committed publication are distinct ownership stages.
#[derive(Debug, Default)]
pub(super) struct Sender {
    pub epoch: u64,
    pub allowed: Amount,
    pub pending: Amount,
    pub sent: Amount,
    pub applied: Amount,
    pub freezing: Option<u64>,
    ends: VecDeque<Amount>,
}

impl Sender {
    pub fn room(&self) -> Amount {
        if self.freezing.is_some() {
            return Amount::default();
        }
        self.allowed
            .sub(self.sent)
            .and_then(|v| v.sub(self.pending))
            .unwrap_or_default()
    }

    pub fn grant(&mut self, limit: Limit) -> Result<(), RuntimeError> {
        if limit.epoch < self.epoch {
            return Ok(());
        }
        if (limit.applied != self.applied && !self.ends.contains(&limit.applied))
            || !self.sent.contains(limit.applied)
            || !limit.applied.contains(self.applied)
            || !limit.allowed.contains(self.sent.add(self.pending)?)
        {
            return Err(RuntimeError::Protocol);
        }
        if limit.epoch == self.epoch {
            if self.freezing.is_some() {
                return Ok(());
            }
            if !limit.allowed.contains(self.allowed) {
                return Err(RuntimeError::Protocol);
            }
        } else if self.freezing != Some(limit.epoch) || self.pending != Amount::default() {
            return Err(RuntimeError::Protocol);
        }
        self.epoch = limit.epoch;
        self.allowed = limit.allowed;
        self.applied = limit.applied;
        while self
            .ends
            .front()
            .is_some_and(|end| end.records <= limit.applied.records)
        {
            self.ends.pop_front();
        }
        self.freezing = None;
        Ok(())
    }

    pub fn apply_receipt(&mut self, applied: Amount) -> Result<(), RuntimeError> {
        // A matched older progress probe may arrive after a newer CREDIT.
        if self.applied.contains(applied) {
            return Ok(());
        }
        if !self.sent.contains(applied)
            || !applied.contains(self.applied)
            || (applied != self.applied && !self.ends.contains(&applied))
        {
            return Err(RuntimeError::Protocol);
        }
        self.applied = applied;
        while self
            .ends
            .front()
            .is_some_and(|end| end.records <= applied.records)
        {
            self.ends.pop_front();
        }
        Ok(())
    }
    pub fn reserve(&mut self, amount: Amount) -> Result<(), RuntimeError> {
        if !self.room().contains(amount) {
            return Err(RuntimeError::Protocol);
        }
        self.pending = self.pending.add(amount)?;
        Ok(())
    }

    /// Only a frame proven never passed to publication can be cancelled locally.
    #[cfg(test)]
    pub fn cancel_pending(&mut self, amount: Amount) -> Result<(), RuntimeError> {
        self.pending = self.pending.sub(amount)?;
        Ok(())
    }

    pub fn commit(&mut self, amount: Amount) -> Result<(), RuntimeError> {
        let pending = self.pending.sub(amount)?;
        let sent = self.sent.add(amount)?;
        if !self.allowed.contains(sent.add(pending)?) {
            return Err(RuntimeError::Protocol);
        }
        if amount.records != 1 {
            return Err(RuntimeError::Protocol);
        }
        self.pending = pending;
        self.sent = sent;
        self.ends.push_back(sent);
        Ok(())
    }

    pub fn freeze(&mut self, next_epoch: u64) -> Result<(), RuntimeError> {
        if self.epoch.checked_add(1) != Some(next_epoch)
            || self.freezing.is_some_and(|old| old != next_epoch)
        {
            return Err(RuntimeError::Protocol);
        }
        self.freezing = Some(next_epoch);
        Ok(())
    }

    pub fn frozen_prefix(&self) -> Option<Amount> {
        (self.freezing.is_some() && self.pending == Amount::default()).then_some(self.sent)
    }

    pub fn outstanding(&self) -> Amount {
        self.sent
            .sub(self.applied)
            .expect("validated delivery receipt")
    }
}

#[derive(Debug, Default)]
pub(super) struct Recipient {
    pub epoch: u64,
    pub allowed: Amount,
    pub applied: Amount,
    pub freezing: Option<u64>,
    pub frozen: Option<Amount>,
}

impl Recipient {
    pub fn snapshot(&self) -> Limit {
        Limit {
            epoch: self.epoch,
            allowed: self.allowed,
            applied: self.applied,
        }
    }

    pub fn obligation(&self) -> Amount {
        self.allowed
            .sub(self.applied)
            .expect("conserved delivery grant")
    }

    /// The caller reserves this increase in the shared bytes/record pool first.
    pub fn extend(&mut self, amount: Amount) -> Result<(), RuntimeError> {
        if self.freezing.is_some() {
            return Err(RuntimeError::Protocol);
        }
        self.allowed = self.allowed.add(amount)?;
        Ok(())
    }

    pub fn apply(&mut self, amount: Amount) -> Result<(), RuntimeError> {
        let applied = self.applied.add(amount)?;
        if !self.allowed.contains(applied)
            || self.frozen.is_some_and(|prefix| !prefix.contains(applied))
        {
            return Err(RuntimeError::Protocol);
        }
        self.applied = applied;
        Ok(())
    }

    pub fn freeze(&mut self) -> Result<u64, RuntimeError> {
        let next = self
            .epoch
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        if self.freezing.is_some_and(|old| old != next) {
            return Err(RuntimeError::Protocol);
        }
        self.freezing = Some(next);
        Ok(next)
    }

    /// CONTROL may overtake DATA; this alone never releases the unused grant.
    pub fn observe_frozen(&mut self, epoch: u64, prefix: Amount) -> Result<(), RuntimeError> {
        if self.freezing != Some(epoch)
            || !self.allowed.contains(prefix)
            || !prefix.contains(self.applied)
            || self.frozen.is_some_and(|old| old != prefix)
        {
            return Err(RuntimeError::Protocol);
        }
        self.frozen = Some(prefix);
        Ok(())
    }

    pub fn reclaim(&mut self) -> Result<Option<Amount>, RuntimeError> {
        if self.frozen != Some(self.applied) {
            return Ok(None);
        }
        let epoch = self.freezing.ok_or(RuntimeError::Protocol)?;
        let released = self.obligation();
        self.allowed = self.applied;
        self.epoch = epoch;
        self.frozen = None;
        self.freezing = None;
        Ok(Some(released))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn amount(bytes: u64, records: u64) -> Amount {
        Amount { bytes, records }
    }

    #[test]
    fn refill_overlaps_publication_without_a_new_demand_round() {
        let mut rx = Recipient::default();
        let mut tx = Sender::default();
        rx.extend(amount(4096, 4)).unwrap();
        tx.grant(rx.snapshot()).unwrap();
        for _ in 0..128 {
            tx.reserve(amount(1024, 1)).unwrap();
            tx.commit(amount(1024, 1)).unwrap();
            rx.apply(amount(1024, 1)).unwrap();
            rx.extend(amount(1024, 1)).unwrap();
            assert_eq!(rx.obligation(), amount(4096, 4));
            assert!(tx.room().contains(amount(1024, 1)));
            tx.grant(rx.snapshot()).unwrap();
            assert_eq!(tx.room(), amount(4096, 4));
        }
        assert_eq!(tx.sent, amount(131072, 128));
        assert_eq!(tx.outstanding(), Amount::default());
    }

    #[test]
    fn uncommitted_reservations_can_be_cancelled_but_sent_bytes_cannot() {
        let mut tx = Sender::default();
        tx.grant(Limit {
            allowed: amount(10, 2),
            ..Limit::default()
        })
        .unwrap();
        tx.reserve(amount(8, 1)).unwrap();
        assert!(!tx.room().contains(amount(3, 1)));
        tx.cancel_pending(amount(8, 1)).unwrap();
        tx.reserve(amount(8, 1)).unwrap();
        tx.commit(amount(8, 1)).unwrap();
        assert!(tx.cancel_pending(amount(8, 1)).is_err());
        assert_eq!(tx.outstanding(), amount(8, 1));
    }

    #[test]
    fn early_frozen_control_never_reuses_data_still_in_flight() {
        let mut rx = Recipient::default();
        let mut tx = Sender::default();
        rx.extend(amount(4096, 4)).unwrap();
        tx.grant(rx.snapshot()).unwrap();
        tx.reserve(amount(1024, 1)).unwrap();
        let epoch = rx.freeze().unwrap();
        tx.freeze(epoch).unwrap();
        assert!(tx.frozen_prefix().is_none());
        assert!(tx.reserve(amount(1, 1)).is_err());
        tx.commit(amount(1024, 1)).unwrap();
        rx.observe_frozen(epoch, tx.frozen_prefix().unwrap())
            .unwrap();
        assert_eq!(rx.reclaim().unwrap(), None);
        assert_eq!(rx.obligation(), amount(4096, 4));
        rx.apply(amount(1024, 1)).unwrap();
        assert_eq!(rx.reclaim().unwrap(), Some(amount(3072, 3)));
        tx.grant(rx.snapshot()).unwrap();
        assert_eq!(tx.room(), Amount::default());
        rx.extend(amount(512, 1)).unwrap();
        tx.grant(rx.snapshot()).unwrap();
        assert_eq!(tx.room(), amount(512, 1));
    }

    #[test]
    fn record_limits_apply_independently_of_byte_capacity() {
        let mut rx = Recipient::default();
        rx.extend(amount(1 << 20, 1)).unwrap();
        rx.apply(amount(1, 1)).unwrap();
        assert!(rx.apply(amount(1, 1)).is_err());
        assert_eq!(rx.obligation(), amount((1 << 20) - 1, 0));
    }

    #[test]
    fn forged_receipts_and_unrequested_epochs_fail_without_mutation() {
        let mut tx = Sender::default();
        let initial = Limit {
            allowed: amount(4096, 4),
            ..Limit::default()
        };
        tx.grant(initial).unwrap();
        for invalid in [
            Limit {
                applied: amount(1, 0),
                ..initial
            },
            Limit {
                epoch: 1,
                ..initial
            },
            Limit {
                allowed: amount(1, 4),
                ..initial
            },
        ] {
            assert!(tx.grant(invalid).is_err());
            assert_eq!(tx.allowed, initial.allowed);
            assert_eq!(tx.epoch, 0);
        }
    }

    #[test]
    fn aggregate_promises_include_idle_and_quarantined_sources() {
        let budget = amount(8192, 8);
        let mut sources = [Recipient::default(), Recipient::default()];
        sources[0].extend(amount(4096, 4)).unwrap();
        sources[1].extend(amount(4096, 4)).unwrap();
        let total = sources[0]
            .obligation()
            .add(sources[1].obligation())
            .unwrap();
        assert_eq!(total, budget);
        // Retirement transfers the same obligation into quarantine. It cannot
        // disappear merely because the live source table no longer contains it.
        let quarantine = sources[0].obligation();
        let remaining = sources[1].obligation().add(quarantine).unwrap();
        assert_eq!(remaining, budget);
        assert!(!budget.sub(remaining).unwrap().contains(amount(1, 1)));
    }

    #[test]
    fn bounded_control_never_requires_receipt_of_a_receipt() {
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut a = ControlWindow::default();
        let mut b = ControlWindow::default();
        let seq = a.begin(now, timeout).unwrap().unwrap();
        assert!(a.begin(now, timeout).unwrap().is_none());
        assert!(b.fresh(seq).unwrap());
        // A prefix-bearing control may be received before its DATA. It is not
        // acknowledged as applied until that prefix has actually been handled.
        assert!(!b.receipt_dirty());
        b.applied(seq).unwrap();
        assert!(b.receipt_dirty());
        a.acknowledge(b.received).unwrap();
        b.mark_receipt_sent();
        assert!(a.deadline.is_none());
        assert!(b.deadline.is_none());
        assert!(!b.receipt_dirty());
        assert!(!b.fresh(seq).unwrap());
        assert_eq!(a.begin(now, timeout).unwrap(), Some(seq + 1));
    }

    #[test]
    fn future_control_ack_and_sequence_gaps_are_protocol_errors() {
        let mut c = ControlWindow::default();
        assert!(c.acknowledge(1).is_err());
        assert!(c.fresh(0).is_err());
        assert!(c.fresh(2).is_err());
        c.begin(Instant::now(), Duration::from_secs(10)).unwrap();
        assert!(c.acknowledge(2).is_err());
        assert!(c.deadline.is_some());
    }
}
