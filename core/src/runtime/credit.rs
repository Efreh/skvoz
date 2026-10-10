use super::RuntimeError;
use std::time::{Duration, Instant};

/// One finite demand round, never payload or an application replay buffer.
#[derive(Default)]
pub(super) struct Sender {
    pub round: u128,
    pub demand: u64,
    pub granted: Option<u64>,
    pub used: u64,
    pub sealed: bool,
    pub published: bool,
    pub deadline: Option<Instant>,
}
impl Sender {
    pub fn request(
        &mut self,
        bytes: u64,
        now: Instant,
        timeout: Duration,
    ) -> Result<bool, RuntimeError> {
        if bytes == 0 || self.deadline.is_some() {
            return Ok(false);
        }
        self.round = self
            .round
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        self.demand = bytes;
        self.granted = None;
        self.used = 0;
        self.sealed = false;
        self.published = false;
        self.deadline = Some(now + timeout);
        Ok(true)
    }
    pub fn grant(&mut self, round: u128, bytes: u64) -> Result<(), RuntimeError> {
        if round != self.round || self.deadline.is_none() || self.sealed {
            return Ok(());
        }
        if bytes == 0 || bytes > self.demand {
            return Err(RuntimeError::Protocol);
        }
        if let Some(old) = self.granted {
            if old != bytes {
                return Err(RuntimeError::Protocol);
            }
        } else {
            self.granted = Some(bytes);
            self.published = false;
        }
        Ok(())
    }
    pub fn admits(&self, bytes: u64) -> bool {
        !self.sealed && self.granted.is_some_and(|n| bytes <= n - self.used)
    }
    pub fn consume(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        if !self.admits(bytes) {
            return Err(RuntimeError::Protocol);
        }
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or(RuntimeError::IdentityExhausted)?;
        Ok(())
    }
    pub fn seal(&mut self) {
        if self.granted.is_some() && !self.sealed {
            self.sealed = true;
            self.published = false;
        }
    }
    pub fn acknowledge(&mut self, round: u128, used: u64) -> Result<(), RuntimeError> {
        if round != self.round || !self.sealed || self.deadline.is_none() {
            return Ok(());
        }
        if used != self.used {
            return Err(RuntimeError::Protocol);
        }
        self.deadline = None;
        self.granted = None;
        self.demand = 0;
        self.used = 0;
        self.sealed = false;
        self.published = false;
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct Recipient {
    pub round: u128,
    pub demand: u64,
    pub granted: Option<u64>,
    pub applied: u64,
    pub returned: Option<u64>,
    pub published: bool,
    pub deadline: Option<Instant>,
}
impl Recipient {
    pub fn request(
        &mut self,
        round: u128,
        bytes: u64,
        maximum: u64,
        now: Instant,
        timeout: Duration,
    ) -> Result<(), RuntimeError> {
        if round <= self.round {
            return Ok(());
        }
        if self.deadline.is_some() || round == 0 || bytes == 0 || bytes > maximum {
            return Err(RuntimeError::Protocol);
        }
        self.round = round;
        self.demand = bytes;
        self.granted = None;
        self.applied = 0;
        self.returned = None;
        self.published = false;
        self.deadline = Some(now + timeout);
        Ok(())
    }
    pub fn reserve(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        if self.granted.is_some() || bytes == 0 || bytes > self.demand {
            return Err(RuntimeError::Protocol);
        }
        self.granted = Some(bytes);
        Ok(())
    }
    pub fn outstanding(&self) -> u64 {
        self.granted.map_or(0, |n| n - self.applied)
    }
    pub fn apply(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        let next = self
            .applied
            .checked_add(bytes)
            .ok_or(RuntimeError::IdentityExhausted)?;
        if self.granted.is_none_or(|n| next > n) || self.returned.is_some_and(|n| next > n) {
            return Err(RuntimeError::Protocol);
        }
        self.applied = next;
        Ok(())
    }
    pub fn admits(&self, bytes: u64) -> bool {
        self.applied.checked_add(bytes).is_some_and(|next| {
            self.granted.is_some_and(|n| next <= n) && self.returned.is_none_or(|n| next <= n)
        })
    }
    pub fn returned(&mut self, round: u128, bytes: u64) -> Result<(), RuntimeError> {
        if round != self.round || self.deadline.is_none() {
            return Ok(());
        }
        if self.granted.is_none_or(|n| bytes > n) || bytes < self.applied {
            return Err(RuntimeError::Protocol);
        }
        if self.returned.is_some_and(|old| old != bytes) {
            return Err(RuntimeError::Protocol);
        }
        self.returned = Some(bytes);
        Ok(())
    }
    pub fn complete(&self) -> bool {
        self.returned == Some(self.applied)
    }
    pub fn acknowledge(&mut self) {
        // Retain round high-water after history/credit expiry.
        self.granted = None;
        self.deadline = None;
        self.demand = 0;
        self.returned = None;
        self.published = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overtaking_return_and_late_grant_never_reuse_unapplied_credit() {
        let now = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut tx = Sender::default();
        let mut rx = Recipient::default();
        assert!(tx.request(4096, now, timeout).unwrap());
        rx.request(tx.round, 4096, 8192, now, timeout).unwrap();
        rx.reserve(4096).unwrap();
        tx.grant(tx.round, 4096).unwrap();
        tx.consume(1024).unwrap();
        tx.seal();
        tx.grant(tx.round, 4096).unwrap();
        assert!(!tx.admits(1));
        rx.returned(tx.round, 1024).unwrap();
        assert!(!rx.complete());
        assert_eq!(rx.outstanding(), 4096);
        rx.apply(1024).unwrap();
        assert!(rx.complete());
        assert_eq!(rx.outstanding(), 3072);
        rx.acknowledge();
        tx.acknowledge(tx.round, 1024).unwrap();
        assert_eq!(rx.outstanding(), 0);
        rx.request(tx.round, 4096, 8192, now, timeout).unwrap();
        assert!(rx.deadline.is_none());
        assert!(tx.request(1024, now, timeout).unwrap());
        tx.grant(tx.round - 1, 4096).unwrap();
        assert!(!tx.admits(1));
        assert_eq!(tx.deadline, Some(now + timeout));
    }
    #[test]
    fn round_loss_and_counter_exhaustion_fail_closed() {
        let now = Instant::now();
        let mut tx = Sender::default();
        tx.request(100, now, Duration::from_secs(10)).unwrap();
        assert!(
            !tx.request(100, now + Duration::from_secs(20), Duration::from_secs(10))
                .unwrap()
        );
        assert_eq!(tx.deadline, Some(now + Duration::from_secs(10)));
        assert!(!tx.admits(1));
        tx.deadline = None;
        tx.round = u128::MAX;
        assert!(matches!(
            tx.request(100, now, Duration::from_secs(10)),
            Err(RuntimeError::IdentityExhausted)
        ));
        let mut rx = Recipient::default();
        rx.request(1, 100, 100, now, Duration::from_secs(10))
            .unwrap();
        rx.reserve(100).unwrap();
        assert!(rx.apply(101).is_err());
        rx.apply(10).unwrap();
        assert!(rx.returned(1, 9).is_err());
    }
}
