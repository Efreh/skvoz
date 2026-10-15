//! A terminal bidirectional prefix fence. No timeout releases unknown DATA.
use super::{RuntimeError, protocol::Control};
use crate::credit::Amount;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Prefix {
    pub sequence: u64,
    pub amount: Amount,
}
impl Prefix {
    pub fn from_control(c: &Control) -> Result<Self, RuntimeError> {
        if c.records != c.watermark {
            return Err(RuntimeError::Protocol);
        }
        Ok(Self {
            sequence: c.watermark,
            amount: Amount {
                bytes: c.applied_bytes,
                records: c.records,
            },
        })
    }
    pub fn reached(self, sequence: u64, applied: Amount) -> Result<bool, RuntimeError> {
        if sequence > self.sequence || !self.amount.contains(applied) {
            return Err(RuntimeError::Protocol);
        }
        if sequence == self.sequence {
            if applied != self.amount {
                return Err(RuntimeError::Protocol);
            }
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Initiator {
        sent: bool,
        remote: Option<Prefix>,
        done: bool,
    },
    Responder {
        remote: Prefix,
        acknowledged: bool,
        done: bool,
    },
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Closing {
    pub nonce: u128,
    pub local: Prefix,
    pub deadline: Instant,
    phase: Phase,
}
impl Closing {
    pub fn initiate(nonce: u128, local: Prefix, deadline: Instant) -> Self {
        Self {
            nonce,
            local,
            deadline,
            phase: Phase::Initiator {
                sent: false,
                remote: None,
                done: false,
            },
        }
    }
    pub fn respond(nonce: u128, local: Prefix, remote: Prefix, deadline: Instant) -> Self {
        Self {
            nonce,
            local,
            deadline,
            phase: Phase::Responder {
                remote,
                acknowledged: false,
                done: false,
            },
        }
    }
    pub fn initiating(&self) -> bool {
        matches!(self.phase, Phase::Initiator { .. })
    }
    pub fn remote(&self) -> Option<Prefix> {
        match self.phase {
            Phase::Initiator { remote, .. } => remote,
            Phase::Responder { remote, .. } => Some(remote),
        }
    }
    /// ACK proves our committed prefix was applied by the remote Core. Its
    /// reverse prefix is retained until locally applied before publishing DONE.
    pub fn acknowledged(
        &mut self,
        nonce: u128,
        remote_prefix: Prefix,
    ) -> Result<Amount, RuntimeError> {
        if nonce != self.nonce {
            return Err(RuntimeError::Protocol);
        }
        let Phase::Initiator {
            sent: true, remote, ..
        } = &mut self.phase
        else {
            return Err(RuntimeError::Protocol);
        };
        if remote.is_some_and(|old| old != remote_prefix) {
            return Err(RuntimeError::Protocol);
        }
        *remote = Some(remote_prefix);
        Ok(self.local.amount)
    }
    /// Responder releases its own outgoing debt only after final DONE.
    pub fn done(&mut self, nonce: u128) -> Result<Amount, RuntimeError> {
        if nonce != self.nonce {
            return Err(RuntimeError::Protocol);
        }
        let Phase::Responder {
            acknowledged: true,
            done,
            ..
        } = &mut self.phase
        else {
            return Err(RuntimeError::Protocol);
        };
        *done = true;
        Ok(self.local.amount)
    }
    pub fn complete(&self) -> bool {
        matches!(
            self.phase,
            Phase::Initiator { done: true, .. } | Phase::Responder { done: true, .. }
        )
    }
    /// At most one CLOSE, ACK and DONE; no automatic terminal retransmission.
    pub fn output(
        &mut self,
        sequence: u64,
        applied: Amount,
        owner_drained: bool,
    ) -> Result<Option<u8>, RuntimeError> {
        match &mut self.phase {
            Phase::Initiator { sent, .. } if !*sent => {
                *sent = true;
                Ok(Some(12))
            }
            Phase::Initiator {
                remote: Some(remote),
                done,
                ..
            } if !*done && owner_drained => {
                if remote.reached(sequence, applied)? {
                    *done = true;
                    Ok(Some(14))
                } else {
                    Ok(None)
                }
            }
            Phase::Responder {
                remote,
                acknowledged,
                ..
            } if !*acknowledged && owner_drained => {
                if remote.reached(sequence, applied)? {
                    *acknowledged = true;
                    Ok(Some(13))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn prefix(sequence: u64, bytes: u64) -> Prefix {
        Prefix {
            sequence,
            amount: Amount {
                bytes,
                records: sequence,
            },
        }
    }
    #[test]
    fn terminal_fence_proves_both_directions_before_releasing_both_debts() {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (a_prefix, b_prefix) = (prefix(3, 5000), prefix(2, 3000));
        let mut a = Closing::initiate(7, a_prefix, deadline);
        assert_eq!(a.output(0, Amount::default(), false).unwrap(), Some(12));
        let mut b = Closing::respond(7, b_prefix, a_prefix, deadline);
        assert_eq!(b.output(2, prefix(2, 2000).amount, true).unwrap(), None);
        assert_eq!(b.output(3, a_prefix.amount, false).unwrap(), None);
        assert_eq!(b.output(3, a_prefix.amount, true).unwrap(), Some(13));
        assert!(!b.complete());
        assert_eq!(a.acknowledged(7, b_prefix).unwrap(), a_prefix.amount);
        assert_eq!(a.output(1, prefix(1, 1000).amount, true).unwrap(), None);
        assert_eq!(a.output(2, b_prefix.amount, true).unwrap(), Some(14));
        assert!(a.complete());
        assert!(!b.complete());
        assert_eq!(b.done(7).unwrap(), b_prefix.amount);
        assert!(b.complete());
        assert_eq!(a.deadline, deadline);
        assert_eq!(b.deadline, deadline);
    }
    #[test]
    fn missing_prefix_or_owner_never_completes_a_terminal_fence() {
        let mut state = Closing::respond(1, prefix(1, 10), prefix(2, 20), Instant::now());
        assert_eq!(state.output(1, prefix(1, 10).amount, true).unwrap(), None);
        assert_eq!(state.output(2, prefix(2, 20).amount, false).unwrap(), None);
        assert!(state.output(2, prefix(2, 19).amount, true).is_err());
        assert!(state.output(3, prefix(3, 30).amount, true).is_err());
        assert!(state.done(1).is_err());
        assert!(!state.complete());
    }
    #[test]
    fn forged_terminal_ack_or_conflicting_prefix_cannot_release_ownership() {
        let mut state = Closing::initiate(1, prefix(1, 10), Instant::now());
        assert!(state.acknowledged(1, prefix(2, 20)).is_err());
        state.output(0, Amount::default(), true).unwrap();
        assert!(state.acknowledged(2, prefix(2, 20)).is_err());
        state.acknowledged(1, prefix(2, 20)).unwrap();
        assert!(state.acknowledged(1, prefix(2, 21)).is_err());
        assert!(!state.complete());
    }
}
