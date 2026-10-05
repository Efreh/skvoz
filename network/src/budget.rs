//! Explicit byte and record reservation ownership.
use crate::NetworkError;
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    pub bytes: usize,
    pub records: usize,
}
#[derive(Clone)]
pub struct Budget {
    inner: Arc<Mutex<Usage>>,
    limit: Usage,
}
pub struct Reservation {
    budget: Budget,
    usage: Usage,
}
impl Budget {
    pub fn new(bytes: usize, records: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Usage::default())),
            limit: Usage { bytes, records },
        }
    }
    pub fn usage(&self) -> Usage {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn available(&self) -> Usage {
        let usage = self.usage();
        Usage {
            bytes: self.limit.bytes - usage.bytes,
            records: self.limit.records - usage.records,
        }
    }
    pub fn reserve(&self, bytes: usize, records: usize) -> Result<Reservation, NetworkError> {
        let mut usage = self.inner.lock().map_err(|_| NetworkError::Overloaded)?;
        let next = Usage {
            bytes: usage
                .bytes
                .checked_add(bytes)
                .ok_or(NetworkError::Overloaded)?,
            records: usage
                .records
                .checked_add(records)
                .ok_or(NetworkError::Overloaded)?,
        };
        if next.bytes > self.limit.bytes || next.records > self.limit.records {
            return Err(NetworkError::Overloaded);
        }
        *usage = next;
        Ok(Reservation {
            budget: self.clone(),
            usage: Usage { bytes, records },
        })
    }
}
impl Reservation {
    pub fn usage(&self) -> Usage {
        self.usage
    }
    /// Atomically resize an owned reservation, including capacity retained after drain.
    /// Failure leaves both this reservation and the global ledger unchanged.
    pub fn resize(&mut self, bytes: usize, records: usize) -> Result<(), NetworkError> {
        let mut usage = self
            .budget
            .inner
            .lock()
            .map_err(|_| NetworkError::Overloaded)?;
        let next = Usage {
            bytes: (usage.bytes - self.usage.bytes)
                .checked_add(bytes)
                .ok_or(NetworkError::Overloaded)?,
            records: (usage.records - self.usage.records)
                .checked_add(records)
                .ok_or(NetworkError::Overloaded)?,
        };
        if next.bytes > self.budget.limit.bytes || next.records > self.budget.limit.records {
            return Err(NetworkError::Overloaded);
        }
        *usage = next;
        self.usage = Usage { bytes, records };
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.budget.inner.lock().unwrap_or_else(|e| e.into_inner());
        usage.bytes -= self.usage.bytes;
        usage.records -= self.usage.records;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resized_ownership_preserves_failed_growth_and_releases_retained_capacity() {
        let b = Budget::new(100, 8);
        let mut reservation = b.reserve(4, 1).unwrap();
        reservation.resize(80, 2).unwrap();
        assert!(reservation.resize(101, 2).is_err());
        assert_eq!(
            b.usage(),
            Usage {
                bytes: 80,
                records: 2
            }
        );
        reservation.resize(4, 1).unwrap();
        assert_eq!(
            b.usage(),
            Usage {
                bytes: 4,
                records: 1
            }
        );
        drop(reservation);
        assert_eq!(b.usage(), Usage::default());
    }
    #[test]
    fn simultaneous_copies_count_and_failed_admission_releases_nothing() {
        let b = Budget::new(8, 2);
        let x = b.reserve(4, 1).unwrap();
        let y = b.reserve(4, 1).unwrap();
        assert!(b.reserve(0, 1).is_err());
        assert!(b.reserve(usize::MAX, 0).is_err());
        drop(x);
        assert_eq!(
            b.usage(),
            Usage {
                bytes: 4,
                records: 1
            }
        );
        drop(y);
        assert_eq!(b.usage(), Usage::default());
    }
}
