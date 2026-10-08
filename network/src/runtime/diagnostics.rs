//! One fixed-size embedding snapshot; no packet, profile or wire data is retained.
use super::RuntimeFailure;
use serde::Serialize;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

pub(crate) const RESERVATION: usize = 1024;
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RuntimeDiagnostics {
    pub enabled: bool,
    pub collection: u64,
    pub sample_age_ms: Option<u64>,
    pub samples: u64,
    pub elapsed_ms: u64,
    pub turns: u64,
    pub native_us: u64,
    pub drive_us: u64,
    pub read_full: u64,
    pub write_full: u64,
    pub read_block: u64,
    pub write_block: u64,
    pub read_paused: u64,
    pub core_turns: u64,
    pub core_turn_us: u64,
    pub core_progress: u64,
    pub core_idle_count: u64,
    pub core_idle_us: u64,
    pub core_output_us: u64,
}
pub(crate) struct Mailbox {
    control: AtomicU64,
    latest: Mutex<Option<(u64, Instant, RuntimeDiagnostics)>>,
}
impl Mailbox {
    pub(crate) fn new() -> Self {
        Self {
            control: AtomicU64::new(0),
            latest: Mutex::new(None),
        }
    }
    pub(crate) fn control(&self) -> u64 {
        self.control.load(Ordering::Acquire)
    }
    pub(crate) fn read(&self, enabled: bool) -> Result<RuntimeDiagnostics, RuntimeFailure> {
        // Readers serialize only this control path, never packet processing.
        let mut latest = self.latest.lock().map_err(|_| RuntimeFailure::Internal)?;
        let mut old = self.control();
        let control = loop {
            if (old & 1 != 0) == enabled {
                break old;
            }
            let epoch = (old >> 1)
                .checked_add(1)
                .filter(|value| *value <= u64::MAX >> 1)
                .ok_or(RuntimeFailure::Internal)?;
            let value = epoch << 1 | u64::from(enabled);
            match self
                .control
                .compare_exchange(old, value, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break value,
                Err(current) => old = current,
            }
        };
        if latest.as_ref().is_some_and(|sample| sample.0 != control) {
            *latest = None;
        }
        if enabled
            && let Some((epoch, time, value)) = latest.as_ref().filter(|sample| sample.0 == control)
        {
            let mut snapshot = *value;
            snapshot.collection = epoch >> 1;
            snapshot.sample_age_ms =
                Some(u64::try_from(time.elapsed().as_millis()).unwrap_or(u64::MAX));
            return Ok(snapshot);
        }
        Ok(RuntimeDiagnostics {
            enabled,
            collection: control >> 1,
            ..RuntimeDiagnostics::default()
        })
    }
    pub(crate) fn publish(
        &self,
        control: u64,
        value: RuntimeDiagnostics,
    ) -> Result<(), RuntimeFailure> {
        let mut latest = self.latest.lock().map_err(|_| RuntimeFailure::Internal)?;
        if control & 1 != 0 && self.control() == control {
            *latest = Some((control, Instant::now(), value));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_publication_cannot_reappear_after_disable_or_new_collection() {
        let mailbox = Mailbox::new();
        assert!(!mailbox.read(false).unwrap().enabled);
        assert_eq!(mailbox.read(true).unwrap().sample_age_ms, None);
        let first = mailbox.control();
        mailbox
            .publish(
                first,
                RuntimeDiagnostics {
                    enabled: true,
                    turns: 10,
                    ..RuntimeDiagnostics::default()
                },
            )
            .unwrap();
        assert_eq!(mailbox.read(true).unwrap().turns, 10);
        assert!(mailbox.read(true).unwrap().sample_age_ms.is_some());
        let disabled = mailbox.read(false).unwrap();
        assert_eq!(disabled.sample_age_ms, None);
        assert_eq!(disabled.turns, 0);
        mailbox
            .publish(
                first,
                RuntimeDiagnostics {
                    enabled: true,
                    turns: 99,
                    ..RuntimeDiagnostics::default()
                },
            )
            .unwrap();
        let fresh = mailbox.read(true).unwrap();
        assert_ne!(fresh.collection, first >> 1);
        assert_eq!(fresh.turns, 0);
        assert_eq!(fresh.sample_age_ms, None);
        mailbox
            .publish(
                first,
                RuntimeDiagnostics {
                    enabled: true,
                    turns: 99,
                    ..RuntimeDiagnostics::default()
                },
            )
            .unwrap();
        assert_eq!(mailbox.read(true).unwrap().sample_age_ms, None);
    }
    #[test]
    fn concurrent_readers_preserve_epoch_and_final_disable() {
        let mailbox = std::sync::Arc::new(Mailbox::new());
        std::thread::scope(|scope| {
            for index in 0..4 {
                let mailbox = mailbox.clone();
                scope.spawn(move || {
                    for turn in 0..100 {
                        mailbox.read((turn + index) % 2 == 0).unwrap();
                    }
                });
            }
        });
        let old = mailbox.control();
        let disabled = mailbox.read(false).unwrap();
        mailbox
            .publish(
                old | 1,
                RuntimeDiagnostics {
                    enabled: true,
                    turns: 999,
                    ..RuntimeDiagnostics::default()
                },
            )
            .unwrap();
        let current = mailbox.read(false).unwrap();
        assert_eq!(disabled.collection, current.collection);
        assert!(!current.enabled);
        assert_eq!(current.turns, 0);
        assert_eq!(current.sample_age_ms, None);
    }
    #[test]
    fn fixed_storage_and_numeric_json_are_bounded() {
        assert!(
            std::mem::size_of::<Mailbox>()
                + std::mem::size_of::<super::super::ActorDiagnostics>()
                + std::mem::size_of::<skvoz_core::runtime::TurnDiagnostics>()
                + 64
                <= RESERVATION
        );
        let json = serde_json::to_vec(&RuntimeDiagnostics::default()).unwrap();
        assert!(json.len() <= 4096);
        let mut largest = serde_json::to_value(RuntimeDiagnostics::default()).unwrap();
        for value in largest.as_object_mut().unwrap().values_mut() {
            if value.is_number() || value.is_null() {
                *value = serde_json::Value::from(u64::MAX);
            }
        }
        assert!(serde_json::to_vec(&largest).unwrap().len() <= 4096);
    }
}
