//! Generation-checked indices; caller-visible handles never contain pointers.
use std::sync::{Arc, Mutex};

struct Slot<T> {
    generation: u32,
    value: Option<Arc<Mutex<T>>>,
}

pub(crate) struct Handles<T> {
    slots: Vec<Slot<T>>,
}

impl<T> Default for Handles<T> {
    fn default() -> Self {
        Self { slots: Vec::new() }
    }
}

impl<T> Handles<T> {
    pub(crate) fn insert(&mut self, value: T) -> Option<u64> {
        let value = Arc::new(Mutex::new(value));
        if let Some((index, slot)) = self
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.value.is_none() && slot.generation != u32::MAX)
        {
            slot.value = Some(value);
            return Some(encode(index, slot.generation));
        }
        if self.slots.len() >= u32::MAX as usize {
            return None;
        }
        let index = self.slots.len();
        self.slots.push(Slot {
            generation: 1,
            value: Some(value),
        });
        Some(encode(index, 1))
    }

    pub(crate) fn get(&self, handle: u64) -> Option<Arc<Mutex<T>>> {
        let (index, generation) = decode(handle)?;
        let slot = self.slots.get(index)?;
        (slot.generation == generation)
            .then(|| slot.value.clone())
            .flatten()
    }

    pub(crate) fn remove(&mut self, handle: u64) -> Option<Arc<Mutex<T>>> {
        let (index, generation) = decode(handle)?;
        let slot = self.slots.get_mut(index)?;
        if slot.generation != generation {
            return None;
        }
        let value = slot.value.take()?;
        // The last generation is permanently retired instead of wrapping.
        slot.generation = slot.generation.saturating_add(1);
        Some(value)
    }
}

fn encode(index: usize, generation: u32) -> u64 {
    (u64::from(generation) << 32) | (index as u64 + 1)
}

fn decode(handle: u64) -> Option<(usize, u32)> {
    let index = handle as u32;
    let generation = (handle >> 32) as u32;
    (index != 0 && generation != 0).then(|| ((index - 1) as usize, generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_handles_cannot_access_reused_slot() {
        let mut table = Handles::default();
        let old = table.insert(11).unwrap();
        assert!(table.get(old).is_some());
        assert!(table.remove(old).is_some());
        let current = table.insert(22).unwrap();
        assert_ne!(old, current);
        assert_eq!(old as u32, current as u32);
        assert!(table.get(old).is_none());
        assert!(table.remove(old).is_none());
        assert_eq!(*table.get(current).unwrap().lock().unwrap(), 22);
    }

    #[test]
    fn exhausted_generation_is_retired() {
        let mut table = Handles {
            slots: vec![Slot {
                generation: u32::MAX - 1,
                value: Some(Arc::new(Mutex::new(()))),
            }],
        };
        let old = encode(0, u32::MAX - 1);
        assert!(table.remove(old).is_some());
        let new = table.insert(()).unwrap();
        assert_eq!(new as u32, 2);
        assert!(table.get(old).is_none());
        assert!(table.get(encode(0, u32::MAX)).is_none());
    }

    #[test]
    fn malformed_and_unknown_handles_are_rejected() {
        let mut table = Handles::<()>::default();
        for handle in [0, 1, 1 << 32, u64::MAX] {
            assert!(table.get(handle).is_none());
            assert!(table.remove(handle).is_none());
        }
    }

    #[test]
    fn outstanding_lookup_cannot_resurrect_retired_slot() {
        let mut table = Handles::default();
        let old = table.insert(Some(11)).unwrap();
        let outstanding = table.get(old).unwrap();
        let retired = table.remove(old).unwrap();
        assert_eq!(retired.lock().unwrap().take(), Some(11));
        let current = table.insert(Some(22)).unwrap();
        assert_ne!(old, current);
        assert!(outstanding.lock().unwrap().is_none());
        assert!(table.get(old).is_none());
        assert_eq!(*table.get(current).unwrap().lock().unwrap(), Some(22));
    }
}
