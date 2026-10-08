//! Opt-in owner-local timings; these are elapsed durations, not CPU samples.

#[derive(Clone, Copy, Debug, Default)]
pub struct TurnDiagnostics {
    pub enabled: bool,
    pub turns: u64,
    pub turn_us: u64,
    pub progress: u64,
    pub idle_count: u64,
    pub idle_us: u64,
    pub output_us: u64,
}
impl TurnDiagnostics {
    pub(crate) fn set_enabled(&mut self, enabled: bool) {
        if self.enabled != enabled {
            *self = Self {
                enabled,
                ..Self::default()
            };
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_and_new_collection_do_not_report_old_timings() {
        let mut value = TurnDiagnostics::default();
        assert!(!value.enabled);
        value.set_enabled(true);
        value.turns = 10;
        value.idle_us = 123;
        value.set_enabled(true);
        assert_eq!(value.turns, 10);
        value.set_enabled(false);
        assert_eq!(value.turns, 0);
        assert_eq!(value.idle_us, 0);
        value.set_enabled(true);
        assert_eq!(value.turns, 0);
    }
}
