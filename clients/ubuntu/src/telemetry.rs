//! Bounded metadata history and payload counters; no payload inspection or disk I/O.
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
pub const HISTORY_LIMIT: usize = 500;
#[derive(Clone, Debug, Serialize)]
pub struct Request {
    pub id: u64,
    pub time: u64,
    pub protocol: &'static str,
    pub host: String,
    pub port: u16,
    pub result: &'static str,
    pub uploaded: u64,
    pub downloaded: u64,
}
const RUNTIME_FLOW_LIMIT: usize = 512;
#[derive(Default)]
struct RuntimeJournal {
    live: BTreeMap<u64, &'static str>,
    completed: BTreeSet<u64>,
    completion_order: VecDeque<u64>,
    max_seen: u64,
    clear_barrier: u64,
}
#[derive(Default)]
pub struct Telemetry {
    pub uploaded: AtomicU64,
    pub downloaded: AtomicU64,
    enabled: AtomicBool,
    history: Mutex<VecDeque<Request>>,
    runtime_journal: Mutex<RuntimeJournal>,
    revision: AtomicU64,
    epoch: AtomicU64,
    next_id: AtomicU64,
    pub skipped: AtomicU64,
}
impl Telemetry {
    pub fn new(enabled: bool) -> Arc<Self> {
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            ..Self::default()
        })
    }
    pub fn enable(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if !enabled {
            self.clear();
        }
    }
    pub fn clear(&self) {
        if let Ok(mut history) = self.history.lock() {
            self.epoch.fetch_add(1, Ordering::Relaxed);
            history.clear();
            if let Ok(mut journal) = self.runtime_journal.lock() {
                journal.clear_barrier = journal.max_seen;
                journal.live.clear();
                journal.completed.clear();
                journal.completion_order.clear();
            }
            self.skipped.store(0, Ordering::Relaxed);
            self.revision.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn runtime_generation(&self) {
        // A new child restarts request IDs; keep history but retire old tracking.
        self.epoch.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut journal) = self.runtime_journal.lock() {
            *journal = RuntimeJournal::default();
        }
    }
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Relaxed)
    }
    pub fn history(&self) -> Vec<Request> {
        self.history
            .lock()
            .map(|history| history.iter().cloned().collect())
            .unwrap_or_default()
    }
    fn record(&self, request: Request, epoch: u64) {
        if !self.enabled.load(Ordering::Relaxed) {
            return;
        }
        // Diagnostics never wait for a UI snapshot on the proxy path.
        let Ok(mut history) = self.history.try_lock() else {
            self.skipped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !self.enabled.load(Ordering::Relaxed) || epoch != self.epoch.load(Ordering::Relaxed) {
            return;
        }
        if history.len() == HISTORY_LIMIT {
            history.pop_front();
        }
        let mut request = request;
        request.time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        history.push_back(request);
        self.revision.fetch_add(1, Ordering::Relaxed);
    }
    pub fn runtime_request(&self, data: &serde_json::Value) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        let protocol = match data["protocol"].as_str() {
            Some("HTTP") => "HTTP",
            Some("CONNECT") => "CONNECT",
            Some("SOCKS5") => "SOCKS5",
            Some("TCP") => "TCP",
            _ => return,
        };
        let result = match data["result"].as_str() {
            Some("opening") => "opening",
            Some("active") => "active",
            Some("finished") => "finished",
            Some("cancelled") => "cancelled",
            Some("forbidden") => "forbidden",
            Some("overloaded") => "overloaded",
            Some("network_unavailable") => "network_unavailable",
            Some("timeout") => "timeout",
            Some("local_setup_failed") => "local_setup_failed",
            Some("invalid_request") => "invalid_request",
            _ => return,
        };
        let (Some(id), Some(host), Some(port), Some(uploaded), Some(downloaded)) = (
            data["id"].as_u64(),
            data["host"].as_str(),
            data["port"].as_u64(),
            data["uploaded"].as_u64(),
            data["downloaded"].as_u64(),
        ) else {
            return;
        };
        if id == 0 || host.len() > 253 || port == 0 || port > 65535 {
            return;
        }
        {
            let Ok(mut journal) = self.runtime_journal.lock() else {
                return;
            };
            journal.max_seen = journal.max_seen.max(id);
            if !self.enabled.load(Ordering::Relaxed) {
                journal.clear_barrier = journal.max_seen;
                return;
            }
            if id <= journal.clear_barrier || journal.completed.contains(&id) {
                return;
            }
            if result == "opening" || result == "active" {
                if journal
                    .live
                    .get(&id)
                    .is_some_and(|previous| *previous == result || *previous == "active")
                {
                    return;
                }
                if journal.live.len() == RUNTIME_FLOW_LIMIT && !journal.live.contains_key(&id) {
                    self.skipped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                journal.live.insert(id, result);
            } else {
                // Reliable API output may coalesce opening/active before delivery.
                // Terminal IDs are deduplicated within a finite recent window;
                // the runtime itself guarantees one terminal event per request.
                journal.live.remove(&id);
                if journal.completion_order.len() == RUNTIME_FLOW_LIMIT {
                    let old = journal.completion_order.pop_front().unwrap();
                    journal.completed.remove(&old);
                }
                journal.completed.insert(id);
                journal.completion_order.push_back(id);
            }
        }
        self.record(
            Request {
                id,
                time: 0,
                protocol,
                host: host.into(),
                port: port as u16,
                result,
                uploaded,
                downloaded,
            },
            epoch,
        );
    }
    pub fn flow(self: &Arc<Self>) -> Flow {
        Flow {
            telemetry: self.clone(),
            request: None,
            epoch: self.epoch.load(Ordering::Relaxed),
            uploaded: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
        }
    }
}
pub struct Flow {
    telemetry: Arc<Telemetry>,
    request: Option<Request>,
    epoch: u64,
    uploaded: AtomicU64,
    downloaded: AtomicU64,
}
impl Flow {
    pub fn destination(&mut self, protocol: &'static str, host: &str, port: u16) {
        if self.telemetry.enabled.load(Ordering::Relaxed) {
            self.request = Some(Request {
                id: self.telemetry.next_id.fetch_add(1, Ordering::Relaxed) + 1,
                time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                protocol,
                host: host.to_owned(),
                port,
                result: "opening",
                uploaded: 0,
                downloaded: 0,
            });
        }
    }
    pub fn opened(&mut self) {
        if let Some(request) = &mut self.request {
            request.result = "active";
            self.telemetry.record(request.clone(), self.epoch);
            request.result = "cancelled";
        }
    }
    pub fn finish(&mut self, result: crate::Result<()>) {
        if let Some(request) = &mut self.request {
            request.result = result.err().map_or("finished", |error| error.0);
        }
    }
    pub fn upload(&self, bytes: u64) {
        self.uploaded.fetch_add(bytes, Ordering::Relaxed);
        self.telemetry.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn download(&self, bytes: u64) {
        self.downloaded.fetch_add(bytes, Ordering::Relaxed);
        self.telemetry
            .downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }
}
impl Drop for Flow {
    fn drop(&mut self) {
        if let Some(mut request) = self.request.take() {
            request.uploaded = self.uploaded.load(Ordering::Relaxed);
            request.downloaded = self.downloaded.load(Ordering::Relaxed);
            self.telemetry.record(request, self.epoch);
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeedUnit {
    Bytes,
    Bits,
    Kilobytes,
    #[default]
    Kibibytes,
    Megabytes,
    Mebibytes,
    Kilobits,
    Megabits,
}
impl SpeedUnit {
    pub const ALL: [Self; 8] = [
        Self::Bytes,
        Self::Bits,
        Self::Kilobytes,
        Self::Kibibytes,
        Self::Megabytes,
        Self::Mebibytes,
        Self::Kilobits,
        Self::Megabits,
    ];
    pub fn label(self) -> &'static str {
        match self {
            Self::Bytes => "Б/с",
            Self::Bits => "бит/с",
            Self::Kilobytes => "КБ/с",
            Self::Kibibytes => "КиБ/с",
            Self::Megabytes => "МБ/с",
            Self::Mebibytes => "МиБ/с",
            Self::Kilobits => "Кбит/с",
            Self::Megabits => "Мбит/с",
        }
    }
    fn scale(self) -> f64 {
        match self {
            Self::Bytes => 1.0,
            Self::Bits => 8.0,
            Self::Kilobytes => 1.0 / 1000.0,
            Self::Kibibytes => 1.0 / 1024.0,
            Self::Megabytes => 1.0 / 1_000_000.0,
            Self::Mebibytes => 1.0 / 1_048_576.0,
            Self::Kilobits => 8.0 / 1000.0,
            Self::Megabits => 8.0 / 1_000_000.0,
        }
    }
    pub fn guide(self) -> String {
        format!("↓999.99 {} ↑999.99 {}", self.label(), self.label())
    }
}
pub fn rate(bytes: u64, seconds: f64, unit: SpeedUnit) -> String {
    let value = bytes as f64 / seconds.max(0.001) * unit.scale();
    let mut number = format!("{value:.2}");
    for precision in [1, 0] {
        if number.len() <= 6 {
            break;
        }
        number = format!("{value:.precision$}");
    }
    if number.len() > 5 && !number.contains('.') {
        number = format!("{value:.1e}");
    }
    // Keep the decimal glyph's advance even for integer precision, and reserve
    // digit cells after the unit so arrows stay close to the visible number.
    let punctuation = if number.contains('.') { "" } else { "\u{2008}" };
    let digits = number.len() - usize::from(number.contains('.'));
    format!(
        "{number} {}{}{punctuation}",
        unit.label(),
        "\u{2007}".repeat(5usize.saturating_sub(digits))
    )
}
pub fn rates(down: u64, up: u64, seconds: f64, unit: SpeedUnit) -> String {
    format!(
        "↓{} ↑{}",
        rate(down, seconds, unit),
        rate(up, seconds, unit)
    )
}
#[cfg(test)]
mod tests {
    #[test]
    fn rates_keep_digit_cells_and_never_truncate_large_values() {
        for bytes in [
            0,
            30,
            307,
            1023,
            1024,
            1_000_000,
            1_023_999_980,
            1_023_999_995,
            1_024_000_000,
            u64::MAX,
        ] {
            for unit in super::SpeedUnit::ALL {
                let line = super::rates(bytes, 0, 1.0, unit);
                assert_eq!(line.chars().count(), unit.guide().chars().count());
            }
        }
        assert!(super::rate(307, 1.0, super::SpeedUnit::Kibibytes).starts_with("0.30 КиБ/с"));
        assert!(super::rate(u64::MAX, 0.001, super::SpeedUnit::Kibibytes).contains("e19"));
    }
    #[test]
    fn speed_units_convert_bits_bytes_and_decimal_binary_prefixes() {
        use super::{SpeedUnit::*, rate};
        for (unit, bytes, seconds, expected) in [
            (Bytes, 12, 2.0, "6.00 Б/с"),
            (Bits, 12, 2.0, "48.00 бит/с"),
            (Kilobytes, 1024, 1.0, "1.02 КБ/с"),
            (Kibibytes, 1024, 1.0, "1.00 КиБ/с"),
            (Megabytes, 1_000_000, 2.0, "0.50 МБ/с"),
            (Mebibytes, 1_048_576, 2.0, "0.50 МиБ/с"),
            (Kilobits, 125, 1.0, "1.00 Кбит/с"),
            (Megabits, 125_000, 1.0, "1.00 Мбит/с"),
        ] {
            assert!(rate(bytes, seconds, unit).starts_with(expected));
        }
    }
    #[test]
    fn events_use_recording_time_instead_of_connection_start() {
        let telemetry = super::Telemetry::new(true);
        let mut flow = telemetry.flow();
        flow.destination("SOCKS5", "example.org", 443);
        flow.request.as_mut().unwrap().time = 1;
        flow.opened();
        assert!(telemetry.history().last().unwrap().time > 1);
        flow.request.as_mut().unwrap().time = 2;
        flow.finish(Err(crate::Error("io_connection_reset")));
        drop(flow);
        let record = telemetry.history().pop().unwrap();
        assert!(record.time > 2);
        assert_eq!(record.result, "io_connection_reset");
    }
    use super::*;
    #[test]
    fn bounded_history_counts_bytes_even_when_disabled_or_contended() {
        let telemetry = Telemetry::new(true);
        for _ in 0..HISTORY_LIMIT {
            let mut flow = telemetry.flow();
            flow.destination("CONNECT", "example.org", 443);
            flow.opened();
            flow.upload(31);
            flow.download(17);
            flow.finish(Ok(()));
        }
        assert_eq!(telemetry.history().len(), HISTORY_LIMIT);
        assert_eq!(
            telemetry.uploaded.load(Ordering::Relaxed),
            31 * HISTORY_LIMIT as u64
        );
        assert_eq!(telemetry.history().last().unwrap().downloaded, 17);
        let lock = telemetry.history.lock().unwrap();
        let mut flow = telemetry.flow();
        flow.destination("HTTP", "example.org", 80);
        flow.opened();
        flow.upload(1);
        drop(flow);
        drop(lock);
        assert_eq!(telemetry.skipped.load(Ordering::Relaxed), 2);
        let mut ongoing = telemetry.flow();
        ongoing.destination("HTTP", "old.example", 80);
        ongoing.opened();
        telemetry.enable(false);
        telemetry.enable(true);
        drop(ongoing);
        assert!(telemetry.history().is_empty());
        telemetry.enable(false);
        let mut flow = telemetry.flow();
        flow.destination("HTTP", "example.org", 80);
        flow.download(9);
        drop(flow);
        assert!(telemetry.history().is_empty());
        assert_eq!(
            telemetry.downloaded.load(Ordering::Relaxed),
            17 * HISTORY_LIMIT as u64 + 9
        );
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn clearing_or_disabling_does_not_restore_old_runtime_flows() {
        let telemetry = Telemetry::new(true);
        let mut event = json!({"id":1,"protocol":"CONNECT","host":"example.org","port":443,"result":"opening","uploaded":0,"downloaded":0});
        telemetry.runtime_request(&event);
        assert_eq!(telemetry.history().len(), 1);
        telemetry.clear();
        event["result"] = json!("active");
        telemetry.runtime_request(&event);
        assert!(telemetry.history().is_empty());
        event["id"] = json!(2);
        event["result"] = json!("opening");
        telemetry.runtime_request(&event);
        telemetry.enable(false);
        telemetry.enable(true);
        event["result"] = json!("finished");
        telemetry.runtime_request(&event);
        assert!(telemetry.history().is_empty());
        event["id"] = json!(3);
        event["result"] = json!("opening");
        telemetry.runtime_request(&event);
        event["result"] = json!("finished");
        event["downloaded"] = json!(12);
        telemetry.runtime_request(&event);
        assert_eq!(telemetry.history().last().unwrap().downloaded, 12);
    }
    fn event(id: u64, result: &str) -> serde_json::Value {
        json!({"id":id,"protocol":"HTTP","host":"example.org","port":80,"result":result,"uploaded":31,"downloaded":17})
    }
    #[test]
    fn coalesced_first_active_or_terminal_and_out_of_order_completion_are_recorded_once() {
        let telemetry = Telemetry::new(true);
        telemetry.runtime_request(&event(1, "active"));
        telemetry.runtime_request(&event(2, "finished"));
        telemetry.runtime_request(&event(1, "finished"));
        telemetry.runtime_request(&event(1, "finished"));
        telemetry.runtime_request(&event(2, "active"));
        let history = telemetry.history();
        assert_eq!(history.len(), 3);
        assert_eq!(history[1].id, 2);
        assert_eq!(history[2].id, 1);
        assert_eq!(history[2].downloaded, 17);
        telemetry.runtime_request(&event(3, "overloaded"));
        assert_eq!(telemetry.history().last().unwrap().result, "overloaded");
    }
    #[test]
    fn all_512_live_flows_and_terminal_only_churn_use_finite_tracking() {
        let telemetry = Telemetry::new(true);
        for id in 1..=512 {
            telemetry.runtime_request(&event(id, "active"));
        }
        assert_eq!(telemetry.runtime_journal.lock().unwrap().live.len(), 512);
        for id in (1..=512).rev() {
            telemetry.runtime_request(&event(id, "finished"));
        }
        for id in 513..=10000 {
            telemetry.runtime_request(&event(id, "overloaded"));
        }
        let journal = telemetry.runtime_journal.lock().unwrap();
        assert!(journal.live.is_empty());
        assert_eq!(journal.completed.len(), 512);
        assert_eq!(journal.completion_order.len(), 512);
        assert_eq!(telemetry.history().len(), HISTORY_LIMIT);
        assert_eq!(telemetry.skipped.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn disabled_observed_flows_stay_hidden_but_new_child_ids_restart() {
        let telemetry = Telemetry::new(true);
        telemetry.runtime_request(&event(9, "active"));
        telemetry.clear();
        telemetry.runtime_request(&event(7, "finished"));
        assert!(telemetry.history().is_empty());
        telemetry.enable(false);
        telemetry.runtime_request(&event(10, "active"));
        telemetry.enable(true);
        telemetry.runtime_request(&event(10, "finished"));
        assert!(telemetry.history().is_empty());
        telemetry.runtime_request(&event(11, "finished"));
        telemetry.runtime_generation();
        telemetry.runtime_request(&event(1, "finished"));
        assert_eq!(telemetry.history().len(), 2);
        assert_eq!(telemetry.history().last().unwrap().id, 1);
    }
}
