//! Bounded metadata history and payload counters; no payload inspection or disk I/O.
use serde::Serialize;
use std::{
    collections::VecDeque,
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
#[derive(Default)]
pub struct Telemetry {
    pub uploaded: AtomicU64,
    pub downloaded: AtomicU64,
    enabled: AtomicBool,
    history: Mutex<VecDeque<Request>>,
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
            self.skipped.store(0, Ordering::Relaxed);
            self.revision.fetch_add(1, Ordering::Relaxed);
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
pub const RATE_GUIDE: &str = "↓ 999999.99 КиБ/с   ↑ 999999.99 КиБ/с";
pub fn rate(bytes: u64, seconds: f64) -> String {
    // A fixed unit avoids resizing panel hosts that ignore XAyatanaLabelGuide.
    // Figure spaces reserve digit cells without distracting leading zeroes.
    let value = bytes as f64 / seconds.max(0.001) / 1024.0;
    let mut number = format!("{value:.2}");
    if number.len() > 9 {
        number = format!("{value:.2e}");
    }
    format!(
        "{}{} КиБ/с",
        "\u{2007}".repeat(9usize.saturating_sub(number.len())),
        number
    )
}
pub fn rates(down: u64, up: u64, seconds: f64) -> String {
    format!("↓ {}   ↑ {}", rate(down, seconds), rate(up, seconds))
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
            let line = super::rates(bytes, 0, 1.0);
            assert_eq!(line.chars().count(), super::RATE_GUIDE.chars().count());
        }
        assert!(super::rate(307, 1.0).ends_with("0.30 КиБ/с"));
        assert!(super::rate(u64::MAX, 0.001).contains("e19"));
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
