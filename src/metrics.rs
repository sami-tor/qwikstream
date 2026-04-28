use std::time::{Duration, Instant};

use clap::ValueEnum;
use serde_json::json;

use crate::size::format_bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ProgressMode {
    Plain,
    Json,
    Quiet,
}

#[derive(Debug, Clone)]
pub struct StreamStats {
    started: Instant,
    pub input_size_bytes: u64,
    pub bytes_sent: u64,
    pub records_sent: u64,
    pub batches_sent: u64,
    pub retries: u64,
    pub last_latency: Option<Duration>,
}

impl StreamStats {
    pub fn new(input_size_bytes: u64) -> Self {
        Self {
            started: Instant::now(),
            input_size_bytes,
            bytes_sent: 0,
            records_sent: 0,
            batches_sent: 0,
            retries: 0,
            last_latency: None,
        }
    }

    pub fn record_batch(&mut self, bytes: u64, records: u64, attempts: u32, latency: Duration) {
        self.bytes_sent += bytes;
        self.records_sent += records;
        self.batches_sent += 1;
        self.retries += attempts.saturating_sub(1) as u64;
        self.last_latency = Some(latency);
    }

    pub fn bytes_per_sec(&self) -> f64 {
        let elapsed = self.started.elapsed().as_secs_f64().max(0.001);
        self.bytes_sent as f64 / elapsed
    }

    pub fn eta(&self) -> Option<Duration> {
        if self.input_size_bytes == 0 || self.bytes_sent >= self.input_size_bytes {
            return Some(Duration::from_secs(0));
        }
        let rate = self.bytes_per_sec();
        if rate <= 0.0 {
            None
        } else {
            Some(Duration::from_secs_f64(
                (self.input_size_bytes - self.bytes_sent) as f64 / rate,
            ))
        }
    }

    pub fn line(&self) -> String {
        let latency = self
            .last_latency
            .map(|d| format!("{} ms", d.as_millis()))
            .unwrap_or_else(|| "n/a".to_string());
        let eta = self
            .eta()
            .map(format_duration)
            .unwrap_or_else(|| "n/a".to_string());
        format!(
            "sent={} records={} batches={} rate={}/s retries={} latency={} eta={}",
            format_bytes(self.bytes_sent),
            self.records_sent,
            self.batches_sent,
            format_bytes(self.bytes_per_sec() as u64),
            self.retries,
            latency,
            eta
        )
    }
    pub fn json_line(&self) -> String {
        json!({
            "bytes_sent": self.bytes_sent,
            "records_sent": self.records_sent,
            "batches_sent": self.batches_sent,
            "retries": self.retries,
            "bytes_per_sec": self.bytes_per_sec(),
            "eta_seconds": self.eta().map(|duration| duration.as_secs()),
            "latency_ms": self.last_latency.map(|duration| duration.as_millis()),
        })
        .to_string()
    }
}

pub fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    if hours > 0 {
        format!("{hours}h {minutes}m {secs}s")
    } else if minutes > 0 {
        format!("{minutes}m {secs}s")
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_batch_progress() {
        let mut stats = StreamStats::new(100);
        stats.record_batch(20, 2, 3, Duration::from_millis(50));

        assert_eq!(stats.bytes_sent, 20);
        assert_eq!(stats.records_sent, 2);
        assert_eq!(stats.batches_sent, 1);
        assert_eq!(stats.retries, 2);
        assert!(stats.line().contains("records=2"));
    }

    #[test]
    fn formats_duration() {
        assert_eq!(format_duration(Duration::from_secs(61)), "1m 1s");
        assert_eq!(format_duration(Duration::from_secs(3661)), "1h 1m 1s");
    }
}
