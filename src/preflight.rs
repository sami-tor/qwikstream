use std::{
    fmt,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use sysinfo::System;
use tokio::{fs::File, io::AsyncReadExt};

use clap::ValueEnum;

use crate::{quickwit::QuickwitApiClient, size::format_bytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BenchmarkPreset {
    Hdd,
    Ssd,
    Nvme,
}

#[derive(Debug, Clone)]
pub struct PreflightConfig {
    pub input: PathBuf,
    pub quickwit: String,
    pub index: Option<String>,
    pub sample_size_bytes: u64,
    pub quickwit_benchmark: bool,
    pub benchmark_preset: Option<BenchmarkPreset>,
}

#[derive(Debug, Clone)]
pub struct PreflightReport {
    pub input_size_bytes: u64,
    pub available_memory_bytes: u64,
    pub cpu_cores: usize,
    pub disk_read_bytes_per_sec: f64,
    pub line_split_bytes_per_sec: f64,
    pub quickwit_latency_ms: Option<u128>,
    pub quickwit_index_ok: Option<bool>,
    pub quickwit_benchmark_latency_ms: Option<u128>,
    pub bottleneck: String,
    pub recommended_memory_bytes: u64,
    pub recommended_concurrency: usize,
    pub estimated_bytes_per_sec_low: f64,
    pub estimated_bytes_per_sec_high: f64,
    pub sampled_lines: u64,
    pub average_line_bytes: u64,
    pub p95_line_bytes: u64,
    pub max_line_bytes: u64,
}

impl fmt::Display for PreflightReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "System profile:")?;
        writeln!(
            f,
            "RAM available: {}",
            format_bytes(self.available_memory_bytes)
        )?;
        writeln!(f, "CPU cores: {}", self.cpu_cores)?;
        writeln!(f, "Input size: {}", format_bytes(self.input_size_bytes))?;
        writeln!(
            f,
            "Disk read: {}/s",
            format_bytes(self.disk_read_bytes_per_sec as u64)
        )?;
        writeln!(
            f,
            "Line split: {}/s",
            format_bytes(self.line_split_bytes_per_sec as u64)
        )?;
        writeln!(f, "Sampled lines: {}", self.sampled_lines)?;
        writeln!(
            f,
            "Average line size: {}",
            format_bytes(self.average_line_bytes)
        )?;
        writeln!(f, "P95 line size: {}", format_bytes(self.p95_line_bytes))?;
        writeln!(
            f,
            "Max sampled line size: {}",
            format_bytes(self.max_line_bytes)
        )?;
        match self.quickwit_latency_ms {
            Some(ms) => writeln!(f, "Quickwit reachability latency: {ms} ms")?,
            None => writeln!(f, "Quickwit reachability latency: unavailable")?,
        }
        match self.quickwit_index_ok {
            Some(true) => writeln!(f, "Quickwit index: reachable")?,
            Some(false) => writeln!(f, "Quickwit index: unavailable")?,
            None => writeln!(f, "Quickwit index: not checked")?,
        }
        match self.quickwit_benchmark_latency_ms {
            Some(ms) => writeln!(f, "Quickwit ingest benchmark latency: {ms} ms")?,
            None => writeln!(f, "Quickwit ingest benchmark latency: not run")?,
        }
        writeln!(f)?;
        writeln!(f, "Bottleneck: {}", self.bottleneck)?;
        writeln!(
            f,
            "Recommended memory limit: {}",
            format_bytes(self.recommended_memory_bytes)
        )?;
        writeln!(
            f,
            "Recommended concurrency: {}",
            self.recommended_concurrency
        )?;
        writeln!(
            f,
            "Estimated stream speed: {}/s - {}/s",
            format_bytes(self.estimated_bytes_per_sec_low as u64),
            format_bytes(self.estimated_bytes_per_sec_high as u64)
        )?;
        writeln!(
            f,
            "Estimated input time: {} - {}",
            format_duration(
                self.input_size_bytes as f64 / self.estimated_bytes_per_sec_high.max(1.0)
            ),
            format_duration(
                self.input_size_bytes as f64 / self.estimated_bytes_per_sec_low.max(1.0)
            )
        )
    }
}

pub async fn run_preflight(config: PreflightConfig) -> Result<PreflightReport> {
    let metadata = tokio::fs::metadata(&config.input)
        .await
        .with_context(|| format!("failed to inspect input {}", config.input.display()))?;
    if !metadata.is_file() {
        return Err(anyhow!("input must be a file: {}", config.input.display()));
    }

    let mut system = System::new_all();
    system.refresh_memory();

    let available_memory_bytes = system.available_memory();
    let cpu_cores = system.cpus().len().max(1);
    let benchmark = benchmark_input(&config.input, config.sample_size_bytes).await?;
    let disk_read_bytes_per_sec = benchmark.disk_read_bytes_per_sec;
    let line_split_bytes_per_sec = benchmark.line_split_bytes_per_sec;
    let quickwit = QuickwitApiClient::new(config.quickwit.clone())?;
    let quickwit_latency_ms = quickwit.check_version().await.ok();
    let quickwit_index_ok = match config.index.as_deref() {
        Some(index) => Some(quickwit.check_index(index).await.is_ok()),
        None => None,
    };
    let quickwit_benchmark_latency_ms = match (config.quickwit_benchmark, config.index.as_deref()) {
        (true, Some(index)) => quickwit.tiny_ingest_benchmark(index).await.ok(),
        _ => None,
    };

    let bottleneck = if line_split_bytes_per_sec < disk_read_bytes_per_sec {
        "line splitting".to_string()
    } else {
        "disk read".to_string()
    };
    let raw_limit = disk_read_bytes_per_sec.min(line_split_bytes_per_sec);
    let estimated_bytes_per_sec_low = raw_limit * 0.70;
    let estimated_bytes_per_sec_high = raw_limit * 0.90;
    let recommended_memory_bytes =
        available_memory_bytes.clamp(64 * 1024 * 1024, 512 * 1024 * 1024);
    let recommended_concurrency = match config.benchmark_preset {
        Some(BenchmarkPreset::Hdd) => cpu_cores.clamp(1, 2),
        Some(BenchmarkPreset::Ssd) => cpu_cores.clamp(2, 4),
        Some(BenchmarkPreset::Nvme) => cpu_cores.clamp(3, 8),
        None => cpu_cores.clamp(1, 8),
    };

    Ok(PreflightReport {
        input_size_bytes: metadata.len(),
        available_memory_bytes,
        cpu_cores,
        disk_read_bytes_per_sec,
        line_split_bytes_per_sec,
        quickwit_latency_ms,
        quickwit_index_ok,
        quickwit_benchmark_latency_ms,
        bottleneck,
        recommended_memory_bytes,
        recommended_concurrency,
        estimated_bytes_per_sec_low,
        estimated_bytes_per_sec_high,
        sampled_lines: benchmark.sampled_lines,
        average_line_bytes: benchmark.average_line_bytes,
        p95_line_bytes: benchmark.p95_line_bytes,
        max_line_bytes: benchmark.max_line_bytes,
    })
}

#[derive(Debug, Clone)]
struct InputBenchmark {
    disk_read_bytes_per_sec: f64,
    line_split_bytes_per_sec: f64,
    sampled_lines: u64,
    average_line_bytes: u64,
    p95_line_bytes: u64,
    max_line_bytes: u64,
}

async fn benchmark_input(path: &PathBuf, sample_size_bytes: u64) -> Result<InputBenchmark> {
    let mut file = File::open(path).await?;
    let target = sample_size_bytes.max(1);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut total = 0_u64;
    let mut lines = 0_u64;
    let mut line_sizes = Vec::new();
    let mut current_line_bytes = 0_u64;
    let started = Instant::now();

    while total < target {
        let remaining = (target - total) as usize;
        let read_len = buffer.len().min(remaining);
        let n = file.read(&mut buffer[..read_len]).await?;
        if n == 0 {
            break;
        }
        for byte in &buffer[..n] {
            if *byte == b'\n' {
                line_sizes.push(current_line_bytes);
                current_line_bytes = 0;
                lines += 1;
            } else {
                current_line_bytes += 1;
            }
        }
        total += n as u64;
    }

    if current_line_bytes > 0 {
        line_sizes.push(current_line_bytes);
    }
    line_sizes.sort_unstable();
    let sampled_lines = line_sizes.len() as u64;
    let total_line_bytes: u64 = line_sizes.iter().sum();
    let average_line_bytes = total_line_bytes.checked_div(sampled_lines).unwrap_or(0);
    let p95_index = ((line_sizes.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    let p95_line_bytes = line_sizes.get(p95_index).copied().unwrap_or(0);
    let max_line_bytes = line_sizes.last().copied().unwrap_or(0);

    let elapsed = started.elapsed().max(Duration::from_millis(1));
    let bytes_per_sec = total as f64 / elapsed.as_secs_f64();
    let line_split_bytes_per_sec = if lines == 0 {
        bytes_per_sec
    } else {
        bytes_per_sec * 0.95
    };
    Ok(InputBenchmark {
        disk_read_bytes_per_sec: bytes_per_sec.max(1.0),
        line_split_bytes_per_sec: line_split_bytes_per_sec.max(1.0),
        sampled_lines,
        average_line_bytes,
        p95_line_bytes,
        max_line_bytes,
    })
}

fn format_duration(seconds: f64) -> String {
    let seconds = seconds.max(0.0).round() as u64;
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
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn preflight_reports_file_size_and_positive_rates() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{{\"message\":\"a\"}}").unwrap();
        writeln!(file, "{{\"message\":\"b\"}}").unwrap();

        let report = run_preflight(PreflightConfig {
            input: file.path().to_path_buf(),
            quickwit: "http://127.0.0.1:1".to_string(),
            index: None,
            quickwit_benchmark: false,
            benchmark_preset: None,
            sample_size_bytes: 1024,
        })
        .await
        .unwrap();

        assert!(report.input_size_bytes > 0);
        assert!(report.disk_read_bytes_per_sec > 0.0);
        assert!(report.line_split_bytes_per_sec > 0.0);
        assert!(report.sampled_lines >= 2);
        assert!(report.average_line_bytes > 0);
        assert!(report.max_line_bytes > 0);
        assert!(report.recommended_memory_bytes >= 64 * 1024 * 1024);
    }

    #[test]
    fn duration_format_is_human_readable() {
        assert_eq!(format_duration(45.0), "45s");
        assert_eq!(format_duration(125.0), "2m 5s");
        assert_eq!(format_duration(3723.0), "1h 2m 3s");
    }
}
