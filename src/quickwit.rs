use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use reqwest::Client;

#[derive(Debug, Clone)]
pub struct QuickwitApiClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuickwitCheck {
    pub version_ok: bool,
    pub index_ok: bool,
    pub latency_ms: u128,
}

impl QuickwitApiClient {
    pub fn new(base_url: String) -> Result<Self> {
        Ok(Self {
            client: Client::builder().timeout(Duration::from_secs(3)).build()?,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    pub async fn check_version(&self) -> Result<u128> {
        let started = Instant::now();
        let response = self
            .client
            .get(format!("{}/api/v1/version", self.base_url))
            .send()
            .await?;
        let elapsed = started.elapsed().as_millis();
        if response.status().is_success() {
            Ok(elapsed)
        } else {
            Err(anyhow!(
                "Quickwit version endpoint returned {}",
                response.status()
            ))
        }
    }

    pub async fn check_index(&self, index: &str) -> Result<()> {
        let response = self
            .client
            .get(format!("{}/api/v1/indexes/{index}", self.base_url))
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!(
                "Quickwit index {index} returned {}",
                response.status()
            ))
        }
    }

    pub async fn tiny_ingest_benchmark(&self, index: &str) -> Result<u128> {
        let started = Instant::now();
        let response = self
            .client
            .post(format!("{}/api/v1/{index}/ingest", self.base_url))
            .header("content-type", "application/x-ndjson")
            .body(b"{\"qwhyper_benchmark\":true}\n".as_slice().to_vec())
            .send()
            .await?;
        let elapsed = started.elapsed().as_millis();
        if response.status().is_success() {
            Ok(elapsed)
        } else {
            Err(anyhow!(
                "Quickwit benchmark ingest returned {}",
                response.status()
            ))
        }
    }
}
