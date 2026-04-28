use std::{
    io::Write,
    time::{Duration, Instant},
};

use anyhow::Result;
use bytes::Bytes;
use clap::ValueEnum;
use flate2::{write::GzEncoder, Compression as GzipLevel};
use reqwest::{Client, StatusCode};
use tokio::time::sleep;

use crate::{
    batcher::Batch,
    quickwit_error::{classify_quickwit_failure, QuickwitErrorKind},
    retry::{classify_status, retry_delay, RetryDecision},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Compression {
    None,
    Gzip,
    Auto,
}

impl Compression {
    pub fn resolve_for_quickwit(self, base_url: &str) -> Self {
        match self {
            Compression::Auto if is_local_quickwit(base_url) => Compression::None,
            Compression::Auto => Compression::Gzip,
            explicit => explicit,
        }
    }
}

fn is_local_quickwit(base_url: &str) -> bool {
    base_url.contains("localhost") || base_url.contains("127.0.0.1") || base_url.contains("[::1]")
}

#[derive(Debug, Clone)]
pub struct QuickwitClient {
    client: Client,
    endpoint: String,
    max_attempts: u32,
    compression: Compression,
}

#[derive(Debug, Clone)]
pub struct IngestOutcome {
    pub status: StatusCode,
    pub latency: Duration,
    pub attempts: u32,
}

#[derive(Debug, Clone)]
pub struct IngestFailure {
    pub kind: QuickwitErrorKind,
    pub message: String,
}

impl std::fmt::Display for IngestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for IngestFailure {}

impl QuickwitClient {
    pub fn new(base_url: String, index: String, compression: Compression) -> Result<Self> {
        let endpoint = format!("{}/api/v1/{}/ingest", base_url.trim_end_matches('/'), index);
        let client = Client::builder().timeout(Duration::from_secs(30)).build()?;
        Ok(Self {
            client,
            endpoint,
            max_attempts: 5,
            compression: compression.resolve_for_quickwit(&base_url),
        })
    }

    pub async fn ingest_batch(
        &self,
        batch: &Batch,
    ) -> std::result::Result<IngestOutcome, IngestFailure> {
        let mut attempt = 0_u32;
        loop {
            let started = Instant::now();
            let encoded_body =
                encode_body(&batch.body, self.compression).map_err(|err| IngestFailure {
                    kind: QuickwitErrorKind::Unknown,
                    message: format!("failed to encode ingest body: {err}"),
                })?;
            let mut request = self
                .client
                .post(&self.endpoint)
                .header("content-type", "application/x-ndjson");
            if self.compression == Compression::Gzip {
                request = request.header("content-encoding", "gzip");
            }
            let response = request.body(encoded_body).send().await;

            match response {
                Ok(response) if response.status().is_success() => {
                    return Ok(IngestOutcome {
                        status: response.status(),
                        latency: started.elapsed(),
                        attempts: attempt + 1,
                    });
                }
                Ok(response) => {
                    let status = response.status();
                    if classify_status(status) == RetryDecision::Retry
                        && attempt + 1 < self.max_attempts
                    {
                        sleep(retry_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    let body = response.text().await.unwrap_or_default();
                    let kind = classify_quickwit_failure(status, &body);
                    return Err(IngestFailure {
                        kind,
                        message: format!("Quickwit ingest failed with {status}: {body}"),
                    });
                }
                Err(err) => {
                    if attempt + 1 < self.max_attempts {
                        sleep(retry_delay(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(IngestFailure {
                        kind: QuickwitErrorKind::Network,
                        message: format!("Quickwit ingest request failed: {err}"),
                    });
                }
            }
        }
    }
}

fn encode_body(body: &Bytes, compression: Compression) -> Result<Bytes> {
    match compression {
        Compression::None | Compression::Auto => Ok(body.clone()),
        Compression::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::fast());
            encoder.write_all(body)?;
            Ok(Bytes::from(encoder.finish()?))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use super::*;

    fn batch() -> Batch {
        Batch {
            body: Bytes::from_static(b"{\"a\":1}\n"),
            record_count: 1,
            byte_count: 8,
            start_offset: 0,
            end_offset: 8,
            spans: Vec::new(),
        }
    }

    #[test]
    fn auto_compression_resolves_from_quickwit_host() {
        assert_eq!(
            Compression::Auto.resolve_for_quickwit("http://localhost:7280"),
            Compression::None
        );
        assert_eq!(
            Compression::Auto.resolve_for_quickwit("http://127.0.0.1:7280"),
            Compression::None
        );
        assert_eq!(
            Compression::Auto.resolve_for_quickwit("https://quickwit.example.com"),
            Compression::Gzip
        );
    }

    #[tokio::test]
    async fn sends_batch_to_quickwit_ingest_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client =
            QuickwitClient::new(server.uri(), "logs".to_string(), Compression::None).unwrap();
        let outcome = client.ingest_batch(&batch()).await.unwrap();

        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.attempts, 1);
    }

    #[tokio::test]
    async fn retries_server_errors_before_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client =
            QuickwitClient::new(server.uri(), "logs".to_string(), Compression::None).unwrap();
        let outcome = client.ingest_batch(&batch()).await.unwrap();

        assert_eq!(outcome.status, StatusCode::OK);
        assert_eq!(outcome.attempts, 2);
    }

    #[tokio::test]
    async fn sends_gzip_encoded_batches_when_enabled() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .and(wiremock::matchers::header("content-encoding", "gzip"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let client =
            QuickwitClient::new(server.uri(), "logs".to_string(), Compression::Gzip).unwrap();
        let outcome = client.ingest_batch(&batch()).await.unwrap();

        assert_eq!(outcome.status, StatusCode::OK);
    }
}
