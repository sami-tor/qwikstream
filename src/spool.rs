use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::{fs, io::AsyncWriteExt};

use crate::{
    batcher::{Batch, BatchRecordSpan},
    ingest::QuickwitClient,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpooledBatchMeta {
    sequence: u64,
    record_count: usize,
    byte_count: usize,
    start_offset: u64,
    end_offset: u64,
    spans: Vec<BatchRecordSpanMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchRecordSpanMeta {
    body_start: usize,
    body_end: usize,
    start_offset: u64,
    end_offset: u64,
}

impl From<&BatchRecordSpan> for BatchRecordSpanMeta {
    fn from(span: &BatchRecordSpan) -> Self {
        Self {
            body_start: span.body_start,
            body_end: span.body_end,
            start_offset: span.start_offset,
            end_offset: span.end_offset,
        }
    }
}

impl From<BatchRecordSpanMeta> for BatchRecordSpan {
    fn from(span: BatchRecordSpanMeta) -> Self {
        Self {
            body_start: span.body_start,
            body_end: span.body_end,
            start_offset: span.start_offset,
            end_offset: span.end_offset,
        }
    }
}

pub async fn ensure_spool_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).await?;
    Ok(())
}

pub async fn spool_usage_bytes(dir: &Path) -> Result<u64> {
    if fs::metadata(dir).await.is_err() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let ext = path.extension().and_then(|ext| ext.to_str());
        if matches!(ext, Some("ndjson") | Some("json")) {
            total = total.saturating_add(entry.metadata().await?.len());
        }
    }
    Ok(total)
}

pub async fn spool_batch(dir: &Path, sequence: u64, batch: &Batch) -> Result<PathBuf> {
    spool_batch_with_limit(dir, sequence, batch, 0).await
}

pub async fn spool_batch_with_limit(
    dir: &Path,
    sequence: u64,
    batch: &Batch,
    limit_bytes: u64,
) -> Result<PathBuf> {
    ensure_spool_dir(dir).await?;
    let path = spool_body_path(dir, sequence);
    let meta_path = spool_meta_path(dir, sequence);
    let meta = SpooledBatchMeta {
        sequence,
        record_count: batch.record_count,
        byte_count: batch.byte_count,
        start_offset: batch.start_offset,
        end_offset: batch.end_offset,
        spans: batch.spans.iter().map(BatchRecordSpanMeta::from).collect(),
    };
    let meta_body = serde_json::to_vec(&meta)?;

    if limit_bytes > 0 {
        let projected = spool_usage_bytes(dir)
            .await?
            .saturating_add(batch.body.len() as u64)
            .saturating_add(meta_body.len() as u64);
        if projected > limit_bytes {
            return Err(anyhow!(
                "spool limit exceeded: projected {} bytes exceeds limit {} bytes",
                projected,
                limit_bytes
            ));
        }
    }

    let tmp = path.with_extension("ndjson.tmp");
    let meta_tmp = meta_path.with_extension("json.tmp");
    let mut file = fs::File::create(&tmp).await?;
    file.write_all(&batch.body).await?;
    file.flush().await?;
    drop(file);

    let mut meta_file = fs::File::create(&meta_tmp).await?;
    meta_file.write_all(&meta_body).await?;
    meta_file.flush().await?;
    drop(meta_file);

    fs::rename(&meta_tmp, &meta_path).await?;
    fs::rename(&tmp, &path).await?;
    Ok(path)
}

pub async fn read_spooled_batch(path: &Path) -> Result<Batch> {
    let body = fs::read(path).await?;
    let meta_path = path.with_extension("json");
    if let Ok(meta_body) = fs::read(&meta_path).await {
        let meta: SpooledBatchMeta = serde_json::from_slice(&meta_body)?;
        return Ok(Batch {
            body: Bytes::from(body),
            record_count: meta.record_count,
            byte_count: meta.byte_count,
            start_offset: meta.start_offset,
            end_offset: meta.end_offset,
            spans: meta.spans.into_iter().map(BatchRecordSpan::from).collect(),
        });
    }

    let len = body.len();
    Ok(Batch {
        byte_count: len,
        record_count: body.iter().filter(|byte| **byte == b'\n').count().max(1),
        start_offset: 0,
        end_offset: 0,
        spans: Vec::new(),
        body: Bytes::from(body),
    })
}

pub async fn replay_spool(dir: &Path, client: &QuickwitClient) -> Result<u64> {
    if fs::metadata(dir).await.is_err() {
        return Ok(0);
    }
    let mut entries = fs::read_dir(dir).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("ndjson") {
            paths.push(path);
        }
    }
    paths.sort();

    let mut replayed = 0_u64;
    for path in paths {
        let batch = read_spooled_batch(&path).await?;
        client.ingest_batch(&batch).await?;
        let meta_path = path.with_extension("json");
        fs::remove_file(&path).await?;
        if fs::metadata(&meta_path).await.is_ok() {
            fs::remove_file(&meta_path).await?;
        }
        replayed += 1;
    }
    Ok(replayed)
}

fn spool_body_path(dir: &Path, sequence: u64) -> PathBuf {
    dir.join(format!("batch-{sequence:020}.ndjson"))
}

fn spool_meta_path(dir: &Path, sequence: u64) -> PathBuf {
    dir.join(format!("batch-{sequence:020}.json"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> Batch {
        Batch {
            body: Bytes::from_static(b"{\"a\":1}\n{\"b\":2}\n"),
            record_count: 2,
            byte_count: 16,
            start_offset: 10,
            end_offset: 26,
            spans: vec![
                BatchRecordSpan {
                    body_start: 0,
                    body_end: 8,
                    start_offset: 10,
                    end_offset: 18,
                },
                BatchRecordSpan {
                    body_start: 8,
                    body_end: 16,
                    start_offset: 18,
                    end_offset: 26,
                },
            ],
        }
    }

    #[tokio::test]
    async fn spool_preserves_batch_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sample_batch();
        let path = spool_batch_with_limit(dir.path(), 7, &batch, 1024)
            .await
            .unwrap();

        let restored = read_spooled_batch(&path).await.unwrap();

        assert_eq!(restored.record_count, batch.record_count);
        assert_eq!(restored.byte_count, batch.byte_count);
        assert_eq!(restored.start_offset, batch.start_offset);
        assert_eq!(restored.end_offset, batch.end_offset);
        assert_eq!(restored.spans, batch.spans);
        assert_eq!(restored.body, batch.body);
        assert!(path.with_extension("json").exists());
    }

    #[tokio::test]
    async fn spool_limit_rejects_projected_usage() {
        let dir = tempfile::tempdir().unwrap();
        let batch = sample_batch();
        spool_batch_with_limit(dir.path(), 1, &batch, 1024)
            .await
            .unwrap();

        let err = spool_batch_with_limit(dir.path(), 2, &batch, 1)
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("spool limit"));
    }
}
