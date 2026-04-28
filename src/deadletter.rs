use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::format::BadRecord;

#[derive(Debug)]
pub struct DeadLetterWriter {
    file: File,
    written: u64,
}

#[derive(Debug, Serialize)]
struct DeadLetterLine<'a> {
    line_number: u64,
    start_offset: u64,
    end_offset: u64,
    reason: &'a str,
    raw: String,
}

impl DeadLetterWriter {
    pub async fn create(path: &Path) -> Result<Self> {
        let file = File::create(path).await?;
        Ok(Self { file, written: 0 })
    }

    pub async fn write_bad_record(&mut self, record: &BadRecord) -> Result<()> {
        let line = DeadLetterLine {
            line_number: record.line_number,
            start_offset: record.start_offset,
            end_offset: record.end_offset,
            reason: &record.reason,
            raw: String::from_utf8_lossy(&record.raw).into_owned(),
        };
        let mut bytes = serde_json::to_vec(&line)?;
        bytes.push(b'\n');
        self.file.write_all(&bytes).await?;
        self.written += 1;
        Ok(())
    }

    pub fn written(&self) -> u64 {
        self.written
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn writes_bad_records_as_json_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bad.dlq");
        let mut writer = DeadLetterWriter::create(&path).await.unwrap();
        writer
            .write_bad_record(&BadRecord {
                line_number: 3,
                start_offset: 10,
                end_offset: 20,
                reason: "invalid json".to_string(),
                raw: Bytes::from_static(b"oops"),
            })
            .await
            .unwrap();

        assert_eq!(writer.written(), 1);
        drop(writer);
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(contents.contains("invalid json"));
        assert!(contents.contains("oops"));
    }
}
