use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub input_path: PathBuf,
    pub input_len: u64,
    pub input_modified_unix_secs: u64,
    pub offset: u64,
    pub qwhyper_version: String,
    pub index: String,
    pub input_format: String,
    pub compression: String,
    pub fingerprint: String,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSettings {
    pub index: String,
    pub input_format: String,
    pub compression: String,
}

impl Checkpoint {
    pub async fn for_input(input_path: PathBuf, offset: u64) -> Result<Self> {
        Self::for_input_with_settings(
            input_path,
            offset,
            CheckpointSettings {
                index: String::new(),
                input_format: String::new(),
                compression: String::new(),
            },
        )
        .await
    }

    pub async fn for_input_with_settings(
        input_path: PathBuf,
        offset: u64,
        settings: CheckpointSettings,
    ) -> Result<Self> {
        let metadata = fs::metadata(&input_path).await?;
        let modified = metadata.modified()?;
        let fingerprint = fingerprint_input(&input_path).await?;
        Ok(Self {
            input_path,
            input_len: metadata.len(),
            input_modified_unix_secs: unix_secs(modified),
            offset,
            qwhyper_version: env!("CARGO_PKG_VERSION").to_string(),
            index: settings.index,
            input_format: settings.input_format,
            compression: settings.compression,
            fingerprint,
            complete: false,
        })
    }

    pub async fn still_matches_input(&self) -> Result<bool> {
        let metadata = fs::metadata(&self.input_path).await?;
        let modified = metadata.modified()?;
        Ok(self.input_len == metadata.len()
            && self.input_modified_unix_secs == unix_secs(modified)
            && self.fingerprint == fingerprint_input(&self.input_path).await?)
    }

    pub fn settings_match(&self, settings: &CheckpointSettings) -> bool {
        self.qwhyper_version == env!("CARGO_PKG_VERSION")
            && self.index == settings.index
            && self.input_format == settings.input_format
            && self.compression == settings.compression
    }
}

pub fn checkpoint_path(input: &Path, index: &str) -> PathBuf {
    let file_name = input
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("input");
    input.with_file_name(format!(".{file_name}.{index}.qwhyper.checkpoint"))
}

pub async fn load_checkpoint(path: &Path) -> Result<Option<Checkpoint>> {
    if fs::metadata(path).await.is_err() {
        return Ok(None);
    }
    let bytes = fs::read(path).await?;
    let checkpoint = serde_json::from_slice(&bytes)?;
    Ok(Some(checkpoint))
}

pub async fn save_checkpoint(path: &Path, checkpoint: &Checkpoint) -> Result<()> {
    let tmp_path = path.with_extension("checkpoint.tmp");
    let bytes = serde_json::to_vec_pretty(checkpoint)?;
    let mut file = fs::File::create(&tmp_path).await?;
    file.write_all(&bytes).await?;
    file.flush().await?;
    drop(file);
    fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("failed to move checkpoint into place: {}", path.display()))
}

pub async fn delete_checkpoint(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

async fn fingerprint_input(path: &Path) -> Result<String> {
    const WINDOW: usize = 64 * 1024;
    let metadata = fs::metadata(path).await?;
    let mut file = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    hasher.update(metadata.len().to_le_bytes());

    let first_len = WINDOW.min(metadata.len() as usize);
    let mut first = vec![0_u8; first_len];
    if first_len > 0 {
        file.read_exact(&mut first).await?;
        hasher.update(&first);
    }

    if metadata.len() > WINDOW as u64 {
        let last_len = WINDOW.min(metadata.len() as usize);
        file.seek(SeekFrom::Start(metadata.len() - last_len as u64))
            .await?;
        let mut last = vec![0_u8; last_len];
        file.read_exact(&mut last).await?;
        hasher.update(&last);
    }

    Ok(hex::encode(hasher.finalize()))
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn saves_and_loads_checkpoint() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("logs.ndjson");
        let mut file = std::fs::File::create(&input).unwrap();
        writeln!(file, "{{}}").unwrap();

        let path = checkpoint_path(&input, "logs");
        let checkpoint = Checkpoint::for_input(input.clone(), 3).await.unwrap();
        save_checkpoint(&path, &checkpoint).await.unwrap();
        let loaded = load_checkpoint(&path).await.unwrap().unwrap();

        assert_eq!(loaded.offset, 3);
        assert_eq!(loaded.input_path, input);
        assert!(!loaded.fingerprint.is_empty());
        assert!(loaded.still_matches_input().await.unwrap());
    }

    #[tokio::test]
    async fn checkpoint_settings_must_match() {
        let dir = tempdir().unwrap();
        let input = dir.path().join("logs.ndjson");
        std::fs::write(&input, b"{}\n").unwrap();
        let settings = CheckpointSettings {
            index: "logs".to_string(),
            input_format: "ndjson".to_string(),
            compression: "none".to_string(),
        };
        let checkpoint = Checkpoint::for_input_with_settings(input, 3, settings.clone())
            .await
            .unwrap();

        assert!(checkpoint.settings_match(&settings));
        assert!(!checkpoint.settings_match(&CheckpointSettings {
            index: "other".to_string(),
            input_format: "ndjson".to_string(),
            compression: "none".to_string(),
        }));
    }

    #[tokio::test]
    async fn returns_none_for_missing_checkpoint() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("missing.checkpoint");
        assert!(load_checkpoint(&missing).await.unwrap().is_none());
    }
}
