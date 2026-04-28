use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore},
};

use crate::{
    batcher::{Batch, Batcher, BatcherConfig},
    checkpoint::{
        checkpoint_path, delete_checkpoint, load_checkpoint, save_checkpoint, Checkpoint,
        CheckpointSettings,
    },
    controller::AdaptiveController,
    deadletter::DeadLetterWriter,
    format::{
        BadRecord, BadRecordPolicy, FormatConfig, FormatOutcome, InputFormat,
        OversizedRecordPolicy, RecordFormatter,
    },
    ingest::{Compression, IngestFailure, IngestOutcome, QuickwitClient},
    metrics::{ProgressMode, StreamStats},
    prometheus::serve_metrics,
    quickwit_error::QuickwitErrorKind,
    reader::{LineSplitter, Record},
    spool::{ensure_spool_dir, replay_spool, spool_batch_with_limit},
};

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub input: PathBuf,
    pub quickwit: String,
    pub index: String,
    pub memory_limit_bytes: u64,
    pub adaptive: bool,
    pub resume: bool,
    pub input_format: InputFormat,
    pub csv_has_headers: bool,
    pub dead_letter: Option<PathBuf>,
    pub bad_record_policy: BadRecordPolicy,
    pub fail_on_bad_record: bool,
    pub max_record_bytes: usize,
    pub oversized_record_policy: OversizedRecordPolicy,
    pub trust_input: bool,
    pub csv_infer_types: bool,
    pub timestamp_field: Option<String>,
    pub timestamp_format: String,
    pub rename_fields: Vec<(String, String)>,
    pub drop_fields: Vec<String>,
    pub adaptive_workers: bool,
    pub progress: ProgressMode,
    pub metrics_file: Option<PathBuf>,
    pub dry_run: bool,
    pub keep_checkpoint: bool,
    pub metrics_listen: Option<String>,
    pub spool_dir: Option<PathBuf>,
    pub spool_limit_bytes: u64,
    pub compression: Compression,
    pub ingest_workers: usize,
}

pub async fn run_stream(config: StreamConfig) -> Result<()> {
    if config.memory_limit_bytes < 64 * 1024 * 1024 {
        return Err(anyhow!("memory limit must be at least 64mb"));
    }

    let metadata = tokio::fs::metadata(&config.input)
        .await
        .with_context(|| format!("failed to inspect input {}", config.input.display()))?;
    if !metadata.is_file() {
        return Err(anyhow!("input must be a file: {}", config.input.display()));
    }

    if let Some(spool_dir) = &config.spool_dir {
        ensure_spool_dir(spool_dir).await?;
    }
    if config.spool_limit_bytes > 0 && config.progress != ProgressMode::Quiet {
        println!("spool limit: {} bytes", config.spool_limit_bytes);
    }

    let checkpoint_file = checkpoint_path(&config.input, &config.index);
    let checkpoint_settings = CheckpointSettings {
        index: config.index.clone(),
        input_format: format!("{:?}", config.input_format).to_ascii_lowercase(),
        compression: format!(
            "{:?}",
            config.compression.resolve_for_quickwit(&config.quickwit)
        )
        .to_ascii_lowercase(),
    };
    let start_offset = if config.resume {
        match load_checkpoint(&checkpoint_file).await? {
            Some(checkpoint)
                if checkpoint.still_matches_input().await?
                    && checkpoint.settings_match(&checkpoint_settings)
                    && !checkpoint.complete =>
            {
                checkpoint.offset
            }
            Some(_) => 0,
            None => 0,
        }
    } else {
        0
    };

    let batch_channel_capacity = channel_capacity(config.memory_limit_bytes / 8, 8, 128);
    let read_buffer_bytes = read_buffer_size(config.memory_limit_bytes);
    let (batch_tx, batch_rx) = mpsc::channel::<SequencedBatch>(batch_channel_capacity);
    let stats = Arc::new(Mutex::new(StreamStats::new(metadata.len())));
    let client = if config.dry_run {
        None
    } else {
        Some(QuickwitClient::new(
            config.quickwit.clone(),
            config.index.clone(),
            config.compression,
        )?)
    };

    if let Some(addr) = &config.metrics_listen {
        serve_metrics(addr.clone(), stats.clone()).await?;
    }
    if let (Some(spool_dir), Some(client)) = (&config.spool_dir, client.as_ref()) {
        replay_spool(spool_dir, client).await?;
    }

    let reader_task = tokio::spawn(read_batches(
        ReadBatchesConfig {
            input: config.input.clone(),
            start_offset,
            read_buffer_bytes,
            memory_limit_bytes: config.memory_limit_bytes,
            adaptive: config.adaptive,
            batch_channel_capacity,
            format_config: FormatConfig {
                input_format: config.input_format,
                csv_has_headers: config.csv_has_headers,
                trust_input: config.trust_input,
                csv_infer_types: config.csv_infer_types,
                timestamp_field: config.timestamp_field.clone(),
                timestamp_format: config.timestamp_format.clone(),
                rename_fields: config.rename_fields.clone(),
                drop_fields: config.drop_fields.clone(),
            },
            dead_letter: config.dead_letter.clone(),
            bad_record_policy: if config.fail_on_bad_record {
                BadRecordPolicy::Fail
            } else {
                config.bad_record_policy
            },
            max_record_bytes: config.max_record_bytes,
            oversized_record_policy: config.oversized_record_policy,
        },
        batch_tx,
    ));
    let ingest_task = tokio::spawn(ingest_batches(IngestBatchesConfig {
        rx: batch_rx,
        client,
        input: config.input.clone(),
        checkpoint_file,
        checkpoint_settings,
        keep_checkpoint: config.keep_checkpoint,
        stats: stats.clone(),
        worker_count: config.ingest_workers,
        adaptive_workers: config.adaptive_workers,
        progress: config.progress,
        metrics_file: config.metrics_file.clone(),
        spool_dir: config.spool_dir.clone(),
        dry_run: config.dry_run,
        spool_limit_bytes: config.spool_limit_bytes,
    }));

    reader_task.await??;
    ingest_task.await??;

    let stats = stats.lock().await;
    match config.progress {
        ProgressMode::Plain => println!("completed: {}", stats.line()),
        ProgressMode::Json => println!("{}", stats.json_line()),
        ProgressMode::Quiet => {}
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SequencedBatch {
    sequence: u64,
    batch: Batch,
}

#[derive(Debug, Default)]
struct CheckpointCoordinator {
    next_sequence: u64,
    completed: BTreeMap<u64, u64>,
}

impl CheckpointCoordinator {
    fn new() -> Self {
        Self {
            next_sequence: 1,
            completed: BTreeMap::new(),
        }
    }

    fn complete(&mut self, sequence: u64, end_offset: u64) -> Option<u64> {
        self.completed.insert(sequence, end_offset);
        let mut advanced = None;
        while let Some(offset) = self.completed.remove(&self.next_sequence) {
            advanced = Some(offset);
            self.next_sequence += 1;
        }
        advanced
    }
}

#[derive(Debug)]
struct ReadBatchesConfig {
    input: PathBuf,
    start_offset: u64,
    read_buffer_bytes: usize,
    memory_limit_bytes: u64,
    adaptive: bool,
    batch_channel_capacity: usize,
    format_config: FormatConfig,
    dead_letter: Option<PathBuf>,
    bad_record_policy: BadRecordPolicy,
    max_record_bytes: usize,
    oversized_record_policy: OversizedRecordPolicy,
}

async fn read_batches(config: ReadBatchesConfig, tx: mpsc::Sender<SequencedBatch>) -> Result<()> {
    let mut file = File::open(&config.input).await?;
    file.seek(SeekFrom::Start(config.start_offset)).await?;
    let mut splitter = LineSplitter::new();
    let mut formatter = RecordFormatter::new(config.format_config);
    let mut dead_letter = match config.dead_letter {
        Some(path) => Some(DeadLetterWriter::create(&path).await?),
        None if config.bad_record_policy == BadRecordPolicy::Dlq => {
            let path = config.input.with_extension("qwhyper.dlq");
            Some(DeadLetterWriter::create(&path).await?)
        }
        None => None,
    };
    let mut controller = AdaptiveController::new(config.memory_limit_bytes);
    let mut batcher = Batcher::new(BatcherConfig {
        max_batch_bytes: controller.limits().max_batch_bytes,
        max_batch_records: 10_000,
    });
    let mut offset = config.start_offset;
    let mut next_sequence = 1_u64;
    let mut buffer = vec![0_u8; config.read_buffer_bytes];

    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }

        let mut ready_batches = Vec::new();
        let mut bad_records = Vec::new();
        splitter.push_chunk_for_each(&buffer[..read], offset, |record| {
            if config.adaptive {
                let pressure = tx.capacity() as f32 / config.batch_channel_capacity as f32;
                if pressure < 0.10 {
                    controller.observe_pressure();
                    batcher.update_max_batch_bytes(controller.limits().max_batch_bytes);
                }
            }
            if record.bytes.len() > config.max_record_bytes
                && config.oversized_record_policy != OversizedRecordPolicy::Send
            {
                bad_records.push(BadRecord {
                    line_number: 0,
                    start_offset: record.start_offset,
                    end_offset: record.end_offset,
                    reason: format!(
                        "record exceeds max_record_bytes {}",
                        config.max_record_bytes
                    ),
                    raw: record.bytes,
                });
                return;
            }
            match formatter.format_line(record.bytes, record.start_offset, record.end_offset) {
                FormatOutcome::Emit(bytes) => {
                    let formatted = Record {
                        bytes,
                        start_offset: record.start_offset,
                        end_offset: record.end_offset,
                    };
                    if let Some(batch) = batcher.push(formatted) {
                        ready_batches.push(batch);
                    }
                }
                FormatOutcome::Skip => {}
                FormatOutcome::Bad(bad_record) => bad_records.push(bad_record),
            }
        });

        for bad_record in bad_records {
            handle_bad_record(&mut dead_letter, config.bad_record_policy, bad_record).await?;
        }

        for batch in ready_batches {
            let sequenced = SequencedBatch {
                sequence: next_sequence,
                batch,
            };
            next_sequence += 1;
            tx.send(sequenced)
                .await
                .map_err(|_| anyhow!("batch receiver closed"))?;
        }
        offset += read as u64;
    }

    if let Some(record) = splitter.finish(offset) {
        if record.bytes.len() > config.max_record_bytes
            && config.oversized_record_policy != OversizedRecordPolicy::Send
        {
            handle_bad_record(
                &mut dead_letter,
                config.bad_record_policy,
                BadRecord {
                    line_number: 0,
                    start_offset: record.start_offset,
                    end_offset: record.end_offset,
                    reason: format!(
                        "record exceeds max_record_bytes {}",
                        config.max_record_bytes
                    ),
                    raw: record.bytes,
                },
            )
            .await?;
        } else {
            match formatter.format_line(record.bytes, record.start_offset, record.end_offset) {
                FormatOutcome::Emit(bytes) => {
                    let formatted = Record {
                        bytes,
                        start_offset: record.start_offset,
                        end_offset: record.end_offset,
                    };
                    if let Some(batch) = batcher.push(formatted) {
                        let sequenced = SequencedBatch {
                            sequence: next_sequence,
                            batch,
                        };
                        next_sequence += 1;
                        tx.send(sequenced)
                            .await
                            .map_err(|_| anyhow!("batch receiver closed"))?;
                    }
                }
                FormatOutcome::Skip => {}
                FormatOutcome::Bad(bad_record) => {
                    handle_bad_record(&mut dead_letter, config.bad_record_policy, bad_record)
                        .await?;
                }
            }
        }
    }

    if let Some(batch) = batcher.flush() {
        let sequenced = SequencedBatch {
            sequence: next_sequence,
            batch,
        };
        tx.send(sequenced)
            .await
            .map_err(|_| anyhow!("batch receiver closed"))?;
    }
    Ok(())
}

async fn handle_bad_record(
    dead_letter: &mut Option<DeadLetterWriter>,
    policy: BadRecordPolicy,
    bad_record: BadRecord,
) -> Result<()> {
    match policy {
        BadRecordPolicy::Fail => Err(anyhow!(
            "bad record at line {} offset {}: {}",
            bad_record.line_number,
            bad_record.start_offset,
            bad_record.reason
        )),
        BadRecordPolicy::Skip => Ok(()),
        BadRecordPolicy::Dlq => {
            if let Some(writer) = dead_letter.as_mut() {
                writer.write_bad_record(&bad_record).await?;
            }
            Ok(())
        }
    }
}

struct IngestBatchesConfig {
    rx: mpsc::Receiver<SequencedBatch>,
    client: Option<QuickwitClient>,
    input: PathBuf,
    checkpoint_file: PathBuf,
    checkpoint_settings: CheckpointSettings,
    keep_checkpoint: bool,
    stats: Arc<Mutex<StreamStats>>,
    worker_count: usize,
    adaptive_workers: bool,
    progress: ProgressMode,
    metrics_file: Option<PathBuf>,
    spool_dir: Option<PathBuf>,
    dry_run: bool,
    spool_limit_bytes: u64,
}

#[derive(Clone)]
struct AdaptiveWorkerGate {
    semaphore: Arc<Semaphore>,
    desired: Arc<Mutex<usize>>,
    active: Arc<Mutex<usize>>,
    max_workers: usize,
}

struct AdaptiveWorkerPermit {
    _permit: OwnedSemaphorePermit,
    active: Arc<Mutex<usize>>,
}

impl Drop for AdaptiveWorkerPermit {
    fn drop(&mut self) {
        let active = self.active.clone();
        tokio::spawn(async move {
            let mut active = active.lock().await;
            *active = active.saturating_sub(1);
        });
    }
}

impl AdaptiveWorkerGate {
    fn new(initial_workers: usize, max_workers: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(initial_workers)),
            desired: Arc::new(Mutex::new(initial_workers)),
            active: Arc::new(Mutex::new(0)),
            max_workers,
        }
    }

    async fn acquire(
        &self,
    ) -> std::result::Result<AdaptiveWorkerPermit, tokio::sync::AcquireError> {
        loop {
            let permit = self.semaphore.clone().acquire_owned().await?;
            let desired = *self.desired.lock().await;
            let mut active = self.active.lock().await;
            if *active < desired {
                *active += 1;
                return Ok(AdaptiveWorkerPermit {
                    _permit: permit,
                    active: self.active.clone(),
                });
            }
            drop(active);
            drop(permit);
            tokio::task::yield_now().await;
        }
    }

    async fn set_desired(&self, workers: usize) {
        let workers = workers.clamp(1, self.max_workers);
        let mut desired = self.desired.lock().await;
        if workers > *desired {
            self.semaphore.add_permits(workers - *desired);
        }
        *desired = workers;
    }

    #[cfg(test)]
    async fn desired(&self) -> usize {
        *self.desired.lock().await
    }

    fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

fn should_reduce_workers(outcome: &IngestOutcome) -> bool {
    outcome.status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || outcome.status.is_server_error()
        || outcome.latency > Duration::from_secs(2)
}

async fn ingest_batches(config: IngestBatchesConfig) -> Result<()> {
    let IngestBatchesConfig {
        rx,
        client,
        input,
        checkpoint_file,
        checkpoint_settings,
        keep_checkpoint,
        stats,
        worker_count,
        adaptive_workers,
        progress,
        metrics_file,
        spool_dir,
        dry_run,
        spool_limit_bytes,
    } = config;
    let mut checkpoint = Checkpoint::for_input_with_settings(input, 0, checkpoint_settings).await?;
    let mut last_checkpoint_offset = checkpoint.offset;
    let mut last_checkpoint_at = Instant::now();
    let mut last_print_at = Instant::now();
    let mut pending_checkpoint = false;
    let worker_count = worker_count.clamp(1, 64);
    let spawn_count = if adaptive_workers { 64 } else { worker_count };
    let worker_gate = AdaptiveWorkerGate::new(worker_count, spawn_count);
    let rx = Arc::new(Mutex::new(rx));
    let (completion_tx, mut completion_rx) =
        mpsc::channel::<Result<(u64, Batch, crate::ingest::IngestOutcome)>>(spawn_count * 2);

    for _ in 0..spawn_count {
        let rx = rx.clone();
        let tx = completion_tx.clone();
        let client = client.clone();
        let worker_gate = worker_gate.clone();
        let spool_dir = spool_dir.clone();
        tokio::spawn(async move {
            loop {
                let item = {
                    let mut rx = rx.lock().await;
                    rx.recv().await
                };
                let Some(item) = item else { break };
                let Ok(_permit) = worker_gate.acquire().await else {
                    break;
                };
                let upload_result = if dry_run {
                    Ok((
                        item.batch.clone(),
                        IngestOutcome {
                            status: reqwest::StatusCode::OK,
                            latency: Duration::ZERO,
                            attempts: 1,
                        },
                    ))
                } else if let Some(client) = client.as_ref() {
                    ingest_with_payload_splitting(client, item.batch.clone()).await
                } else {
                    unreachable!("non-dry-run ingestion requires a Quickwit client")
                };
                let result = match (upload_result, spool_dir.as_ref()) {
                    (Ok((batch, outcome)), _) => Ok((item.sequence, batch, outcome)),
                    (Err(_failure), Some(spool_dir)) => {
                        match spool_batch_with_limit(
                            spool_dir,
                            item.sequence,
                            &item.batch,
                            spool_limit_bytes,
                        )
                        .await
                        {
                            Ok(_) => Ok((
                                item.sequence,
                                item.batch,
                                IngestOutcome {
                                    status: reqwest::StatusCode::ACCEPTED,
                                    latency: Duration::ZERO,
                                    attempts: 1,
                                },
                            )),
                            Err(err) => Err(err),
                        }
                    }
                    (Err(failure), None) => Err(anyhow::Error::from(failure)),
                };
                if tx.send(result).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(completion_tx);

    let mut coordinator = CheckpointCoordinator::new();
    let mut worker_controller = AdaptiveController::new(512 * 1024 * 1024);
    while let Some(result) = completion_rx.recv().await {
        let (sequence, batch, outcome) = result?;
        if let Some(offset) = coordinator.complete(sequence, batch.end_offset) {
            checkpoint.offset = offset;
            pending_checkpoint = true;
        }

        let should_checkpoint = pending_checkpoint
            && (checkpoint.offset.saturating_sub(last_checkpoint_offset) >= 64 * 1024 * 1024
                || last_checkpoint_at.elapsed() >= Duration::from_secs(2));
        if should_checkpoint {
            save_checkpoint(&checkpoint_file, &checkpoint).await?;
            last_checkpoint_offset = checkpoint.offset;
            last_checkpoint_at = Instant::now();
            pending_checkpoint = false;
        }

        let mut stats = stats.lock().await;
        stats.record_batch(
            batch.byte_count as u64,
            batch.record_count as u64,
            outcome.attempts,
            outcome.latency,
        );
        if adaptive_workers {
            let pressure =
                (worker_gate.available_permits() as f32 / spawn_count as f32).clamp(0.0, 1.0);
            let desired = if should_reduce_workers(&outcome) {
                worker_controller.observe_ingest_pressure().workers
            } else {
                worker_controller
                    .observe_ingest_success(outcome.latency, 1.0 - pressure)
                    .workers
            }
            .min(spawn_count);
            worker_gate.set_desired(desired).await;
        }
        if last_print_at.elapsed() >= Duration::from_secs(1) {
            match progress {
                ProgressMode::Plain => println!("{}", stats.line()),
                ProgressMode::Json => println!("{}", stats.json_line()),
                ProgressMode::Quiet => {}
            }
            last_print_at = Instant::now();
        }
    }

    if pending_checkpoint {
        save_checkpoint(&checkpoint_file, &checkpoint).await?;
    }
    if keep_checkpoint {
        checkpoint.complete = true;
        save_checkpoint(&checkpoint_file, &checkpoint).await?;
    } else {
        delete_checkpoint(&checkpoint_file).await?;
    }
    if let Some(path) = metrics_file {
        let stats = stats.lock().await;
        tokio::fs::write(path, stats.json_line()).await?;
    }
    Ok(())
}

async fn ingest_with_payload_splitting(
    client: &QuickwitClient,
    batch: Batch,
) -> std::result::Result<(Batch, IngestOutcome), IngestFailure> {
    let original = batch.clone();
    let mut stack = vec![batch];
    let mut attempts = 0_u32;
    let mut latency = Duration::ZERO;
    let mut status = None;

    while let Some(batch) = stack.pop() {
        match client.ingest_batch(&batch).await {
            Ok(outcome) => {
                attempts += outcome.attempts;
                latency += outcome.latency;
                status = Some(outcome.status);
            }
            Err(failure) if failure.kind == QuickwitErrorKind::PayloadTooLarge => {
                if let Some((left, right)) = batch.split_half() {
                    stack.push(right);
                    stack.push(left);
                } else {
                    return Err(failure);
                }
            }
            Err(failure) => return Err(failure),
        }
    }

    Ok((
        original,
        IngestOutcome {
            status: status.unwrap_or(reqwest::StatusCode::OK),
            latency,
            attempts,
        },
    ))
}

fn channel_capacity(memory_limit_bytes: u64, min: usize, max: usize) -> usize {
    ((memory_limit_bytes / (2 * 1024 * 1024)) as usize).clamp(min, max)
}

fn read_buffer_size(memory_limit_bytes: u64) -> usize {
    ((memory_limit_bytes / 256) as usize).clamp(64 * 1024, 1024 * 1024)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;
    use tempfile::NamedTempFile;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use super::*;

    #[tokio::test]
    async fn adaptive_worker_gate_scales_down_and_up() {
        let gate = AdaptiveWorkerGate::new(4, 8);

        gate.set_desired(2).await;
        assert_eq!(gate.desired().await, 2);

        gate.set_desired(6).await;
        assert_eq!(gate.desired().await, 6);
        assert!(gate.available_permits() >= 6);
    }

    #[test]
    fn checkpoint_coordinator_advances_only_contiguous_offsets() {
        let mut coordinator = CheckpointCoordinator::new();
        assert_eq!(coordinator.complete(2, 200), None);
        assert_eq!(coordinator.complete(1, 100), Some(200));
        assert_eq!(coordinator.complete(4, 400), None);
        assert_eq!(coordinator.complete(3, 300), Some(400));
    }

    #[tokio::test]
    async fn streams_file_to_mock_quickwit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "{{\"message\":\"one\"}}").unwrap();
        writeln!(input, "{{\"message\":\"two\"}}").unwrap();

        run_stream(StreamConfig {
            input: input.path().to_path_buf(),
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn streams_plain_lines_as_json_messages() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .and(wiremock::matchers::body_string_contains(
                "\"message\":\"hello\"",
            ))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "hello").unwrap();

        run_stream(StreamConfig {
            input: input.path().to_path_buf(),
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Plain,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn writes_invalid_ndjson_to_dead_letter_and_continues() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.ndjson");
        std::fs::write(&input_path, b"not-json\n{\"ok\":true}\n").unwrap();
        let dlq_path = dir.path().join("bad.dlq");

        run_stream(StreamConfig {
            input: input_path,
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: Some(dlq_path.clone()),
            bad_record_policy: BadRecordPolicy::Dlq,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();

        let dlq = std::fs::read_to_string(dlq_path).unwrap();
        assert!(dlq.contains("invalid json"));
        assert!(dlq.contains("not-json"));
    }

    #[tokio::test]
    async fn oversized_records_fail_by_default() {
        let input = NamedTempFile::new().unwrap();
        std::fs::write(input.path(), b"{\"message\":\"too-big\"}\n").unwrap();

        let err = run_stream(StreamConfig {
            input: input.path().to_path_buf(),
            quickwit: "http://127.0.0.1:7280".to_string(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 4,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("exceeds max_record_bytes"));
    }

    #[tokio::test]
    async fn oversized_records_can_go_to_dead_letter() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.ndjson");
        std::fs::write(&input_path, b"{\"message\":\"too-big\"}\n{\"ok\":true}\n").unwrap();
        let dlq_path = dir.path().join("oversized.dlq");

        run_stream(StreamConfig {
            input: input_path,
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: Some(dlq_path.clone()),
            bad_record_policy: BadRecordPolicy::Dlq,
            fail_on_bad_record: false,
            max_record_bytes: 12,
            oversized_record_policy: OversizedRecordPolicy::Dlq,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();

        let dlq = std::fs::read_to_string(dlq_path).unwrap();
        assert!(dlq.contains("exceeds max_record_bytes"));
        assert!(dlq.contains("too-big"));
    }

    #[tokio::test]
    async fn splits_batches_after_payload_too_large_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(413).set_body_string("payload too large"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let batch = Batch {
            body: Bytes::from_static(b"{\"a\":1}\n{\"b\":2}\n"),
            record_count: 2,
            byte_count: 16,
            start_offset: 0,
            end_offset: 16,
            spans: vec![
                crate::batcher::BatchRecordSpan {
                    body_start: 0,
                    body_end: 8,
                    start_offset: 0,
                    end_offset: 8,
                },
                crate::batcher::BatchRecordSpan {
                    body_start: 8,
                    body_end: 16,
                    start_offset: 8,
                    end_offset: 16,
                },
            ],
        };
        let client =
            QuickwitClient::new(server.uri(), "logs".to_string(), Compression::None).unwrap();

        let (_original, outcome) = ingest_with_payload_splitting(&client, batch).await.unwrap();

        assert_eq!(outcome.status, reqwest::StatusCode::OK);
        assert_eq!(outcome.attempts, 2);
    }

    #[tokio::test]
    async fn rejects_single_record_payload_too_large_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(413).set_body_string("payload too large"))
            .mount(&server)
            .await;

        let batch = Batch {
            body: Bytes::from_static(b"{\"a\":1}\n"),
            record_count: 1,
            byte_count: 8,
            start_offset: 0,
            end_offset: 8,
            spans: vec![crate::batcher::BatchRecordSpan {
                body_start: 0,
                body_end: 8,
                start_offset: 0,
                end_offset: 8,
            }],
        };
        let client =
            QuickwitClient::new(server.uri(), "logs".to_string(), Compression::None).unwrap();

        let err = ingest_with_payload_splitting(&client, batch)
            .await
            .unwrap_err();

        assert_eq!(err.kind, QuickwitErrorKind::PayloadTooLarge);
    }

    #[tokio::test]
    async fn stream_spools_failed_batch_and_replays_it() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.ndjson");
        let spool_dir = dir.path().join("spool");
        std::fs::write(&input_path, b"{\"message\":\"one\"}\n").unwrap();

        run_stream(StreamConfig {
            input: input_path.clone(),
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Quiet,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: Some(spool_dir.clone()),
            spool_limit_bytes: 1024 * 1024,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();

        assert!(spool_dir.join("batch-00000000000000000001.ndjson").exists());
        assert!(spool_dir.join("batch-00000000000000000001.json").exists());

        Mock::given(method("POST"))
            .and(path("/api/v1/logs/ingest"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        run_stream(StreamConfig {
            input: input_path,
            quickwit: server.uri(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Quiet,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: Some(spool_dir.clone()),
            spool_limit_bytes: 1024 * 1024,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();

        assert!(!spool_dir.join("batch-00000000000000000001.ndjson").exists());
        assert!(!spool_dir.join("batch-00000000000000000001.json").exists());
    }

    #[tokio::test]
    async fn dry_run_formats_batches_without_quickwit_requests() {
        let dir = tempfile::tempdir().unwrap();
        let input_path = dir.path().join("input.ndjson");
        let metrics_path = dir.path().join("metrics.json");
        std::fs::write(&input_path, b"{\"message\":\"one\"}\n").unwrap();

        run_stream(StreamConfig {
            input: input_path,
            quickwit: "http://127.0.0.1:1".to_string(),
            index: "logs".to_string(),
            memory_limit_bytes: 64 * 1024 * 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Quiet,
            metrics_file: Some(metrics_path.clone()),
            dry_run: true,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap();

        let metrics = std::fs::read_to_string(metrics_path).unwrap();
        assert!(metrics.contains("records_sent"));
    }

    #[tokio::test]
    async fn rejects_too_small_memory_limit() {
        let input = NamedTempFile::new().unwrap();
        let err = run_stream(StreamConfig {
            input: input.path().to_path_buf(),
            quickwit: "http://127.0.0.1:7280".to_string(),
            index: "logs".to_string(),
            memory_limit_bytes: 1024,
            adaptive: true,
            resume: false,
            input_format: InputFormat::Ndjson,
            csv_has_headers: true,
            dead_letter: None,
            bad_record_policy: BadRecordPolicy::Fail,
            fail_on_bad_record: false,
            max_record_bytes: 10 * 1024 * 1024,
            oversized_record_policy: OversizedRecordPolicy::Fail,
            trust_input: false,
            csv_infer_types: false,
            timestamp_field: None,
            timestamp_format: "auto".to_string(),
            rename_fields: Vec::new(),
            drop_fields: Vec::new(),
            adaptive_workers: false,
            progress: ProgressMode::Plain,
            metrics_file: None,
            dry_run: false,
            keep_checkpoint: false,
            metrics_listen: None,
            spool_dir: None,
            spool_limit_bytes: 0,
            compression: Compression::None,
            ingest_workers: 1,
        })
        .await
        .unwrap_err()
        .to_string();

        assert!(err.contains("memory limit"));
    }
}
