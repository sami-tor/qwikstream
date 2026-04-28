use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};

use crate::format::{BadRecordPolicy, InputFormat, OversizedRecordPolicy};
use crate::ingest::Compression;
use crate::metrics::ProgressMode;
use crate::preflight::{run_preflight, BenchmarkPreset, PreflightConfig};
use crate::size::{format_bytes, parse_byte_size};
use crate::stream::{run_stream, StreamConfig};

#[derive(Debug, Parser)]
#[command(name = "qwhyper")]
#[command(about = "Low-memory schema-agnostic streamer for Quickwit")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Commands {
    Preflight {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quickwit: String,
        #[arg(long)]
        index: Option<String>,
        #[arg(long, default_value_t = false)]
        quickwit_benchmark: bool,
        #[arg(long, value_enum)]
        benchmark_preset: Option<BenchmarkPreset>,
        #[arg(long, default_value = "256mb")]
        sample_size: String,
    },
    Stream {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quickwit: String,
        #[arg(long)]
        index: String,
        #[arg(long, default_value = "512mb")]
        memory_limit: String,
        #[arg(long, default_value_t = true)]
        adaptive: bool,
        #[arg(long, default_value_t = false)]
        resume: bool,
        #[arg(long, value_enum, default_value_t = InputFormat::Ndjson)]
        input_format: InputFormat,
        #[arg(long, default_value_t = true, action = ArgAction::Set)]
        csv_has_headers: bool,
        #[arg(long)]
        dead_letter: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = BadRecordPolicy::Fail)]
        bad_record_policy: BadRecordPolicy,
        #[arg(long, default_value_t = false, hide = true)]
        fail_on_bad_record: bool,
        #[arg(long, default_value = "10mb")]
        max_record_bytes: String,
        #[arg(long, value_enum, default_value_t = OversizedRecordPolicy::Fail)]
        oversized_record_policy: OversizedRecordPolicy,
        #[arg(long, default_value_t = false)]
        trust_input: bool,
        #[arg(long, default_value_t = false)]
        csv_infer_types: bool,
        #[arg(long)]
        timestamp_field: Option<String>,
        #[arg(long, default_value = "auto")]
        timestamp_format: String,
        #[arg(long = "rename-field")]
        rename_fields: Vec<String>,
        #[arg(long = "drop-field")]
        drop_fields: Vec<String>,
        #[arg(long, value_enum, default_value_t = ProgressMode::Plain)]
        progress: ProgressMode,
        #[arg(long)]
        metrics_file: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        metrics_listen: Option<String>,
        #[arg(long)]
        spool_dir: Option<PathBuf>,
        #[arg(long, default_value = "1gb")]
        spool_limit: String,
        #[arg(long, default_value_t = false)]
        keep_checkpoint: bool,
        #[arg(long, default_value_t = false)]
        adaptive_workers: bool,
        #[arg(long, value_enum, default_value_t = Compression::None)]
        compression: Compression,
        #[arg(long, default_value_t = 1)]
        ingest_workers: usize,
    },
    Plan {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        quickwit: String,
        #[arg(long)]
        index: String,
        #[arg(long, default_value = "512mb")]
        memory_limit: String,
        #[arg(long, value_enum, default_value_t = InputFormat::Ndjson)]
        input_format: InputFormat,
        #[arg(long, value_enum, default_value_t = Compression::Auto)]
        compression: Compression,
    },
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Preflight {
            input,
            quickwit,
            index,
            quickwit_benchmark,
            benchmark_preset,
            sample_size,
        } => {
            let report = run_preflight(PreflightConfig {
                input,
                quickwit,
                index,
                quickwit_benchmark,
                benchmark_preset,
                sample_size_bytes: parse_byte_size(&sample_size)?,
            })
            .await?;
            println!("{report}");
        }
        Commands::Stream {
            input,
            quickwit,
            index,
            memory_limit,
            adaptive,
            resume,
            input_format,
            csv_has_headers,
            dead_letter,
            bad_record_policy,
            fail_on_bad_record,
            max_record_bytes,
            oversized_record_policy,
            trust_input,
            csv_infer_types,
            timestamp_field,
            timestamp_format,
            rename_fields,
            drop_fields,
            progress,
            metrics_file,
            dry_run,
            metrics_listen,
            spool_dir,
            spool_limit,
            keep_checkpoint,
            adaptive_workers,
            compression,
            ingest_workers,
        } => {
            run_stream(StreamConfig {
                input,
                quickwit,
                index,
                memory_limit_bytes: parse_byte_size(&memory_limit)?,
                adaptive,
                resume,
                input_format,
                csv_has_headers,
                dead_letter,
                bad_record_policy,
                fail_on_bad_record,
                max_record_bytes: parse_byte_size(&max_record_bytes)? as usize,
                oversized_record_policy,
                trust_input,
                csv_infer_types,
                timestamp_field,
                timestamp_format,
                rename_fields: parse_rename_fields(rename_fields)?,
                drop_fields,
                progress,
                metrics_file,
                dry_run,
                metrics_listen,
                spool_dir,
                spool_limit_bytes: parse_byte_size(&spool_limit)?,
                keep_checkpoint,
                adaptive_workers,
                compression,
                ingest_workers,
            })
            .await?;
        }
        Commands::Plan {
            input,
            quickwit,
            index,
            memory_limit,
            input_format,
            compression,
        } => {
            let memory_limit_bytes = parse_byte_size(&memory_limit)?;
            let resolved_compression = compression.resolve_for_quickwit(&quickwit);
            let checkpoint = crate::checkpoint::checkpoint_path(&input, &index);
            let recommended_workers =
                ((memory_limit_bytes / (128 * 1024 * 1024)) as usize).clamp(1, 8);
            println!("Input: {}", input.display());
            println!("Quickwit: {quickwit}");
            println!("Index: {index}");
            println!("Memory budget: {}", format_bytes(memory_limit_bytes));
            println!("Checkpoint path: {}", checkpoint.display());
            println!("Recommended workers: {recommended_workers}");
            println!("Compression: {:?}", resolved_compression);
            println!("Format: {:?}", input_format);
        }
    }
    Ok(())
}

fn parse_rename_fields(values: Vec<String>) -> Result<Vec<(String, String)>> {
    values
        .into_iter()
        .map(|value| {
            let (old, new) = value
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("rename-field must use old=new syntax"))?;
            if old.is_empty() || new.is_empty() {
                return Err(anyhow::anyhow!("rename-field must use old=new syntax"));
            }
            Ok((old.to_string(), new.to_string()))
        })
        .collect()
}
