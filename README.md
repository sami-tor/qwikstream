# QwikStream

High-throughput, low-memory streaming ingest for Quickwit.

QwikStream is a Rust CLI for moving very large local files into Quickwit safely. It streams line by line, wraps plain text or CSV into JSON, keeps memory bounded, respects backpressure, retries transient Quickwit failures, supports checkpoint resume, and exposes metrics for production runs.

## Why It Exists

Quickwit can ingest huge datasets, but real-world files are often messy: plain text, CSV, inconsistent NDJSON, oversized records, slow disks, Docker limits, network pressure, or temporary Quickwit failures. QwikStream adds a safe streaming layer in front of Quickwit so large ingest jobs can run with predictable RAM use and clear performance feedback.

## Features

- Streams huge files without loading them into memory
- Supports `ndjson`, `jsonl`, `plain`, and `csv` input
- Wraps plain text as JSON records
- Wraps CSV rows as JSON objects or field arrays
- Bounded-memory batching and backpressure
- Concurrent ingest workers with ordered checkpoints
- Retry handling for `429` and `5xx` Quickwit responses
- Payload-too-large recovery by splitting batches
- Bad-record policies: `fail`, `skip`, `dlq`
- Dead-letter output for invalid or oversized records
- Optional gzip request compression
- Checkpoint resume with input fingerprint safety
- Spill-to-disk spool with metadata and disk limit enforcement
- Adaptive worker scaling up and down from Quickwit pressure
- Prometheus-compatible metrics endpoint
- JSON progress and final metrics files
- Dry-run mode for full pipeline validation without ingesting
- Preflight performance report with RAM, disk speed, line stats, Quickwit reachability, and estimated ingest settings
- Windows, Linux, macOS, Docker, and GitHub release workflow support

## Build

```bash
cargo build --release
```

On this machine, if `cargo` is not on PATH:

```bash
"/c/Users/Hp/.cargo/bin/cargo.exe" build --release
```

The release binary is created at:

```bash
target/release/qwhyper
```

On Windows:

```bash
target/release/qwhyper.exe
```

## Preflight

Run preflight before a large ingest:

```bash
qwhyper preflight \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --sample-size 256mb
```

Run an optional tiny Quickwit ingest benchmark when it is safe to write a synthetic record:

```bash
qwhyper preflight \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --quickwit-benchmark \
  --benchmark-preset nvme \
  --sample-size 256mb
```

The report includes available RAM, CPU cores, input size, sampled disk read speed, line split speed, Quickwit reachability latency, index availability, sampled line count, average line size, p95 line size, max sampled line size, estimated stream speed, recommended memory, recommended workers, and estimated input time.

## Stream NDJSON

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --input-format ndjson \
  --memory-limit 512mb \
  --resume true
```

## Stream Plain Text

Each line becomes:

```json
{"message":"raw line text"}
```

Command:

```bash
qwhyper stream \
  --input app.log \
  --quickwit http://localhost:7280 \
  --index logs \
  --input-format plain
```

For a local Quickwit benchmark, start with:

```bash
qwhyper stream \
  --input app.log \
  --quickwit http://localhost:7280 \
  --index logs \
  --input-format plain \
  --memory-limit 512mb \
  --ingest-workers 8 \
  --adaptive-workers \
  --compression none \
  --progress json \
  --metrics-file metrics.json
```

## Stream CSV

With headers:

```bash
qwhyper stream \
  --input users.csv \
  --quickwit http://localhost:7280 \
  --index users \
  --input-format csv \
  --csv-has-headers true
```

Without headers, rows are wrapped as:

```json
{"fields":["value1","value2"]}
```

Enable simple CSV type inference:

```bash
qwhyper stream \
  --input users.csv \
  --quickwit http://localhost:7280 \
  --index users \
  --input-format csv \
  --csv-infer-types
```

## Dead Letter Records

Continue on bad records and write them to a DLQ file:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --dead-letter bad-records.dlq \
  --bad-record-policy dlq
```

Fail immediately on the first bad record:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --fail-on-bad-record true
```

## Compression

Use automatic compression selection:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --compression auto
```

Use gzip when network bandwidth is the bottleneck:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --compression gzip
```

Use no compression for local Quickwit or CPU-limited machines:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --compression none
```

## Concurrent Ingest

Use multiple ingest workers to hide network latency:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --ingest-workers 8
```

Checkpoints advance only after all earlier batches complete, preserving safe resume behavior with concurrent workers.

## Spool And Backpressure Safety

Use durable spool storage when Quickwit is temporarily down or too slow:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --spool-dir ./qwikstream-spool \
  --spool-limit 10gb
```

Spooled batches are written with metadata so replay preserves offsets, record counts, byte counts, and record spans. `--spool-limit` prevents unbounded disk growth.

## Observability

Print progress as JSON lines and write final metrics:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --progress json \
  --metrics-file metrics.json
```

Expose Prometheus-compatible metrics while the stream runs:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --metrics-listen 127.0.0.1:9099
```

Metrics include bytes sent, records sent, batches sent, retries, and current bytes-per-second.

## Dry Run

Validate, transform, batch, and count records without sending anything to Quickwit:

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --dry-run \
  --progress quiet \
  --metrics-file dryrun-metrics.json
```

Dry run is useful for testing parser speed, formatting cost, record counts, and memory behavior before a real ingest.

## Checkpoint Safety

Resume checkpoints include input fingerprints and ingest settings so unsafe resumes are ignored when the file, index, format, or compression changes.

```bash
qwhyper stream \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs \
  --resume true \
  --keep-checkpoint
```

By default, completed checkpoint files are removed after a successful run. Use `--keep-checkpoint` to preserve a completed checkpoint marker.

## Plan Command

Preview runtime choices before starting a job:

```bash
qwhyper plan \
  --input logs.ndjson \
  --quickwit http://localhost:7280 \
  --index logs
```

## Benchmark Notes

On a local NVMe laptop test with Quickwit running in Docker:

- Input file: `2.36 GB` plain text
- Records: `36,998,117`
- Wrapped JSON sent: `3.01 GB`
- Dry-run transform speed: about `199 MB/s`
- Real sustained Quickwit ingest: about `12.63 MB/s`
- Retries: `0`
- Spooled failed batches: `0`

The dry-run result shows QwikStream can feed data much faster than that Quickwit deployment sustained. Real ingest speed depends heavily on Quickwit storage, Docker/WSL overhead, index schema, tokenizers, docstore compression, merge policy, heap size, and worker count.

For realistic benchmarks:

1. Run `preflight` first.
2. Test a file larger than RAM cache when possible.
3. Test `--ingest-workers 1`, `2`, `4`, `8`, `12`, and `16`.
4. Test `--compression none` and `--compression gzip` separately.
5. Watch Quickwit CPU, disk, memory, indexing throughput, and merge activity.
6. Use a dedicated benchmark index with the fields you actually need.

## Docker And Install

Build or run with Docker:

```bash
docker build -t qwikstream .
docker run --rm -v "$PWD:/data" qwikstream stream --input /data/logs.ndjson --quickwit http://host.docker.internal:7280 --index logs
```

Install from GitHub releases:

```bash
sh install.sh
```

On Windows PowerShell:

```powershell
.\install.ps1
```

Set `QWHYPER_REPO=owner/repo` when installing from your own release repository.

## Optional Feature Flags

The core binary stays portable and stable by default. Advanced deployments can build with feature flags:

```bash
cargo build --release --features prometheus-metrics
cargo build --release --features simd-json-validation
cargo build --release --features spill-to-disk
```

## Reliability Model

QwikStream provides at-least-once delivery with checkpoint resume. If a process crashes after Quickwit accepts a batch but before checkpoint persistence, some records may be replayed after resume. Downstream data should tolerate duplicates or use stable document IDs if exactly-once semantics are required.

## License

Apache-2.0
