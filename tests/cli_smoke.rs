use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn help_lists_subcommands() {
    let mut cmd = Command::cargo_bin("qwhyper").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(contains("preflight"))
        .stdout(contains("stream"));
}

#[test]
fn invalid_size_fails_before_streaming() {
    let mut cmd = Command::cargo_bin("qwhyper").unwrap();
    cmd.args([
        "stream",
        "--input",
        "missing.ndjson",
        "--quickwit",
        "http://127.0.0.1:7280",
        "--index",
        "logs",
        "--memory-limit",
        "12xb",
    ])
    .assert()
    .failure()
    .stderr(contains("unsupported size unit"));
}

#[test]
fn stream_accepts_new_format_and_performance_flags() {
    let mut cmd = Command::cargo_bin("qwhyper").unwrap();
    cmd.args([
        "stream",
        "--input",
        "missing.ndjson",
        "--quickwit",
        "http://127.0.0.1:7280",
        "--index",
        "logs",
        "--input-format",
        "plain",
        "--compression",
        "auto",
        "--adaptive-workers",
        "--ingest-workers",
        "2",
        "--csv-has-headers",
        "false",
        "--dead-letter",
        "bad.dlq",
        "--bad-record-policy",
        "dlq",
        "--max-record-bytes",
        "10mb",
        "--oversized-record-policy",
        "dlq",
        "--trust-input",
        "--csv-infer-types",
        "--timestamp-field",
        "ts",
        "--rename-field",
        "old=new",
        "--drop-field",
        "secret",
        "--progress",
        "json",
        "--metrics-file",
        "metrics.json",
        "--dry-run",
        "--keep-checkpoint",
        "--metrics-listen",
        "127.0.0.1:9099",
        "--spool-dir",
        "spool",
        "--spool-limit",
        "1gb",
    ])
    .assert()
    .failure()
    .stderr(contains("failed to inspect input"));
}

#[test]
fn plan_command_prints_execution_plan() {
    let mut cmd = Command::cargo_bin("qwhyper").unwrap();
    cmd.args([
        "plan",
        "--input",
        "logs.ndjson",
        "--quickwit",
        "http://localhost:7280",
        "--index",
        "logs",
    ])
    .assert()
    .success()
    .stdout(contains("Memory budget"))
    .stdout(contains("Recommended workers"));
}
