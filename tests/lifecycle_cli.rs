//! CLI tests for `dayfly lifecycle apply`.
//!
//! Garage returns `403 AccessDenied` for the lifecycle endpoints when the
//! access key is read/write instead of owner. These tests run the real
//! binary against the fake S3 service and cover that failure path plus the
//! merge and dry-run paths.

mod common;

use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use common::FakeS3;

const EXISTING_LIFECYCLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>keep-me</ID>
    <Filter><Prefix>logs/</Prefix></Filter>
    <Status>Enabled</Status>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>"#;

const DOCUMENT: &str = r#"{
  "Rules": [
    {
      "ID": "dayfly-expire-pastes",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "pastes/"
      },
      "Expiration": {
        "Days": 3651
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    }
  ]
}"#;

fn lifecycle_command(fake: &FakeS3) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dayfly"));
    for name in [
        "S3_ENDPOINT",
        "S3_BUCKET",
        "S3_REGION",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_ACCESS_KEY_ID_FILE",
        "S3_SECRET_ACCESS_KEY_FILE",
        "DAYFLY_MAX_TEXT_BYTES",
        "DAYFLY_MAX_FILE_BYTES",
        "DAYFLY_MAX_PASTE_BYTES",
        "DAYFLY_MULTIPART_ABORT_AFTER",
        "DAYFLY_SEARCH_MAX_BYTES",
        "DAYFLY_ALIAS_WORDS",
        "DAYFLY_DEFAULT_TTL_DAYS",
        "DAYFLY_MIN_TTL_DAYS",
        "DAYFLY_MAX_TTL_DAYS",
        "DAYFLY_TTL_CHOICES",
        "DAYFLY_LIST_DEFAULT_DAYS",
        "DAYFLY_RETENTION_BY_SIZE",
        "DAYFLY_RETENTION_MIN_DAYS",
        "DAYFLY_RETENTION_MAX_DAYS",
        "DAYFLY_HTTP_BIND",
        "DAYFLY_LOG_FORMAT",
        "DAYFLY_METRICS_BIND",
        "DAYFLY_METRICS_ALLOWED_NETWORKS",
        "DAYFLY_TRUSTED_PROXIES",
        "DAYFLY_LIST_ENABLED",
        "DAYFLY_LIST_REQUIRES_TOKEN",
        "DAYFLY_WRITE_REQUIRES_TOKEN",
        "DAYFLY_PASTE_TOKENS",
        "DAYFLY_EXPOSE_CREATOR_ADDR",
        "DAYFLY_RATE_LIMIT_PER_MINUTE",
        "DAYFLY_SHOW_WORDS",
        "DAYFLY_SHOW_INTERNAL_PATHS",
        "DAYFLY_INLINE_PDF",
        "DAYFLY_VIEW_LINE_NUMBERS",
        "DAYFLY_HTTP_BEARER_TOKEN",
        "DAYFLY_HTTP_BEARER_TOKEN_FILE",
        "DAYFLY_MCP_LOCAL_ROOT",
        "DAYFLY_FAVICON_PATH",
        "DAYFLY_ROBOTS_TXT_PATH",
        "DAYFLY_SECURITY_TXT_PATH",
        "DAYFLY_SITE_NAME",
        "DAYFLY_CONTACT",
        "DAYFLY_AUP_URL",
        "DAYFLY_CONFIG",
        "RUST_LOG",
        "HOME",
        "XDG_CONFIG_HOME",
    ] {
        command.env_remove(name);
    }
    command
        .env("S3_ENDPOINT", &fake.endpoint)
        .env("S3_BUCKET", &fake.bucket)
        .env("S3_REGION", "us-east-1")
        .env("S3_ACCESS_KEY_ID", "test-access-key")
        .env("S3_SECRET_ACCESS_KEY", "test-secret-key")
        .env("RUST_LOG", "dayfly=warn")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn write_document(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "dayfly-lifecycle-{}-{}-{name}.json",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::write(&path, DOCUMENT).expect("the lifecycle document must write");
    path
}

fn run_lifecycle(fake: &FakeS3, file: &PathBuf, dry_run: bool) -> Output {
    let mut command = lifecycle_command(fake);
    command
        .arg("lifecycle")
        .arg("apply")
        .arg("--file")
        .arg(file);
    if dry_run {
        command.arg("--dry-run");
    }
    command.output().expect("dayfly lifecycle apply must run")
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_apply_reports_get_access_denied() {
    let fake = FakeS3::start().await;
    fake.fail_get_lifecycle();
    let file = write_document("denied");

    let output = run_lifecycle(&fake, &file, false);

    assert!(
        !output.status.success(),
        "a denied lifecycle read must exit nonzero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("an object store request failed"),
        "the CLI must report the failed request: {stderr}"
    );
    assert!(
        stderr.contains("AccessDenied"),
        "the log must retain the Garage error code: {stderr}"
    );
    assert!(
        fake.lifecycle_puts().is_empty(),
        "a denied read must not write a lifecycle configuration"
    );
    let requests = fake.requests();
    assert!(
        requests
            .iter()
            .any(|request| request.method == "GET" && request.query.contains("lifecycle")),
        "the CLI must read the existing rules"
    );
    assert!(
        !requests
            .iter()
            .any(|request| request.method == "PUT" && request.query.contains("lifecycle")),
        "the CLI must not write after a denied read"
    );

    std::fs::remove_file(&file).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_apply_merges_existing_rules_and_writes() {
    let fake = FakeS3::start().await;
    fake.set_lifecycle_xml(EXISTING_LIFECYCLE);
    let file = write_document("merge");

    let output = run_lifecycle(&fake, &file, false);

    assert!(
        output.status.success(),
        "lifecycle apply must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dayfly-expire-pastes"),
        "the plan must name the new rule: {stdout}"
    );
    assert!(
        stdout.contains("applied 2 rule(s) to bucket test-bucket (1 kept)"),
        "the merge must keep the unnamed rule: {stdout}"
    );
    let puts = fake.lifecycle_puts();
    assert_eq!(puts.len(), 1, "one lifecycle write must happen");
    let xml = &puts[0];
    assert!(xml.contains("<ID>keep-me</ID>"), "the kept rule: {xml}");
    assert!(
        xml.contains("<ID>dayfly-expire-pastes</ID>"),
        "the new rule: {xml}"
    );
    assert!(
        xml.contains("<Days>3651</Days>"),
        "the new expiration: {xml}"
    );

    std::fs::remove_file(&file).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_apply_dry_run_does_not_write() {
    let fake = FakeS3::start().await;
    fake.set_lifecycle_xml(EXISTING_LIFECYCLE);
    let file = write_document("dry-run");

    let output = run_lifecycle(&fake, &file, true);

    assert!(
        output.status.success(),
        "the dry run must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dry run: bucket test-bucket would hold 2 rule(s), 1 kept"),
        "the dry run must print the plan: {stdout}"
    );
    assert!(
        fake.lifecycle_puts().is_empty(),
        "the dry run must not write a lifecycle configuration"
    );

    std::fs::remove_file(&file).ok();
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_apply_creates_a_rule_when_none_exists() {
    let fake = FakeS3::start().await;
    let file = write_document("create");

    let output = run_lifecycle(&fake, &file, false);

    assert!(
        output.status.success(),
        "lifecycle apply must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("applied 1 rule(s) to bucket test-bucket (0 kept)"),
        "a missing rule set must be treated as empty: {stdout}"
    );
    assert_eq!(
        fake.lifecycle_puts().len(),
        1,
        "one lifecycle write must happen"
    );

    std::fs::remove_file(&file).ok();
}
