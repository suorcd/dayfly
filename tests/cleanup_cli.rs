//! CLI tests for `dayfly cleanup`.
//!
//! The tests start the one fake S3 service in `tests/common`, seed objects
//! and incomplete uploads, then run the real binary against it.

mod common;

use std::process::{Command, Output, Stdio};

use chrono::{Duration as ChronoDuration, Utc};
use common::FakeS3;

const EXPIRED_PASTE_KEY: &str = "pastes/2020-01-01/expired";
const EXPIRED_SEGMENT_KEY: &str = "pastes/2020-01-01/expired/00000000000000000000-expired00001";
const EXPIRED_ALIAS_KEY: &str = "words/abacus-abdomen-zoology";
const ORPHAN_ALIAS_KEY: &str = "words/abacus-abdomen-abdominal";
const FRESH_PASTE_KEY: &str = "pastes/2999-01-01/fresh";
const FRESH_SEGMENT_KEY: &str = "pastes/2999-01-01/fresh/00000000000000000000-fresh0000001";
const FRESH_ALIAS_KEY: &str = "words/abacus-abdomen-abide";

fn cleanup_command(fake: &FakeS3) -> Command {
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
        .env("DAYFLY_MULTIPART_ABORT_AFTER", "3600")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn run_cleanup(fake: &FakeS3, dry_run: bool) -> Output {
    let mut command = cleanup_command(fake);
    command.arg("cleanup");
    if dry_run {
        command.arg("--dry-run");
    }
    command.output().expect("dayfly cleanup must run")
}

fn seed_cleanup_bucket(fake: &FakeS3) {
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
    let future = (Utc::now() + ChronoDuration::hours(2)).to_rfc3339();

    fake.seed(
        EXPIRED_PASTE_KEY,
        b"expired",
        "text/plain",
        &[
            ("expires-at", past.as_str()),
            ("words", "abacus-abdomen-zoology"),
        ],
    );
    fake.seed(EXPIRED_SEGMENT_KEY, b"segment", "text/plain", &[]);
    fake.seed(
        EXPIRED_ALIAS_KEY,
        EXPIRED_PASTE_KEY.as_bytes(),
        "text/plain",
        &[
            ("expires-at", past.as_str()),
            ("kind", "alias"),
            ("target", EXPIRED_PASTE_KEY),
        ],
    );
    fake.seed(
        ORPHAN_ALIAS_KEY,
        b"pastes/2020-01-01/missing",
        "text/plain",
        &[
            ("expires-at", past.as_str()),
            ("kind", "alias"),
            ("target", "pastes/2020-01-01/missing"),
        ],
    );
    fake.seed(
        FRESH_PASTE_KEY,
        b"fresh",
        "text/plain",
        &[
            ("expires-at", future.as_str()),
            ("words", "abacus-abdomen-abide"),
        ],
    );
    fake.seed(FRESH_SEGMENT_KEY, b"fresh segment", "text/plain", &[]);
    fake.seed(
        FRESH_ALIAS_KEY,
        FRESH_PASTE_KEY.as_bytes(),
        "text/plain",
        &[
            ("expires-at", future.as_str()),
            ("kind", "alias"),
            ("target", FRESH_PASTE_KEY),
        ],
    );

    fake.seed_multipart("pastes/2026-01-01/stale-upload", "upload-stale", &past);
    fake.seed_multipart(
        "pastes/2026-01-01/fresh-upload",
        "upload-fresh",
        &Utc::now().to_rfc3339(),
    );
}

fn assert_no_lifecycle_request(fake: &FakeS3) {
    assert!(
        fake.requests()
            .iter()
            .all(|request| !request.query.contains("lifecycle")),
        "cleanup must not need a lifecycle rule"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_dry_run_reports_without_changes() {
    let fake = FakeS3::start().await;
    seed_cleanup_bucket(&fake);

    let output = run_cleanup(&fake, true);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "a dry run must exit zero: {stderr}\n{stdout}"
    );
    assert!(
        stdout.contains(
            "cleanup: dry_run=true aborted=0 would_abort=1 pastes_deleted=0 \
             pastes_would_delete=1 aliases_deleted=0 aliases_would_delete=2 failed=0"
        ),
        "the dry-run counts must be machine-usable: {stdout}"
    );
    assert!(
        stdout.contains("upload_id=upload-stale"),
        "the stale upload ID must be named: {stdout}"
    );
    assert!(
        stdout.contains(EXPIRED_PASTE_KEY),
        "the expired paste key must be named: {stdout}"
    );

    assert_eq!(
        fake.upload_ids(),
        vec!["upload-fresh".to_string(), "upload-stale".to_string()],
        "a dry run must abort no upload"
    );
    assert!(
        fake.aborted_uploads().is_empty(),
        "a dry run must send no abort request"
    );
    for key in [
        EXPIRED_PASTE_KEY,
        EXPIRED_SEGMENT_KEY,
        EXPIRED_ALIAS_KEY,
        ORPHAN_ALIAS_KEY,
        FRESH_PASTE_KEY,
        FRESH_SEGMENT_KEY,
        FRESH_ALIAS_KEY,
    ] {
        assert!(fake.object(key).is_some(), "a dry run must keep {key}");
    }
    assert_no_lifecycle_request(&fake);
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_deletes_stale_uploads_and_expired_objects() {
    let fake = FakeS3::start().await;
    seed_cleanup_bucket(&fake);

    let output = run_cleanup(&fake, false);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "a live cleanup must exit zero: {stderr}\n{stdout}"
    );
    assert!(
        stdout.contains(
            "cleanup: dry_run=false aborted=1 would_abort=0 pastes_deleted=1 \
             pastes_would_delete=0 aliases_deleted=1 aliases_would_delete=0 failed=0"
        ),
        "the live counts must be machine-usable: {stdout}"
    );

    assert_eq!(
        fake.upload_ids(),
        vec!["upload-fresh".to_string()],
        "only the stale upload must be aborted"
    );
    assert_eq!(
        fake.aborted_uploads(),
        vec!["upload-stale".to_string()],
        "the stale upload must receive one abort request"
    );
    for key in [
        EXPIRED_PASTE_KEY,
        EXPIRED_SEGMENT_KEY,
        EXPIRED_ALIAS_KEY,
        ORPHAN_ALIAS_KEY,
    ] {
        assert!(
            fake.object(key).is_none(),
            "the expired object {key} must be deleted"
        );
    }
    for key in [FRESH_PASTE_KEY, FRESH_SEGMENT_KEY, FRESH_ALIAS_KEY] {
        assert!(
            fake.object(key).is_some(),
            "the fresh object {key} must stay"
        );
    }
    assert_no_lifecycle_request(&fake);
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_failure_names_the_item_and_exits_nonzero() {
    let fake = FakeS3::start().await;
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
    fake.seed_multipart("pastes/2026-01-01/bad-upload", "upload-bad", &past);
    fake.seed_multipart("pastes/2026-01-01/good-upload", "upload-good", &past);
    fake.fail_abort_always("upload-bad");
    let bad_paste = "pastes/2020-01-01/bad-paste";
    let good_paste = "pastes/2020-01-01/good-paste";
    fake.seed(
        bad_paste,
        b"bad",
        "text/plain",
        &[("expires-at", past.as_str())],
    );
    fake.seed(
        good_paste,
        b"good",
        "text/plain",
        &[("expires-at", past.as_str())],
    );
    fake.fail_delete_key(bad_paste);

    let output = run_cleanup(&fake, false);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "a failed item must make the command exit nonzero: {stderr}\n{stdout}"
    );
    assert!(
        stdout.contains("failed=2"),
        "the failure count must be machine-usable: {stdout}"
    );
    assert!(
        stdout.contains("upload_id=upload-bad"),
        "the failed upload ID must be named: {stdout}"
    );
    assert!(
        stdout.contains(bad_paste),
        "the failed paste key must be named: {stdout}"
    );
    assert!(
        stdout.contains("aborted=1") && stdout.contains("pastes_deleted=1"),
        "cleanup must continue after each failure: {stdout}"
    );
    assert_eq!(
        fake.upload_ids(),
        vec!["upload-bad".to_string()],
        "the good upload must be aborted and the bad upload must stay"
    );
    assert!(fake.object(bad_paste).is_some());
    assert!(fake.object(good_paste).is_none());
    assert!(
        stderr.contains("cleanup failed"),
        "the exit reason must be reported: {stderr}"
    );
}
