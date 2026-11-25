//! Cleanup log redaction: the cleanup report logs hashes, not raw IDs or keys.
//!
//! The tests build a cleanup report by hand or drive one real cleanup
//! pass through the in-process fake S3, then read the captured log
//! output. The opt-in `DAYFLY_LOG_RAW_PASTE_IDS` restores raw values.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use chrono::{Duration as ChronoDuration, Utc};
use common::FakeS3;
use dayfly::cleanup_loop::log_cleanup_report;
use dayfly::store::{
    CleanupFailure, CleanupOptions, CleanupReport, MultipartAbortFailure, MultipartAbortReport,
};

/// A log writer that keeps every line in memory.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("log lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl CapturedLog {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log lock").clone())
            .expect("log output must be UTF-8")
    }
}

/// Install a log subscriber that writes into a fresh buffer. The guard
/// must stay alive while the test runs.
fn capture_logs() -> (CapturedLog, tracing::subscriber::DefaultGuard) {
    let captured = CapturedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (captured, guard)
}

const EXPIRED_PASTE_KEY: &str = "pastes/2020-01-01/expired";
const EXPIRED_ALIAS_KEY: &str = "words/abacus-abdomen-zoology";
const ORPHAN_ALIAS_KEY: &str = "words/abacus-abdomen-abdominal";

/// Seed one expired paste and its expired alias.
fn seed_expired_paste(fake: &FakeS3) {
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
    fake.seed(
        EXPIRED_PASTE_KEY,
        b"expired",
        "text/plain",
        &[
            ("expires-at", past.as_str()),
            ("words", "abacus-abdomen-zoology"),
        ],
    );
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
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_report_multipart_failure_logs_the_key_hash() {
    let report = CleanupReport {
        multipart: MultipartAbortReport {
            failed: vec![MultipartAbortFailure {
                key: EXPIRED_PASTE_KEY.to_string(),
                upload_id: "upload-bad".to_string(),
                message: "injected failure".to_string(),
            }],
            ..MultipartAbortReport::default()
        },
        ..CleanupReport::default()
    };
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup multipart abort failed"),
        "the failed abort must be logged: {logs}"
    );
    assert!(
        !logs.contains(EXPIRED_PASTE_KEY),
        "the raw key must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_report_paste_failure_logs_the_key_hash() {
    let report = CleanupReport {
        pastes_failed: vec![CleanupFailure {
            key: EXPIRED_PASTE_KEY.to_string(),
            message: "injected failure".to_string(),
        }],
        ..CleanupReport::default()
    };
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup paste delete failed"),
        "the failed paste delete must be logged: {logs}"
    );
    assert!(
        !logs.contains(EXPIRED_PASTE_KEY),
        "the raw key must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_report_alias_failure_logs_the_key_hash() {
    let report = CleanupReport {
        aliases_failed: vec![CleanupFailure {
            key: EXPIRED_ALIAS_KEY.to_string(),
            message: "injected failure".to_string(),
        }],
        ..CleanupReport::default()
    };
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup alias delete failed"),
        "the failed alias delete must be logged: {logs}"
    );
    assert!(
        !logs.contains(EXPIRED_ALIAS_KEY),
        "the raw alias key must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_paste_list_failure_message_logs_the_error_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    seed_expired_paste(&fake);
    fake.fail_list_prefix("pastes/", StatusCode::NOT_FOUND, "NoSuchBucket");
    let options = CleanupOptions {
        dry_run: false,
        multipart_older_than: Duration::from_secs(3_600),
    };
    let report = store
        .cleanup(options)
        .await
        .expect("cleanup must run against the fake");
    assert!(
        !report.pastes_failed.is_empty(),
        "the list failure must be recorded"
    );
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup paste delete failed"),
        "the failed paste delete must be logged: {logs}"
    );
    assert!(
        !logs.contains("pastes/"),
        "the raw prefix must not appear in default logs: {logs}"
    );
}

/// Seed one expired alias whose target paste does not exist, so the
/// paste pass leaves it for the alias pass.
fn seed_orphan_alias(fake: &FakeS3) {
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
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
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_alias_delete_failure_message_logs_the_error_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    seed_orphan_alias(&fake);
    fake.fail_delete_with(
        "words/",
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "the bucket does not exist",
    );
    let options = CleanupOptions {
        dry_run: false,
        multipart_older_than: Duration::from_secs(3_600),
    };
    let report = store
        .cleanup(options)
        .await
        .expect("cleanup must run against the fake");
    assert!(
        !report.aliases_failed.is_empty(),
        "the delete failure must be recorded"
    );
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup alias delete failed"),
        "the failed alias delete must be logged: {logs}"
    );
    assert!(
        !logs.contains(ORPHAN_ALIAS_KEY),
        "the raw alias key must not appear in default logs: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_paste_delete_failure_message_logs_the_error_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    seed_expired_paste(&fake);
    fake.fail_delete_with(
        "pastes/",
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "the bucket does not exist",
    );
    let options = CleanupOptions {
        dry_run: false,
        multipart_older_than: Duration::from_secs(3_600),
    };
    let report = store
        .cleanup(options)
        .await
        .expect("cleanup must run against the fake");
    assert!(
        !report.pastes_failed.is_empty(),
        "the delete failure must be recorded"
    );
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup paste delete failed"),
        "the failed paste delete must be logged: {logs}"
    );
    assert!(
        !logs.contains(EXPIRED_PASTE_KEY),
        "the raw key must not appear in default logs: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_multipart_abort_failure_message_logs_the_error_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
    fake.seed_multipart("pastes/2026-01-01/bad", "upload-bad", &past);
    fake.fail_abort_with(
        "upload-bad",
        StatusCode::NOT_FOUND,
        "NoSuchBucket",
        "the bucket does not exist",
    );
    let options = CleanupOptions {
        dry_run: false,
        multipart_older_than: Duration::from_secs(3_600),
    };
    let report = store
        .cleanup(options)
        .await
        .expect("cleanup must run against the fake");
    assert!(
        !report.multipart.failed.is_empty(),
        "the abort failure must be recorded"
    );
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, false);
    let logs = captured.text();
    assert!(
        logs.contains("cleanup multipart abort failed"),
        "the failed abort must be logged: {logs}"
    );
    assert!(
        !logs.contains("pastes/2026-01-01/bad"),
        "the raw key must not appear in default logs: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_report_logs_the_raw_key_when_the_opt_in_is_on() {
    let report = CleanupReport {
        pastes_failed: vec![CleanupFailure {
            key: EXPIRED_PASTE_KEY.to_string(),
            message: "injected failure".to_string(),
        }],
        ..CleanupReport::default()
    };
    let (captured, _guard) = capture_logs();
    log_cleanup_report(&report, true);
    let logs = captured.text();
    assert!(
        logs.contains(&format!("key={EXPIRED_PASTE_KEY}")),
        "the raw key must be logged when the opt-in is on: {logs}"
    );
}
