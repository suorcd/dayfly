//! Store log redaction: default store logs carry hashes, not raw IDs or keys.
//!
//! The opt-in `DAYFLY_LOG_RAW_PASTE_IDS` restores raw values. Each test
//! triggers one store log line through the in-process fake S3 and reads
//! the captured log output.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use bytes::Bytes;
use chrono::{Duration, Utc};
use common::FakeS3;
use dayfly::store::{Store, StoreError, Ttl};
use futures::stream;

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

/// The random part of a stored paste ID (`<date>/<random>`).
fn random_part(id: &str) -> &str {
    id.split('/')
        .nth(1)
        .expect("stored id must carry a random part")
}

#[tokio::test(flavor = "current_thread")]
async fn delete_failures_log_the_key_hash_not_the_key() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"delete me", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed against the fake");
    fake.fail_delete_prefix("pastes/");
    let (captured, _guard) = capture_logs();
    let _ = store.delete(&stored.id).await;
    let logs = captured.text();
    assert!(
        logs.contains("cannot delete object"),
        "the failed delete must be logged: {logs}"
    );
    assert!(
        !logs.contains(&stored.key),
        "the raw key must not appear in default logs: {logs}"
    );
    assert!(
        !logs.contains(random_part(&stored.id)),
        "the raw random ID must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn delete_failures_log_the_raw_key_when_the_opt_in_is_on() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.log_raw_paste_ids = true;
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    let stored = store
        .store_text(b"delete me", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed against the fake");
    fake.fail_delete_prefix("pastes/");
    let (captured, _guard) = capture_logs();
    let _ = store.delete(&stored.id).await;
    let logs = captured.text();
    assert!(
        logs.contains(&format!("key={}", stored.key)),
        "the raw key must be logged when the opt-in is on: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn quota_warning_logs_the_key_hash_not_the_key() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_put_with(
        "pastes/",
        StatusCode::FORBIDDEN,
        "QuotaExceeded",
        "the bucket is full",
    );
    let (captured, _guard) = capture_logs();
    let error = store
        .store_text(b"quota", None, None, Some(Ttl::days(1)))
        .await
        .expect_err("a quota failure must fail the upload");
    assert!(
        matches!(error, StoreError::QuotaExceeded),
        "the upload must fail with QuotaExceeded: {error:?}"
    );
    let logs = captured.text();
    assert!(
        logs.contains("bucket storage quota is reached"),
        "the quota warning must be logged: {logs}"
    );
    assert!(
        !logs.contains("bucket_object=pastes/"),
        "the raw key must not appear in the quota warning: {logs}"
    );
    assert!(
        logs.contains("bucket_object=sha256:"),
        "the quota warning must still name the object as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn quota_warning_logs_the_raw_key_when_the_opt_in_is_on() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.log_raw_paste_ids = true;
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    fake.fail_put_with(
        "pastes/",
        StatusCode::FORBIDDEN,
        "QuotaExceeded",
        "the bucket is full",
    );
    let (captured, _guard) = capture_logs();
    let _ = store
        .store_text(b"quota", None, None, Some(Ttl::days(1)))
        .await;
    let logs = captured.text();
    assert!(
        logs.contains("bucket_object=pastes/"),
        "the raw key must be logged when the opt-in is on: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stale_multipart_abort_failure_logs_the_key_hash_not_the_key() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let past = (Utc::now() - Duration::hours(2)).to_rfc3339();
    fake.seed_multipart("pastes/2026-01-01/bad", "upload-bad", &past);
    fake.fail_abort_always("upload-bad");
    let (captured, _guard) = capture_logs();
    let _ = store
        .abort_stale_multipart_uploads_with_report(std::time::Duration::from_secs(3_600), false)
        .await;
    let logs = captured.text();
    assert!(
        logs.contains("cannot abort a stale multipart upload"),
        "the failed abort must be logged: {logs}"
    );
    assert!(
        !logs.contains("pastes/2026-01-01/bad"),
        "the raw key must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn append_segment_write_failure_logs_the_key_hash_not_the_key() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed against the fake");
    fake.fail_put_prefix(&format!("{}/", stored.key));
    let (captured, _guard) = capture_logs();
    let _ = store.append_text(&stored.id, b"more").await;
    let logs = captured.text();
    assert!(
        logs.contains("append segment write failed"),
        "the failed segment write must be logged: {logs}"
    );
    assert!(
        !logs.contains(&stored.key),
        "the raw key must not appear in default logs: {logs}"
    );
    assert!(
        !logs.contains(random_part(&stored.id)),
        "the raw random ID must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the key must still be logged as a hash: {logs}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sdk_failure_debug_text_does_not_carry_the_key() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"delete me", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed against the fake");
    fake.fail_delete_key(&stored.key);
    let (captured, _guard) = capture_logs();
    let _ = store.delete(&stored.id).await;
    let logs = captured.text();
    let line = logs
        .lines()
        .find(|line| line.contains("an object store request failed"))
        .unwrap_or_else(|| panic!("no S3 failure line in: {logs}"));
    assert!(
        !line.contains(&stored.key),
        "the SDK debug text must not carry the raw key: {line}"
    );
    assert!(
        !line.contains(random_part(&stored.id)),
        "the SDK debug text must not carry the raw random ID: {line}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cleanup_alias_failure_logs_the_alias_hash_not_the_alias() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_delete_prefix("pastes/");
    fake.fail_delete_prefix("words/");
    let (captured, _guard) = capture_logs();
    let items: Vec<Result<Bytes, StoreError>> =
        vec![Ok(Bytes::from(common::pattern_bytes(6 * 1024 * 1024)))];
    let _ = store
        .store_stream(
            stream::iter(items),
            Some("large.bin"),
            None,
            None,
            None,
            5 * 1024 * 1024,
        )
        .await;
    let logs = captured.text();
    assert!(
        logs.contains("cannot delete the alias object of a failed store"),
        "the failed alias cleanup must be logged: {logs}"
    );
    assert!(
        !logs.contains("words/"),
        "the raw word alias must not appear in default logs: {logs}"
    );
    assert!(
        logs.contains("key=sha256:"),
        "the alias key must still be logged as a hash: {logs}"
    );
}
