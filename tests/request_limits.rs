//! Request and S3 deadlines and in-flight caps (issue #256).
//!
//! The stalled-S3 test uses the in-process fake S3 harness. The fake
//! holds one HeadObject open for 30 seconds. The store must fail the
//! call within its configured S3 operation deadline instead.

use std::time::{Duration, Instant};

use dayfly::store::Store;

mod common;

use common::{metadata_refs, stored_metadata, FakeS3};

/// A stalled S3 endpoint fails within the configured operation deadline.
///
/// The fake holds HeadObject for 30 seconds. The operation deadline is
/// 1 second, so the call must fail in well under 30 seconds and must
/// release the request task instead of holding it for the full delay.
#[tokio::test]
async fn a_stalled_s3_endpoint_fails_within_the_operation_deadline() {
    let fake = FakeS3::start().await;
    let key = "pastes/2026-11-19/abcdefghijklmnopqrstuv";
    let expires_at = (chrono::Utc::now() + chrono::Duration::days(7)).to_rfc3339();
    let metadata = stored_metadata(&expires_at, "text/plain", 5, "abc");
    fake.seed(key, b"hello", "text/plain", &metadata_refs(&metadata));
    fake.delay_head(key, Duration::from_secs(30));
    let mut config = fake.config();
    config.s3_connect_timeout_seconds = 5;
    config.s3_read_timeout_seconds = 5;
    config.s3_operation_timeout_seconds = 1;
    let store = Store::from_config(config)
        .await
        .expect("fake S3 config must build without network access");
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(25),
        store.head_metadata("2026-11-19/abcdefghijklmnopqrstuv"),
    )
    .await
    .expect("a stalled S3 call must answer within 25 seconds");
    assert!(result.is_err(), "a stalled HeadObject must fail");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "a 1 second operation deadline must fail fast, took {:?}",
        started.elapsed()
    );
}
