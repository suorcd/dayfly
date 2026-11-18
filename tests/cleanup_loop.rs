//! Tests for the periodic cleanup loop inside `serve`.
//!
//! The tests start the one fake S3 service in `tests/common`, seed
//! expired and fresh objects, then drive the real loop in process
//! against a store built from the fake service.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use common::FakeS3;
use dayfly::cleanup_loop::spawn_cleanup_loop;
use dayfly::http::{app_router, AppState};
use dayfly::metrics::Metrics;
use dayfly::store::Store;
use tower::ServiceExt;

const EXPIRED_PASTE_KEY: &str = "pastes/2020-01-01/expired";
const EXPIRED_SEGMENT_KEY: &str = "pastes/2020-01-01/expired/00000000000000000000-expired00001";
const EXPIRED_ALIAS_KEY: &str = "words/abacus-abdomen-zoology";
const ORPHAN_ALIAS_KEY: &str = "words/abacus-abdomen-abdominal";
const FRESH_PASTE_KEY: &str = "pastes/2999-01-01/fresh";
const FRESH_SEGMENT_KEY: &str = "pastes/2999-01-01/fresh/00000000000000000000-fresh0000001";
const FRESH_ALIAS_KEY: &str = "words/abacus-abdomen-abide";

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
}

/// Build a store with metrics and a one-second loop interval.
async fn state_with_loop(fake: &FakeS3) -> (AppState, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::new());
    let mut config = fake.config();
    config.cleanup_interval_seconds = 1;
    let store = Store::from_config_with_metrics(config, Some(metrics.clone()))
        .await
        .expect("test S3 config must build without network access");
    (AppState::with_metrics(store, metrics.clone()), metrics)
}

/// Read one rendered metric value, or `None` when the line is absent.
fn metric_value(body: &str, name: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        let value = rest.split_whitespace().next()?;
        value.parse().ok()
    })
}

/// Render the metrics once.
fn rendered(metrics: &Metrics) -> String {
    String::from_utf8(metrics.render().expect("metrics must render").to_vec())
        .expect("metrics must be UTF-8")
}

/// Wait until `check` is true, or fail after `timeout`.
async fn wait_until(timeout: Duration, check: impl Fn() -> bool) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("condition did not become true within {timeout:?}");
}

/// List pastes through the app router, as a live request would.
async fn list_pastes(state: &AppState) -> StatusCode {
    let app = app_router(state.clone());
    let mut request = Request::builder()
        .method("GET")
        .uri("/v1/pastes")
        .body(Body::empty())
        .expect("request must build");
    request
        .extensions_mut()
        .insert(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()));
    let response = app
        .oneshot(request)
        .await
        .expect("router call must complete");
    response.status()
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_loop_deletes_expired_objects_and_reports_metrics() {
    let fake = FakeS3::start().await;
    seed_cleanup_bucket(&fake);
    let (state, metrics) = state_with_loop(&fake).await;
    let task = spawn_cleanup_loop(
        state.store_handle(),
        Some(metrics.clone()),
        Duration::from_millis(50),
    );

    wait_until(Duration::from_secs(10), || {
        let body = rendered(&metrics);
        metric_value(&body, "dayfly_cleanup_runs_total").unwrap_or(0.0) >= 1.0
            && metric_value(&body, "dayfly_cleanup_pastes_deleted_total").unwrap_or(0.0) == 1.0
            && metric_value(&body, "dayfly_cleanup_aliases_deleted_total").unwrap_or(0.0) == 1.0
    })
    .await;

    for key in [
        EXPIRED_PASTE_KEY,
        EXPIRED_SEGMENT_KEY,
        EXPIRED_ALIAS_KEY,
        ORPHAN_ALIAS_KEY,
    ] {
        assert!(
            fake.object(key).is_none(),
            "the loop must delete the expired object {key}"
        );
    }
    for key in [FRESH_PASTE_KEY, FRESH_SEGMENT_KEY, FRESH_ALIAS_KEY] {
        assert!(
            fake.object(key).is_some(),
            "the loop must keep the fresh object {key}"
        );
    }
    let body = rendered(&metrics);
    assert!(
        metric_value(&body, "dayfly_cleanup_runs_total").unwrap_or(0.0) >= 1.0,
        "the loop must count its runs: {body}"
    );
    assert_eq!(
        metric_value(&body, "dayfly_cleanup_pastes_deleted_total"),
        Some(1.0),
        "the loop must count the deleted paste: {body}"
    );
    assert_eq!(
        metric_value(&body, "dayfly_cleanup_aliases_deleted_total"),
        Some(1.0),
        "the loop must count the deleted alias: {body}"
    );
    assert_eq!(
        metric_value(&body, "dayfly_cleanup_failures_total"),
        Some(0.0),
        "a clean pass must count no failures: {body}"
    );
    assert!(
        metric_value(&body, "dayfly_cleanup_last_success_timestamp_seconds").unwrap_or(0.0) > 0.0,
        "a clean pass must record a success timestamp: {body}"
    );
    assert_eq!(
        list_pastes(&state).await,
        StatusCode::OK,
        "serve must keep answering while the loop runs"
    );
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn cleanup_loop_survives_a_failed_pass_and_recovers() {
    let fake = FakeS3::start().await;
    let past = (Utc::now() - ChronoDuration::hours(2)).to_rfc3339();
    fake.seed(
        EXPIRED_PASTE_KEY,
        b"expired",
        "text/plain",
        &[("expires-at", past.as_str())],
    );
    fake.fail_list_prefix(
        "pastes/",
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
    );
    fake.fail_list_prefix("words/", StatusCode::INTERNAL_SERVER_ERROR, "InternalError");
    let (state, metrics) = state_with_loop(&fake).await;
    let task = spawn_cleanup_loop(
        state.store_handle(),
        Some(metrics.clone()),
        Duration::from_millis(50),
    );

    wait_until(Duration::from_secs(10), || {
        let body = rendered(&metrics);
        metric_value(&body, "dayfly_cleanup_runs_total").unwrap_or(0.0) >= 1.0
            && metric_value(&body, "dayfly_cleanup_failures_total").unwrap_or(0.0) >= 2.0
    })
    .await;
    assert!(
        fake.object(EXPIRED_PASTE_KEY).is_some(),
        "a failed pass must delete nothing"
    );
    let body = rendered(&metrics);
    assert!(
        metric_value(&body, "dayfly_cleanup_failures_total").unwrap_or(0.0) >= 2.0,
        "the failed pass must count the failed lists: {body}"
    );
    assert_eq!(
        metric_value(&body, "dayfly_cleanup_last_success_timestamp_seconds"),
        Some(0.0),
        "a failed pass must not record a success: {body}"
    );

    fake.clear_failures();
    wait_until(Duration::from_secs(10), || {
        let body = rendered(&metrics);
        fake.object(EXPIRED_PASTE_KEY).is_none()
            && metric_value(&body, "dayfly_cleanup_runs_total").unwrap_or(0.0) >= 2.0
            && metric_value(&body, "dayfly_cleanup_last_success_timestamp_seconds").unwrap_or(0.0)
                > 0.0
    })
    .await;
    let body = rendered(&metrics);
    assert!(
        metric_value(&body, "dayfly_cleanup_runs_total").unwrap_or(0.0) >= 2.0,
        "the loop must run again after the failure: {body}"
    );
    assert!(
        metric_value(&body, "dayfly_cleanup_last_success_timestamp_seconds").unwrap_or(0.0) > 0.0,
        "the recovery pass must record a success: {body}"
    );
    assert_eq!(
        list_pastes(&state).await,
        StatusCode::OK,
        "serve must keep answering after a failed pass"
    );
    task.abort();
}
