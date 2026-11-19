//! Access log behavior.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use common::FakeS3;
use dayfly::http::AppState;
use dayfly::store::Store;
use tower::ServiceExt;

async fn app_with_exposure(fake: &FakeS3, expose: bool) -> axum::Router {
    let mut config = fake.config();
    config.expose_creator_addr = expose;
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    dayfly::http::app_router(AppState::new(store))
}

/// Build an app with the log-redaction settings under test.
async fn app_with_log_options(
    fake: &FakeS3,
    log_raw: bool,
    bearer: Option<&str>,
    paste_tokens: bool,
) -> axum::Router {
    let mut config = fake.config();
    config.expose_creator_addr = false;
    config.log_raw_paste_ids = log_raw;
    if let Some(token) = bearer {
        config.http_bearer_token = Some(token.to_string());
    }
    config.paste_tokens = paste_tokens;
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    dayfly::http::app_router(AppState::new(store))
}

/// A paste ID that appears nowhere else, so its absence proves redaction.
const DISTINCT_ID: &str = "AbC123dEf456GhI789jKlM";

/// Run one request against `app` and return the status and the logs
/// captured in `buffer` so far.
async fn run_request(
    app: axum::Router,
    request: Request<Body>,
    buffer: &Buffer,
) -> (axum::http::StatusCode, String) {
    let response = app
        .oneshot(request)
        .await
        .expect("router call must complete");
    let status = response.status();
    let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must collect");
    let output = buffer.0.lock().expect("buffer lock").clone();
    (
        status,
        String::from_utf8(output).expect("log output must be UTF-8"),
    )
}

/// Build a current-thread runtime with log capture, run `run`, and
/// return its result.
fn with_log_capture<T>(run: impl AsyncFnOnce(&Buffer) -> T) -> T {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new("dayfly=info"))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    runtime.block_on(run(&buffer))
}

fn access_line(output: &str) -> &str {
    output
        .lines()
        .find(|line| line.contains("http request"))
        .unwrap_or_else(|| panic!("no access line in: {output}"))
}

/// A writer that keeps every log line in memory.
#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer lock").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
    type Writer = Buffer;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn a_successful_request_writes_one_access_line() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new("dayfly=info"))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let (status, output) = runtime.block_on(async {
        let fake = FakeS3::start().await;
        let app = app_with_exposure(&fake, true).await;
        let mut request = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .body(Body::empty())
            .expect("request must build");
        request
            .extensions_mut()
            .insert(ConnectInfo(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                1234,
            ))));
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        let status = response.status();
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body must collect");
        let output = buffer.0.lock().expect("buffer lock").clone();
        (
            status,
            String::from_utf8(output).expect("log output must be UTF-8"),
        )
    });

    assert_eq!(status, 200, "the list request must succeed");
    let access_lines: Vec<&str> = output
        .lines()
        .filter(|line| line.contains("method="))
        .collect();
    assert_eq!(
        access_lines.len(),
        1,
        "one request must write one access line, got: {output}"
    );
    let line = access_lines[0];
    for field in [
        "method=GET",
        "route=/v1/pastes",
        "status=200",
        "latency_ms=",
        "response_bytes=",
        "client=127.0.0.1",
        "request_id=",
    ] {
        assert!(line.contains(field), "missing {field} in: {line}");
    }
}

#[test]
fn a_json_access_line_has_stable_fields() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new("dayfly=info"))
        .with_ansi(false)
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let output = runtime.block_on(async {
        let fake = FakeS3::start().await;
        let app = app_with_exposure(&fake, true).await;
        let mut request = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .body(Body::empty())
            .expect("request must build");
        request
            .extensions_mut()
            .insert(ConnectInfo(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                1234,
            ))));
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body must collect");
        let output = buffer.0.lock().expect("buffer lock").clone();
        String::from_utf8(output).expect("log output must be UTF-8")
    });

    let line = output
        .lines()
        .find(|line| line.contains("\"method\""))
        .unwrap_or_else(|| panic!("no JSON access line in: {output}"));
    let value: serde_json::Value = serde_json::from_str(line).expect("access line must be JSON");
    assert_eq!(value["method"], "GET");
    assert_eq!(value["route"], "/v1/pastes");
    assert_eq!(value["status"], 200);
    assert!(value["latency_ms"].is_number(), "line: {line}");
    assert!(value["response_bytes"].is_number(), "line: {line}");
    assert!(value["request_id"].is_string(), "line: {line}");
    assert_eq!(value["client"], "127.0.0.1");
}

#[test]
fn the_access_log_omits_the_client_address_when_exposure_is_off() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buffer.clone())
        .with_env_filter(tracing_subscriber::EnvFilter::new("dayfly=info"))
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let output = runtime.block_on(async {
        let fake = FakeS3::start().await;
        let app = app_with_exposure(&fake, false).await;
        let mut request = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .body(Body::empty())
            .expect("request must build");
        request
            .extensions_mut()
            .insert(ConnectInfo(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                1234,
            ))));
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body must collect");
        let output = buffer.0.lock().expect("buffer lock").clone();
        String::from_utf8(output).expect("log output must be UTF-8")
    });

    assert!(
        output.lines().any(|line| line.contains("client=")),
        "the access line must still carry an empty client field: {output}"
    );
    assert!(
        !output.contains("127.0.0.1"),
        "the client address must not be logged when exposure is off: {output}"
    );
}

#[test]
fn the_access_log_redacts_the_paste_id_by_default() {
    let (status, output) = with_log_capture(async |buffer| {
        let fake = FakeS3::start().await;
        let app = app_with_log_options(&fake, false, None, false).await;
        let request = Request::builder()
            .method("GET")
            .uri(format!("/v1/pastes/{DISTINCT_ID}?probe=neverlogthis"))
            .body(Body::empty())
            .expect("request must build");
        run_request(app, request, buffer).await
    });

    assert_eq!(status, 404, "the missing paste must return 404");
    let line = access_line(&output);
    assert!(
        !line.contains(DISTINCT_ID),
        "the raw paste ID must not appear in the access line: {line}"
    );
    assert!(
        line.contains("paste_id=sha256:"),
        "the access line must carry the ID hash for correlation: {line}"
    );
    assert!(
        line.contains("request_id="),
        "the access line must keep the request ID: {line}"
    );
    assert!(
        !output.contains("neverlogthis"),
        "the query string must not appear in any log line: {output}"
    );
}

#[test]
fn the_access_log_restores_the_raw_paste_id_when_the_opt_in_is_on() {
    let (status, output) = with_log_capture(async |buffer| {
        let fake = FakeS3::start().await;
        let app = app_with_log_options(&fake, true, None, false).await;
        let request = Request::builder()
            .method("GET")
            .uri(format!("/v1/pastes/{DISTINCT_ID}"))
            .body(Body::empty())
            .expect("request must build");
        run_request(app, request, buffer).await
    });

    assert_eq!(status, 404, "the missing paste must return 404");
    let line = access_line(&output);
    assert!(
        line.contains(&format!("paste_id={DISTINCT_ID}")),
        "the opt-in must restore the raw paste ID: {line}"
    );
}

#[test]
fn the_auth_failure_log_redacts_the_paste_path_by_default() {
    let (status, output) = with_log_capture(async |buffer| {
        let fake = FakeS3::start().await;
        let app = app_with_log_options(&fake, false, Some("bearer-secret"), false).await;
        let request = Request::builder()
            .method("GET")
            .uri(format!("/v1/pastes/{DISTINCT_ID}"))
            .body(Body::empty())
            .expect("request must build");
        run_request(app, request, buffer).await
    });

    assert_eq!(status, 401, "the request without a token must return 401");
    assert!(
        output.contains("auth check failed"),
        "the auth failure must be logged: {output}"
    );
    assert!(
        !output.contains(DISTINCT_ID),
        "the raw paste ID must not appear in any log line: {output}"
    );
    assert!(
        !output.contains("bearer-secret"),
        "the bearer token must not appear in any log line: {output}"
    );
}

#[test]
fn the_paste_token_failure_log_redacts_the_paste_id_and_token_by_default() {
    let (status, output, paste_id) = with_log_capture(async |buffer| {
        let fake = FakeS3::start().await;
        let mut config = fake.config();
        config.paste_tokens = true;
        let store = Store::from_config(config)
            .await
            .expect("test S3 config must build without network access");
        let stored = store
            .store_text(b"token log target", None, None, None)
            .await
            .expect("the paste must store");
        let app = dayfly::http::app_router(AppState::new(store));
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/v1/pastes/{}", stored.id))
            .header("x-paste-token", "0".repeat(32))
            .body(Body::empty())
            .expect("request must build");
        let (status, output) = run_request(app, request, buffer).await;
        (status, output, stored.id)
    });

    assert_eq!(status, 401, "the wrong token must return 401");
    assert!(
        output.contains("paste token check failed"),
        "the token failure must be logged: {output}"
    );
    assert!(
        !output.contains(&paste_id),
        "the raw paste ID must not appear in any log line: {output}"
    );
    assert!(
        !output.contains(&"0".repeat(32)),
        "the offered paste token must not appear in any log line: {output}"
    );
    assert!(
        output.contains("paste_id=sha256:"),
        "the token failure line must carry the ID hash: {output}"
    );
}
