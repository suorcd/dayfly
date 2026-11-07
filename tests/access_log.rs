//! Access log behavior.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use common::FakeS3;
use dayfly::http::AppState;
use tower::ServiceExt;

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
        let app = dayfly::http::app_router(AppState::new(fake.store().await));
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
        let app = dayfly::http::app_router(AppState::new(fake.store().await));
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
