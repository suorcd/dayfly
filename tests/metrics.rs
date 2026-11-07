//! Private Prometheus metrics listener.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{header, Request, StatusCode};
use common::FakeS3;
use dayfly::http::{metrics_router, AppState};
use dayfly::metrics::Metrics;
use dayfly::store::Store;
use tower::ServiceExt;

async fn state_with_metrics(allow: &[&str]) -> (AppState, FakeS3) {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.metrics_allowed_networks = allow
        .iter()
        .map(|value| value.parse().expect("test CIDR must parse"))
        .collect();
    let metrics = Arc::new(Metrics::new());
    let store = Store::from_config_with_metrics(config, Some(metrics.clone()))
        .await
        .expect("test S3 config must build without network access");
    (AppState::with_metrics(store, metrics), fake)
}

fn scrape(uri: &str, peer: SocketAddr) -> Request<Body> {
    let mut request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .expect("request must build");
    request.extensions_mut().insert(ConnectInfo(peer));
    request
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must collect");
    String::from_utf8(bytes.to_vec()).expect("metrics body must be UTF-8")
}

#[tokio::test]
async fn metrics_are_served_on_the_private_listener() {
    let (state, _fake) = state_with_metrics(&["10.0.0.0/8"]).await;
    let app = metrics_router(state);
    let response = app
        .oneshot(scrape("/metrics", "10.1.2.3:1234".parse().unwrap()))
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = body_text(response).await;
    assert!(
        body.contains("dayfly_build_info"),
        "the registry must expose the build info: {body}"
    );
}

#[tokio::test]
async fn metrics_refuse_a_source_outside_the_allowlist() {
    let (state, _fake) = state_with_metrics(&["10.0.0.0/8"]).await;
    let app = metrics_router(state);
    let response = app
        .oneshot(scrape("/metrics", "203.0.113.9:1234".parse().unwrap()))
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn the_api_listener_has_no_metrics_route() {
    let fake = FakeS3::start().await;
    let app = dayfly::http::app_router(AppState::new(
        Store::from_config(fake.config())
            .await
            .expect("test S3 config must build without network access"),
    ));
    let mut request = Request::builder()
        .method("GET")
        .uri("/metrics")
        .body(Body::empty())
        .expect("request must build");
    request
        .extensions_mut()
        .insert(ConnectInfo("127.0.0.1:1234".parse::<SocketAddr>().unwrap()));
    let response = app
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_request_is_counted_with_method_route_and_status() {
    let (state, _fake) = state_with_metrics(&["10.0.0.0/8"]).await;
    let api = dayfly::http::app_router(state.clone());
    let mut request = Request::builder()
        .method("GET")
        .uri("/v1/pastes")
        .body(Body::empty())
        .expect("request must build");
    request
        .extensions_mut()
        .insert(ConnectInfo("10.1.2.3:1234".parse::<SocketAddr>().unwrap()));
    let response = api
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);
    let _ = body_text(response).await;

    let metrics = metrics_router(state);
    let response = metrics
        .oneshot(scrape("/metrics", "10.1.2.3:1234".parse().unwrap()))
        .await
        .expect("router call must complete");
    let body = body_text(response).await;
    assert!(
        body.contains(
            "dayfly_http_requests_total{method=\"GET\",route=\"/v1/pastes\",status=\"200\"} 1"
        ),
        "the request must increment the counter: {body}"
    );
    assert!(
        body.contains(
            "dayfly_http_request_duration_seconds_count{method=\"GET\",route=\"/v1/pastes\"} 1"
        ),
        "the request must observe the latency histogram: {body}"
    );
}

#[tokio::test]
async fn an_s3_request_is_counted() {
    let (state, _fake) = state_with_metrics(&["10.0.0.0/8"]).await;
    let api = dayfly::http::app_router(state.clone());
    let mut request = Request::builder()
        .method("GET")
        .uri("/v1/pastes")
        .body(Body::empty())
        .expect("request must build");
    request
        .extensions_mut()
        .insert(ConnectInfo("10.1.2.3:1234".parse::<SocketAddr>().unwrap()));
    let response = api
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);
    let _ = body_text(response).await;

    let metrics = metrics_router(state);
    let response = metrics
        .oneshot(scrape("/metrics", "10.1.2.3:1234".parse().unwrap()))
        .await
        .expect("router call must complete");
    let body = body_text(response).await;
    let line = body
        .lines()
        .find(|line| {
            line.starts_with(
                "dayfly_s3_requests_total{operation=\"list_objects_v2\",outcome=\"ok\"}",
            )
        })
        .unwrap_or_else(|| panic!("no list_objects_v2 S3 counter in: {body}"));
    assert!(!line.ends_with(" 0"), "the S3 call must be counted: {line}");
}
