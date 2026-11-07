//! Creator address metadata and the exposure gate.

mod common;

use std::net::SocketAddr;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{header, Request, StatusCode};
use chrono::{Duration, Utc};
use common::{metadata_refs, stored_metadata, FakeS3};
use dayfly::http::AppState;
use dayfly::store::Store;
use tower::ServiceExt;

async fn app_for(fake: &FakeS3, expose: bool, trusted: &[&str]) -> axum::Router {
    let mut config = fake.config();
    config.expose_creator_addr = expose;
    config.trusted_proxies = trusted
        .iter()
        .map(|value| value.parse().expect("test CIDR must parse"))
        .collect();
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    dayfly::http::app_router(AppState::new(store))
}

fn upload_request(peer: SocketAddr, forwarded_for: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/upload")
        .header(header::CONTENT_TYPE, "text/plain")
        .header(header::ACCEPT, "application/json");
    if let Some(value) = forwarded_for {
        builder = builder.header("x-forwarded-for", value);
    }
    let mut request = builder
        .body(Body::from("hello"))
        .expect("request must build");
    request.extensions_mut().insert(ConnectInfo(peer));
    request
}

async fn create_paste(
    app: &axum::Router,
    peer: SocketAddr,
    forwarded_for: Option<&str>,
) -> (String, String) {
    let response = app
        .clone()
        .oneshot(upload_request(peer, forwarded_for))
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must collect");
    let json: serde_json::Value =
        serde_json::from_slice(&body).expect("upload response must be JSON");
    let id = json["id"].as_str().expect("id").to_string();
    let key = format!("pastes/{id}");
    (id, key)
}

fn object_created_by(fake: &FakeS3, key: &str) -> Option<String> {
    fake.object(key)
        .and_then(|object| object.metadata.get("created-by").cloned())
}

#[tokio::test]
async fn http_upload_records_the_ipv4_creator() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &[]).await;
    let (_, key) = create_paste(&app, "203.0.113.7:1234".parse().expect("peer"), None).await;
    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("203.0.113.7")
    );
}

#[tokio::test]
async fn http_upload_records_the_ipv6_creator() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &[]).await;
    let (_, key) = create_paste(&app, "[2001:db8::1]:1234".parse().expect("peer"), None).await;
    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("2001:db8::1")
    );
}

#[tokio::test]
async fn a_trusted_proxy_creator_is_the_forwarded_client() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &["10.0.0.0/8"]).await;
    let (_, key) = create_paste(
        &app,
        "10.0.0.5:1234".parse().expect("peer"),
        Some("198.51.100.9"),
    )
    .await;
    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("198.51.100.9")
    );
}

#[tokio::test]
async fn an_unlisted_peer_cannot_spoof_the_creator() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &["10.0.0.0/8"]).await;
    let (_, key) = create_paste(
        &app,
        "203.0.113.5:1234".parse().expect("peer"),
        Some("198.51.100.9"),
    )
    .await;
    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("203.0.113.5")
    );
}

#[tokio::test]
async fn an_append_keeps_the_base_creator() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &[]).await;
    let (id, key) = create_paste(&app, "203.0.113.7:1234".parse().expect("peer"), None).await;

    let request = Request::builder()
        .method("POST")
        .uri(format!("/v1/pastes/{id}/append"))
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("more"))
        .expect("request must build");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);

    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("203.0.113.7"),
        "the base object must keep the creator address"
    );
}

#[tokio::test]
async fn a_multipart_upload_records_the_creator() {
    let fake = FakeS3::start().await;
    let app = app_for(&fake, false, &[]).await;
    let body = vec![b'x'; 9 * 1024 * 1024];
    let length = body.len();
    let mut request = Request::builder()
        .method("POST")
        .uri("/upload")
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::ACCEPT, "application/json")
        .header("x-filename", "large.bin")
        .header(header::CONTENT_LENGTH, length)
        .body(Body::from(body))
        .expect("request must build");
    request.extensions_mut().insert(ConnectInfo(
        "203.0.113.7:1234".parse::<SocketAddr>().expect("peer"),
    ));
    let response = app
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must collect");
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("upload response must be JSON");
    let id = json["id"].as_str().expect("id");
    let key = format!("pastes/{id}");
    assert_eq!(
        object_created_by(&fake, &key).as_deref(),
        Some("203.0.113.7"),
        "the multipart base object must record the creator"
    );
}

fn seeded_paste(fake: &FakeS3) {
    let expires_at = (Utc::now() + Duration::days(1)).to_rfc3339();
    let mut metadata = stored_metadata(&expires_at, "text/plain", 5, "deadbeef");
    metadata.push(("created-by".to_string(), "203.0.113.7".to_string()));
    fake.seed(
        "pastes/2026-10-28/abc123",
        b"hello",
        "text/plain",
        &metadata_refs(&metadata),
    );
}

async fn meta_json(app: &axum::Router) -> serde_json::Value {
    let request = Request::builder()
        .method("GET")
        .uri("/v1/pastes/2026-10-28/abc123/meta")
        .body(Body::empty())
        .expect("request must build");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must collect");
    serde_json::from_slice(&bytes).expect("meta response must be JSON")
}

#[tokio::test]
async fn the_meta_route_hides_the_creator_by_default() {
    let fake = FakeS3::start().await;
    seeded_paste(&fake);
    let app = app_for(&fake, false, &[]).await;
    let json = meta_json(&app).await;
    assert!(
        json.get("created_by").is_none(),
        "created_by must be absent by default: {json}"
    );
}

#[tokio::test]
async fn the_meta_route_returns_the_creator_when_exposed() {
    let fake = FakeS3::start().await;
    seeded_paste(&fake);
    let app = app_for(&fake, true, &[]).await;
    let json = meta_json(&app).await;
    assert_eq!(json["created_by"], "203.0.113.7");
}

async fn head_created_by(app: &axum::Router) -> Option<String> {
    let request = Request::builder()
        .method("HEAD")
        .uri("/v1/pastes/2026-10-28/abc123")
        .body(Body::empty())
        .expect("request must build");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router call must complete");
    assert_eq!(response.status(), StatusCode::OK);
    response
        .headers()
        .get("x-created-by")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[tokio::test]
async fn the_head_route_hides_and_returns_the_creator() {
    let fake = FakeS3::start().await;
    seeded_paste(&fake);
    let hidden = app_for(&fake, false, &[]).await;
    assert_eq!(head_created_by(&hidden).await, None);
    let exposed = app_for(&fake, true, &[]).await;
    assert_eq!(
        head_created_by(&exposed).await.as_deref(),
        Some("203.0.113.7")
    );
}

#[tokio::test]
async fn the_service_info_reports_the_exposure_setting() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.expose_creator_addr = true;
    let store = Store::from_config(config)
        .await
        .expect("test S3 config must build without network access");
    let info = dayfly::http::service_info(&store);
    assert_eq!(info["expose_creator_addr"], true);
}
