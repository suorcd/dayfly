//! HTTP route integration tests, in process through the axum router.

mod common;

use axum::http::{header, HeaderMap, StatusCode};
use axum::Router;
use chrono::{DateTime, Duration, Utc};
use common::{FailOp, FakeS3};
use dayfly::http::{api_router, app_router, AppState, MULTIPART_METADATA_MAX_BYTES};
use dayfly::store::{normalize_words, Store, Ttl};
use serde_json::{json, Value};

fn id_from_plain_url(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .trim()
        .rsplit("/v1/pastes/")
        .next()
        .expect("plain response must contain a paste URL")
        .to_string()
}

fn expires_in_seconds(expires_at: &str) -> i64 {
    let expires = DateTime::parse_from_rfc3339(expires_at)
        .expect("expires_at must be RFC 3339")
        .with_timezone(&Utc);
    (expires - Utc::now()).num_seconds()
}

fn assert_expires_about(expires_at: &str, days: i64) {
    let seconds = expires_in_seconds(expires_at);
    let target = days * 86_400;
    assert!(
        (seconds - target).abs() < 120,
        "expiry should be about {days} days away, got {seconds} seconds"
    );
}

fn content_length(body_len: usize) -> (&'static str, String) {
    ("content-length", body_len.to_string())
}

fn multipart_headers(boundary: &str) -> Vec<(&'static str, String)> {
    vec![(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    )]
}

#[tokio::test]
async fn raw_text_upload_returns_plain_url_or_json() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = b"hello".to_vec();
    let request = common::request(
        "POST",
        "/upload",
        &[
            ("host", "dayfly.test".to_string()),
            ("x-forwarded-proto", "https".to_string()),
            ("content-type", "text/plain".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .starts_with("text/plain"));
    let text = String::from_utf8(body).expect("plain response must be UTF-8");
    let id = id_from_plain_url(text.as_bytes());
    assert!(
        text.starts_with(&format!("https://dayfly.test/v1/pastes/{id}")),
        "plain response must be the full URL: {text}"
    );
    assert!(fake.object(&format!("pastes/{id}")).is_some());

    let body = b"json body".to_vec();
    let request = common::request(
        "POST",
        "/v1/pastes?format=json",
        &[
            ("host", "dayfly.test".to_string()),
            ("content-type", "text/plain".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["size"], 9);
    assert_eq!(json["filename"], Value::Null);
    assert_eq!(json["detection_source"], "client");
    assert_eq!(
        json["url"],
        format!(
            "http://dayfly.test/v1/pastes/{}",
            json["id"].as_str().unwrap()
        )
    );
}

#[tokio::test]
async fn ttl_days_zero_is_rejected() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = b"header zero".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[("x-ttl-days", "0".to_string()), content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "X-TTL-Days: 0 must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );

    let body = b"query zero".to_vec();
    let request = common::request(
        "POST",
        "/upload?ttl_days=0&format=json",
        &[content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "ttl_days=0 must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );

    let boundary = "dayfly-boundary";
    let body = common::multipart_body(
        boundary,
        None,
        Some("text/plain"),
        b"form zero",
        &[("ttl_days", "0")],
    );
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    headers.push(("host", "dayfly.test".to_string()));
    let request = common::request("POST", "/upload?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a multipart ttl_days=0 must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn mixed_ttl_forms_are_rejected() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let mixed_headers = [("x-ttl", "1h".to_string()), ("x-ttl-days", "7".to_string())];
    let body = b"both headers".to_vec();
    let mut headers = vec![content_length(body.len())];
    headers.extend(mixed_headers.iter().cloned());
    let request = common::request("POST", "/upload?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "X-TTL plus X-TTL-Days must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );

    let body = b"both query fields".to_vec();
    let request = common::request(
        "POST",
        "/upload?ttl=1h&ttl_days=7&format=json",
        &[content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "ttl plus ttl_days must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );

    let body = b"mixed header and query".to_vec();
    let request = common::request(
        "POST",
        "/upload?ttl_days=7&format=json",
        &[("x-ttl", "1h".to_string()), content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a duration header plus a day query must be rejected, got body: {}",
        String::from_utf8_lossy(&body)
    );

    let boundary = "dayfly-boundary";
    for fields in [
        [("ttl", "1h"), ("ttl_days", "7")],
        [("ttl_days", "7"), ("ttl", "1h")],
    ] {
        let body =
            common::multipart_body(boundary, None, Some("text/plain"), b"mixed form", &fields);
        let mut headers = multipart_headers(boundary);
        headers.push(content_length(body.len()));
        headers.push(("host", "dayfly.test".to_string()));
        let request = common::request("POST", "/upload?format=json", &headers, body);
        let (status, _, body) = common::send(&app, request).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "multipart ttl plus ttl_days must be rejected in either order, got body: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

#[tokio::test]
async fn raw_upload_reads_filename_and_ttl_from_headers_and_query() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = b"header wins".to_vec();
    let request = common::request(
        "POST",
        "/upload?filename=query.txt&ttl_days=3&format=json",
        &[
            ("x-filename", "header.txt".to_string()),
            ("x-ttl-days", "1".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["filename"], "header.txt");
    assert_expires_about(json["expires_at"].as_str().unwrap(), 1);

    let body = b"query wins".to_vec();
    let request = common::request(
        "POST",
        "/upload?filename=query.txt&ttl_days=3&format=json",
        &[content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["filename"], "query.txt");
    assert_expires_about(json["expires_at"].as_str().unwrap(), 3);
}

#[tokio::test]
async fn raw_text_cap_rejects_without_url_and_filename_switches_to_file_cap() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(5, 100).await;
    let app = common::app(store);

    let body = b"123456".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("send X-Filename"), "got: {text}");
    assert!(!text.contains("/v1/pastes"), "no URL on failure: {text}");
    assert!(fake.keys().is_empty());

    let body = b"123456".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[
            ("x-filename", "big.txt".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(common::json_body(&body)["size"], 6);
}

#[tokio::test]
async fn multipart_upload_reads_form_fields_and_enforces_caps() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(5, 100).await;
    let app = common::app(store);
    let boundary = "dayfly-boundary";

    let body = common::multipart_body(
        boundary,
        None,
        Some("text/plain"),
        b"123456",
        &[("filename", "form.txt"), ("ttl_days", "2")],
    );
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    headers.push(("host", "dayfly.test".to_string()));
    let request = common::request("POST", "/upload?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["filename"], "form.txt");
    assert_expires_about(json["expires_at"].as_str().unwrap(), 2);

    let body = common::multipart_body(boundary, Some("own.bin"), None, b"bytes", &[]);
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    let request = common::request("POST", "/v1/pastes?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(common::json_body(&body)["filename"], "own.bin");

    let body = common::multipart_body(boundary, None, Some("text/plain"), b"123456", &[]);
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    let request = common::request("POST", "/upload?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a multipart file without a filename uses the text cap"
    );
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));

    let body = common::multipart_fields_only(boundary, &[("filename", "empty.txt")]);
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    let request = common::request("POST", "/upload", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("requires a 'file' field"));
}

#[tokio::test]
async fn multipart_metadata_fields_are_bounded() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "metadata-boundary";
    let oversized = "a".repeat(MULTIPART_METADATA_MAX_BYTES + 1);

    for field_name in ["filename", "ttl", "ttl_days"] {
        let body = common::multipart_body(
            boundary,
            Some("small.txt"),
            Some("text/plain"),
            b"body",
            &[(field_name, oversized.as_str())],
        );
        let mut headers = multipart_headers(boundary);
        headers.push(content_length(body.len()));
        headers.push(("host", "dayfly.test".to_string()));
        let request = common::request("POST", "/upload?format=json", &headers, body);
        let (status, _, body) = common::send(&app, request).await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "an oversized {field_name} field must be a 413, got: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            fake.keys().is_empty(),
            "an oversized {field_name} field must store nothing: {:?}",
            fake.keys()
        );
    }
}

#[tokio::test]
async fn malformed_multipart_body_returns_400_without_url() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);

    let request = common::request(
        "POST",
        "/upload?format=json",
        &[("content-type", "multipart/form-data".to_string())],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("invalid multipart body"), "got: {text}");
    assert!(!text.contains("/v1/pastes"), "no URL on failure: {text}");
    assert!(fake.requests().is_empty());

    let request = common::request(
        "POST",
        "/ui/upload",
        &[
            ("host", "localhost".to_string()),
            ("origin", "http://localhost".to_string()),
            ("content-type", "multipart/form-data".to_string()),
        ],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));
    assert!(fake.requests().is_empty());

    let broken = b"--xyz\r\nBroken header\r\n\r\nx\r\n--xyz--\r\n".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[
            (
                "content-type",
                "multipart/form-data; boundary=xyz".to_string(),
            ),
            ("content-length", broken.len().to_string()),
        ],
        broken.clone(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("invalid multipart body"), "got: {text}");
    assert!(!text.contains("/v1/pastes"));
    assert!(fake.requests().is_empty());

    let request = common::request(
        "POST",
        "/ui/upload",
        &[
            ("host", "localhost".to_string()),
            ("origin", "http://localhost".to_string()),
            (
                "content-type",
                "multipart/form-data; boundary=xyz".to_string(),
            ),
            ("content-length", broken.len().to_string()),
        ],
        broken,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(String::from_utf8_lossy(&body).contains("invalid multipart body"));
    assert!(fake.requests().is_empty());
}

#[tokio::test]
async fn bearer_auth_is_required_and_accepted_on_api_and_ui() {
    let fake = FakeS3::start().await;
    let config = common::config_with_token(fake.config(), "secret-token");
    let store = Store::from_config(config)
        .await
        .expect("authorized store must build");
    let app = common::app(store);

    let request = common::request("GET", "/v1/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(common::json_body(&body)["error"], "unauthorized");

    let request = common::request(
        "GET",
        "/v1/pastes",
        &[("authorization", "Bearer wrong-token".to_string())],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let request = common::request(
        "GET",
        "/v1/pastes",
        &[("authorization", "Bearer secret-token".to_string())],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);

    let body = b"auth body".to_vec();
    let requests_before = fake.requests().len();
    let request = common::request(
        "POST",
        "/upload",
        &[content_length(body.len())],
        body.clone(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        fake.requests().len(),
        requests_before,
        "an unauthorized upload must not reach S3"
    );

    let request = common::request(
        "POST",
        "/upload",
        &[
            ("authorization", "Bearer secret-token".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);

    let request = common::request("GET", "/", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        common::json_body(&body)["error"],
        "unauthorized",
        "the UI must use the shared JSON auth error"
    );

    let request = common::request(
        "GET",
        "/",
        &[("authorization", "Bearer secret-token".to_string())],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn download_head_and_range_responses_carry_the_right_headers() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"abcdefghij", None, None, None)
        .await
        .expect("text must store");
    let app = common::app(store);
    let uri = format!("/v1/pastes/{}", stored.id);

    let request = common::request("GET", &uri, &[], Vec::new());
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"abcdefghij");
    assert_eq!(headers[header::CONTENT_LENGTH], "10");
    assert_eq!(headers[header::CONTENT_TYPE], "text/plain; charset=utf-8");
    assert!(headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .starts_with("inline"));
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert_eq!(headers["x-content-type-options"], "nosniff");

    let request = common::request("HEAD", &uri, &[], Vec::new());
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty(), "HEAD must not return a body");
    assert_eq!(headers[header::CONTENT_LENGTH], "10");

    let request = common::request(
        "GET",
        &uri,
        &[("range", "bytes=2-4".to_string())],
        Vec::new(),
    );
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"cde");
    assert_eq!(headers[header::CONTENT_RANGE], "bytes 2-4/10");
    assert_eq!(headers[header::CONTENT_LENGTH], "3");

    let request = common::request(
        "GET",
        &uri,
        &[("range", "bytes=-3".to_string())],
        Vec::new(),
    );
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, b"hij");
    assert_eq!(headers[header::CONTENT_RANGE], "bytes 7-9/10");

    let request = common::request(
        "GET",
        &uri,
        &[("range", "bytes=20-".to_string())],
        Vec::new(),
    );
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(headers[header::CONTENT_RANGE], "bytes */10");
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));

    let request = common::request("GET", &uri, &[("range", "garbage".to_string())], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let bare = stored.id.split_once('/').unwrap().1;
    let request = common::request("GET", &format!("/v1/pastes/{bare}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"abcdefghij");
}

#[tokio::test]
async fn expired_and_missing_downloads_return_gone_and_not_found() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    let key = format!("pastes/{today}/expired");
    fake.seed(
        &key,
        b"abc",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    let app = common::app(store);

    for method in ["GET", "HEAD"] {
        let request = common::request(
            method,
            &format!("/v1/pastes/{today}/expired"),
            &[],
            Vec::new(),
        );
        let (status, _, _) = common::send(&app, request).await;
        assert_eq!(status, StatusCode::GONE, "{method} must report expiry");
    }

    let request = common::request(
        "GET",
        &format!("/v1/pastes/{today}/expired/meta"),
        &[],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::GONE);

    let request = common::request("GET", "/v1/pastes/2026-10-28/missing", &[], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metadata_and_delete_routes_work_for_dated_and_bare_ids() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"meta body", Some("meta.txt"), None, Some(Ttl::days(4)))
        .await
        .expect("text must store");
    let other = store
        .store_text(b"second body", None, None, None)
        .await
        .expect("second text must store");
    let app = common::app(store);
    let bare = stored.id.split_once('/').unwrap().1;
    let other_bare = other.id.split_once('/').unwrap().1;

    let request = common::request(
        "GET",
        &format!("/v1/pastes/{}/meta", stored.id),
        &[],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = common::json_body(&body);
    assert_eq!(json["id"], stored.id);
    assert_eq!(json["filename"], "meta.txt");
    assert_eq!(json["size"], 9);
    assert_eq!(json["expired"], false);
    assert!(json["sha256"].is_string());

    let request = common::request("GET", &format!("/v1/pastes/{bare}/meta"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(common::json_body(&body)["id"], stored.id);

    let request = common::request(
        "DELETE",
        &format!("/v1/pastes/{other_bare}"),
        &[],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(fake.object(&other.key).is_none());

    let request = common::request(
        "DELETE",
        &format!("/v1/pastes/{}", stored.id),
        &[],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    assert!(fake.object(&stored.key).is_none());

    let request = common::request("GET", &format!("/v1/pastes/{}", stored.id), &[], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let request = common::request(
        "DELETE",
        &format!("/v1/pastes/{}", stored.id),
        &[],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn word_alias_works_on_fetch_metadata_append_delete_and_list() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = b"alias over http".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[
            ("content-type", "text/plain".to_string()),
            content_length(body.len()),
        ],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    let id = json["id"]
        .as_str()
        .expect("id must be a string")
        .to_string();
    let words = json["words"]
        .as_str()
        .expect("the upload JSON must include words")
        .to_string();
    assert_eq!(
        normalize_words(&words).as_deref(),
        Some(words.as_str()),
        "the response must use the canonical hyphen form"
    );

    let request = common::request("GET", &format!("/v1/pastes/{words}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_slice(), b"alias over http");

    let spaced = words.replace('-', "%20");
    let request = common::request("GET", &format!("/v1/pastes/{spaced}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_slice(), b"alias over http");

    let request = common::request("GET", &format!("/v1/pastes/{words}/meta"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let meta = common::json_body(&body);
    assert_eq!(meta["id"], id);
    assert_eq!(meta["words"], words);

    let request = common::request(
        "POST",
        &format!("/v1/pastes/{words}/append"),
        &[("content-type", "text/plain".to_string())],
        b"!".to_vec(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let appended = common::json_body(&body);
    assert_eq!(appended["id"], id);
    assert_eq!(appended["words"], words);

    let date = id.split_once('/').expect("dated id").0;
    let request = common::request("GET", &format!("/v1/pastes?date={date}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let listed = common::json_body(&body);
    assert_eq!(listed["items"][0]["words"], words);

    let request = common::request("DELETE", &format!("/v1/pastes/{words}"), &[], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(fake.object(&format!("pastes/{id}")).is_none());
    assert!(fake.object(&format!("words/{words}")).is_none());

    let request = common::request("GET", &format!("/v1/pastes/{words}"), &[], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_route_filters_expired_objects_and_validates_dates() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let valid = common::stored_metadata(
        &(Utc::now() + Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    let expired = common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    fake.seed(
        &format!("pastes/{today}/aaa"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&valid),
    );
    fake.seed(
        &format!("pastes/{today}/bbb"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&expired),
    );
    fake.seed(
        &format!("pastes/{today}/ccc"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&valid),
    );
    let app = common::app(store);

    let request = common::request("GET", "/v1/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = common::json_body(&body);
    assert_eq!(json["items"].as_array().unwrap().len(), 2);
    assert_eq!(json["date"], Value::Null);

    let request = common::request(
        "GET",
        &format!("/v1/pastes?date={today}&limit=1"),
        &[],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = common::json_body(&body);
    assert_eq!(json["date"], today);
    assert_eq!(json["items"].as_array().unwrap().len(), 1);

    let request = common::request("GET", "/v1/pastes?date=28-10-2026", &[], Vec::new());
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_route_returns_502_when_the_metadata_scan_fails() {
    let fake = FakeS3::start().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = common::stored_metadata(
        &(Utc::now() + Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    let key = format!("pastes/{today}/denied");
    fake.seed(
        &key,
        b"abc",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    fake.fail_head_with(&key, StatusCode::FORBIDDEN, "AccessDenied");
    let app = common::app(fake.store().await);

    let request = common::request("GET", &format!("/v1/pastes?date={today}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "an S3 metadata failure must be a 502, got: {}",
        String::from_utf8_lossy(&body)
    );
    assert!(
        String::from_utf8_lossy(&body).contains("s3 error"),
        "the error must name the S3 failure: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn failed_uploads_return_an_error_and_never_a_paste_url() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);
    fake.fail_put_prefix("pastes/");

    let body = b"will fail".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[content_length(body.len())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    let text = String::from_utf8_lossy(&body);
    assert!(!text.contains("/v1/pastes"), "no URL on failure: {text}");
    assert!(fake.keys().is_empty());

    fake.fail(FailOp::CompleteMultipartUpload);
    let body = common::pattern_bytes(6 * 1024 * 1024);
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[("x-filename", "large.bin".to_string())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "chunked file write must fail through multipart"
    );
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));
    assert_eq!(fake.aborted_uploads().len(), 1);
}

#[tokio::test]
async fn multipart_part_failure_over_http_aborts_and_returns_no_url() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);
    fake.fail_part(1);

    let body = common::pattern_bytes(6 * 1024 * 1024);
    let request = common::request(
        "POST",
        "/upload",
        &[("x-filename", "large.bin".to_string())],
        body,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));
    assert_eq!(fake.aborted_uploads().len(), 1);
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn chunked_text_upload_over_cap_is_rejected_before_s3() {
    let fake = FakeS3::start().await;
    let store = fake
        .store_with_caps(5 * 1024 * 1024, 1024 * 1024 * 1024)
        .await;
    let app = common::app(store);

    let body = common::pattern_bytes(6 * 1024 * 1024);
    let request = common::request("POST", "/upload?format=json", &[], body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("send X-Filename"), "got: {text}");
    assert!(!text.contains("/v1/pastes"));
    assert!(
        fake.requests().is_empty(),
        "an oversize text paste must be rejected before any S3 write"
    );
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn content_type_detection_and_disposition_are_applied() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let png = b"\x89PNG\r\n\x1a\nfake-body".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[
            (
                "content-type",
                "application/x-www-form-urlencoded".to_string(),
            ),
            content_length(png.len()),
        ],
        png,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["content_type"], "image/png");
    assert_eq!(json["detection_source"], "sniffed");
    let id = json["id"].as_str().unwrap();

    let request = common::request("GET", &format!("/v1/pastes/{id}"), &[], Vec::new());
    let (status, headers, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .starts_with("inline"));

    let html = b"<html><script>alert(1)</script></html>".to_vec();
    let request = common::request(
        "POST",
        "/upload?format=json",
        &[
            ("content-type", "text/html".to_string()),
            content_length(html.len()),
        ],
        html,
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["content_type"], "text/html");
    let id = json["id"].as_str().unwrap();

    let request = common::request("GET", &format!("/v1/pastes/{id}"), &[], Vec::new());
    let (status, headers, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .starts_with("attachment"));
    assert_eq!(headers["x-content-type-options"], "nosniff");
}

#[tokio::test]
async fn chunked_text_body_without_content_length_uses_one_put() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = vec![b'a'; 20 * 1024];
    let request = common::request("POST", "/upload?format=json", &[], body.clone());
    let (status, _, response_body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&response_body);
    assert_eq!(json["size"], 20 * 1024);
    assert_eq!(json["content_type"], "text/plain; charset=utf-8");
    let id = json["id"].as_str().unwrap();
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("buffered text object must exist");
    assert_eq!(object.body, body);
    assert!(
        !fake
            .requests()
            .iter()
            .any(|request| request.query.contains("uploads")),
        "a buffered text paste must use one PutObject"
    );
}

#[tokio::test]
async fn multipart_upload_above_the_axum_default_body_limit() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 16 * 1024 * 1024).await;
    let app = common::app(store);
    let boundary = "dayfly-large-boundary";
    let content = common::pattern_bytes(3 * 1024 * 1024);
    let body = common::multipart_body(
        boundary,
        Some("large.bin"),
        Some("application/octet-stream"),
        &content,
        &[],
    );
    let mut headers = multipart_headers(boundary);
    headers.push(content_length(body.len()));
    let request = common::request("POST", "/upload?format=json", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "multipart uploads above 2 MiB must not be rejected by the body limit: {}",
        String::from_utf8_lossy(&body)
    );
    let json = common::json_body(&body);
    assert_eq!(json["size"], 3 * 1024 * 1024);
    assert_eq!(json["filename"], "large.bin");
}

#[tokio::test]
async fn empty_upload_and_zero_length_download() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let request = common::request(
        "POST",
        "/upload?format=json",
        &[content_length(0)],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["size"], 0);
    let id = json["id"].as_str().unwrap();

    let request = common::request("GET", &format!("/v1/pastes/{id}"), &[], Vec::new());
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(headers[header::CONTENT_LENGTH], "0");
}

#[tokio::test]
async fn cross_origin_preflight_is_not_answered_for_upload() {
    // The API has no CORS layer. This test records the current behaviour so
    // a future CORS change is a deliberate test change.
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);
    let request = common::request("OPTIONS", "/upload", &[], Vec::new());
    let (status, headers, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none());
}

#[tokio::test]
async fn response_headers_do_not_leak_secrets() {
    let fake = FakeS3::start().await;
    let config = common::config_with_token(fake.config(), "secret-token");
    let store = Store::from_config(config)
        .await
        .expect("authorized store must build");
    let app = common::app(store);
    let request = common::request(
        "GET",
        "/",
        &[("authorization", "Bearer secret-token".to_string())],
        Vec::new(),
    );
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let header_text = format!("{headers:?}");
    let body_text = String::from_utf8_lossy(&body);
    for secret in ["secret-token", "test-secret-key", "test-access-key"] {
        assert!(!header_text.contains(secret));
        assert!(!body_text.contains(secret));
    }
}

// --- Append tests that need a fake S3 service -----------------------------

async fn fake_state(max_text_bytes: u64) -> (AppState, FakeS3) {
    let fake = FakeS3::start().await;
    let state = AppState {
        store: fake.store_with_caps(max_text_bytes, 1024 * 1024).await,
    };
    (state, fake)
}

#[tokio::test]
async fn append_endpoint_extends_the_paste_and_fetch_returns_the_concatenation() {
    let (state, _fake) = fake_state(1024).await;
    let stored = state
        .store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let app = api_router().with_state(state);

    let request = common::request(
        "POST",
        &format!("/v1/pastes/{}/append", stored.id),
        &[("content-type", "text/plain".to_string())],
        b" world".to_vec(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = common::json_body(&body);
    assert_eq!(json["appended_bytes"], 6);
    assert_eq!(json["size"], 11);

    let request = common::request("GET", &format!("/v1/pastes/{}", stored.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_slice(), b"hello world");

    let request = common::request(
        "GET",
        &format!("/v1/pastes/{}", stored.id),
        &[("range", "bytes=3-7".to_string())],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body.as_slice(), b"lo wo");

    let request = common::request(
        "GET",
        &format!("/v1/pastes/{}/meta", stored.id),
        &[],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let json = common::json_body(&body);
    assert_eq!(json["size"], 11);
    assert!(
        json["sha256"].is_null(),
        "a base-only hash must not be reported after an append"
    );
}

#[tokio::test]
async fn append_to_a_missing_id_returns_not_found_and_writes_nothing() {
    let (state, fake) = fake_state(1024).await;
    let app = api_router().with_state(state);
    let request = common::request(
        "POST",
        "/v1/pastes/2026-10-28/missing/append",
        &[],
        b"x".to_vec(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(fake.puts().is_empty(), "a missing id must write nothing");
}

const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

fn initialize_body() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "dayfly-test", "version": "0.1.0"}
        }
    })
}

fn initialized_body() -> Value {
    json!({"jsonrpc": "2.0", "method": "notifications/initialized"})
}

fn store_file_body(id: u64, path: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {"name": "store_file", "arguments": {"path": path}}
    })
}

fn fetch_file_body(id: u64, target: &str, dest_path: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {"name": "fetch_file", "arguments": {"id": target, "dest_path": dest_path}}
    })
}

fn store_text_body(id: u64, text: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {"name": "store_text", "arguments": {"text": text}}
    })
}

async fn mcp_post(
    app: &Router,
    body: &Value,
    session_id: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut headers = vec![
        ("host", "localhost".to_string()),
        ("content-type", "application/json".to_string()),
        ("accept", "application/json, text/event-stream".to_string()),
        ("mcp-protocol-version", MCP_PROTOCOL_VERSION.to_string()),
    ];
    if let Some(session_id) = session_id {
        headers.push(("mcp-session-id", session_id.to_string()));
    }
    let request = common::request("POST", "/mcp", &headers, body.to_string().into_bytes());
    common::send(app, request).await
}

fn sse_json(body: &[u8]) -> Value {
    let text = String::from_utf8_lossy(body);
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find(|data| !data.is_empty())
        .unwrap_or_else(|| panic!("response must carry an SSE JSON-RPC payload, body: {text}"));
    serde_json::from_str(data).expect("SSE payload must be JSON")
}

fn stored_paste(payload: &Value) -> Value {
    let text = payload["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("store_text must return JSON text, got: {payload}"));
    serde_json::from_str(text).expect("store_text result must be JSON")
}

async fn initialize_mcp_session(app: &Router) -> String {
    let (status, headers, body) = mcp_post(app, &initialize_body(), None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initialize must succeed, got: {}",
        String::from_utf8_lossy(&body)
    );
    let session = headers
        .get("mcp-session-id")
        .expect("initialize must create a session")
        .to_str()
        .expect("session id must be ASCII")
        .to_string();

    let (status, _, body) = mcp_post(app, &initialized_body(), Some(&session)).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "the initialized notification must be accepted, got: {}",
        String::from_utf8_lossy(&body)
    );
    session
}

#[tokio::test]
async fn append_text_works_through_the_http_mcp_endpoint() {
    let (state, _fake) = fake_state(1024).await;
    let app = app_router(state.clone());
    let session = initialize_mcp_session(&app).await;

    let (status, _, body) = mcp_post(&app, &store_text_body(10, "hello"), Some(&session)).await;
    assert_eq!(status, StatusCode::OK);
    let stored = stored_paste(&sse_json(&body));
    let id = stored["id"].as_str().expect("id must be a string");

    let append_body = json!({
        "jsonrpc": "2.0",
        "id": 11,
        "method": "tools/call",
        "params": {"name": "append_text", "arguments": {"id": id, "text": " world"}}
    });
    let (status, _, body) = mcp_post(&app, &append_body, Some(&session)).await;
    assert_eq!(status, StatusCode::OK);
    let payload = sse_json(&body);
    assert_eq!(payload["result"]["isError"], false);
    let appended = stored_paste(&payload);
    assert_eq!(appended["appended_bytes"], 6);
    assert_eq!(appended["size"], 11);

    let slice = state
        .store
        .fetch_text(id, 0, None)
        .await
        .expect("fetch must succeed");
    assert_eq!(slice.text, "hello world");
}

#[tokio::test]
async fn x_ttl_header_stores_exact_hour_expiry() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);

    let request = common::request(
        "POST",
        "/upload",
        &[
            ("content-type", "text/plain".to_string()),
            ("content-length", "5".to_string()),
            ("x-ttl", "1h".to_string()),
        ],
        b"hello".to_vec(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "a valid X-TTL header must be accepted"
    );

    let key = fake
        .puts()
        .into_iter()
        .find(|(key, _)| key.starts_with("pastes/"))
        .expect("the upload must reach S3")
        .0;
    let meta = fake
        .object(&key)
        .expect("the upload must be stored")
        .metadata;
    assert_eq!(
        meta.get("ttl-seconds").map(String::as_str),
        Some("3600"),
        "ttl-seconds must hold the exact sub-day duration"
    );
    let created = meta
        .get("created-at")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .expect("created-at must be stored");
    let expires = meta
        .get("expires-at")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .expect("expires-at must be stored");
    assert_eq!(
        expires - created,
        chrono::Duration::hours(1),
        "expires-at must be exactly one hour after created-at"
    );
}

#[tokio::test]
async fn x_ttl_days_still_stores_whole_day_expiry() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);

    let request = common::request(
        "POST",
        "/upload",
        &[
            ("content-type", "text/plain".to_string()),
            ("content-length", "5".to_string()),
            ("x-ttl-days", "7".to_string()),
        ],
        b"hello".to_vec(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);

    let key = fake
        .puts()
        .into_iter()
        .find(|(key, _)| key.starts_with("pastes/"))
        .expect("the upload must reach S3")
        .0;
    let meta = fake
        .object(&key)
        .expect("the upload must be stored")
        .metadata;
    assert_eq!(meta.get("ttl-seconds").map(String::as_str), Some("604800"));
    assert_eq!(meta.get("ttl-days").map(String::as_str), Some("7"));
    let created = meta
        .get("created-at")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .expect("created-at must be stored");
    let expires = meta
        .get("expires-at")
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .expect("expires-at must be stored");
    assert_eq!(expires - created, chrono::Duration::days(7));
}

#[tokio::test]
async fn two_mcp_clients_store_text_through_one_shared_store() {
    let fake = FakeS3::start().await;
    let app = app_router(AppState {
        store: fake.store().await,
    });
    let session_a = initialize_mcp_session(&app).await;
    let session_b = initialize_mcp_session(&app).await;
    assert_ne!(
        session_a, session_b,
        "each initialize must create a distinct session"
    );

    let request_a = store_text_body(10, "paste from client a");
    let request_b = store_text_body(11, "paste from client b");
    let ((status_a, _, body_a), (status_b, _, body_b)) = tokio::join!(
        mcp_post(&app, &request_a, Some(&session_a)),
        mcp_post(&app, &request_b, Some(&session_b))
    );
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(status_b, StatusCode::OK);
    let payload_a = sse_json(&body_a);
    let payload_b = sse_json(&body_b);
    assert_eq!(payload_a["result"]["isError"], false);
    assert_eq!(payload_b["result"]["isError"], false);
    let paste_a = stored_paste(&payload_a);
    let paste_b = stored_paste(&payload_b);
    assert_ne!(
        paste_a["id"], paste_b["id"],
        "each store_text call must create a new paste"
    );

    let puts = fake.puts();
    assert_eq!(
        puts.len(),
        4,
        "each store_text call writes the paste and its alias"
    );
    let paste_keys: Vec<&str> = puts
        .iter()
        .map(|(key, _)| key.as_str())
        .filter(|key| key.starts_with("pastes/"))
        .collect();
    assert_eq!(paste_keys.len(), 2, "each store_text call writes one paste");
    assert_ne!(
        paste_keys[0], paste_keys[1],
        "each paste must use a distinct key"
    );
}

// --- Issue #63: local-path tools on the HTTP transport ---

/// Build a state whose MCP handler is configured with an allowed local root.
async fn state_with_local_root(fake: &FakeS3, root: &std::path::Path) -> AppState {
    let mut config = fake.config();
    config.mcp_local_root = Some(
        root.canonicalize()
            .expect("the allowed root must exist before the store is built"),
    );
    AppState {
        store: Store::from_config(config).await.expect("store builds"),
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = common::temp_path(name);
    std::fs::create_dir_all(&dir).expect("temporary test directory must be creatable");
    dir
}

fn rpc_error_code(payload: &Value) -> Option<i64> {
    payload["error"]["code"].as_i64()
}

/// An HTTP MCP client must not be able to read a file the process can reach.
#[tokio::test]
async fn http_mcp_cannot_position_itself_to_read_a_local_file() {
    let fake = FakeS3::start().await;
    let app = app_router(AppState {
        store: fake.store().await,
    });
    let session = initialize_mcp_session(&app).await;

    let canary = b"dayfly-test-canary-credential-material";
    let canary_path = common::write_temp_file("issue63-canary.txt", canary);
    let paths = [
        canary_path
            .to_str()
            .expect("path must be UTF-8")
            .to_string(),
        "/proc/self/environ".to_string(),
    ];

    for (index, path) in paths.iter().enumerate() {
        let id = 20 + index as u64;
        let (status, _, body) = mcp_post(&app, &store_file_body(id, path), Some(&session)).await;
        assert_eq!(status, StatusCode::OK);
        let payload = sse_json(&body);
        assert_eq!(
            rpc_error_code(&payload),
            Some(-32602),
            "the HTTP transport must refuse a local read of {path}, got: {payload}"
        );
        assert!(
            payload["result"]["content"].is_null(),
            "a refused read must return no tool result: {payload}"
        );
    }

    assert!(
        fake.puts()
            .iter()
            .all(|(_, body)| body.as_slice() != canary),
        "the canary must never reach the bucket"
    );
    assert!(
        fake.puts().is_empty(),
        "a refused local read must issue no PutObject"
    );
}

/// An HTTP MCP client must not be able to write a file the process can reach.
#[tokio::test]
async fn http_mcp_cannot_position_itself_to_write_a_local_file() {
    let fake = FakeS3::start().await;
    let app = app_router(AppState {
        store: fake.store().await,
    });
    let session = initialize_mcp_session(&app).await;

    let (status, _, body) = mcp_post(&app, &store_text_body(30, "payload"), Some(&session)).await;
    assert_eq!(status, StatusCode::OK);
    let id = stored_paste(&sse_json(&body))["id"]
        .as_str()
        .expect("stored paste must carry an id")
        .to_string();

    let dest = common::temp_path("issue63-dest.txt");
    let _ = std::fs::remove_file(&dest);
    let (status, _, body) = mcp_post(
        &app,
        &fetch_file_body(31, &id, dest.to_str().expect("path must be UTF-8")),
        Some(&session),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let payload = sse_json(&body);
    assert_eq!(
        rpc_error_code(&payload),
        Some(-32602),
        "the HTTP transport must refuse a local write, got: {payload}"
    );
    assert!(
        !dest.exists(),
        "a refused local write must not create the destination"
    );
}

/// With an allowed root configured, the same tools work inside it.
#[tokio::test]
async fn http_mcp_allows_a_local_file_under_the_configured_root() {
    let fake = FakeS3::start().await;
    let root = temp_dir("issue63-root");
    let app = app_router(state_with_local_root(&fake, &root).await);
    let session = initialize_mcp_session(&app).await;

    let inside = root.join("note.txt");
    std::fs::write(&inside, b"allowed payload").expect("test file must be writable");
    let (status, _, body) = mcp_post(
        &app,
        &store_file_body(40, inside.to_str().expect("path must be UTF-8")),
        Some(&session),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let payload = sse_json(&body);
    assert_eq!(
        payload["result"]["isError"], false,
        "a file under the allowed root must be accepted, got: {payload}"
    );
    assert!(
        fake.puts()
            .iter()
            .any(|(_, body)| body.as_slice() == b"allowed payload"),
        "the allowed file must reach the bucket"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// A path outside the allowed root is refused, including through a symlink.
#[tokio::test]
async fn http_mcp_rejects_a_local_path_outside_the_configured_root() {
    let fake = FakeS3::start().await;
    let root = temp_dir("issue63-root-escape");
    let outside = common::write_temp_file("issue63-outside.txt", b"outside payload");
    let app = app_router(state_with_local_root(&fake, &root).await);
    let session = initialize_mcp_session(&app).await;

    let link = root.join("escape.txt");
    std::os::unix::fs::symlink(&outside, &link).expect("test symlink must be creatable");

    // `..` out of the root, with a file that exists, so the check runs
    // against a real path instead of a resolution failure.
    let traversal = root
        .join("..")
        .join(outside.file_name().expect("temp file has a name"));
    let candidates = [
        outside.clone(),
        link.clone(),
        traversal,
        std::path::PathBuf::from("/etc/hostname"),
    ];

    for (index, path) in candidates.iter().enumerate() {
        let id = 50 + index as u64;
        let (status, _, body) = mcp_post(
            &app,
            &store_file_body(id, path.to_str().expect("path must be UTF-8")),
            Some(&session),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let payload = sse_json(&body);
        assert_eq!(
            rpc_error_code(&payload),
            Some(-32602),
            "a path outside the root must be refused ({}), got: {payload}",
            path.display()
        );
    }

    assert!(
        fake.puts().is_empty(),
        "no out-of-root file may reach the bucket"
    );

    let _ = std::fs::remove_file(&outside);
    let _ = std::fs::remove_dir_all(&root);
}
