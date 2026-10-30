//! HTTP route integration tests, in process through the axum router.

mod common;

use axum::http::{header, StatusCode};
use chrono::{DateTime, Duration, Utc};
use common::{FailOp, FakeS3};
use dayfly::store::Store;
use serde_json::Value;

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
        .store_text(b"meta body", Some("meta.txt"), None, Some(4))
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
async fn failed_uploads_return_an_error_and_never_a_paste_url() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);
    fake.fail(FailOp::PutObject);

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
    let request = common::request("POST", "/upload?format=json", &[], body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "chunked write must fail through multipart"
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
    let request = common::request("POST", "/upload", &[], body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!String::from_utf8_lossy(&body).contains("/v1/pastes"));
    assert_eq!(fake.aborted_uploads().len(), 1);
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn chunked_text_upload_over_cap_is_rejected_and_aborted() {
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
    assert_eq!(fake.aborted_uploads().len(), 1);
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
async fn chunked_body_without_content_length_uses_multipart_s3() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let app = common::app(store);

    let body = b"five!".to_vec();
    let request = common::request("POST", "/upload?format=json", &[], body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::CREATED);
    let json = common::json_body(&body);
    assert_eq!(json["size"], 5);
    let id = json["id"].as_str().unwrap();
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("multipart object must exist");
    assert_eq!(object.body, b"five!");
    assert!(fake
        .requests()
        .iter()
        .any(|request| request.query == "uploads"));
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
