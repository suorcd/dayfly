//! Human web handler integration tests, in process through the axum router.

mod common;

use axum::http::{header, StatusCode};
use chrono::{Duration, Utc};
use common::FakeS3;
use dayfly::http::MULTIPART_METADATA_MAX_BYTES;
use dayfly::store::Store;

const TEST_HOST: &str = "localhost";
const TEST_ORIGIN: &str = "http://localhost";

fn form_headers(body_len: usize) -> Vec<(&'static str, String)> {
    vec![
        ("host", TEST_HOST.to_string()),
        ("origin", TEST_ORIGIN.to_string()),
        (
            "content-type",
            "application/x-www-form-urlencoded".to_string(),
        ),
        ("content-length", body_len.to_string()),
    ]
}

fn multipart_headers(boundary: &str, body_len: usize) -> Vec<(&'static str, String)> {
    vec![
        ("host", TEST_HOST.to_string()),
        ("origin", TEST_ORIGIN.to_string()),
        (
            "content-type",
            format!("multipart/form-data; boundary={boundary}"),
        ),
        ("content-length", body_len.to_string()),
    ]
}

fn html_text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

fn id_from_success(body: &str) -> String {
    let marker = "<p>Paste/file ID: <code>";
    let start = body.find(marker).expect("success page must show the id") + marker.len();
    let rest = &body[start..];
    let end = rest.find("</code>").expect("id must be in a code element");
    rest[..end].to_string()
}

fn future_metadata(size: usize) -> Vec<(String, String)> {
    common::stored_metadata(
        &(Utc::now() + Duration::days(2)).to_rfc3339(),
        "text/plain",
        size,
        "abc",
    )
}

#[tokio::test]
async fn index_page_renders_both_forms_and_the_default_ttl() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);

    let request = common::request("GET", "/", &[], Vec::new());
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    let page = html_text(&body);
    assert!(page.contains("action=\"/ui/text\""));
    assert!(page.contains("action=\"/ui/upload\""));
    assert!(page.contains("enctype=\"multipart/form-data\""));
    assert!(page.contains("<option value=\"7\" selected>7 days</option>"));
    assert!(page.contains("The selector uses whole days"));
}

#[tokio::test]
async fn text_submit_rejects_zero_ttl_days() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let form = b"text=hello&ttl_days=0".to_vec();

    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a zero TTL must not create an expired paste, got: {}",
        String::from_utf8_lossy(&body)
    );
}

#[tokio::test]
async fn text_submit_creates_a_paste_and_shows_links() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let form = b"text=hello+world&filename=note.txt&ttl_days=1".to_vec();

    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/html"));
    let page = html_text(&body);
    let id = id_from_success(&page);
    assert!(page.contains(&format!("href=\"/view/{id}\"")));
    assert!(page.contains(&format!("href=\"/v1/pastes/{id}\"")));
    assert!(page.contains("Paste created"));

    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the paste must be stored");
    assert_eq!(object.body, b"hello world");
    assert_eq!(
        object.metadata.get("filename").map(String::as_str),
        Some("note%2Etxt")
    );
    assert_eq!(
        object.metadata.get("ttl-days").map(String::as_str),
        Some("1")
    );
}

#[tokio::test]
async fn text_submit_over_cap_returns_413_error_page() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store_with_caps(5, 1024).await);
    let form = b"text=123456".to_vec();

    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let page = html_text(&body);
    assert!(page.contains("payload too large"));
    assert!(!page.contains("/v1/pastes/"));
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn upload_submit_creates_a_paste_with_form_ttl() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-boundary";
    let body = common::multipart_body(
        boundary,
        Some("report.txt"),
        Some("text/plain"),
        b"file body",
        &[("ttl_days", "3")],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    let id = id_from_success(&page);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the upload must be stored");
    assert_eq!(object.body, b"file body");
    assert_eq!(
        object.metadata.get("filename").map(String::as_str),
        Some("report%2Etxt")
    );
    assert_eq!(
        object.metadata.get("ttl-days").map(String::as_str),
        Some("3")
    );
}

#[tokio::test]
async fn upload_submit_rejects_an_oversized_ttl_field() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-metadata-boundary";
    let oversized = "a".repeat(MULTIPART_METADATA_MAX_BYTES + 1);
    let body = common::multipart_body(
        boundary,
        Some("small.txt"),
        Some("text/plain"),
        b"file body",
        &[("ttl_days", oversized.as_str())],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "an oversized ttl_days field must be a 413, got: {}",
        html_text(&body)
    );
    assert!(
        fake.keys().is_empty(),
        "an oversized metadata field must store nothing: {:?}",
        fake.keys()
    );
}

#[tokio::test]
async fn upload_submit_reports_missing_fields_and_caps() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store_with_caps(5, 5).await);
    let boundary = "web-boundary";

    let body = common::multipart_fields_only(boundary, &[("ttl_days", "1")]);
    let headers = multipart_headers(boundary, body.len());
    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(html_text(&body).contains("upload requires a"));

    let body = common::multipart_body(boundary, Some("big.bin"), None, b"123456", &[]);
    let headers = multipart_headers(boundary, body.len());
    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a filename must select the file cap, which this body exceeds"
    );
}

#[tokio::test]
async fn upload_submit_accepts_bodies_above_the_axum_default_limit() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store_with_caps(1024, 16 * 1024 * 1024).await);
    let boundary = "web-large-boundary";
    let content = common::pattern_bytes(3 * 1024 * 1024);
    let body = common::multipart_body(
        boundary,
        Some("large.bin"),
        Some("application/octet-stream"),
        &content,
        &[],
    );
    let headers = multipart_headers(boundary, body.len());
    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a 3 MiB web upload must not hit the axum body limit: {}",
        html_text(&body)
    );
    assert!(id_from_success(&html_text(&body)).contains('/'));
}

#[tokio::test]
async fn list_page_shows_current_pastes_and_hides_expired_ones() {
    let fake = FakeS3::start().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    let expired = common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        5,
        "abc",
    );
    fake.seed(
        &format!("pastes/{today}/visible"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&valid),
    );
    fake.seed(
        &format!("pastes/{today}/expired"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&expired),
    );
    let app = common::app(fake.store().await);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains(&format!("{today}/visible")));
    assert!(!page.contains(&format!("{today}/expired")));
    assert!(page.contains("2 days"));
    assert!(page.contains("name=\"date\""));
    assert!(page.contains("name=\"start\""));
    assert!(page.contains("name=\"end\""));
    assert!(page.contains("yesterday and today in UTC"));

    let request = common::request("GET", "/pastes?date=2020-01-01", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains("value=\"2020-01-01\""));
    assert!(page.contains("No current pastes in this window."));

    let request = common::request("GET", "/pastes?date=bad-date", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(html_text(&body).contains("invalid date"));
}

#[tokio::test]
async fn view_page_renders_text_escaped_and_images_inline() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let text = store
        .store_text(b"a < b & c > d \"quoted\"", None, None, None)
        .await
        .expect("text must store");
    let png = store
        .store_text(
            b"\x89PNG\r\n\x1a\nfake-body",
            Some("pic.png"),
            Some("image/png"),
            None,
        )
        .await
        .expect("png must store");
    let html = store
        .store_text(
            b"<html>body</html>",
            Some("page.html"),
            Some("text/html"),
            None,
        )
        .await
        .expect("html must store");
    let app = common::app(store);

    let request = common::request("GET", &format!("/view/{}", text.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains("<pre>a &lt; b &amp; c &gt; d &quot;quoted&quot;</pre>"));
    assert!(page.contains(&format!("/v1/pastes/{}", text.id)));

    let request = common::request("GET", &format!("/view/{}", png.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains(&format!("<img src=\"/v1/pastes/{}\"", png.id)));
    assert!(!page.contains("<pre>"));

    let request = common::request("GET", &format!("/view/{}", html.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(!page.contains("<img"));
    assert!(!page.contains("<pre>"));
    assert!(page.contains(&format!("/v1/pastes/{}", html.id)));

    let request = common::request(
        "GET",
        &format!("/view/{}", text.id.split_once('/').unwrap().1),
        &[],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn view_page_marks_truncated_large_text() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let large = vec![b'a'; 1_000_001];
    let stored = store
        .store_text(&large, None, None, None)
        .await
        .expect("large text must store");
    let app = common::app(store);

    let request = common::request("GET", &format!("/view/{}", stored.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html_text(&body).contains("Text truncated at 1 MiB"));
}

#[tokio::test]
async fn view_missing_and_expired_pages_return_not_found_and_gone() {
    let fake = FakeS3::start().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    fake.seed(
        &format!("pastes/{today}/expired"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    let app = common::app(fake.store().await);

    let request = common::request("GET", "/view/2026-10-28/missing", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(html_text(&body).contains("paste not found"));

    let request = common::request("GET", &format!("/view/{today}/expired"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::GONE);
    assert!(html_text(&body).contains("paste expired"));
}

#[tokio::test]
async fn delete_form_removes_the_paste_and_redirects() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"delete me", None, None, None)
        .await
        .expect("text must store");
    let app = common::app(store);
    let form = format!("id={}", stored.id).into_bytes();

    let request = common::request("POST", "/ui/delete", &form_headers(form.len()), form);
    let (status, headers, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(headers[header::LOCATION], "/pastes");
    assert!(fake.object(&stored.key).is_none());

    let form = b"id=..%2Fetc".to_vec();
    let request = common::request("POST", "/ui/delete", &form_headers(form.len()), form);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(html_text(&body).contains("invalid id"));
}

#[tokio::test]
async fn ui_auth_rejects_requests_without_the_bearer_token() {
    let fake = FakeS3::start().await;
    let config = common::config_with_token(fake.config(), "web-secret");
    let store = Store::from_config(config)
        .await
        .expect("authorized store must build");
    let app = common::app(store);

    for uri in ["/", "/pastes", "/view/2026-10-28/missing"] {
        let request = common::request("GET", uri, &[], Vec::new());
        let (status, _, body) = common::send(&app, request).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(
            common::json_body(&body)["error"],
            "unauthorized",
            "{uri} must use the shared JSON auth error"
        );
    }

    let form = b"text=blocked".to_vec();
    let requests_before = fake.requests().len();
    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(fake.requests().len(), requests_before);

    let request = common::request(
        "GET",
        "/",
        &[("authorization", "Bearer web-secret".to_string())],
        Vec::new(),
    );
    let (status, _, _) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn cross_site_form_post_is_forbidden() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let form = b"text=blocked".to_vec();
    let headers = vec![
        ("host", TEST_HOST.to_string()),
        ("origin", "http://evil.example".to_string()),
        (
            "content-type",
            "application/x-www-form-urlencoded".to_string(),
        ),
        ("content-length", form.len().to_string()),
    ];

    let request = common::request("POST", "/ui/text", &headers, form);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(html_text(&body).contains("does not match the Host header"));
    assert!(
        fake.keys().is_empty(),
        "a cross-site post must write nothing"
    );
}

#[tokio::test]
async fn upload_error_page_never_shows_a_paste_url() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    fake.fail_put_prefix("pastes/");
    let form = b"text=will+fail".to_vec();

    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let page = html_text(&body);
    assert!(!page.contains("/v1/pastes/"));
    assert!(fake.keys().is_empty());
}
