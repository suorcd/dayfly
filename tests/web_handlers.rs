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

/// Percent-encode only the non-alphanumeric bytes, so an ASCII form body
/// stays close to the decoded size.
fn text_form_body(text: &str) -> Vec<u8> {
    let mut body = String::from("text=");
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() {
            body.push(byte as char);
        } else {
            body.push_str(&format!("%{byte:02X}"));
        }
    }
    body.into_bytes()
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

fn id_from_redirect(headers: &axum::http::HeaderMap) -> String {
    let location = headers[header::LOCATION]
        .to_str()
        .expect("redirect must carry a UTF-8 Location header");
    let prefix = "/view/";
    let id = location
        .strip_prefix(prefix)
        .unwrap_or_else(|| panic!("unexpected redirect target: {location}"));
    assert!(!id.is_empty(), "redirect must name a paste id");
    id.to_string()
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
async fn index_page_renders_the_combined_form_and_the_default_ttl() {
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
    assert!(page.contains("action=\"/ui/create\""));
    assert!(page.contains("enctype=\"multipart/form-data\""));
    assert!(page.contains("id=\"dropzone\""));
    assert!(page.contains("id=\"file-status\""));
    assert!(page.contains("id=\"clear-file\""));
    assert!(page.contains("data-max-file-bytes=\"1073741824\""));
    assert!(page.contains("Max file size: 1 GiB"));
    assert!(page.contains("File is too large"));
    assert!(page.contains("name=\"file\""));
    assert!(page.contains("name=\"text\""));
    assert!(page.contains("name=\"filename\""));
    assert!(page.contains("name=\"ttl_days\""));
    let ttl_at = page.find("name=\"ttl_days\"").expect("TTL selector");
    let file_at = page.find("name=\"file\"").expect("file input");
    assert!(
        ttl_at < file_at,
        "the TTL field must precede the file field"
    );
    let text_at = page.find("name=\"text\"").expect("text field");
    assert!(
        text_at < file_at,
        "the text field must precede the file field"
    );
    let filename_at = page.find("name=\"filename\"").expect("filename field");
    assert!(
        ttl_at < filename_at,
        "the TTL field must precede the filename field"
    );
    assert!(page.contains("<option value=\"7\" selected>7 days</option>"));
    assert!(page.contains("The selector uses whole days"));
    assert!(page.contains("~/dayfly $"));
    assert!(page.contains("href=\"/.well-known/dayfly.json\""));
    assert!(page.contains("&copy; 2026 suorcd"));
    assert!(
        page.contains("<a href=\"/\" class=\"active\" aria-current=\"page\">new paste</a>"),
        "the current page must be marked in the nav: {page}"
    );
    assert!(
        !page.contains("<h1>New paste</h1>"),
        "the page title must not repeat the nav link: {page}"
    );
    assert!(
        !page.contains("<h2>Paste or upload</h2>"),
        "the form must not repeat a section heading: {page}"
    );
    assert!(
        page.contains("<title>New paste — dayfly</title>"),
        "the page title must use the product name: {page}"
    );
    assert!(
        page.contains("<footer>dayfly — internal use only"),
        "the footer must use the product name: {page}"
    );
}

#[tokio::test]
async fn index_page_shows_curl_examples_for_the_request_base() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let request = common::request(
        "GET",
        "/",
        &[
            ("host", "dayfly.test".to_string()),
            ("x-forwarded-proto", "https".to_string()),
        ],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains("BASE=https://dayfly.test"),
        "the hint must use the request base URL: {page}"
    );
    assert!(
        page.contains("<details class=\"cli-hint\"><summary>cli</summary>"),
        "the hint must collapse under a cli summary: {page}"
    );
    assert!(
        page.contains("curl -sS --data-binary @note.txt &quot;$BASE/upload&quot;"),
        "the text example is missing: {page}"
    );
    assert!(
        page.contains("cat error.log | curl -sS --data-binary @- &quot;$BASE/upload&quot;"),
        "the stdin example is missing: {page}"
    );
    assert!(
        page.contains(
            "curl -sS --data-binary @report.pdf -H &quot;X-Filename: report.pdf&quot; \
             &quot;$BASE/upload&quot;"
        ),
        "the file example is missing: {page}"
    );
    assert!(
        page.contains(
            "curl -sS -H &quot;Accept: application/json&quot; -H &quot;X-TTL: 1h&quot; \
             --data-binary @note.txt &quot;$BASE/upload&quot;"
        ),
        "the JSON and TTL example is missing: {page}"
    );
    assert!(
        !page.contains("Authorization:"),
        "a page without a configured token must not show the header: {page}"
    );
}

#[tokio::test]
async fn index_page_curl_examples_carry_the_bearer_placeholder() {
    const TOKEN: &str = "index-page-secret-token";
    let fake = FakeS3::start().await;
    let config = common::config_with_token(fake.config(), TOKEN);
    let store = Store::from_config(config)
        .await
        .expect("the store must build without network access");
    let app = common::app(store);
    let request = common::request(
        "GET",
        "/",
        &[
            ("host", "dayfly.test".to_string()),
            ("authorization", format!("Bearer {TOKEN}")),
        ],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains(
            "curl -sS -H &quot;Authorization: Bearer $DAYFLY_HTTP_BEARER_TOKEN&quot; \
             --data-binary @note.txt &quot;$BASE/upload&quot;"
        ),
        "the example must carry the bearer placeholder: {page}"
    );
    assert!(
        !page.contains(TOKEN),
        "the configured token must not reach the page: {page}"
    );
}

#[tokio::test]
async fn index_page_escapes_the_base_url_in_the_curl_examples() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let request = common::request(
        "GET",
        "/",
        &[(
            "host",
            "evil.example\"><script>alert(1)</script>".to_string(),
        )],
        Vec::new(),
    );
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains("BASE=http://evil.example&quot;&gt;&lt;script&gt;"),
        "the base URL must be escaped: {page}"
    );
    assert!(
        !page.contains("<script>alert(1)</script>"),
        "the base URL must not inject markup: {page}"
    );
}

#[tokio::test]
async fn index_page_selects_a_default_outside_the_fixed_list() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.default_ttl_days = 2;
    let store = Store::from_config(config)
        .await
        .expect("the store must build without network access");
    let app = common::app(store);

    let request = common::request("GET", "/", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains("<option value=\"2\" selected>2 days</option>"),
        "the configured default must be selected: {page}"
    );
    assert_eq!(
        page.matches("<option value=\"2\" selected>2 days</option>")
            .count(),
        1,
        "the combined form must select the configured default"
    );
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
async fn text_submit_creates_a_paste_and_redirects_to_the_view() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let form = b"text=hello+world&filename=note.txt&ttl_days=1".to_vec();

    let request = common::request("POST", "/ui/text", &form_headers(form.len()), form);
    let (status, headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&headers);
    assert_eq!(
        headers[header::LOCATION].to_str().unwrap(),
        format!("/view/{id}")
    );

    let request = common::request("GET", &format!("/view/{id}"), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html_text(&body).contains(&id));

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
async fn text_submit_accepts_a_paste_above_the_axum_default_limit() {
    let fake = FakeS3::start().await;
    let cap = 3 * 1024 * 1024;
    let app = common::app(fake.store_with_caps(cap, 1024 * 1024 * 1024).await);
    let text = "a".repeat(2 * 1024 * 1024 + 1);
    let body = text_form_body(&text);

    let request = common::request("POST", "/ui/text", &form_headers(body.len()), body);
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "a paste above 2 MiB but below the text cap must succeed: {}",
        html_text(&body)
    );
    let id = id_from_redirect(&headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the paste must be stored");
    assert_eq!(object.body, text.as_bytes());
}

#[tokio::test]
async fn text_submit_accepts_url_encoded_non_ascii_above_the_default_limit() {
    let fake = FakeS3::start().await;
    let cap = 3 * 1024 * 1024;
    let app = common::app(fake.store_with_caps(cap, 1024 * 1024 * 1024).await);
    // 2.4 MiB decoded, 7.2 MiB after percent-encoding.
    let text = "é".repeat(1_200_000);
    let body = text_form_body(&text);
    assert!(body.len() > 2 * 1024 * 1024);

    let request = common::request("POST", "/ui/text", &form_headers(body.len()), body);
    let (status, headers, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "encoded text above 2 MiB but below the cap must succeed: {}",
        html_text(&body)
    );
    let id = id_from_redirect(&headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the paste must be stored");
    assert_eq!(object.body, text.as_bytes());
}

#[tokio::test]
async fn text_submit_over_the_configured_cap_returns_413_without_storing() {
    let fake = FakeS3::start().await;
    let cap = 1024 * 1024;
    let app = common::app(fake.store_with_caps(cap, 1024 * 1024 * 1024).await);
    let text = "a".repeat(1024 * 1024 + 1);
    let body = text_form_body(&text);

    let request = common::request("POST", "/ui/text", &form_headers(body.len()), body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        html_text(&body).contains("payload too large"),
        "the error page must name the size limit"
    );
    assert!(fake.keys().is_empty());
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
async fn create_submit_creates_a_file_with_form_ttl() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);

    let request = common::request("GET", "/", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let rendered = html_text(&body);
    let create_form = rendered
        .split("action=\"/ui/create\"")
        .nth(1)
        .expect("the combined form must be rendered");
    let ttl_position = create_form
        .find("name=\"ttl_days\"")
        .expect("the combined form must render the TTL selector");
    let file_position = create_form
        .find("name=\"file\"")
        .expect("the combined form must render the file input");
    assert!(
        ttl_position < file_position,
        "the TTL selector must come before the file input, so the handler reads it: {create_form}"
    );

    let boundary = "web-boundary";
    let body = common::multipart_body(
        boundary,
        Some("report.txt"),
        Some("text/plain"),
        b"file body",
        &[("ttl_days", "3")],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&response_headers);
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
async fn create_submit_creates_a_text_paste_with_filename_and_ttl() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-create-text";
    let body = common::multipart_fields_only(
        boundary,
        &[
            ("ttl_days", "1"),
            ("text", "hello world"),
            ("filename", "note.txt"),
        ],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&response_headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the text must be stored");
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
async fn create_submit_redirects_to_the_words_when_enabled() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store_with_show_words(true).await);
    let boundary = "web-create-words";
    let body =
        common::multipart_fields_only(boundary, &[("ttl_days", "1"), ("text", "word redirect")]);
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let location = response_headers[header::LOCATION].to_str().unwrap();
    let alias = location.strip_prefix("/view/").expect("view redirect");
    assert!(
        !alias.contains('/'),
        "the redirect must use the alias, not the date/id: {location}"
    );
    let alias_object = fake
        .object(&format!("words/{alias}"))
        .expect("the alias object must exist");
    let target = String::from_utf8(alias_object.body.clone()).expect("alias body is UTF-8");
    assert!(target.starts_with("pastes/"));

    let request = common::request("GET", location, &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains(&format!("Words: <a href=\"/view/{alias}\">")));
}

#[tokio::test]
async fn create_submit_prefers_a_file_when_both_are_supplied() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-create-both";
    let body = common::multipart_body(
        boundary,
        Some("report.txt"),
        Some("text/plain"),
        b"file body",
        &[
            ("ttl_days", "3"),
            ("text", "text that must be ignored"),
            ("filename", "note.txt"),
        ],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&response_headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the file must be stored");
    assert_eq!(object.body, b"file body");
    assert_eq!(
        object.metadata.get("filename").map(String::as_str),
        Some("report%2Etxt"),
        "the file name must win over the text filename"
    );
}

#[tokio::test]
async fn multipart_over_the_declared_cap_is_rejected_before_reading() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-cap-preflight";
    let body = common::multipart_fields_only(boundary, &[("ttl_days", "1")]);
    let mut headers = multipart_headers(boundary, body.len());
    for (name, value) in headers.iter_mut() {
        if *name == "content-length" {
            *value = (2u64 * 1024 * 1024 * 1024).to_string();
        }
    }

    for route in ["/ui/create", "/ui/upload"] {
        let request = common::request("POST", route, &headers, body.clone());
        let (status, _, response_body) = common::send(&app, request).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{route}");
        let page = html_text(&response_body);
        assert!(page.contains("payload too large"), "{route}: {page}");
        assert!(page.contains("1 GiB"), "{route}: {page}");
    }
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn create_submit_ignores_an_empty_file_part_and_uses_text() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-create-empty-file";
    let body = common::multipart_body(
        boundary,
        None,
        None,
        b"",
        &[("ttl_days", "1"), ("text", "hello from text")],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&response_headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the text must be stored");
    assert_eq!(object.body, b"hello from text");
}

#[tokio::test]
async fn create_submit_rejects_an_empty_form() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-create-empty";
    let body = common::multipart_fields_only(boundary, &[("ttl_days", "1")]);
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(html_text(&body).contains("provide text or a file"));
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn create_submit_rejects_an_oversized_text_without_storing() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store_with_caps(5, 1024).await);
    let boundary = "web-create-oversized";
    let body = common::multipart_fields_only(boundary, &[("ttl_days", "1"), ("text", "123456")]);
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(html_text(&body).contains("payload too large"));
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn create_submit_rejects_an_oversized_ttl_field() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-create-ttl";
    let oversized = "a".repeat(MULTIPART_METADATA_MAX_BYTES + 1);
    let body = common::multipart_fields_only(
        boundary,
        &[("ttl_days", oversized.as_str()), ("text", "hello")],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/create", &headers, body);
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(html_text(&body).contains("payload too large"));
    assert!(fake.keys().is_empty());
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
async fn upload_submit_uses_the_default_ttl_without_a_form_selection() {
    let fake = FakeS3::start().await;
    let app = common::app(fake.store().await);
    let boundary = "web-default-ttl";
    let body = common::multipart_body(
        boundary,
        Some("default.txt"),
        Some("text/plain"),
        b"file body",
        &[],
    );
    let headers = multipart_headers(boundary, body.len());

    let request = common::request("POST", "/ui/upload", &headers, body);
    let (status, response_headers, _body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let id = id_from_redirect(&response_headers);
    let object = fake
        .object(&format!("pastes/{id}"))
        .expect("the upload must be stored");
    assert_eq!(
        object.metadata.get("ttl-days").map(String::as_str),
        Some("7"),
        "an upload without a TTL selection must use the default"
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
    let (status, response_headers, body) = common::send(&app, request).await;
    assert_eq!(
        status,
        StatusCode::SEE_OTHER,
        "a 3 MiB web upload must not hit the axum body limit: {}",
        html_text(&body)
    );
    assert!(id_from_redirect(&response_headers).contains('/'));
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
    assert!(page.contains("<td>02</td>"));
    assert!(
        !page.contains("name=\"date\""),
        "the page must not offer the single-date selector: {page}"
    );
    assert!(
        !page.contains("Show date"),
        "the page must not offer the single-date button: {page}"
    );
    assert!(page.contains("name=\"start\""));
    assert!(page.contains("name=\"end\""));
    let yesterday = (Utc::now() - Duration::days(1))
        .format("%Y-%m-%d")
        .to_string();
    assert!(
        page.contains(&format!("name=\"start\" value=\"{yesterday}\"")),
        "the start input must show yesterday: {page}"
    );
    assert!(
        page.contains(&format!("name=\"end\" value=\"{today}\"")),
        "the end input must show today: {page}"
    );
    assert!(page.contains("last 2 days, ending today in UTC"));
    assert!(
        !page.contains("sort=expires"),
        "the redundant expires column must be gone: {page}"
    );
    assert!(
        page.contains("class=\"content-type-cell\" title=\"text/plain"),
        "the content type cell must keep the full type in a title attribute: {page}"
    );
    assert!(
        page.contains(">text/plain"),
        "the content type text must stay unedited: {page}"
    );
    assert!(
        page.contains(
            "<td class=\"size-cell\"><span class=\"size-number\">5</span>\
             <span class=\"size-unit\">B</span></td>"
        ),
        "the size must split into a right-aligned value and a unit: {page}"
    );
    assert!(
        page.contains("<th>Action</th>"),
        "the delete column must have a header: {page}"
    );
    assert!(
        page.contains("<th title=\"days\">TTL</th>"),
        "the duration column must be labelled TTL with a days title: {page}"
    );
    assert!(
        !page.contains("<th>Remaining</th>"),
        "the old duration header must be gone: {page}"
    );

    let request = common::request("GET", "/pastes?date=2020-01-01", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains("No current pastes in this window."));

    let request = common::request("GET", "/pastes?date=bad-date", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(html_text(&body).contains("invalid date"));
}

#[tokio::test]
async fn list_page_middle_truncates_long_filenames_with_a_title() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let name = "very-long-report-name-that-keeps-going.xlsx";
    store
        .store_text(b"data", Some(name), None, None)
        .await
        .expect("text with a filename must store");
    let app = common::app(store);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains(&format!("title=\"{name}\"")),
        "the cell must keep the full filename in a title attribute: {page}"
    );
    assert!(
        page.contains("very-long-repor…going.xlsx"),
        "the visible name must be truncated in the middle: {page}"
    );
}

#[tokio::test]
async fn list_page_prepopulates_the_configured_default_window() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.list_default_days = 3;
    let store = Store::from_config(config)
        .await
        .expect("the store must build without network access");
    let app = common::app(store);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    let start = (Utc::now() - Duration::days(2))
        .format("%Y-%m-%d")
        .to_string();
    let end = Utc::now().format("%Y-%m-%d").to_string();
    assert!(
        page.contains(&format!("name=\"start\" value=\"{start}\"")),
        "the start input must follow DAYFLY_LIST_DEFAULT_DAYS: {page}"
    );
    assert!(
        page.contains(&format!("name=\"end\" value=\"{end}\"")),
        "the end input must follow DAYFLY_LIST_DEFAULT_DAYS: {page}"
    );
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
    assert!(page.contains("<details><summary>Metadata &amp; delete</summary>"));
    assert!(page.contains("Delete this paste"));
    let details_at = page
        .find("</details>")
        .expect("the metadata details must close");
    let download_at = page
        .find("Download / raw")
        .expect("the raw link must render");
    let pre_at = page.find("<pre>").expect("the paste text must render");
    assert!(
        details_at < download_at && download_at < pre_at,
        "metadata must be collapsed above the raw link, and the paste content must come last"
    );

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
async fn view_page_shows_words_when_enabled_and_hides_them_when_disabled() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_show_words(true).await;
    let stored = store
        .store_text(b"word view", None, None, None)
        .await
        .expect("text must store");
    let words = stored.words.clone().expect("a new paste must have words");
    let app = common::app(store);

    let request = common::request("GET", &format!("/view/{}", stored.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(page.contains(&format!("Words: <a href=\"/view/{words}\">")));

    let fake = FakeS3::start().await;
    let store = fake.store_with_show_words(false).await;
    let stored = store
        .store_text(b"word view", None, None, None)
        .await
        .expect("text must store");
    let app = common::app(store);

    let request = common::request("GET", &format!("/view/{}", stored.id), &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html_text(&body).contains("Words:"));
}

#[tokio::test]
async fn list_page_shows_words_when_enabled_and_hides_them_when_disabled() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_show_words(true).await;
    let stored = store
        .store_text(b"word list", None, None, None)
        .await
        .expect("text must store");
    let words = stored.words.clone().expect("a new paste must have words");
    let id = stored.id.clone();
    let app = common::app(store);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains(&format!("href=\"/view/{words}\"")),
        "the alias must be the primary link: {page}"
    );
    assert!(
        !page.contains(&format!("<span class=\"muted\">{id}</span>")),
        "the canonical ID must be hidden by default: {page}"
    );
    assert!(
        page.contains("<th>Link</th>"),
        "the header must say Link by default: {page}"
    );

    let fake = FakeS3::start().await;
    let store = fake.store_with_show_words(false).await;
    let stored = store
        .store_text(b"word list", None, None, None)
        .await
        .expect("text must store");
    let words = stored.words.clone().expect("a new paste must have words");
    let id = stored.id.clone();
    let app = common::app(store);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(!page.contains(&format!("href=\"/view/{words}\"")));
    assert!(page.contains(&format!("href=\"/view/{id}\"")));
}

#[tokio::test]
async fn list_page_shows_the_internal_path_when_configured() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.show_words = true;
    config.show_internal_paths = true;
    let store = Store::from_config(config)
        .await
        .expect("the store must build without network access");
    let stored = store
        .store_text(b"path list", None, None, None)
        .await
        .expect("text must store");
    let id = stored.id.clone();
    let app = common::app(store);

    let request = common::request("GET", "/pastes", &[], Vec::new());
    let (status, _, body) = common::send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let page = html_text(&body);
    assert!(
        page.contains(&format!("<span class=\"muted\">{id}</span>")),
        "the configured page must show the canonical ID: {page}"
    );
    assert!(
        page.contains("<th>Link / ID</th>"),
        "the configured header must say Link / ID: {page}"
    );
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
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "an S3 failure must not look like a client error"
    );
    let page = html_text(&body);
    assert!(!page.contains("/v1/pastes/"));
    assert!(fake.keys().is_empty());
}
