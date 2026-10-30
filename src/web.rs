//! Simple human web frontend, served by the same binary in HTTP mode.
//!
//! Server-rendered HTML only: plain forms and links, no JavaScript
//! framework and no frontend build step. Two pages, plus a view page
//! that falls out naturally:
//!
//! * `GET /` — new paste/upload form (text and file, TTL selector).
//! * `GET /pastes` — current pastes for a date, from the bucket scan
//!   (no database), with a delete action per row.
//! * `GET /view/<id>` — one paste: text in `<pre>`, image inline,
//!   everything else as a download link.
//!
//! Theme: adapted base look from the user's own site stylesheet,
//! https://suorcd.us/style.css (verified live as the site's single
//! stylesheet), palette `cyberpunkNeon` — the live look. Only the
//! relevant base rules are adapted here; the whole stylesheet is NOT
//! copied, and the file's other two palettes (`green-dark`,
//! `neon-dark01`) are deliberately NOT used. Blog-specific rules
//! (post lists, hero scanline animation, code tabs, tag pills) are
//! omitted unless they directly serve a pastebin page.
//!
//! Fonts: the site's stack is JetBrainsMono Nerd Font / JetBrains
//! Mono / Hack / Fira Code for body and Source Code Pro / Hack for
//! code. Those fonts are NOT bundled here (licensing for this use
//! was not established); the stack falls back to system monospace on
//! machines without them. Flagged in README.
//!
//! Auth: these pages are under the same model as the API. When
//! `DAYFLY_HTTP_BEARER_TOKEN` is set, the API check applies to
//! these routes too, which plain browser forms cannot satisfy (a
//! form cannot send an `Authorization` header). For browser use,
//! run on a private network / localhost without the token set, and
//! use the token for API clients. See README. No public sharing,
//! accounts, or comments are added.

use std::collections::HashMap;

use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::header::{HOST, ORIGIN, REFERER};
use axum::http::uri::{Authority, Uri};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::http::{check_auth, AppState};
use crate::store::{parse_list_window_request, ListWindowRequest, StoreError, Ttl};

/// Adapted base look from suorcd.us/style.css, palette cyberpunkNeon
/// (background #091833, text #e3e8fc, accent #ea00d9, code background
/// #133e7c, border #0abdc6). Dark-only, no light-mode switch in v1.
const CSS: &str = r#"
body{font-family:"JetBrainsMono Nerd Font","JetBrains Mono",Hack,"Fira Code",ui-monospace,Menlo,Consolas,monospace;line-height:1.6;color:#e3e8fc;background:#091833;max-width:1200px;margin:0 auto;padding:20px}
header{padding-bottom:1rem;background:linear-gradient(to bottom,transparent 96%,transparent 100%)}
header::after{content:"";display:block;height:2px;margin-top:1rem;background:linear-gradient(to right,#ea00d9,transparent)}
nav{display:flex;gap:1rem;flex-wrap:wrap;align-items:center}
nav .prompt{color:#0abdc6;opacity:.6}
nav a{padding:.25rem .5rem;border:1px dashed transparent;border-radius:4px}
nav a:hover{border-color:#0abdc6;box-shadow:0 0 10px rgba(10,189,198,.35)}
h1{font-size:1.8rem;text-transform:lowercase;color:#ea00d9;text-shadow:0 0 12px rgba(234,0,217,.55)}
h2{font-size:1.3rem;text-transform:lowercase;color:#ea00d9;border-bottom:1px dotted #0abdc6;padding-bottom:.25rem}
hr{border:none;border-top:1px dashed #0abdc6;margin:1.5rem 0;opacity:.6}
a{color:#ea00d9;text-decoration:none}
a:hover{text-decoration:underline;text-shadow:0 0 8px rgba(234,0,217,.8)}
table{border-collapse:collapse;width:100%} th,td{text-align:left;padding:.4rem .5rem;border:1px dashed #0abdc6;vertical-align:top}
th{color:#ea00d9}
.card,form{background:rgba(19,62,124,.35);border:1px dashed #0abdc6;border-radius:8px;padding:1rem;backdrop-filter:blur(4px)}
textarea{width:100%;min-height:12rem;box-sizing:border-box;font-family:"Source Code Pro",Hack,ui-monospace,Menlo,Consolas,monospace;background:#133e7c;color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px;padding:.5rem}
input,select{font-size:1rem;padding:.3rem .5rem;background:#133e7c;color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px}
button{font-size:1rem;padding:.35rem .8rem;cursor:pointer;background:rgba(19,62,124,.5);color:#e3e8fc;border:1px dashed #0abdc6;border-radius:4px}
button:hover{box-shadow:0 0 10px rgba(10,189,198,.4)}
pre,code{font-family:"Source Code Pro",Hack,ui-monospace,Menlo,Consolas,monospace;background:#133e7c;border:1px dashed #0abdc6;border-radius:4px}
pre{white-space:pre-wrap;word-wrap:break-word;padding:1rem;overflow-x:auto}
img{max-width:100%;height:auto;border:1px dashed #0abdc6;border-radius:8px}
footer{text-align:center;font-size:.85rem;margin-top:2rem;padding-top:1rem;border-top:1px dashed #0abdc6;color:#e3e8fc;opacity:.8}
.muted{opacity:.7} .expired{color:#ff6b6b}
@media (max-width:768px){body{padding:12px} header{flex-direction:column} nav{flex-direction:column;align-items:flex-start;gap:.5rem} nav a{font-size:.9rem}}
"#;

fn page(title: &str, body: &str) -> Html<String> {
    Html(format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>{title} — pastebin</title>\n<style>{CSS}</style>\n</head>\n<body>\n<header><nav><span class=\"prompt\">~/pastebin $</span><a href=\"/\">new paste</a><a href=\"/pastes\">current pastes</a></nav></header>\n<h1>{title}</h1>\n{body}\n<footer>pastebin — internal use only</footer>\n</body>\n</html>\n"
    ))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn human_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = size as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{size} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

fn fmt_dt(dt: Option<DateTime<Utc>>) -> String {
    dt.map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "—".to_string())
}

/// Days remaining until expiry, for the list page. Consistent with the
/// API's read-time expiry check: an object past its `expires_at` is
/// expired, even if it is still physically in the bucket.
fn days_remaining(expires_at: Option<DateTime<Utc>>) -> String {
    let Some(exp) = expires_at else {
        return "—".to_string();
    };
    let now = Utc::now();
    if exp <= now {
        return "expired".to_string();
    }
    let secs = (exp - now).num_seconds();
    // Ceiling division, in stable Rust (div_ceil is still unstable).
    let days = (secs + 86_400 - 1) / 86_400;
    if days == 1 {
        "1 day".to_string()
    } else {
        format!("{days} days")
    }
}

fn ttl_options(selected: u32) -> String {
    let mut out = String::new();
    for days in [1u32, 3, 7, 14, 30, 90] {
        let sel = if days == selected { " selected" } else { "" };
        out.push_str(&format!(
            "<option value=\"{days}\"{sel}>{days} day{}</option>",
            if days == 1 { "" } else { "s" }
        ));
    }
    out
}

/// Return true when the request's `Origin`, or its fallback `Referer`,
/// has the same authority as the `Host` header.
///
/// The three browser write routes use this check. Only absolute
/// `http://` and `https://` values are accepted. The authority
/// comparison ignores ASCII case, and a missing port uses the scheme's
/// default port.
fn same_origin_allowed(headers: &HeaderMap) -> bool {
    let Some(host) = headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Authority>().ok())
    else {
        return false;
    };

    let candidate = match headers.get(ORIGIN) {
        Some(value) => match value.to_str() {
            Ok(value) => value,
            Err(_) => return false,
        },
        None => match headers.get(REFERER).and_then(|value| value.to_str().ok()) {
            Some(value) => value,
            None => return false,
        },
    };

    let Ok(uri) = candidate.parse::<Uri>() else {
        return false;
    };
    let Some(scheme) = uri.scheme_str() else {
        return false;
    };
    let is_https = scheme.eq_ignore_ascii_case("https");
    if !is_https && !scheme.eq_ignore_ascii_case("http") {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    if authority.as_str().contains('@') {
        return false;
    }
    let default_port = if is_https { 443 } else { 80 };
    authority.host().eq_ignore_ascii_case(host.host())
        && authority.port_u16().unwrap_or(default_port) == host.port_u16().unwrap_or(default_port)
}

fn forbidden_page() -> Response {
    (
        StatusCode::FORBIDDEN,
        Html("<p>Forbidden: the request origin does not match the Host header.</p>".to_string()),
    )
        .into_response()
}

async fn index_handler(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let default_ttl = state.store.config.default_ttl_days;
    page(
        "New paste",
        &format!(
            r#"<h2>Paste text</h2>
<form method="post" action="/ui/text">
<p><label>Text<br><textarea name="text" required></textarea></label></p>
<p><label>Filename (optional) <input type="text" name="filename"></label></p>
<p><label>Keep for <select name="ttl_days">{}</select></label></p>
<p><button type="submit">Create paste</button></p>
</form>
<hr>
<h2>Upload a file</h2>
<form method="post" action="/ui/upload" enctype="multipart/form-data">
<p><label>File <input type="file" name="file" required></label></p>
<p><label>Keep for <select name="ttl_days">{}</select></label></p>
<p><button type="submit">Upload file</button></p>
</form>
<p class="muted">The selector uses whole days. API clients can send a sub-day TTL with X-TTL. Expiry is checked on read; the bucket sweeper is day-rounded. The bucket itself is private; only this binary talks to Garage.</p>"#,
            ttl_options(default_ttl),
            ttl_options(default_ttl),
        ),
    )
    .into_response()
}

#[derive(Debug, Deserialize)]
struct TextForm {
    text: String,
    filename: Option<String>,
    ttl_days: Option<u32>,
}

async fn text_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<TextForm>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let filename = form.filename.filter(|f| !f.trim().is_empty());
    let ttl = match form.ttl_days {
        Some(days) => match Ttl::checked_days(days) {
            Ok(ttl) => Some(ttl),
            Err(e) => return bad_request_page(&e.to_string()),
        },
        None => None,
    };
    match state
        .store
        .store_text(form.text.as_bytes(), filename.as_deref(), None, ttl)
        .await
    {
        Ok(stored) => success_page(
            &stored.id,
            &stored.content_type,
            stored.size,
            stored.expires_at,
        ),
        Err(e) => error_page(e),
    }
}

fn success_page(id: &str, content_type: &str, size: u64, expires_at: DateTime<Utc>) -> Response {
    let id_esc = html_escape(id);
    page(
        "Paste created",
        &format!(
            "<p>URL: <a href=\"/view/{id_esc}\">/view/{id_esc}</a></p>\
             <p>Raw API URL: <a href=\"/v1/pastes/{id_esc}\">/v1/pastes/{id_esc}</a></p>\
             <p>Paste/file ID: <code>{id_esc}</code></p>\
             <p>Content-Type: {}</p>\
             <p>Size: {}</p>\
             <p>Expires: {}</p>\
             <p><a href=\"/\">Create another</a> · <a href=\"/pastes\">Current pastes</a></p>",
            html_escape(content_type),
            human_size(size),
            expires_at.format("%Y-%m-%d %H:%M UTC"),
        ),
    )
    .into_response()
}

fn error_page(e: StoreError) -> Response {
    let (status, msg) = match &e {
        StoreError::TooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, e.to_string()),
        StoreError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
        StoreError::Expired(_) => (StatusCode::GONE, e.to_string()),
        StoreError::AliasUnavailable { .. } => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        _ => (StatusCode::BAD_REQUEST, e.to_string()),
    };
    (
        status,
        page(
            "Error",
            &format!("<p>{}</p><p><a href=\"/\">Back</a></p>", html_escape(&msg)),
        ),
    )
        .into_response()
}

/// A malformed multipart body is a client error, not a missing field.
fn bad_request_page(message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        page(
            "Error",
            &format!(
                "<p>{}</p><p><a href=\"/\">Back</a></p>",
                html_escape(message)
            ),
        ),
    )
        .into_response()
}

async fn upload_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    let mut ttl: Option<Ttl> = None;
    // Stream the file field inside the loop, not moved out of it: an
    // axum Field borrows its Multipart, so holding it across a
    // next_field call does not compile (see http.rs for the same note).
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return bad_request_page(&format!("invalid multipart body: {e}")),
        };
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let field_content_type = field.content_type().map(|s| s.to_string());
            let filename = field.file_name().map(|s| s.to_string());
            let max_bytes = state.store.config.max_bytes_for_upload(filename.as_deref());
            let stream = futures::stream::unfold(field, |mut f| async move {
                match f.chunk().await {
                    Ok(Some(chunk)) => Some((Ok(chunk), f)),
                    Ok(None) => None,
                    Err(e) => Some((Err(StoreError::Io(e.to_string())), f)),
                }
            });
            return match state
                .store
                .store_stream(
                    Box::pin(stream),
                    filename.as_deref(),
                    field_content_type.as_deref(),
                    ttl,
                    None,
                    max_bytes,
                )
                .await
            {
                Ok(stored) => success_page(
                    &stored.id,
                    &stored.content_type,
                    stored.size,
                    stored.expires_at,
                ),
                Err(e) => error_page(e),
            };
        }
        if name == "ttl_days" {
            if let Ok(text) = field.text().await {
                match Ttl::parse_days(&text) {
                    Ok(parsed) => ttl = Some(parsed),
                    Err(e) => return bad_request_page(&e.to_string()),
                }
            }
        }
    }
    error_page(StoreError::InvalidId(
        "upload requires a 'file' field".into(),
    ))
}

async fn list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let date = query.get("date").cloned().unwrap_or_default();
    let start = query.get("start").cloned().unwrap_or_default();
    let end = query.get("end").cloned().unwrap_or_default();
    let request = match parse_list_window_request(
        query.get("date").map(String::as_str),
        query.get("start").map(String::as_str),
        query.get("end").map(String::as_str),
    ) {
        Ok(request) => request,
        Err(e) => return error_page(e),
    };
    // list_window already filters expired objects (HeadObject per key),
    // so this page is consistent with the API's read-time expiry check.
    let items = match &request {
        ListWindowRequest::Date(date) => state.store.list_recent(Some(date), 1000).await,
        ListWindowRequest::Range { start, end } => {
            state.store.list_window(Some(start), Some(end), 1000).await
        }
        ListWindowRequest::Default => state.store.list_window(None, None, 1000).await,
    };
    let items = match items {
        Ok(items) => items,
        Err(e) => return error_page(e),
    };
    let mut rows = String::new();
    for item in &items {
        let id_esc = html_escape(&item.id);
        rows.push_str(&format!(
            "<tr><td><a href=\"/view/{id_esc}\">{id_esc}</a></td>\
             <td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
             <td><form method=\"post\" action=\"/ui/delete\" style=\"display:inline\"><input type=\"hidden\" name=\"id\" value=\"{id_esc}\"><button type=\"submit\">Delete</button></form></td></tr>\n",
            html_escape(item.filename.as_deref().unwrap_or("—")),
            html_escape(item.content_type.as_deref().unwrap_or("—")),
            human_size(item.size),
            fmt_dt(item.created_at),
            fmt_dt(item.expires_at),
            days_remaining(item.expires_at),
        ));
    }
    if rows.is_empty() {
        rows.push_str(
            "<tr><td colspan=\"8\" class=\"muted\">No current pastes in this window.</td></tr>\n",
        );
    }
    page(
        "Current pastes",
        &format!(
            r#"<form method="get" action="/pastes"><p><label>Date <input type="date" name="date" value="{}"></label> <button type="submit">Show date</button></p></form>
<form method="get" action="/pastes"><p><label>Start <input type="date" name="start" value="{}"></label> <label>End <input type="date" name="end" value="{}"></label> <button type="submit">Show range</button></p></form>
<p class="muted">Dates are UTC. With no dates, the page shows yesterday and today in UTC, so a Pacific evening is included. Built from a direct bucket scan (ListObjectsV2 for each date prefix, plus HeadObject per paste). No database. Listing is O(n) in objects scanned and gets slower as the bucket grows — that slowdown is the trigger for adding a database later.</p>
<table><thead><tr><th>Link / ID</th><th>Filename</th><th>Content-Type</th><th>Size</th><th>Created</th><th>Expires</th><th>Remaining</th><th></th></tr></thead><tbody>{}</tbody></table>"#,
            html_escape(&date),
            html_escape(&start),
            html_escape(&end),
            rows,
        ),
    )
    .into_response()
}

#[derive(Debug, Deserialize)]
struct DeleteForm {
    id: String,
}

async fn delete_submit_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<DeleteForm>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    if !same_origin_allowed(&headers) {
        return forbidden_page();
    }
    match state.store.delete(&form.id).await {
        Ok(()) => Redirect::to("/pastes").into_response(),
        Err(e) => error_page(e),
    }
}

async fn view_inner(state: &AppState, headers: &HeaderMap, id: &str) -> Response {
    if let Err(resp) = check_auth(state, headers) {
        return resp;
    }
    let meta = match state.store.head_metadata(id).await {
        Ok(m) => m,
        Err(e) => return error_page(e),
    };
    if meta.expired {
        return error_page(StoreError::Expired(meta.id));
    }
    let id_esc = html_escape(&meta.id);
    let main = meta.content_type.split(';').next().unwrap_or("").trim();
    let mut body = format!(
        "<p>Filename: {}</p><p>Content-Type: {}</p><p>Size: {}</p><p>Created: {}</p><p>Expires: {} ({})</p>",
        html_escape(meta.filename.as_deref().unwrap_or("—")),
        html_escape(&meta.content_type),
        human_size(meta.size),
        fmt_dt(meta.created_at),
        fmt_dt(meta.expires_at),
        days_remaining(meta.expires_at),
    );
    if main == "text/plain"
        || main == "text/markdown"
        || main == "text/csv"
        || main == "application/json"
    {
        // Text view, capped at 1 MiB so the page stays usable; the API
        // remains the route for full / sliced reads of large pastes.
        let slice = state.store.fetch_text(&meta.id, 0, Some(1_000_000)).await;
        match slice {
            Ok(s) => {
                body.push_str(&format!("<pre>{}</pre>", html_escape(&s.text)));
                if s.truncated {
                    body.push_str("<p class=\"muted\">Text truncated at 1 MiB for this view. Use the raw API URL below for the full paste.</p>");
                }
            }
            Err(e) => return error_page(e),
        }
    } else if main.starts_with("image/") && main != "image/svg+xml" {
        // Safe images inline. SVG and HTML are never rendered inline
        // here (see README safety note); they get a download link.
        body.push_str(&format!(
            "<p><img src=\"/v1/pastes/{id_esc}\" alt=\"{}\"></p>",
            html_escape(meta.filename.as_deref().unwrap_or("image"))
        ));
    }
    body.push_str(&format!(
        "<p><a href=\"/v1/pastes/{id_esc}\">Download / raw</a> · <a href=\"/pastes\">Current pastes</a></p>"
    ));
    // Delete from the view page too, as a plain form.
    body.push_str(&format!(
        "<form method=\"post\" action=\"/ui/delete\"><input type=\"hidden\" name=\"id\" value=\"{id_esc}\"><button type=\"submit\">Delete this paste</button></form>"
    ));
    page("View paste", &body).into_response()
}

async fn view_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    view_inner(&state, &headers, &format!("{date}/{random}")).await
}

async fn view_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    view_inner(&state, &headers, &random).await
}

/// UI routes, without state attached. `main` merges these with the
/// API routes and attaches a single shared [`AppState`].
pub fn ui_router() -> Router<AppState> {
    // See http.rs: the store caps streams while it reads them. The 2 MiB
    // axum default would reject real file uploads early. Keep the limit on
    // the form routes, which buffer their bodies in memory.
    let upload_routes = Router::new()
        .route("/ui/upload", post(upload_submit_handler))
        .layer(DefaultBodyLimit::disable());
    Router::new()
        .merge(upload_routes)
        .route("/", get(index_handler))
        .route("/ui/text", post(text_submit_handler))
        .route("/ui/delete", post(delete_submit_handler))
        .route("/pastes", get(list_handler))
        .route("/view/{date}/{random}", get(view_dated))
        .route("/view/{random}", get(view_bare))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::header::{CONTENT_TYPE, HOST, ORIGIN, REFERER};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;
    use crate::store::Store;

    const TEST_TOKEN: &str = "correct-horse-battery-staple";

    fn test_config() -> Config {
        Config {
            endpoint: "http://127.0.0.1:1".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            max_text_bytes: 10 * 1024 * 1024,
            max_file_bytes: 1024 * 1024 * 1024,
            default_ttl_days: 7,
            http_bind: "127.0.0.1:0".to_string(),
            http_bearer_token: None,
            mcp_local_root: None,
        }
    }

    async fn test_state(token: Option<&str>) -> AppState {
        let mut config = test_config();
        config.http_bearer_token = token.map(str::to_string);
        AppState {
            store: Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        }
    }

    /// Call the UI list route. A passing token reaches the unreachable test
    /// S3 endpoint, and the UI maps that S3 error to 400. A 401 means the
    /// auth check rejected the request.
    async fn ui_list_status(state: AppState, auth: Option<&str>) -> StatusCode {
        let app = ui_router().with_state(state);
        let mut builder = Request::builder().method("GET").uri("/pastes");
        if let Some(auth) = auth {
            builder = builder.header(axum::http::header::AUTHORIZATION, auth);
        }
        let request = builder.body(Body::empty()).expect("request must build");
        app.oneshot(request)
            .await
            .expect("router call must complete")
            .status()
    }

    #[tokio::test]
    async fn ui_wrong_bearer_token_returns_401() {
        let state = test_state(Some(TEST_TOKEN)).await;
        assert_eq!(
            ui_list_status(state, Some("Bearer wrong-horse-battery-staple")).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn ui_right_bearer_token_passes_auth_and_reaches_the_store() {
        let state = test_state(Some(TEST_TOKEN)).await;
        let auth = format!("Bearer {TEST_TOKEN}");
        assert_eq!(
            ui_list_status(state, Some(&auth)).await,
            StatusCode::BAD_REQUEST,
            "the right token must pass auth and reach the S3 sink"
        );
    }

    #[tokio::test]
    async fn ui_without_configured_token_passes_auth_and_reaches_the_store() {
        let state = test_state(None).await;
        assert_eq!(
            ui_list_status(state, None).await,
            StatusCode::BAD_REQUEST,
            "an unset token must leave the UI routes open"
        );
    }

    #[test]
    fn html_escape_covers_script() {
        assert_eq!(
            html_escape("<script>alert(\"x\")</script>"),
            "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"
        );
    }

    #[test]
    fn human_size_units() {
        assert_eq!(human_size(0), "0 B");
        assert_eq!(human_size(512), "512 B");
        assert_eq!(human_size(2048), "2.0 KiB");
        assert_eq!(human_size(5 * 1024 * 1024), "5.0 MiB");
    }

    #[test]
    fn days_remaining_expired_and_future() {
        let past = Utc::now() - chrono::Duration::days(1);
        assert_eq!(days_remaining(Some(past)), "expired");
        let future = Utc::now() + chrono::Duration::days(3);
        // Ceil of ~3 days; allow the exact string for 3 days.
        assert_eq!(days_remaining(Some(future)), "3 days");
        assert_eq!(days_remaining(None), "—");
    }

    fn same_origin(host: &str, origin: Option<&str>, referer: Option<&str>) -> bool {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, host.parse().expect("host header"));
        if let Some(origin) = origin {
            headers.insert(ORIGIN, origin.parse().expect("origin header"));
        }
        if let Some(referer) = referer {
            headers.insert(REFERER, referer.parse().expect("referer header"));
        }
        same_origin_allowed(&headers)
    }

    #[test]
    fn same_origin_origin_is_allowed() {
        assert!(same_origin(
            "dayfly.example",
            Some("https://dayfly.example"),
            None
        ));
    }

    #[test]
    fn cross_site_origin_is_denied() {
        assert!(!same_origin(
            "dayfly.example",
            Some("https://evil.example"),
            None
        ));
    }

    #[test]
    fn same_origin_referer_with_path_is_allowed() {
        assert!(same_origin(
            "dayfly.example:8080",
            None,
            Some("http://dayfly.example:8080/pastes")
        ));
    }

    #[test]
    fn cross_site_referer_is_denied() {
        assert!(!same_origin(
            "dayfly.example",
            None,
            Some("https://evil.example/pastes")
        ));
    }

    #[test]
    fn missing_origin_and_referer_is_denied() {
        assert!(!same_origin("dayfly.example", None, None));
    }

    #[test]
    fn null_origin_is_denied() {
        assert!(!same_origin("dayfly.example", Some("null"), None));
    }

    #[test]
    fn malformed_origin_is_denied() {
        assert!(!same_origin("dayfly.example", Some("https://"), None));
        assert!(!same_origin("dayfly.example", Some("not a url"), None));
    }

    #[test]
    fn malformed_origin_does_not_fall_back_to_referer() {
        assert!(!same_origin(
            "dayfly.example",
            Some("null"),
            Some("https://dayfly.example/pastes")
        ));
    }

    #[test]
    fn host_comparison_ignores_ascii_case() {
        assert!(same_origin(
            "DAYFLY.EXAMPLE",
            Some("https://dayfly.EXAMPLE"),
            None
        ));
    }

    #[test]
    fn default_port_matches_omitted_port() {
        assert!(same_origin(
            "dayfly.example",
            Some("https://dayfly.example:443"),
            None
        ));
    }

    #[tokio::test]
    async fn cross_site_delete_post_is_forbidden() {
        let state = AppState {
            store: Store::from_config(test_config())
                .await
                .expect("test S3 config must build without network access"),
        };
        let app = ui_router().with_state(state);
        let request = Request::builder()
            .method("POST")
            .uri("/ui/delete")
            .header(HOST, "dayfly.example")
            .header(ORIGIN, "https://evil.example")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("id=2026-10-28/abc123"))
            .expect("request must build");
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "a cross-site delete post must be rejected"
        );
    }
}
