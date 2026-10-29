//! HTTP mode (axum) for plain-curl agents.
//!
//! The binary is the sole S3 reader/writer. Agents talk only to this
//! HTTP interface (or to MCP over stdio); they never get Garage
//! credentials and no presigned URL is ever issued.
//!
//! Deployment note (see README): the intended deployment is
//! private-network / localhost. When `DAYFLY_HTTP_BEARER_TOKEN` is
//! set, every HTTP route requires `Authorization: Bearer <token>`.
//! That token is a simple option for the binary's own endpoint; it is
//! not the Garage credential and bucket privacy is not a substitute
//! for securing this endpoint.

use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Query, Request, State};
use axum::http::header::{
    ACCEPT, AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, HOST, RANGE,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::content_type::content_disposition;
use crate::store::{
    parse_range_header, Store, StoreError, Ttl, TtlParseError, MULTIPART_CHUNK_BYTES,
};

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
}

/// Check the bearer token for a route. The API routes and the human UI
/// routes share this one function, so both use the constant-time compare.
// Axum handlers return `Response` directly; boxing it here would only add
// indirection at every call site.
#[allow(clippy::result_large_err)]
pub(crate) fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(token) = state.store.config.http_bearer_token.as_deref() else {
        return Ok(());
    };
    // Compare the credential in constant time. A byte-by-byte comparison
    // stops at the first difference, so response timing would reveal how
    // many leading bytes of the token are correct. The credential length
    // is still visible through the timing, which is accepted.
    // A non-UTF-8 header value is rejected here.
    let ok = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| bearer_matches(v, token));
    if ok {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response())
    }
}

/// Return true when an `Authorization` header value carries the expected
/// bearer token. The scheme is case-insensitive. The credential is compared
/// with `subtle::ConstantTimeEq`, so the comparison time does not depend on
/// the number of matching leading bytes. A credential length difference
/// still leaks through the timing, which is accepted.
fn bearer_matches(header_value: &str, token: &str) -> bool {
    let Some((scheme, credential)) = header_value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return false;
    }
    // Reject an empty credential and any extra whitespace. Only one ASCII
    // space separates the scheme from the credential.
    if credential.is_empty() || credential.contains(|c: char| c.is_ascii_whitespace()) {
        return false;
    }
    credential.as_bytes().ct_eq(token.as_bytes()).into()
}

fn status_for(e: &StoreError) -> StatusCode {
    match e {
        StoreError::NotFound(_) => StatusCode::NOT_FOUND,
        StoreError::Expired(_) => StatusCode::GONE,
        StoreError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        StoreError::InvalidId(_) | StoreError::InvalidDate(_) | StoreError::InvalidRange(_) => {
            StatusCode::BAD_REQUEST
        }
        StoreError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        StoreError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        StoreError::S3(_) => StatusCode::BAD_GATEWAY,
        StoreError::BucketUnavailable(_) => StatusCode::BAD_GATEWAY,
    }
}

fn error_response(e: StoreError) -> Response {
    let status = status_for(&e);
    (status, Json(serde_json::json!({"error": e.to_string()}))).into_response()
}

/// A malformed multipart body is a client error, not an S3 or I/O failure.
fn bad_request(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error": message})),
    )
        .into_response()
}

/// A 413 for a text paste points at the way out: a filename uses the file
/// cap. Other errors, and file uploads, keep the generic message.
fn upload_error_response(e: StoreError, text_paste: bool) -> Response {
    match e {
        StoreError::TooLarge { limit } if text_paste => (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "error": format!(
                    "payload too large: limit is {limit} bytes for a text paste; send X-Filename for larger files"
                )
            })),
        )
            .into_response(),
        other => error_response(other),
    }
}

#[derive(Serialize)]
struct UploadEnvelope {
    id: String,
    url: String,
    size: u64,
    sha256: String,
    expires_at: String,
    content_type: String,
    filename: Option<String>,
    detection_source: Option<String>,
    detection_conflict: Option<String>,
}

#[derive(Serialize)]
struct MetaEnvelope {
    id: String,
    key: String,
    filename: Option<String>,
    content_type: String,
    detection_source: Option<String>,
    detection_conflict: Option<String>,
    size: u64,
    sha256: Option<String>,
    created_at: Option<String>,
    expires_at: Option<String>,
    expired: bool,
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TtlSource {
    /// `X-TTL`, `ttl`, or a `ttl` form field. Prefer this form.
    Duration,
    /// `X-TTL-Days`, `ttl_days`, or a `ttl_days` form field.
    Days,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TtlInput {
    ttl: Ttl,
    source: TtlSource,
}

impl TtlInput {
    fn duration(ttl: Ttl) -> Self {
        Self {
            ttl,
            source: TtlSource::Duration,
        }
    }

    fn days(ttl: Ttl) -> Self {
        Self {
            ttl,
            source: TtlSource::Days,
        }
    }
}

fn ttl_error_response(e: TtlParseError) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error": e.to_string()})),
    )
        .into_response()
}

fn parse_days(value: &str) -> Result<Ttl, TtlParseError> {
    value
        .trim()
        .parse::<u32>()
        .map(Ttl::days)
        .map_err(|_| TtlParseError::invalid_days(value))
}

/// Apply a multipart `ttl` or `ttl_days` field.
///
/// A duration always wins over a day count, regardless of field order.
/// A later `ttl_days` field cannot replace a duration that is already set.
fn apply_ttl_field(
    current: Option<TtlInput>,
    name: &str,
    value: &str,
) -> Result<Option<TtlInput>, TtlParseError> {
    match name {
        "ttl" => Ttl::parse(value).map(|ttl| Some(TtlInput::duration(ttl))),
        "ttl_days" => {
            let parsed = parse_days(value)?;
            if current.is_some_and(|input| input.source == TtlSource::Duration) {
                Ok(current)
            } else {
                Ok(Some(TtlInput::days(parsed)))
            }
        }
        _ => Ok(current),
    }
}

/// Read a TTL from headers and query parameters.
///
/// A duration from `X-TTL` or `ttl` wins over a day count from
/// `X-TTL-Days` or `ttl_days`. A header wins over the query parameter of
/// the same form. An invalid value is an error, not a fallback.
fn parse_ttl(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> Result<Option<TtlInput>, TtlParseError> {
    if let Some(value) = header_str(headers, "x-ttl").or_else(|| query.get("ttl").cloned()) {
        return Ttl::parse(&value).map(|ttl| Some(TtlInput::duration(ttl)));
    }
    if let Some(value) =
        header_str(headers, "x-ttl-days").or_else(|| query.get("ttl_days").cloned())
    {
        return parse_days(&value).map(|ttl| Some(TtlInput::days(ttl)));
    }
    Ok(None)
}

fn wants_json(headers: &HeaderMap, query: &HashMap<String, String>) -> bool {
    if query.get("format").map(|s| s.as_str()) == Some("json") {
        return true;
    }
    headers
        .get(ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("application/json"))
        .unwrap_or(false)
}

fn paste_url(headers: &HeaderMap, id: &str) -> String {
    let path = format!("/v1/pastes/{id}");
    let Some(host) = header_str(headers, HOST.as_str()) else {
        return path;
    };
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .filter(|s| *s == "https" || *s == "http")
        .unwrap_or("http");
    format!("{scheme}://{host}{path}")
}

fn upload_response(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
    stored: &crate::store::StoredPaste,
) -> Response {
    let url = paste_url(headers, &stored.id);
    if wants_json(headers, query) {
        (
            StatusCode::CREATED,
            Json(UploadEnvelope {
                id: stored.id.clone(),
                url,
                size: stored.size,
                sha256: stored.sha256.clone(),
                expires_at: stored.expires_at.to_rfc3339(),
                content_type: stored.content_type.clone(),
                filename: stored.filename.clone(),
                detection_source: stored.detection_source.clone(),
                detection_conflict: stored.detection_conflict.clone(),
            }),
        )
            .into_response()
    } else {
        // Plain-text URL only, so `url=$(curl ...)` works.
        (StatusCode::CREATED, format!("{url}\n")).into_response()
    }
}

/// Single upload entry point for POST /upload and POST /v1/pastes.
/// Dispatches on Content-Type: multipart/form-data goes to the
/// multipart handler, everything else is a raw streaming body
/// (Content-Length or chunked transfer encoding).
async fn upload_dispatch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let content_type = header_str(&headers, CONTENT_TYPE.as_str()).unwrap_or_default();
    if content_type.starts_with("multipart/form-data") {
        match Multipart::from_request(req, &()).await {
            Ok(multipart) => multipart_inner(state, headers, query, multipart).await,
            Err(e) => bad_request(format!("invalid multipart body: {e}")),
        }
    } else {
        let filename =
            header_str(&headers, "x-filename").or_else(|| query.get("filename").cloned());
        let ttl = match parse_ttl(&headers, &query) {
            Ok(ttl) => ttl.map(|input| input.ttl),
            Err(e) => return ttl_error_response(e),
        };
        let known_length = header_str(&headers, CONTENT_LENGTH.as_str())
            .and_then(|s| s.trim().parse::<u64>().ok());
        // A filename means a file upload; no filename means a text paste,
        // which must obey the smaller text cap rather than the file cap.
        let text_paste = filename
            .as_deref()
            .is_none_or(|name| name.trim().is_empty());
        let max_bytes = state.store.config.max_bytes_for_upload(filename.as_deref());
        let body = req.into_body();
        let stream = body
            .into_data_stream()
            .map(|item| item.map_err(|e| StoreError::Io(e.to_string())));
        // The Content-Type here is the raw-body type, including curl's
        // --data-binary default, which detection treats as unspecified.
        let explicit = if content_type.is_empty() {
            None
        } else {
            Some(content_type.as_str())
        };
        match state
            .store
            .store_stream(
                Box::pin(stream),
                filename.as_deref(),
                explicit,
                ttl,
                known_length,
                max_bytes,
            )
            .await
        {
            Ok(stored) => upload_response(&headers, &query, &stored),
            Err(e) => upload_error_response(e, text_paste),
        }
    }
}

async fn multipart_inner(
    state: AppState,
    headers: HeaderMap,
    query: HashMap<String, String>,
    mut multipart: Multipart,
) -> Response {
    let mut filename: Option<String> =
        header_str(&headers, "x-filename").or_else(|| query.get("filename").cloned());
    let mut ttl = match parse_ttl(&headers, &query) {
        Ok(ttl) => ttl,
        Err(e) => return ttl_error_response(e),
    };
    // The file field is streamed inside the loop, not moved out of it:
    // an axum Field borrows its Multipart, so holding it across a
    // next_field call does not compile. Streaming inside the iteration
    // keeps the field alive only while it is being read. Text fields
    // after the file field are not read (see README); use
    // headers/query for filename/TTL instead.
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(field)) => field,
            Ok(None) => break,
            Err(e) => return bad_request(format!("invalid multipart body: {e}")),
        };
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let field_content_type = field.content_type().map(|s| s.to_string());
            if filename.is_none() {
                filename = field.file_name().map(|s| s.to_string());
            }
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
                    ttl.map(|input| input.ttl),
                    None,
                    max_bytes,
                )
                .await
            {
                Ok(stored) => upload_response(&headers, &query, &stored),
                Err(e) => error_response(e),
            };
        }
        if name == "filename" {
            if let Ok(text) = field.text().await {
                if !text.trim().is_empty() {
                    filename = Some(text.trim().to_string());
                }
            }
        } else if name == "ttl" || name == "ttl_days" {
            if let Ok(text) = field.text().await {
                match apply_ttl_field(ttl, &name, &text) {
                    Ok(next) => ttl = next,
                    Err(e) => return ttl_error_response(e),
                }
            }
        }
    }
    error_response(StoreError::InvalidId(
        "multipart upload requires a 'file' field".into(),
    ))
}

async fn meta_inner(state: &AppState, headers: &HeaderMap, id: &str) -> Response {
    if let Err(resp) = check_auth(state, headers) {
        return resp;
    }
    match state.store.head_metadata(id).await {
        Ok(meta) => {
            if meta.expired {
                return error_response(StoreError::Expired(meta.id));
            }
            Json(MetaEnvelope {
                id: meta.id,
                key: meta.key,
                filename: meta.filename,
                content_type: meta.content_type,
                detection_source: meta.detection_source,
                detection_conflict: meta.detection_conflict,
                size: meta.size,
                sha256: meta.sha256,
                created_at: meta.created_at.map(|d| d.to_rfc3339()),
                expires_at: meta.expires_at.map(|d| d.to_rfc3339()),
                expired: meta.expired,
            })
            .into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn download_inner(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
    is_head: bool,
) -> Response {
    if let Err(resp) = check_auth(state, headers) {
        return resp;
    }
    let meta = match state.store.head_metadata(id).await {
        Ok(m) => m,
        Err(e) => return error_response(e),
    };
    if meta.expired {
        return error_response(StoreError::Expired(meta.id.clone()));
    }
    let range_header = header_str(headers, RANGE.as_str());
    let range = match &range_header {
        Some(h) => match parse_range_header(h, meta.size) {
            Ok(r) => Some(r),
            Err(StoreError::RangeNotSatisfiable) => {
                return (
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    [(
                        axum::http::header::CONTENT_RANGE,
                        format!("bytes */{}", meta.size),
                    )],
                    Json(serde_json::json!({"error": "range not satisfiable"})),
                )
                    .into_response();
            }
            Err(e) => return error_response(e),
        },
        None => None,
    };
    let (start, end) = range.unwrap_or((0, meta.size.saturating_sub(1)));
    let length = if meta.size == 0 { 0 } else { end - start + 1 };
    let mut resp_headers = HeaderMap::new();
    resp_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&meta.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    resp_headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(
            &meta.content_type,
            meta.filename.as_deref(),
        ))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
    );
    resp_headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    resp_headers.insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    if range.is_some() {
        resp_headers.insert(
            axum::http::header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{}", meta.size))
                .unwrap_or_else(|_| HeaderValue::from_static("bytes */0")),
        );
    }
    resp_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&length.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("0")),
    );
    if is_head || meta.size == 0 {
        let status = if range.is_some() && meta.size > 0 {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        return (status, resp_headers, Body::empty()).into_response();
    }
    // Stream back in 8 MiB range chunks: the server holds one chunk at
    // a time, never the whole object.
    let store = state.store.clone();
    let object_key = meta.key.clone();
    let stream = futures::stream::unfold(
        (start, end, store, object_key),
        move |(cur, end, store, key)| async move {
            if cur > end {
                return None;
            }
            let chunk_end = (cur + MULTIPART_CHUNK_BYTES as u64 - 1).min(end);
            match store.fetch_range_by_key(&key, Some((cur, chunk_end))).await {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        return None;
                    }
                    let next = chunk_end + 1;
                    Some((
                        Ok::<Bytes, StoreError>(Bytes::from(bytes)),
                        (next, end, store, key),
                    ))
                }
                // Propagate instead of ending the body. Ending it here sent a
                // `200 OK` with the full `Content-Length` already on the wire
                // and only part of the object delivered, which reads to a
                // client as a short but successful read.
                Err(e) => Some((Err(e), (end.saturating_add(1), end, store, key))),
            }
        },
    );
    let body_stream = stream.map(|item| item.map_err(|e| std::io::Error::other(e.to_string())));
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    (status, resp_headers, Body::from_stream(body_stream)).into_response()
}

async fn list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let date = query.get("date").map(|s| s.as_str());
    let limit = query
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    match state.store.list_recent(date, limit).await {
        Ok(items) => Json(serde_json::json!({
            "date": date,
            "items": items.iter().map(|i| serde_json::json!({
                "id": i.id,
                "key": i.key,
                "size": i.size,
                "last_modified": i.last_modified.map(|d| d.to_rfc3339()),
                "expires_at": i.expires_at.map(|d| d.to_rfc3339()),
                "created_at": i.created_at.map(|d| d.to_rfc3339()),
                "content_type": i.content_type,
                "filename": i.filename,
            })).collect::<Vec<_>>(),
        }))
        .into_response(),
        Err(e) => error_response(e),
    }
}

// --- Thin route handlers: two-segment (date/random) and one-segment
// (bare random) variants, avoiding wildcard routing ambiguity with /meta.

async fn download_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    download_inner(&state, &headers, &format!("{date}/{random}"), false).await
}

async fn download_dated_head(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    download_inner(&state, &headers, &format!("{date}/{random}"), true).await
}

async fn download_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    download_inner(&state, &headers, &random, false).await
}

async fn download_bare_head(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    download_inner(&state, &headers, &random, true).await
}

async fn meta_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    meta_inner(&state, &headers, &format!("{date}/{random}")).await
}

async fn meta_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    meta_inner(&state, &headers, &random).await
}

async fn delete_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    match state.store.delete(&format!("{date}/{random}")).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_response(e),
    }
}

async fn delete_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    match state.store.delete(&random).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_response(e),
    }
}

/// API routes, without state attached. `main` merges these with the
/// human UI routes and attaches a single shared [`AppState`].
pub fn api_router() -> Router<AppState> {
    // Uploads are streamed and capped by the store while they are read.
    // The axum default limit (2 MiB) would reject real file uploads before
    // the store can apply the file cap or abort a multipart upload cleanly.
    // Keep the limit on the other routes, where bodies are buffered.
    let upload_routes = Router::new()
        .route("/upload", post(upload_dispatch))
        .route("/v1/pastes", post(upload_dispatch).get(list_handler))
        .layer(DefaultBodyLimit::disable());
    Router::new()
        .merge(upload_routes)
        .route(
            "/v1/pastes/{date}/{random}",
            get(download_dated)
                .head(download_dated_head)
                .delete(delete_dated),
        )
        .route(
            "/v1/pastes/{random}",
            get(download_bare)
                .head(download_bare_head)
                .delete(delete_bare),
        )
        .route("/v1/pastes/{date}/{random}/meta", get(meta_dated))
        .route("/v1/pastes/{random}/meta", get(meta_bare))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::Poll;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;

    type CapturedMetadata = Arc<std::sync::Mutex<Option<HashMap<String, String>>>>;

    fn test_config(max_text_bytes: u64, max_file_bytes: u64) -> Config {
        Config {
            endpoint: "http://127.0.0.1:1".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            max_text_bytes,
            max_file_bytes,
            default_ttl_days: 7,
            http_bind: "127.0.0.1:0".to_string(),
            http_bearer_token: None,
        }
    }

    async fn test_state(max_text_bytes: u64, max_file_bytes: u64) -> AppState {
        AppState {
            store: Store::from_config(test_config(max_text_bytes, max_file_bytes))
                .await
                .expect("test S3 config must build without network access"),
        }
    }

    async fn test_state_with_token(token: &str) -> AppState {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.http_bearer_token = Some(token.to_string());
        AppState {
            store: Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        }
    }

    /// HTTP auth schemes are case-insensitive. A `bearer` scheme with the
    /// right credential must pass authentication. The request then reaches
    /// the unreachable test S3 endpoint and returns 502; a 401 means the
    /// auth check rejected it.
    #[tokio::test]
    async fn lowercase_bearer_scheme_is_accepted() {
        let state = test_state_with_token("correct-horse").await;
        let app = api_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .header(AUTHORIZATION, "bearer correct-horse")
            .body(Body::empty())
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "a lowercase bearer scheme must pass auth and reach the S3 sink"
        );
    }

    const TEST_TOKEN: &str = "correct-horse-battery-staple";

    #[test]
    fn bearer_matches_accepts_the_expected_credential() {
        assert!(bearer_matches(&format!("Bearer {TEST_TOKEN}"), TEST_TOKEN));
    }

    #[test]
    fn bearer_matches_accepts_any_scheme_case() {
        assert!(bearer_matches(&format!("bearer {TEST_TOKEN}"), TEST_TOKEN));
        assert!(bearer_matches(&format!("BEARER {TEST_TOKEN}"), TEST_TOKEN));
        assert!(bearer_matches(&format!("BeArEr {TEST_TOKEN}"), TEST_TOKEN));
    }

    #[test]
    fn bearer_matches_rejects_a_wrong_credential_of_equal_length() {
        let wrong = "x".repeat(TEST_TOKEN.len());
        assert!(!bearer_matches(&format!("Bearer {wrong}"), TEST_TOKEN));
    }

    #[test]
    fn bearer_matches_rejects_a_wrong_credential_of_different_length() {
        assert!(!bearer_matches("Bearer short", TEST_TOKEN));
        assert!(!bearer_matches(
            &format!("Bearer {TEST_TOKEN}extra"),
            TEST_TOKEN
        ));
        assert!(!bearer_matches("Bearer ", TEST_TOKEN));
    }

    #[test]
    fn bearer_matches_rejects_a_wrong_scheme() {
        assert!(!bearer_matches(&format!("Basic {TEST_TOKEN}"), TEST_TOKEN));
        assert!(!bearer_matches(&format!("Token {TEST_TOKEN}"), TEST_TOKEN));
        assert!(!bearer_matches(TEST_TOKEN, TEST_TOKEN));
        assert!(!bearer_matches(
            &format!("Bearerx {TEST_TOKEN}"),
            TEST_TOKEN
        ));
    }

    #[test]
    fn bearer_matches_rejects_malformed_values() {
        for value in [
            "",
            "Bearer",
            "Bearer ",
            " Bearer correct-horse-battery-staple",
            "Bearer  correct-horse-battery-staple",
            "Bearer correct-horse-battery-staple ",
            "Bearer correct-horse-battery-staple extra",
            "Bearer\tcorrect-horse-battery-staple",
        ] {
            assert!(
                !bearer_matches(value, TEST_TOKEN),
                "malformed value must be rejected: {value:?}"
            );
        }
    }

    async fn status_for_auth_request(state: AppState, auth: Option<HeaderValue>) -> StatusCode {
        let app = api_router().with_state(state);
        let mut builder = Request::builder().method("GET").uri("/v1/pastes");
        if let Some(value) = auth {
            builder = builder.header(AUTHORIZATION, value);
        }
        let req = builder.body(Body::empty()).expect("request must build");
        app.oneshot(req)
            .await
            .expect("router call must complete")
            .status()
    }

    /// A wrong credential must be rejected with the documented 401 JSON
    /// error before any store call.
    #[tokio::test]
    async fn wrong_bearer_token_returns_401() {
        let state = test_state_with_token(TEST_TOKEN).await;
        let app = api_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .header(AUTHORIZATION, "Bearer wrong-horse-battery-staple")
            .body(Body::empty())
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024)
            .await
            .expect("error body must be readable");
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            String::from_utf8_lossy(&body),
            r#"{"error":"unauthorized"}"#
        );
    }

    #[tokio::test]
    async fn missing_authorization_header_returns_401() {
        let state = test_state_with_token(TEST_TOKEN).await;
        assert_eq!(
            status_for_auth_request(state, None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn non_utf8_authorization_header_returns_401() {
        let state = test_state_with_token(TEST_TOKEN).await;
        let value = HeaderValue::from_bytes(b"Bearer \xff\xfe").expect("opaque value must build");
        assert_eq!(
            status_for_auth_request(state, Some(value)).await,
            StatusCode::UNAUTHORIZED
        );
    }

    /// The right credential must pass auth. The request reaches the
    /// unreachable test S3 endpoint and returns 502. A full 200 needs a real
    /// S3 service.
    #[tokio::test]
    async fn right_bearer_token_passes_auth() {
        let state = test_state_with_token(TEST_TOKEN).await;
        assert_eq!(
            status_for_auth_request(
                state,
                Some(
                    HeaderValue::from_str(&format!("Bearer {TEST_TOKEN}"))
                        .expect("header value must build")
                )
            )
            .await,
            StatusCode::BAD_GATEWAY
        );
    }

    /// An unset token keeps the endpoint open, as before.
    #[tokio::test]
    async fn request_without_token_configured_passes_auth() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        assert_eq!(
            status_for_auth_request(state, None).await,
            StatusCode::BAD_GATEWAY
        );
    }

    /// A raw body with no filename is a text paste. Its declared length is
    /// over the text cap, so the handler must return 413 before polling the
    /// body. This is the HTTP-level counterpart of issue #2.
    #[tokio::test]
    async fn raw_text_upload_over_text_cap_is_rejected_before_body_read() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let app = api_router().with_state(state);

        let polled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&polled);
        let body = Body::from_stream(futures::stream::poll_fn(move |_| {
            flag.store(true, Ordering::SeqCst);
            Poll::<Option<Result<Bytes, std::io::Error>>>::Ready(None)
        }));

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_LENGTH, (64u64 * 1024 * 1024).to_string())
            .body(body)
            .expect("request must build");

        let resp = app.oneshot(req).await.expect("router call must complete");
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024)
            .await
            .expect("error body must be readable");
        let body = String::from_utf8_lossy(&body);
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "a no-filename upload over the text cap must be 413"
        );
        assert!(
            body.contains("send X-Filename"),
            "the 413 should name the remedy, got: {body}"
        );
        assert!(
            !polled.load(Ordering::SeqCst),
            "the body must not be read after an early cap rejection"
        );
    }

    /// A mock S3 endpoint that records the user metadata of the last
    /// PutObject request and accepts it.
    async fn mock_s3() -> (String, CapturedMetadata) {
        async fn capture_put(
            State(captured): State<CapturedMetadata>,
            headers: HeaderMap,
            _body: axum::body::Bytes,
        ) -> impl IntoResponse {
            let mut meta = HashMap::new();
            for (name, value) in headers.iter() {
                if let Some(key) = name.as_str().strip_prefix("x-amz-meta-") {
                    if let Ok(value) = value.to_str() {
                        meta.insert(key.to_string(), value.to_string());
                    }
                }
            }
            *captured.lock().expect("metadata lock must not be poisoned") = Some(meta);
            (
                StatusCode::OK,
                [(axum::http::header::ETAG, "\"test-etag\"")],
            )
        }
        let captured = Arc::new(std::sync::Mutex::new(None));
        let app = Router::new()
            .fallback(capture_put)
            .with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock S3 listener must bind");
        let addr = listener.local_addr().expect("mock S3 address must exist");
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock S3 server must run");
        });
        (format!("http://{addr}"), captured)
    }

    /// The mirror image of the rejection test: a filename must select the
    /// file cap. The request reaches the unreachable test S3 endpoint and
    /// returns 502; if the cap selection regressed to the text cap it would
    /// be rejected locally with 413 instead.
    #[tokio::test]
    async fn filename_selects_file_cap_at_the_http_boundary() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_LENGTH, (64u64 * 1024 * 1024).to_string())
            .header("x-filename", "large.bin")
            .body(Body::empty())
            .expect("request must build");

        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "X-Filename must select the file cap and reach the S3 sink"
        );
    }

    /// A sub-day TTL must reach S3 as an exact duration. The mock S3
    /// endpoint records the stored user metadata, so this test observes
    /// what the object would carry, not only the HTTP status.
    #[tokio::test]
    async fn x_ttl_header_stores_exact_hour_expiry() {
        let (endpoint, captured) = mock_s3().await;
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.endpoint = endpoint;
        let state = AppState {
            store: Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        };
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "5")
            .header("x-ttl", "1h")
            .body(Body::from("hello"))
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "a valid X-TTL header must be accepted"
        );

        let meta = captured
            .lock()
            .expect("metadata lock must not be poisoned")
            .take()
            .expect("the upload must reach S3");
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
    async fn x_ttl_takes_precedence_over_x_ttl_days_at_the_http_boundary() {
        let (endpoint, captured) = mock_s3().await;
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.endpoint = endpoint;
        let state = AppState {
            store: Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        };
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "5")
            .header("x-ttl", "1h30m")
            .header("x-ttl-days", "7")
            .body(Body::from("hello"))
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(resp.status(), StatusCode::CREATED);

        let meta = captured
            .lock()
            .expect("metadata lock must not be poisoned")
            .take()
            .expect("the upload must reach S3");
        assert_eq!(
            meta.get("ttl-seconds").map(String::as_str),
            Some("5400"),
            "X-TTL must win when both TTL forms are present"
        );
        assert!(
            !meta.contains_key("ttl-days"),
            "a compound TTL must not store a rounded ttl-days value"
        );
    }

    #[tokio::test]
    async fn x_ttl_days_still_stores_whole_day_expiry() {
        let (endpoint, captured) = mock_s3().await;
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.endpoint = endpoint;
        let state = AppState {
            store: Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        };
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "5")
            .header("x-ttl-days", "7")
            .body(Body::from("hello"))
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(resp.status(), StatusCode::CREATED);

        let meta = captured
            .lock()
            .expect("metadata lock must not be poisoned")
            .take()
            .expect("the upload must reach S3");
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
    async fn malformed_x_ttl_is_rejected_with_json_400() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "5")
            .header("x-ttl", "1x")
            .body(Body::from("hello"))
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 4096)
            .await
            .expect("error body must be readable");
        let body = String::from_utf8_lossy(&body);
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a malformed X-TTL must be a bad argument, not a default: {body}"
        );
        assert!(
            body.contains("invalid TTL"),
            "the 400 body must name the TTL error, got: {body}"
        );
    }

    #[tokio::test]
    async fn malformed_x_ttl_days_is_rejected_with_json_400() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let app = api_router().with_state(state);

        let req = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "5")
            .header("x-ttl-days", "soon")
            .body(Body::from("hello"))
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn ttl_query_duration_wins_over_day_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ttl-days", HeaderValue::from_static("7"));
        let query = HashMap::from([("ttl".to_string(), "90m".to_string())]);
        let input = parse_ttl(&headers, &query)
            .expect("a valid query TTL must parse")
            .expect("the query TTL must be present");
        assert_eq!(input.ttl.num_seconds(), 5_400);
        assert_eq!(input.source, TtlSource::Duration);
    }

    #[test]
    fn ttl_query_days_still_selects_whole_days() {
        let query = HashMap::from([("ttl_days".to_string(), "3".to_string())]);
        let input = parse_ttl(&HeaderMap::new(), &query)
            .expect("a valid day TTL must parse")
            .expect("the day TTL must be present");
        assert_eq!(input.ttl.num_seconds(), 3 * 86_400);
        assert_eq!(input.source, TtlSource::Days);
    }

    #[test]
    fn multipart_ttl_field_sets_subday_duration() {
        let input = apply_ttl_field(None, "ttl", "1h30m")
            .expect("a valid ttl field must parse")
            .expect("a ttl must be present");
        assert_eq!(input.ttl.num_seconds(), 5_400);
        assert_eq!(input.source, TtlSource::Duration);
    }

    #[test]
    fn multipart_ttl_days_field_still_sets_whole_days() {
        let input = apply_ttl_field(None, "ttl_days", "3")
            .expect("a valid ttl_days field must parse")
            .expect("a ttl must be present");
        assert_eq!(input.ttl.num_seconds(), 3 * 86_400);
        assert_eq!(input.source, TtlSource::Days);
    }

    #[test]
    fn multipart_duration_field_wins_over_day_field_in_either_order() {
        let days_first =
            apply_ttl_field(None, "ttl_days", "7").expect("a valid ttl_days field must parse");
        let duration_wins = apply_ttl_field(days_first, "ttl", "1h")
            .expect("a valid ttl field must parse")
            .expect("a ttl must be present");
        assert_eq!(duration_wins.ttl.num_seconds(), 3_600);
        assert_eq!(duration_wins.source, TtlSource::Duration);

        let days_cannot_replace = apply_ttl_field(Some(duration_wins), "ttl_days", "7")
            .expect("a valid ttl_days field must parse")
            .expect("a ttl must be present");
        assert_eq!(days_cannot_replace.ttl.num_seconds(), 3_600);
        assert_eq!(days_cannot_replace.source, TtlSource::Duration);
    }

    #[test]
    fn multipart_malformed_ttl_field_is_an_error() {
        assert!(apply_ttl_field(None, "ttl", "1x").is_err());
        assert!(apply_ttl_field(None, "ttl_days", "soon").is_err());
    }
}
