//! HTTP mode (axum) for plain-curl agents.
//!
//! This module serves the REST routes and mounts the streamable HTTP MCP
//! endpoint at `/mcp`. The binary is the sole S3 reader/writer. Agents talk
//! only to this HTTP interface (or to MCP over stdio); they never get
//! Garage credentials and no presigned URL is ever issued.
//!
//! Deployment note (see README): the intended deployment is
//! private-network / localhost. When `DAYFLY_HTTP_BEARER_TOKEN` is
//! set, every HTTP route requires `Authorization: Bearer <token>`.
//! That token is a simple option for the binary's own endpoint; it is
//! not the Garage credential and bucket privacy is not a substitute
//! for securing this endpoint.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, FromRequest, Multipart, Path, Query, Request, State};
use axum::http::header::{
    ACCEPT, AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, HOST, RANGE,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use futures::StreamExt;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::content_type::content_disposition;
use crate::store::{
    parse_list_window_request, parse_range_header, ListWindowRequest, Store, StoreError, Ttl,
    TtlParseError, MULTIPART_CHUNK_BYTES,
};
use crate::web::api_write_allowed;

/// Maximum bytes for one multipart metadata text field.
///
/// The upload routes disable the axum body limit, so a metadata field
/// needs its own bound. The file field keeps the configured file cap.
pub const MULTIPART_METADATA_MAX_BYTES: usize = 1024;

/// Why a multipart metadata field could not be read.
pub(crate) enum MultipartFieldError {
    /// The field exceeded [`MULTIPART_METADATA_MAX_BYTES`].
    TooLarge,
    /// The multipart body failed, or the field was not UTF-8.
    Malformed(String),
}

/// Read one multipart metadata field through a bounded reader.
///
/// `Field::text()` collects the complete field, and the upload routes
/// disable the axum body limit. This helper stops at the metadata bound
/// instead of buffering an arbitrary field.
pub(crate) async fn read_metadata_field(
    mut field: axum::extract::multipart::Field<'_>,
) -> Result<String, MultipartFieldError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| MultipartFieldError::Malformed(e.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MULTIPART_METADATA_MAX_BYTES {
            return Err(MultipartFieldError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .map_err(|_| MultipartFieldError::Malformed("multipart field is not UTF-8".to_string()))
}

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

pub(crate) fn status_for(e: &StoreError) -> StatusCode {
    match e {
        StoreError::NotFound(_) => StatusCode::NOT_FOUND,
        StoreError::Expired(_) => StatusCode::GONE,
        StoreError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        StoreError::InvalidId(_)
        | StoreError::InvalidDate(_)
        | StoreError::InvalidRange(_)
        | StoreError::LengthMismatch { .. } => StatusCode::BAD_REQUEST,
        StoreError::InvalidPath(_) => StatusCode::BAD_REQUEST,
        // The local-path tools are MCP-only. If one of these reaches an
        // HTTP route, the caller asked for something it does not offer.
        StoreError::LocalFilesDisabled | StoreError::PathOutsideRoot(_) => StatusCode::FORBIDDEN,
        StoreError::RangeNotSatisfiable => StatusCode::RANGE_NOT_SATISFIABLE,
        StoreError::AliasUnavailable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
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

/// A browser write whose origin does not match the request host.
fn forbidden_write() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "error": "forbidden: request origin does not match the Host header"
        })),
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
    words: Option<String>,
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
    words: Option<String>,
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

/// The two TTL forms that a request can supply.
///
/// A request must use one form only. Repeated values of the same form keep
/// the last value that appears before the file field.
#[derive(Debug, Default)]
struct TtlSelection {
    duration: Option<Ttl>,
    days: Option<Ttl>,
}

impl TtlSelection {
    fn set(&mut self, name: &str, value: &str) -> Result<(), TtlParseError> {
        match name {
            "ttl" => self.duration = Some(Ttl::parse(value)?),
            "ttl_days" => self.days = Some(Ttl::parse_days(value)?),
            _ => {}
        }
        Ok(())
    }

    fn is_mixed(&self) -> bool {
        self.duration.is_some() && self.days.is_some()
    }

    fn resolve(self) -> Result<Option<Ttl>, TtlParseError> {
        match (self.duration, self.days) {
            (Some(_), Some(_)) => Err(TtlParseError::mixed_forms()),
            (Some(ttl), None) | (None, Some(ttl)) => Ok(Some(ttl)),
            (None, None) => Ok(None),
        }
    }
}

fn ttl_error_response(e: TtlParseError) -> Response {
    bad_request(e.to_string())
}

/// Read a TTL from headers and query parameters.
///
/// A header wins over the query parameter of the same form. A request
/// must use one form only: a duration form plus a day form is an error.
fn parse_ttl(
    headers: &HeaderMap,
    query: &HashMap<String, String>,
) -> Result<TtlSelection, TtlParseError> {
    let mut selection = TtlSelection::default();
    if let Some(value) = header_str(headers, "x-ttl").or_else(|| query.get("ttl").cloned()) {
        selection.set("ttl", &value)?;
    }
    if let Some(value) =
        header_str(headers, "x-ttl-days").or_else(|| query.get("ttl_days").cloned())
    {
        selection.set("ttl_days", &value)?;
    }
    Ok(selection)
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
                words: stored.words.clone(),
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
    if !api_write_allowed(&headers) {
        return forbidden_write();
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
        let ttl = match parse_ttl(&headers, &query).and_then(TtlSelection::resolve) {
            Ok(ttl) => ttl,
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
    let mut ttl_selection = match parse_ttl(&headers, &query) {
        Ok(selection) => selection,
        Err(e) => return ttl_error_response(e),
    };
    if ttl_selection.is_mixed() {
        return ttl_error_response(TtlParseError::mixed_forms());
    }
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
            let ttl = match ttl_selection.resolve() {
                Ok(ttl) => ttl,
                Err(e) => return ttl_error_response(e),
            };
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
                    ttl,
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
            match read_metadata_field(field).await {
                Ok(text) => {
                    if !text.trim().is_empty() {
                        filename = Some(text.trim().to_string());
                    }
                }
                Err(MultipartFieldError::TooLarge) => {
                    return error_response(StoreError::TooLarge {
                        limit: MULTIPART_METADATA_MAX_BYTES as u64,
                    });
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request(format!("invalid multipart body: {message}"));
                }
            }
        } else if name == "ttl" || name == "ttl_days" {
            match read_metadata_field(field).await {
                Ok(text) => {
                    if let Err(e) = ttl_selection.set(&name, &text) {
                        return ttl_error_response(e);
                    }
                }
                Err(MultipartFieldError::TooLarge) => {
                    return error_response(StoreError::TooLarge {
                        limit: MULTIPART_METADATA_MAX_BYTES as u64,
                    });
                }
                Err(MultipartFieldError::Malformed(message)) => {
                    return bad_request(format!("invalid multipart body: {message}"));
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
                words: meta.words,
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
    // a time, never the whole paste. The plan is built once, so each
    // chunk maps to the base object or to the segment that holds it.
    let store = state.store.clone();
    let plan = meta.plan();
    let stream = futures::stream::unfold(
        (start, end, store, plan),
        move |(cur, end, store, plan)| async move {
            if cur > end {
                return None;
            }
            let chunk_end = (cur + MULTIPART_CHUNK_BYTES as u64 - 1).min(end);
            match store.fetch_range_with_plan(&plan, cur, chunk_end).await {
                Ok(bytes) => {
                    if bytes.is_empty() {
                        return None;
                    }
                    let next = chunk_end + 1;
                    Some((
                        Ok::<Bytes, StoreError>(Bytes::from(bytes)),
                        (next, end, store, plan),
                    ))
                }
                // Propagate instead of ending the body. Ending it here sent a
                // `200 OK` with the full `Content-Length` already on the wire
                // and only part of the object delivered, which reads to a
                // client as a short but successful read.
                Err(e) => Some((Err(e), (end.saturating_add(1), end, store, plan))),
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

/// Parse the listing query. `date` takes precedence over `start` and `end`.
/// A range must supply both dates, and the range must not be inverted.
fn parse_list_query(query: &HashMap<String, String>) -> Result<ListWindowRequest, StoreError> {
    parse_list_window_request(
        query.get("date").map(String::as_str),
        query.get("start").map(String::as_str),
        query.get("end").map(String::as_str),
    )
}

async fn list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Err(resp) = check_auth(&state, &headers) {
        return resp;
    }
    let window = match parse_list_query(&query) {
        Ok(window) => window,
        Err(e) => return error_response(e),
    };
    let limit = query
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    let result = match &window {
        ListWindowRequest::Date(date) => state.store.list_recent(Some(date), limit).await,
        ListWindowRequest::Range { start, end } => {
            state.store.list_window(Some(start), Some(end), limit).await
        }
        ListWindowRequest::Default => state.store.list_window(None, None, limit).await,
    };
    match result {
        Ok(items) => Json(serde_json::json!({
            "date": query.get("date"),
            "start": query.get("start"),
            "end": query.get("end"),
            "items": items.iter().map(|i| serde_json::json!({
                "id": i.id,
                "key": i.key,
                "words": i.words,
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

/// Compose the complete HTTP app from one [`AppState`].
///
/// The API routes, the human UI routes, and the MCP transport share the
/// same store and configuration. Do not build a second store here.
pub fn app_router(state: AppState) -> Router {
    api_router()
        .merge(mcp_router(state.clone()))
        .merge(crate::web::ui_router(state.store.config.max_text_bytes))
        .with_state(state)
}

/// Require the REST bearer token on the MCP endpoint.
///
/// The check is [`check_auth`], so `/mcp` and the REST routes agree on the
/// token, the scheme, and the 401 JSON error.
async fn require_bearer(State(state): State<AppState>, request: Request, next: Next) -> Response {
    match check_auth(&state, request.headers()) {
        Ok(()) => next.run(request).await,
        Err(response) => response,
    }
}

/// Streamable HTTP MCP endpoint at `/mcp`.
///
/// The service factory clones the one [`Store`] in `state`, so every MCP
/// session reads and writes the same bucket as the REST routes. Each
/// `initialize` request creates an in-memory session with
/// [`LocalSessionManager`]. A restart ends the sessions, and a client then
/// initializes again.
///
/// rmcp rejects a request whose `Host` is not loopback by default. The REST
/// routes accept any `Host`, and a private-network deployment can use a LAN
/// name, so the endpoint disables that list. The bearer token and the
/// private network guard the endpoint, as they guard the REST routes.
pub fn mcp_router(state: AppState) -> Router<AppState> {
    let store = state.store.clone();
    let service = StreamableHttpService::new(
        move || Ok(crate::mcp::Pastebin::http(store.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().disable_allowed_hosts(),
    );
    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(state, require_bearer))
}

async fn append_inner(state: &AppState, headers: &HeaderMap, id: &str, req: Request) -> Response {
    if let Err(resp) = check_auth(state, headers) {
        return resp;
    }
    if !api_write_allowed(headers) {
        return forbidden_write();
    }
    let limit = state.store.config.max_text_bytes;
    let known_length =
        header_str(headers, CONTENT_LENGTH.as_str()).and_then(|s| s.trim().parse::<u64>().ok());
    if let Some(len) = known_length {
        if len > limit {
            return error_response(StoreError::TooLarge { limit });
        }
    }
    let mut stream = req.into_body().into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return error_response(StoreError::Io(e.to_string())),
        };
        if u64::try_from(bytes.len().saturating_add(chunk.len())).unwrap_or(u64::MAX) > limit {
            return error_response(StoreError::TooLarge { limit });
        }
        bytes.extend_from_slice(&chunk);
    }
    if let Some(expected) = known_length {
        let actual = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if actual != expected {
            return error_response(StoreError::LengthMismatch { expected, actual });
        }
    }
    match state.store.append_text(id, &bytes).await {
        Ok(appended) => Json(serde_json::json!({
            "id": appended.meta.id,
            "words": appended.meta.words,
            "url": paste_url(headers, &appended.meta.id),
            "appended_bytes": appended.appended_bytes,
            "segment_key": appended.segment_key,
            "size": appended.meta.size,
            "sha256": appended.meta.sha256,
            "expires_at": appended.meta.expires_at.map(|d| d.to_rfc3339()),
        }))
        .into_response(),
        Err(e) => error_response(e),
    }
}

async fn append_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
    req: Request,
) -> Response {
    append_inner(&state, &headers, &format!("{date}/{random}"), req).await
}

async fn append_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
    req: Request,
) -> Response {
    append_inner(&state, &headers, &random, req).await
}

/// API routes, without state attached. [`app_router`] merges these with the
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
        .route("/v1/pastes/{date}/{random}/append", post(append_dated))
        .route("/v1/pastes/{random}/append", post(append_bare))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::Poll;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::{BodyExt, Full};
    use hyper_util::client::legacy::connect::HttpConnector;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;

    type McpClient = Client<HttpConnector, Full<Bytes>>;

    struct McpResponse {
        status: StatusCode,
        headers: HeaderMap,
        body: Bytes,
    }

    fn test_config_at(endpoint: &str, max_text_bytes: u64, max_file_bytes: u64) -> Config {
        Config {
            endpoint: endpoint.to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            max_text_bytes,
            max_file_bytes,
            default_ttl_days: 7,
            http_bind: "127.0.0.1:0".to_string(),
            http_bearer_token: None,
            mcp_local_root: None,
        }
    }

    fn test_config(max_text_bytes: u64, max_file_bytes: u64) -> Config {
        test_config_at("http://127.0.0.1:1", max_text_bytes, max_file_bytes)
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

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn list_query_keeps_date_as_one_day() {
        assert_eq!(
            parse_list_query(&query(&[("date", "2026-10-28")])).unwrap(),
            ListWindowRequest::Date("2026-10-28".to_string())
        );
    }

    #[test]
    fn list_query_accepts_an_inclusive_range() {
        assert_eq!(
            parse_list_query(&query(&[("start", "2026-10-28"), ("end", "2026-10-29"),])).unwrap(),
            ListWindowRequest::Range {
                start: "2026-10-28".to_string(),
                end: "2026-10-29".to_string(),
            }
        );
    }

    #[test]
    fn list_query_rejects_a_one_sided_range() {
        assert!(matches!(
            parse_list_query(&query(&[("start", "2026-10-28")])),
            Err(StoreError::InvalidDate(_))
        ));
        assert!(matches!(
            parse_list_query(&query(&[("end", "2026-10-28")])),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn list_query_rejects_an_inverted_range() {
        assert!(matches!(
            parse_list_query(&query(&[("start", "2026-10-29"), ("end", "2026-10-28"),])),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn list_query_rejects_a_malformed_range() {
        assert!(matches!(
            parse_list_query(&query(&[("start", "2026-13-01"), ("end", "2026-12-01"),])),
            Err(StoreError::InvalidDate(_))
        ));
    }

    /// An invalid listing window must fail at the HTTP boundary. The parse
    /// happens before the store call, so this test does not reach S3.
    #[tokio::test]
    async fn list_endpoint_rejects_invalid_ranges_with_400() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let app = api_router().with_state(state);

        for uri in [
            "/v1/pastes?start=2026-10-28",
            "/v1/pastes?end=2026-10-28",
            "/v1/pastes?start=2026-10-29&end=2026-10-28",
            "/v1/pastes?start=2026-13-01&end=2026-12-01",
        ] {
            let req = Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request must build");
            let resp = app
                .clone()
                .oneshot(req)
                .await
                .expect("router call must complete");
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "{uri} must be a 400"
            );
        }
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

    #[tokio::test]
    async fn mixed_x_ttl_and_x_ttl_days_are_rejected_at_the_http_boundary() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
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
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "both TTL forms must be rejected, not silently resolved"
        );
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
    fn mixed_duration_query_and_day_header_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ttl-days", HeaderValue::from_static("7"));
        let query = HashMap::from([("ttl".to_string(), "90m".to_string())]);
        let error = parse_ttl(&headers, &query)
            .and_then(TtlSelection::resolve)
            .expect_err("a duration query plus a day header must fail");
        assert!(error.to_string().contains("one TTL form"), "got: {error}");
    }

    #[test]
    fn ttl_query_days_still_selects_whole_days() {
        let query = HashMap::from([("ttl_days".to_string(), "3".to_string())]);
        let ttl = parse_ttl(&HeaderMap::new(), &query)
            .expect("a valid day TTL must parse")
            .resolve()
            .expect("the day TTL must be valid")
            .expect("the day TTL must be present");
        assert_eq!(ttl.num_seconds(), 3 * 86_400);
    }

    #[test]
    fn multipart_ttl_field_sets_subday_duration() {
        let mut selection = TtlSelection::default();
        selection
            .set("ttl", "1h30m")
            .expect("a valid ttl field must parse");
        let ttl = selection
            .resolve()
            .expect("one TTL form must resolve")
            .expect("a ttl must be present");
        assert_eq!(ttl.num_seconds(), 5_400);
    }

    #[test]
    fn multipart_ttl_days_field_still_sets_whole_days() {
        let mut selection = TtlSelection::default();
        selection
            .set("ttl_days", "3")
            .expect("a valid ttl_days field must parse");
        let ttl = selection
            .resolve()
            .expect("one TTL form must resolve")
            .expect("a ttl must be present");
        assert_eq!(ttl.num_seconds(), 3 * 86_400);
    }

    #[test]
    fn multipart_duration_and_day_fields_are_rejected_in_either_order() {
        for fields in [
            [("ttl", "1h"), ("ttl_days", "7")],
            [("ttl_days", "7"), ("ttl", "1h")],
        ] {
            let mut selection = TtlSelection::default();
            for (name, value) in fields {
                selection
                    .set(name, value)
                    .expect("each field value must parse");
            }
            assert!(
                selection.resolve().is_err(),
                "both TTL forms must be rejected"
            );
        }
    }

    #[test]
    fn repeated_ttl_field_keeps_the_last_value() {
        let mut selection = TtlSelection::default();
        selection
            .set("ttl", "1h")
            .expect("the first value must parse");
        selection
            .set("ttl", "90m")
            .expect("the second value must parse");
        let ttl = selection
            .resolve()
            .expect("one TTL form must resolve")
            .expect("a ttl must be present");
        assert_eq!(ttl.num_seconds(), 5_400);
    }

    #[test]
    fn multipart_malformed_or_zero_ttl_field_is_an_error() {
        assert!(TtlSelection::default().set("ttl", "1x").is_err());
        assert!(TtlSelection::default().set("ttl", "0").is_err());
        assert!(TtlSelection::default().set("ttl_days", "soon").is_err());
        assert!(TtlSelection::default().set("ttl_days", "0").is_err());
    }

    // --- Streamable HTTP MCP endpoint -------------------------------------

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

    fn tools_list_body() -> Value {
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    }

    fn initialized_body() -> Value {
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"})
    }

    fn mcp_initialize_request(auth: Option<HeaderValue>) -> Request<Body> {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header(HOST, "localhost")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream");
        if let Some(value) = auth {
            builder = builder.header(AUTHORIZATION, value);
        }
        builder
            .body(Body::from(
                serde_json::to_vec(&initialize_body()).expect("initialize body must serialize"),
            ))
            .expect("request must build")
    }

    async fn mcp_oneshot(
        state: AppState,
        auth: Option<HeaderValue>,
    ) -> (StatusCode, HeaderMap, Bytes) {
        let app = app_router(state);
        let resp = app
            .oneshot(mcp_initialize_request(auth))
            .await
            .expect("router call must complete");
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .expect("response body must be readable");
        (status, headers, body)
    }

    /// The MCP endpoint must reject a request with no credential when the
    /// token is set. The 401 body is the same JSON error as the REST routes.
    #[tokio::test]
    async fn mcp_endpoint_rejects_missing_bearer_token() {
        let state = test_state_with_token(TEST_TOKEN).await;
        let (status, _, body) = mcp_oneshot(state, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            String::from_utf8_lossy(&body),
            r#"{"error":"unauthorized"}"#
        );
    }

    #[tokio::test]
    async fn mcp_endpoint_rejects_wrong_bearer_token() {
        let state = test_state_with_token(TEST_TOKEN).await;
        let auth = HeaderValue::from_static("Bearer wrong-horse-battery-staple");
        let (status, _, body) = mcp_oneshot(state, Some(auth)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(
            String::from_utf8_lossy(&body),
            r#"{"error":"unauthorized"}"#
        );
    }

    /// The right credential must pass auth and reach the MCP handler, which
    /// answers the initialize request and creates a session.
    #[tokio::test]
    async fn mcp_endpoint_accepts_correct_bearer_token() {
        let state = test_state_with_token(TEST_TOKEN).await;
        let auth = HeaderValue::from_str(&format!("Bearer {TEST_TOKEN}"))
            .expect("header value must build");
        let (status, headers, body) = mcp_oneshot(state, Some(auth)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the correct token must pass auth, got: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            headers.contains_key("mcp-session-id"),
            "initialize must create a session"
        );
    }

    /// An unset token keeps the MCP endpoint open, as it does for REST.
    #[tokio::test]
    async fn mcp_endpoint_is_open_without_configured_token() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let (status, headers, body) = mcp_oneshot(state, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "an unset token must leave the endpoint open, got: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(headers.contains_key("mcp-session-id"));
    }

    /// A client that speaks HTTP to a real listener. A new instance is a new
    /// connection pool, so a test can use two independent clients.
    fn mcp_http_client() -> McpClient {
        Client::builder(TokioExecutor::new()).build_http()
    }

    async fn start_mcp_server(state: AppState) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener must bind");
        let addr = listener.local_addr().expect("bound address must exist");
        let app = app_router(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test HTTP server must run");
        });
        (addr, handle)
    }

    async fn mcp_request(
        client: &McpClient,
        addr: SocketAddr,
        method: &str,
        body: &Value,
        session_id: Option<&str>,
    ) -> McpResponse {
        let mut builder = Request::builder()
            .method(method)
            .uri(format!("http://{addr}/mcp"))
            .header(HOST, addr.to_string())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json, text/event-stream")
            .header("mcp-protocol-version", MCP_PROTOCOL_VERSION);
        if let Some(session_id) = session_id {
            builder = builder.header("mcp-session-id", session_id);
        }
        let request = builder
            .body(Full::new(Bytes::from(body.to_string())))
            .expect("request must build");
        let response = client
            .request(request)
            .await
            .expect("HTTP request must complete");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body must be readable")
            .to_bytes();
        McpResponse {
            status,
            headers,
            body,
        }
    }

    /// Return the first non-empty JSON-RPC payload from an SSE response.
    fn sse_json(response: &McpResponse) -> Value {
        let text = String::from_utf8_lossy(&response.body);
        let data = text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .find(|data| !data.is_empty())
            .unwrap_or_else(|| {
                panic!(
                    "response must carry an SSE JSON-RPC payload, status {} body: {text}",
                    response.status
                )
            });
        serde_json::from_str(data).expect("SSE payload must be JSON")
    }

    async fn initialize_session(client: &McpClient, addr: SocketAddr) -> (String, Value) {
        let response = mcp_request(client, addr, "POST", &initialize_body(), None).await;
        let payload = sse_json(&response);
        assert_eq!(
            response.status,
            StatusCode::OK,
            "initialize must succeed, payload: {payload}"
        );
        let session_id = response
            .headers
            .get("mcp-session-id")
            .expect("initialize response must carry a session id")
            .to_str()
            .expect("session id must be ASCII")
            .to_string();
        assert_eq!(payload["result"]["serverInfo"]["name"], "dayfly");
        let initialized =
            mcp_request(client, addr, "POST", &initialized_body(), Some(&session_id)).await;
        assert_eq!(
            initialized.status,
            StatusCode::ACCEPTED,
            "the initialized notification must be accepted"
        );
        (session_id, payload)
    }

    async fn list_tools(client: &McpClient, addr: SocketAddr, session_id: &str) -> McpResponse {
        mcp_request(client, addr, "POST", &tools_list_body(), Some(session_id)).await
    }

    fn tool_names(payload: &Value) -> Vec<String> {
        payload["result"]["tools"]
            .as_array()
            .expect("tools/list result must carry a tools array")
            .iter()
            .map(|tool| {
                tool["name"]
                    .as_str()
                    .expect("tool must carry a name")
                    .to_string()
            })
            .collect()
    }

    /// Two independent HTTP clients must get two independent MCP sessions on
    /// one server and one store. Closing one session must not close the other.
    #[tokio::test]
    async fn two_http_clients_get_independent_mcp_sessions() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let (addr, server) = start_mcp_server(state).await;
        let client_a = mcp_http_client();
        let client_b = mcp_http_client();

        let (session_a, _) = initialize_session(&client_a, addr).await;
        let (session_b, _) = initialize_session(&client_b, addr).await;
        assert_ne!(
            session_a, session_b,
            "each initialize must create a distinct session"
        );

        let (tools_a, tools_b) = tokio::join!(
            list_tools(&client_a, addr, &session_a),
            list_tools(&client_b, addr, &session_b)
        );
        assert_eq!(tools_a.status, StatusCode::OK);
        assert_eq!(tools_b.status, StatusCode::OK);
        let expected = [
            "store_text",
            "store_file",
            "append_text",
            "fetch_text",
            "fetch_file",
            "fetch_metadata",
            "delete_paste",
            "list_recent",
        ];
        for response in [&tools_a, &tools_b] {
            let names = tool_names(&sse_json(response));
            for name in expected {
                assert!(
                    names.iter().any(|candidate| candidate == name),
                    "tools/list must expose {name}, got: {names:?}"
                );
            }
        }

        let closed = mcp_request(&client_a, addr, "DELETE", &json!({}), Some(&session_a)).await;
        assert_eq!(closed.status, StatusCode::ACCEPTED);
        let (gone, alive) = tokio::join!(
            list_tools(&client_a, addr, &session_a),
            list_tools(&client_b, addr, &session_b)
        );
        assert_eq!(
            gone.status,
            StatusCode::NOT_FOUND,
            "the closed session must be gone"
        );
        assert_eq!(
            alive.status,
            StatusCode::OK,
            "closing one session must not close the other"
        );
        server.abort();
    }

    #[tokio::test]
    async fn two_concurrent_mcp_initializations_create_independent_sessions() {
        let state = test_state(10 * 1024 * 1024, 1024 * 1024 * 1024).await;
        let (addr, server) = start_mcp_server(state).await;
        let client_a = mcp_http_client();
        let client_b = mcp_http_client();

        let (client_a, client_b) = tokio::join!(
            initialize_session(&client_a, addr),
            initialize_session(&client_b, addr)
        );
        assert_ne!(
            client_a.0, client_b.0,
            "concurrent initializations must not share a session"
        );
        server.abort();
    }

    #[tokio::test]
    async fn append_over_the_text_cap_is_rejected_before_body_read() {
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
            .uri("/v1/pastes/2026-10-28/abc/append")
            .header(CONTENT_LENGTH, (64u64 * 1024 * 1024).to_string())
            .body(body)
            .expect("request must build");

        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(
            resp.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an append over the text cap must be 413"
        );
        assert!(
            !polled.load(Ordering::SeqCst),
            "the body must not be read after an early cap rejection"
        );
    }
}
