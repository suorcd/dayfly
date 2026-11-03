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
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{
    ConnectInfo, DefaultBodyLimit, FromRequest, Multipart, Path, Query, Request, State,
};
use axum::http::header::{
    ACCEPT, AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, ETAG, HOST,
    IF_NONE_MATCH, RANGE, RETRY_AFTER,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
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

use crate::client_addr::{self, ClientAddr};
use crate::config::MAX_TTL_SECONDS;
use crate::content_type::content_disposition;
use crate::rate_limit::RateLimiter;
use crate::store::{
    parse_list_window_request, parse_range_header, ListWindowRequest, PasteMetadata, Store,
    StoreError, StoreHandle, Ttl, TtlParseError, MULTIPART_CHUNK_BYTES,
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
    store: StoreHandle,
    rate_limiter: Arc<RateLimiter>,
}

impl AppState {
    pub fn new(store: Store) -> Self {
        Self {
            store: StoreHandle::new(store),
            rate_limiter: Arc::new(RateLimiter::new()),
        }
    }

    /// The process-wide request limiter.
    ///
    /// It survives a store reload: the configured rate changes, and the
    /// buckets keep counting.
    pub(crate) fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }

    /// The store snapshot for one operation.
    ///
    /// An operation must take one snapshot and use it for its full
    /// duration. A reload affects the next request, not one in progress.
    pub fn store(&self) -> Arc<Store> {
        self.store.get()
    }

    /// A shared handle for the MCP service and the reload watcher.
    pub fn store_handle(&self) -> StoreHandle {
        self.store.clone()
    }

    /// Replace the store after a successful reload.
    pub fn replace_store(&self, store: Store) {
        self.store.replace(store);
    }
}

/// Check the bearer token for a route. The API routes and the human UI
/// routes share this one function, so both use the constant-time compare.
// Axum handlers return `Response` directly; boxing it here would only add
// indirection at every call site.
#[allow(clippy::result_large_err)]
pub(crate) fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let store = state.store();
    let Some(token) = store.config.http_bearer_token.as_deref() else {
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
        Err(unauthorized_response())
    }
}

fn unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({"error": "unauthorized"})),
    )
        .into_response()
}

/// Check whether a request may delete a paste or change its expiry.
///
/// With `DAYFLY_PASTE_TOKENS` off this is exactly [`check_auth`], so the
/// current behavior is unchanged. With it on, a valid bearer token still
/// allows the request. Without one, the request must carry the paste
/// token for this paste. A paste without a stored token can only be
/// changed with the bearer token.
// The error carries the shared auth response unchanged.
#[allow(clippy::result_large_err)]
pub(crate) async fn check_paste_control(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
    paste_token: Option<&str>,
) -> Result<(), Response> {
    let store = state.store();
    if store.config.http_bearer_token.is_some() && check_auth(state, headers).is_ok() {
        return Ok(());
    }
    if store.config.paste_tokens {
        let ok = match paste_token {
            Some(token) => store.verify_paste_token(id, token).await,
            None => false,
        };
        if ok {
            return Ok(());
        }
        if paste_token.is_some() {
            tracing::warn!(id = %id, "paste token check failed");
        }
        return Err(unauthorized_response());
    }
    check_auth(state, headers)
}

/// Why a listing request was refused.
pub(crate) enum ListAccessError {
    /// The request did not carry the configured bearer token. The shared
    /// auth response is carried through unchanged.
    Unauthorized(Response),
    /// The setting requires a token, but the server has none configured.
    MissingToken,
}

impl ListAccessError {
    /// A message safe to show a client. It names the setting and the
    /// missing variable, never a credential value.
    pub(crate) const MISSING_TOKEN_MESSAGE: &'static str =
        "DAYFLY_LIST_REQUIRES_TOKEN is set, but DAYFLY_HTTP_BEARER_TOKEN is not";
}

/// Check whether a request may list pastes.
///
/// With `DAYFLY_LIST_REQUIRES_TOKEN` off this is exactly [`check_auth`], so
/// listing keeps today's behaviour in both the token and no-token cases.
/// With it on, the bearer token is required even when reads are anonymous.
/// When no token is configured, the request is refused: there is nothing to
/// check against.
// The error carries the shared auth response unchanged. Boxing it here
// would only add an allocation at every refused listing request.
#[allow(clippy::result_large_err)]
pub(crate) fn check_list_access(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(), ListAccessError> {
    let store = state.store();
    if !store.config.list_requires_token {
        return check_auth(state, headers).map_err(ListAccessError::Unauthorized);
    }
    if store.config.http_bearer_token.is_none() {
        return Err(ListAccessError::MissingToken);
    }
    check_auth(state, headers).map_err(ListAccessError::Unauthorized)
}

fn list_access_response(error: ListAccessError) -> Response {
    match error {
        ListAccessError::Unauthorized(response) => response,
        ListAccessError::MissingToken => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "unauthorized",
                "detail": ListAccessError::MISSING_TOKEN_MESSAGE,
            })),
        )
            .into_response(),
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
        StoreError::TooManySegments { .. } => StatusCode::CONFLICT,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    paste_token: Option<String>,
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

impl From<PasteMetadata> for MetaEnvelope {
    fn from(meta: PasteMetadata) -> Self {
        Self {
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
        }
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// True when `If-None-Match` names the current entity tag.
///
/// S3 clients send a comma-separated list, a weak `W/` prefix, or `*`.
/// The comparison uses the weak form, as RFC 9110 requires for this header.
fn if_none_match_matches(headers: &HeaderMap, etag: &str) -> bool {
    let Some(value) = headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        if candidate == "*" {
            return true;
        }
        candidate.strip_prefix("W/").unwrap_or(candidate) == etag
    })
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

/// Public base URL for the current request, without a trailing slash.
///
/// The scheme comes from `x-forwarded-proto` when the value is `http`
/// or `https`; otherwise it is `http`. The host comes from the `Host`
/// header. A request without a host has no base URL.
pub(crate) fn public_base(headers: &HeaderMap) -> Option<String> {
    let host = header_str(headers, HOST.as_str())?;
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .filter(|s| *s == "https" || *s == "http")
        .unwrap_or("http");
    Some(format!("{scheme}://{host}"))
}

fn paste_url(headers: &HeaderMap, id: &str) -> String {
    let path = format!("/v1/pastes/{id}");
    match public_base(headers) {
        Some(base) => format!("{base}{path}"),
        None => path,
    }
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
                paste_token: stored.paste_token.clone(),
            }),
        )
            .into_response()
    } else {
        // Plain-text URL only, so `url=$(curl ...)` works. The token is
        // a header, so the body stays a single URL.
        let mut response = (StatusCode::CREATED, format!("{url}\n")).into_response();
        if let Some(token) = &stored.paste_token {
            response.headers_mut().insert(
                HeaderName::from_static("x-paste-token"),
                HeaderValue::from_str(token).expect("a hex token is a valid header value"),
            );
        }
        response
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
    let store = state.store();
    let content_type = header_str(&headers, CONTENT_TYPE.as_str()).unwrap_or_default();
    if content_type.starts_with("multipart/form-data") {
        match Multipart::from_request(req, &()).await {
            Ok(multipart) => multipart_inner(store, headers, query, multipart).await,
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
        let max_bytes = store.config.max_bytes_for_upload(filename.as_deref());
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
        match store
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
    store: Arc<Store>,
    headers: HeaderMap,
    query: HashMap<String, String>,
    mut multipart: Multipart,
) -> Response {
    let max_request = store
        .config
        .max_file_paste_bytes()
        .saturating_add(store.config.max_text_paste_bytes())
        .saturating_add(1024 * 1024);
    if let Some(length) =
        header_str(&headers, CONTENT_LENGTH.as_str()).and_then(|s| s.trim().parse::<u64>().ok())
    {
        if length > max_request {
            return error_response(StoreError::TooLarge {
                limit: store.config.max_file_paste_bytes(),
            });
        }
    }
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
            let max_bytes = store.config.max_bytes_for_upload(filename.as_deref());
            let stream = futures::stream::unfold(field, |mut f| async move {
                match f.chunk().await {
                    Ok(Some(chunk)) => Some((Ok(chunk), f)),
                    Ok(None) => None,
                    Err(e) => Some((Err(StoreError::Io(e.to_string())), f)),
                }
            });
            return match store
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
    let store = state.store();
    match store.head_metadata_live(id).await {
        Ok(meta) => Json(MetaEnvelope::from(meta)).into_response(),
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
    let store = state.store();
    let meta = match store.head_metadata(id).await {
        Ok(m) => m,
        Err(e) => return error_response(e),
    };
    if meta.expired {
        return error_response(StoreError::Expired(meta.id.clone()));
    }
    let etag = meta.etag();
    if if_none_match_matches(headers, &etag) {
        let mut not_modified = HeaderMap::new();
        not_modified.insert(
            ETAG,
            HeaderValue::from_str(&etag).unwrap_or_else(|_| HeaderValue::from_static("\"\"")),
        );
        return (StatusCode::NOT_MODIFIED, not_modified).into_response();
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
        ETAG,
        HeaderValue::from_str(&etag).unwrap_or_else(|_| HeaderValue::from_static("\"\"")),
    );
    resp_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&meta.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let disposition = content_disposition(
        &meta.content_type,
        meta.filename.as_deref(),
        store.config.inline_pdf,
    );
    let inline = disposition.starts_with("inline");
    resp_headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
    );
    if inline {
        resp_headers.insert(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static("sandbox"),
        );
    }
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
    if let Err(error) = check_list_access(&state, &headers) {
        return list_access_response(error);
    }
    let window = match parse_list_query(&query) {
        Ok(window) => window,
        Err(e) => return error_response(e),
    };
    let limit = query
        .get("limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    let store = state.store();
    let result = match &window {
        ListWindowRequest::Date(date) => store.list_recent(Some(date), limit).await,
        ListWindowRequest::Range { start, end } => {
            store.list_window(Some(start), Some(end), limit).await
        }
        ListWindowRequest::Default => store.list_window(None, None, limit).await,
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

async fn set_ttl_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    set_ttl_inner(&state, &headers, &format!("{date}/{random}"), &query).await
}

async fn set_ttl_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    set_ttl_inner(&state, &headers, &random, &query).await
}

async fn set_ttl_inner(
    state: &AppState,
    headers: &HeaderMap,
    id: &str,
    query: &HashMap<String, String>,
) -> Response {
    let paste_token = header_str(headers, "x-paste-token");
    if let Err(resp) = check_paste_control(state, headers, id, paste_token.as_deref()).await {
        return resp;
    }
    if !api_write_allowed(headers) {
        return forbidden_write();
    }
    let selection = match parse_ttl(headers, query) {
        Ok(selection) => selection,
        Err(e) => return ttl_error_response(e),
    };
    let ttl = match selection.resolve() {
        Ok(Some(ttl)) => ttl,
        Ok(None) => return bad_request("a TTL is required".to_string()),
        Err(e) => return ttl_error_response(e),
    };
    match state.store().set_ttl(id, ttl).await {
        Ok(meta) => Json(MetaEnvelope::from(meta)).into_response(),
        Err(e) => error_response(e),
    }
}

async fn delete_dated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((date, random)): Path<(String, String)>,
) -> Response {
    let id = format!("{date}/{random}");
    let paste_token = header_str(&headers, "x-paste-token");
    if let Err(resp) = check_paste_control(&state, &headers, &id, paste_token.as_deref()).await {
        return resp;
    }
    if !api_write_allowed(&headers) {
        return forbidden_write();
    }
    match state.store().delete(&id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_response(e),
    }
}

async fn delete_bare(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(random): Path<String>,
) -> Response {
    let paste_token = header_str(&headers, "x-paste-token");
    if let Err(resp) = check_paste_control(&state, &headers, &random, paste_token.as_deref()).await
    {
        return resp;
    }
    if !api_write_allowed(&headers) {
        return forbidden_write();
    }
    match state.store().delete(&random).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_response(e),
    }
}

/// Resolve the client address once, then log a failed auth check.
///
/// The socket peer is the client unless it is a listed trusted proxy; in
/// that case `X-Forwarded-For` is walked from the right. The resolved
/// address is attached as [`ClientAddr`] so every later layer and handler
/// reads the same value. An unusable chain falls back to the socket peer
/// with one warning.
///
/// The method, path, and resolved address are safe to log. Never log the
/// offered credential or the expected credential.
async fn client_context(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0);
    let trusted = {
        let store = state.store();
        store.config.trusted_proxies.clone()
    };
    let client =
        peer.map(
            |peer| match client_addr::resolve(peer.ip(), request.headers(), &trusted) {
                Ok(address) => address,
                Err(reason) => {
                    tracing::warn!(
                        peer = %peer,
                        reason = %reason,
                        method = %request.method(),
                        path = %request.uri().path(),
                        "ignoring an unusable X-Forwarded-For chain"
                    );
                    peer.ip()
                }
            },
        );
    if let Some(address) = client {
        request.extensions_mut().insert(ClientAddr(address));
    }
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let response = next.run(request).await;
    if response.status() == StatusCode::UNAUTHORIZED {
        tracing::warn!(%method, %path, client = ?client, "auth check failed");
    }
    response
}

/// Refuse a client over its request budget.
///
/// This runs inside [`client_context`], so the key is the same resolved
/// address the auth log uses. The refusal happens before any extractor or
/// handler reads the body, so an upload that is turned away costs no
/// bytes. Every route counts, including `/` and `/mcp`.
async fn rate_limit(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let limit = state.store().config.rate_limit_per_minute;
    if limit > 0 {
        if let Some(ClientAddr(client)) = request.extensions().get::<ClientAddr>().copied() {
            if let Err(error) = state.rate_limiter().check(client, limit) {
                // `Retry-After` is whole seconds, and truncation would tell
                // a client to retry just before a token exists.
                let retry_after = error.retry_after.as_secs_f64().ceil().max(1.0) as u64;
                let header = HeaderValue::from_str(&retry_after.to_string())
                    .expect("a whole number is a valid header value");
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(RETRY_AFTER, header)],
                    Json(serde_json::json!({
                        "error": "rate_limited",
                        "retry_after_seconds": retry_after,
                    })),
                )
                    .into_response();
            }
        }
    }
    next.run(request).await
}

/// Compose the complete HTTP app from one [`AppState`].
///
/// The API routes, the human UI routes, and the MCP transport share the
/// same store and configuration. Do not build a second store here.
///
/// `client_context` is added last, so it runs first and attaches the
/// address that `rate_limit` reads.
pub fn app_router(state: AppState) -> Router {
    api_router()
        .merge(mcp_router(state.clone()))
        .merge(crate::web::ui_router(state.clone()))
        .layer(middleware::from_fn_with_state(state.clone(), rate_limit))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            client_context,
        ))
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
/// The service factory shares the reloadable [`StoreHandle`] in `state`, so
/// every MCP session reads and writes the same bucket as the REST routes.
/// Each tool call takes the current snapshot, so a reload affects the next
/// call. Each `initialize` request creates an in-memory session with
/// [`LocalSessionManager`]. A restart ends the sessions, and a client then
/// initializes again.
///
/// rmcp rejects a request whose `Host` is not loopback by default. The REST
/// routes accept any `Host`, and a private-network deployment can use a LAN
/// name, so the endpoint disables that list. The bearer token and the
/// private network guard the endpoint, as they guard the REST routes.
pub fn mcp_router(state: AppState) -> Router<AppState> {
    let store = state.store_handle();
    let service = StreamableHttpService::new(
        move || Ok(crate::mcp::Pastebin::http_shared(store.clone())),
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
    let store = state.store();
    let limit = store.config.max_text_paste_bytes();
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
    match store.append_text(id, &bytes).await {
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

/// One API route as described by the well-known service document.
#[derive(Clone, Copy)]
pub struct ApiRoute {
    pub method: &'static str,
    pub path: &'static str,
    pub purpose: &'static str,
}

/// The API routes that the service document describes.
///
/// The test at the bottom of this module compares this list with the
/// `.route(...)` calls in `api_router`, because axum does not expose its
/// route table. Add a route here when you add one there.
pub const API_ROUTE_DOCS: &[ApiRoute] = &[
    ApiRoute {
        method: "POST",
        path: "/upload",
        purpose: "Upload a raw body or a multipart file.",
    },
    ApiRoute {
        method: "POST",
        path: "/v1/pastes",
        purpose: "Store a paste.",
    },
    ApiRoute {
        method: "GET",
        path: "/v1/pastes",
        purpose: "List recent pastes.",
    },
    ApiRoute {
        method: "GET",
        path: "/v1/pastes/{date}/{random}",
        purpose: "Download a paste with the full ID.",
    },
    ApiRoute {
        method: "HEAD",
        path: "/v1/pastes/{date}/{random}",
        purpose: "Read download headers for a paste with the full ID.",
    },
    ApiRoute {
        method: "DELETE",
        path: "/v1/pastes/{date}/{random}",
        purpose: "Delete a paste with the full ID.",
    },
    ApiRoute {
        method: "PATCH",
        path: "/v1/pastes/{date}/{random}",
        purpose: "Change the expiry of a paste with the full ID.",
    },
    ApiRoute {
        method: "GET",
        path: "/v1/pastes/{random}",
        purpose: "Download a paste with a bare ID or alias.",
    },
    ApiRoute {
        method: "HEAD",
        path: "/v1/pastes/{random}",
        purpose: "Read download headers for a bare ID or alias.",
    },
    ApiRoute {
        method: "DELETE",
        path: "/v1/pastes/{random}",
        purpose: "Delete a paste with a bare ID or alias.",
    },
    ApiRoute {
        method: "PATCH",
        path: "/v1/pastes/{random}",
        purpose: "Change the expiry of a bare ID or alias.",
    },
    ApiRoute {
        method: "GET",
        path: "/v1/pastes/{date}/{random}/meta",
        purpose: "Read metadata for a paste with the full ID.",
    },
    ApiRoute {
        method: "GET",
        path: "/v1/pastes/{random}/meta",
        purpose: "Read metadata for a bare ID or alias.",
    },
    ApiRoute {
        method: "POST",
        path: "/v1/pastes/{date}/{random}/append",
        purpose: "Append bytes to a paste with the full ID.",
    },
    ApiRoute {
        method: "POST",
        path: "/v1/pastes/{random}/append",
        purpose: "Append bytes to a bare ID or alias.",
    },
    ApiRoute {
        method: "GET",
        path: "/.well-known/dayfly.json",
        purpose: "Read this machine-readable service description.",
    },
];

/// Serve the unauthenticated service document.
async fn service_description(State(state): State<AppState>) -> Response {
    let store = state.store();
    let config = &store.config;
    let routes: Vec<serde_json::Value> = API_ROUTE_DOCS
        .iter()
        .map(|route| {
            serde_json::json!({
                "method": route.method,
                "path": route.path,
                "purpose": route.purpose,
            })
        })
        .collect();
    Json(serde_json::json!({
        "name": "dayfly",
        "version": env!("CARGO_PKG_VERSION"),
        "mcp_endpoint": "/mcp",
        "auth": if config.http_bearer_token.is_some() {
            "bearer"
        } else {
            "none"
        },
        "paste_tokens": config.paste_tokens,
        "limits": {
            "max_text_bytes": config.max_text_bytes,
            "max_file_bytes": config.max_file_bytes,
            "max_paste_bytes": config.max_paste_bytes,
            "default_ttl_seconds": u64::from(config.default_ttl_days) * 86_400,
            "max_ttl_seconds": MAX_TTL_SECONDS,
            "retention_by_size": config.retention_by_size,
            "retention_min_seconds": u64::from(config.retention_min_days) * 86_400,
            "retention_max_seconds": u64::from(config.retention_max_days) * 86_400,
        },
        "routes": routes,
    }))
    .into_response()
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
        .route("/.well-known/dayfly.json", get(service_description))
        .route(
            "/v1/pastes/{date}/{random}",
            get(download_dated)
                .head(download_dated_head)
                .delete(delete_dated)
                .patch(set_ttl_dated),
        )
        .route(
            "/v1/pastes/{random}",
            get(download_bare)
                .head(download_bare_head)
                .delete(delete_bare)
                .patch(set_ttl_bare),
        )
        .route("/v1/pastes/{date}/{random}/meta", get(meta_dated))
        .route("/v1/pastes/{random}/meta", get(meta_bare))
        .route("/v1/pastes/{date}/{random}/append", post(append_dated))
        .route("/v1/pastes/{random}/append", post(append_bare))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::Poll;

    use axum::body::Body;
    use axum::extract::Extension;
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
            max_paste_bytes: 1024 * 1024 * 1024,
            alias_words: 3,
            default_ttl_days: 7,
            retention_by_size: false,
            retention_min_days: 30,
            retention_max_days: 365,
            http_bind: "127.0.0.1:0".to_string(),
            trusted_proxies: Vec::new(),
            show_words: false,
            inline_pdf: true,
            allow_unauthenticated: false,
            list_requires_token: false,
            paste_tokens: false,
            rate_limit_per_minute: 0,
            http_bearer_token: None,
            mcp_local_root: None,
        }
    }

    fn test_config(max_text_bytes: u64, max_file_bytes: u64) -> Config {
        test_config_at("http://127.0.0.1:1", max_text_bytes, max_file_bytes)
    }

    async fn test_state(max_text_bytes: u64, max_file_bytes: u64) -> AppState {
        AppState::new(
            Store::from_config(test_config(max_text_bytes, max_file_bytes))
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    async fn test_state_with_token(token: &str) -> AppState {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.http_bearer_token = Some(token.to_string());
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
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

    async fn state_with_trusted_proxies(proxies: &[&str]) -> AppState {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.trusted_proxies = proxies
            .iter()
            .map(|proxy| proxy.parse().expect("test proxy must parse"))
            .collect();
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    fn client_echo_router(state: AppState) -> Router {
        Router::new()
            .route(
                "/",
                get(|Extension(address): Extension<ClientAddr>| async move {
                    address.0.to_string()
                }),
            )
            .layer(middleware::from_fn_with_state(state.clone(), client_context))
            .with_state(state)
    }

    async fn resolved_client(app: Router, peer: SocketAddr, forwarded: Option<&str>) -> String {
        let mut builder = Request::builder().uri("/");
        if let Some(value) = forwarded {
            builder = builder.header("x-forwarded-for", value);
        }
        let mut request = builder.body(Body::empty()).expect("request must build");
        request.extensions_mut().insert(ConnectInfo(peer));
        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body must collect")
            .to_bytes();
        String::from_utf8(body.to_vec()).expect("body must be UTF-8")
    }

    #[tokio::test]
    async fn an_untrusted_peer_is_the_client_even_with_a_forwarded_header() {
        let state = state_with_trusted_proxies(&["10.0.0.0/8"]).await;
        let app = client_echo_router(state);
        let client = resolved_client(
            app,
            SocketAddr::from(([198, 51, 100, 7], 40000)),
            Some("203.0.113.9"),
        )
        .await;
        assert_eq!(client, "198.51.100.7");
    }

    #[tokio::test]
    async fn a_trusted_proxy_supplies_the_client_address() {
        let state = state_with_trusted_proxies(&["10.0.0.0/8"]).await;
        let app = client_echo_router(state);
        let client = resolved_client(
            app,
            SocketAddr::from(([10, 0, 0, 5], 40000)),
            Some("203.0.113.9"),
        )
        .await;
        assert_eq!(client, "203.0.113.9");
    }

    #[tokio::test]
    async fn a_forged_leading_hop_is_ignored() {
        let state = state_with_trusted_proxies(&["10.0.0.0/8"]).await;
        let app = client_echo_router(state);
        let client = resolved_client(
            app,
            SocketAddr::from(([10, 0, 0, 5], 40000)),
            Some("1.2.3.4, 203.0.113.9"),
        )
        .await;
        assert_eq!(client, "203.0.113.9");
    }

    #[tokio::test]
    async fn a_malformed_chain_falls_back_to_the_peer() {
        let state = state_with_trusted_proxies(&["10.0.0.0/8"]).await;
        let app = client_echo_router(state);
        let client = resolved_client(
            app,
            SocketAddr::from(([10, 0, 0, 5], 40000)),
            Some("not-an-address"),
        )
        .await;
        assert_eq!(client, "10.0.0.5");
    }

    #[tokio::test]
    async fn a_chain_of_only_proxies_falls_back_to_the_peer() {
        let state = state_with_trusted_proxies(&["10.0.0.0/8"]).await;
        let app = client_echo_router(state);
        let client = resolved_client(
            app,
            SocketAddr::from(([10, 0, 0, 5], 40000)),
            Some("10.0.0.6, 10.0.0.7"),
        )
        .await;
        assert_eq!(client, "10.0.0.5");
    }

    async fn state_with_rate_limit(limit: u32) -> AppState {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.rate_limit_per_minute = limit;
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    fn request_from(peer: SocketAddr, method: &str, uri: &str) -> Request<Body> {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request must build");
        request.extensions_mut().insert(ConnectInfo(peer));
        request
    }

    async fn status_from(app: &Router, peer: SocketAddr, method: &str, uri: &str) -> StatusCode {
        app.clone()
            .oneshot(request_from(peer, method, uri))
            .await
            .expect("router call must complete")
            .status()
    }

    #[tokio::test]
    async fn the_rate_limit_is_off_by_default() {
        let state = state_with_rate_limit(0).await;
        let app = app_router(state);
        let peer = SocketAddr::from(([198, 51, 100, 7], 40000));
        for _ in 0..100 {
            assert_eq!(
                status_from(&app, peer, "GET", "/").await,
                StatusCode::OK,
                "a zero limit must never refuse"
            );
        }
    }

    #[tokio::test]
    async fn a_client_over_the_limit_gets_429_with_retry_after() {
        let state = state_with_rate_limit(1).await;
        let app = app_router(state);
        let peer = SocketAddr::from(([198, 51, 100, 7], 40000));
        assert_eq!(
            status_from(&app, peer, "GET", "/v1/pastes").await,
            StatusCode::BAD_GATEWAY,
            "the first request must reach the S3 sink"
        );

        let response = app
            .clone()
            .oneshot(request_from(peer, "GET", "/v1/pastes"))
            .await
            .expect("router call must complete");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("a 429 must carry a numeric Retry-After");
        assert!(
            (1..=60).contains(&retry_after),
            "retry_after: {retry_after}"
        );
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("error body must be readable");
        let value: serde_json::Value =
            serde_json::from_slice(&body).expect("the error body must be JSON");
        assert_eq!(value["error"], "rate_limited");
        assert_eq!(value["retry_after_seconds"].as_u64(), Some(retry_after));
    }

    #[tokio::test]
    async fn a_refused_upload_body_is_not_polled() {
        let state = state_with_rate_limit(1).await;
        let app = app_router(state);
        let peer = SocketAddr::from(([198, 51, 100, 7], 40000));
        assert_eq!(
            status_from(&app, peer, "GET", "/").await,
            StatusCode::OK,
            "the first request consumes the only token"
        );

        let polled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&polled);
        let body = Body::from_stream(futures::stream::poll_fn(move |_| {
            flag.store(true, Ordering::SeqCst);
            Poll::<Option<Result<Bytes, std::io::Error>>>::Ready(None)
        }));
        let mut request = Request::builder()
            .method("POST")
            .uri("/upload")
            .body(body)
            .expect("request must build");
        request.extensions_mut().insert(ConnectInfo(peer));

        let response = app
            .oneshot(request)
            .await
            .expect("router call must complete");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            !polled.load(Ordering::SeqCst),
            "a refused upload must not read its body"
        );
    }

    #[tokio::test]
    async fn a_forwarded_client_gets_its_own_bucket() {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.rate_limit_per_minute = 1;
        config.trusted_proxies = vec!["10.0.0.0/8".parse().expect("proxy CIDR must parse")];
        let state = AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        );
        let app = app_router(state);
        let peer = SocketAddr::from(([10, 0, 0, 5], 40000));

        let forwarded_request = |forwarded: &str| {
            let mut request = Request::builder()
                .method("GET")
                .uri("/v1/pastes")
                .header("x-forwarded-for", forwarded)
                .body(Body::empty())
                .expect("request must build");
            request.extensions_mut().insert(ConnectInfo(peer));
            request
        };

        for forwarded in ["203.0.113.9", "203.0.113.10"] {
            let status = app
                .clone()
                .oneshot(forwarded_request(forwarded))
                .await
                .expect("router call must complete")
                .status();
            assert_eq!(
                status,
                StatusCode::BAD_GATEWAY,
                "{forwarded} must get its own bucket"
            );
        }
        let status = app
            .oneshot(forwarded_request("203.0.113.9"))
            .await
            .expect("router call must complete")
            .status();
        assert_eq!(
            status,
            StatusCode::TOO_MANY_REQUESTS,
            "the first forwarded client is over its budget"
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

    async fn state_with_gated_listing(token: Option<&str>) -> AppState {
        let mut config = test_config(10 * 1024 * 1024, 1024 * 1024 * 1024);
        config.list_requires_token = true;
        config.http_bearer_token = token.map(str::to_string);
        AppState::new(
            Store::from_config(config)
                .await
                .expect("test S3 config must build without network access"),
        )
    }

    #[tokio::test]
    async fn gated_listing_without_a_configured_token_is_refused() {
        let state = state_with_gated_listing(None).await;
        let app = api_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/v1/pastes")
            .body(Body::empty())
            .expect("request must build");
        let resp = app.oneshot(req).await.expect("router call must complete");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(resp.into_body(), 1024)
            .await
            .expect("error body must be readable");
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains("DAYFLY_HTTP_BEARER_TOKEN"),
            "the error must name the missing token: {text}"
        );
    }

    #[tokio::test]
    async fn gated_listing_requires_the_token() {
        let state = state_with_gated_listing(Some(TEST_TOKEN)).await;
        assert_eq!(
            status_for_auth_request(state, None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn gated_listing_accepts_the_token() {
        let state = state_with_gated_listing(Some(TEST_TOKEN)).await;
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

    #[test]
    fn service_description_covers_every_api_route() {
        let source = include_str!("http.rs");
        let start = source
            .find("pub fn api_router()")
            .expect("api_router must exist");
        let tail = &source[start..];
        let end = tail.find("\n}\n").expect("api_router must close");
        let body = &tail[..end];

        let mut actual = HashSet::new();
        let mut rest = body;
        while let Some(index) = rest.find(".route(") {
            let after = &rest[index..];
            let next = after[1..]
                .find(".route(")
                .map(|offset| offset + 1)
                .unwrap_or(after.len());
            let segment = &after[..next];
            let Some(first_quote) = segment.find('"') else {
                break;
            };
            let after_first = &segment[first_quote + 1..];
            let Some(second_quote) = after_first.find('"') else {
                break;
            };
            let path = &after_first[..second_quote];
            for (token, method) in [
                ("get(", "GET"),
                ("post(", "POST"),
                ("head(", "HEAD"),
                ("delete(", "DELETE"),
                ("patch(", "PATCH"),
            ] {
                if segment.contains(token) {
                    actual.insert(format!("{method} {path}"));
                }
            }
            rest = &after[next..];
        }

        let expected: HashSet<String> = API_ROUTE_DOCS
            .iter()
            .map(|route| format!("{} {}", route.method, route.path))
            .collect();
        assert_eq!(
            actual, expected,
            "update API_ROUTE_DOCS when api_router changes"
        );
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
