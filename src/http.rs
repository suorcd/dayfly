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
use axum::extract::{FromRequest, Multipart, Path, Query, Request, State};
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

use crate::content_type::content_disposition;
use crate::store::{parse_range_header, Store, StoreError, MULTIPART_CHUNK_BYTES};

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
}

fn check_auth(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(token) = state.store.config.http_bearer_token.as_deref() else {
        return Ok(());
    };
    let expected = format!("Bearer {token}");
    let ok = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == expected)
        .unwrap_or(false);
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

fn status_for(e: &StoreError) -> StatusCode {
    match e {
        StoreError::NotFound(_) => StatusCode::NOT_FOUND,
        StoreError::Expired(_) => StatusCode::GONE,
        StoreError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        StoreError::InvalidId(_)
        | StoreError::InvalidDate(_)
        | StoreError::InvalidRange(_) => StatusCode::BAD_REQUEST,
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

fn parse_ttl(headers: &HeaderMap, query: &HashMap<String, String>) -> Option<u32> {
    header_str(headers, "x-ttl-days")
        .or_else(|| query.get("ttl_days").cloned())
        .and_then(|s| s.trim().parse::<u32>().ok())
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
            Err(e) => error_response(StoreError::Io(e.to_string())),
        }
    } else {
        let filename =
            header_str(&headers, "x-filename").or_else(|| query.get("filename").cloned());
        let ttl = parse_ttl(&headers, &query);
        let known_length = header_str(&headers, CONTENT_LENGTH.as_str())
            .and_then(|s| s.trim().parse::<u64>().ok());
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
            )
            .await
        {
            Ok(stored) => upload_response(&headers, &query, &stored),
            Err(e) => error_response(e),
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
    let mut ttl = parse_ttl(&headers, &query);
    // The file field is streamed inside the loop, not moved out of it:
    // an axum Field borrows its Multipart, so holding it across a
    // next_field call does not compile. Streaming inside the iteration
    // keeps the field alive only while it is being read. Text fields
    // after the file field are not read (see README); use
    // headers/query for filename/TTL instead.
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        if name == "file" {
            let field_content_type = field.content_type().map(|s| s.to_string());
            if filename.is_none() {
                filename = field.file_name().map(|s| s.to_string());
            }
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
        } else if name == "ttl_days" {
            if let Ok(text) = field.text().await {
                ttl = text.trim().parse::<u32>().ok().or(ttl);
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
    let body_stream = stream.map(|item| {
        item.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    });
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
    Router::new()
        .route("/upload", post(upload_dispatch))
        .route("/v1/pastes", post(upload_dispatch).get(list_handler))
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
