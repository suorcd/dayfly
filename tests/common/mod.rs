//! Shared test support: the one in-process fake S3 service.
//!
//! Every S3-backed test uses this harness. The fake listens on an ephemeral
//! localhost port and implements the S3 operations that the AWS SDK calls
//! from this crate. Tests point a real [`Store`] at it with dummy
//! credentials. No network access and no Garage instance are needed.
//!
//! The fake records each PutObject body and each GetObject range. A test can
//! therefore assert that an append writes only the new bytes, and that a
//! range read asks for only the bytes it needs.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use chrono::{SecondsFormat, Utc};
use dayfly::config::Config;
use dayfly::http::AppState;
use dayfly::store::Store;
use percent_encoding::percent_decode_str;
use tower::ServiceExt;

/// One stored object.
#[derive(Clone, Debug, Default)]
pub struct S3Object {
    pub body: Vec<u8>,
    pub content_type: String,
    pub metadata: BTreeMap<String, String>,
    pub etag: String,
}

#[derive(Clone, Debug, Default)]
pub struct Multipart {
    bucket: String,
    key: String,
    content_type: String,
    metadata: BTreeMap<String, String>,
    parts: BTreeMap<i32, (String, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub query: String,
    pub headers: Vec<(String, String)>,
}

/// A request that the fake service answers with an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailOp {
    PutObject,
    UploadPart,
    CompleteMultipartUpload,
    CopyObject,
    ListObjectsV2,
}

#[derive(Default)]
pub struct FakeState {
    pub bucket: String,
    pub objects: BTreeMap<String, S3Object>,
    pub uploads: BTreeMap<String, Multipart>,
    pub next_upload: u64,
    pub requests: Vec<RecordedRequest>,
    pub puts: Vec<(String, Vec<u8>)>,
    pub gets: Vec<(String, Option<String>)>,
    pub aborted: Vec<String>,
    pub fail_ops: Vec<FailOp>,
    pub fail_part_number: Option<i32>,
    pub fail_head_keys: Vec<String>,
    pub fail_put_prefixes: Vec<String>,
    pub missing_bucket: bool,
}

impl FakeState {
    fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            ..Default::default()
        }
    }
}

/// An operation that the fake service can hold open.
///
/// A held operation blocks the client inside that request, which is the
/// only way to change a file at a known point in a store call. Without
/// it, the size and hash races are untestable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    /// Every HeadObject, including the alias probe.
    HeadObject,
    /// CreateMultipartUpload.
    CreateMultipartUpload,
}

#[derive(Default)]
struct GateState {
    held: Vec<Gate>,
    blocked: Vec<Gate>,
}

/// The hold-and-release control for [`Gate`].
#[derive(Default)]
struct GateControl {
    state: Mutex<GateState>,
}

impl GateControl {
    /// Wait here while `gate` is held. Called from the fake service.
    async fn pass(&self, gate: Gate) {
        if !self.state.lock().expect("gate lock").held.contains(&gate) {
            return;
        }
        {
            let mut state = self.state.lock().expect("gate lock");
            if !state.blocked.contains(&gate) {
                state.blocked.push(gate);
            }
        }
        for _ in 0..4_000 {
            if !self.state.lock().expect("gate lock").held.contains(&gate) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("the gate for {gate:?} was never released");
    }
}

/// The service state: the fake bucket plus the gate control.
#[derive(Clone)]
struct FakeApp {
    state: Arc<Mutex<FakeState>>,
    gate: Arc<GateControl>,
}

/// Handle to a running fake S3 service.
pub struct FakeS3 {
    pub endpoint: String,
    pub bucket: String,
    state: Arc<Mutex<FakeState>>,
    gate: Arc<GateControl>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl FakeS3 {
    pub async fn start() -> Self {
        Self::start_with_bucket("test-bucket").await
    }

    pub async fn start_with_bucket(bucket: &str) -> Self {
        let state = Arc::new(Mutex::new(FakeState::new(bucket)));
        let gate = Arc::new(GateControl::default());
        let app = Router::new().fallback(handle_request).with_state(FakeApp {
            state: Arc::clone(&state),
            gate: Arc::clone(&gate),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake S3 listener must bind");
        let addr: SocketAddr = listener.local_addr().expect("listener address");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("fake S3 server must run");
        });
        Self {
            endpoint: format!("http://{addr}"),
            bucket: bucket.to_string(),
            state,
            gate,
            handle,
        }
    }

    pub fn config(&self) -> Config {
        self.config_with_caps(10 * 1024 * 1024, 1024 * 1024 * 1024)
    }

    pub fn config_with_caps(&self, max_text_bytes: u64, max_file_bytes: u64) -> Config {
        Config {
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
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

    pub async fn store(&self) -> Store {
        Store::from_config(self.config())
            .await
            .expect("fake S3 config must build without network access")
    }

    pub async fn store_with_caps(&self, max_text_bytes: u64, max_file_bytes: u64) -> Store {
        Store::from_config(self.config_with_caps(max_text_bytes, max_file_bytes))
            .await
            .expect("fake S3 config must build without network access")
    }

    /// Insert an object directly, bypassing the store.
    pub fn seed(&self, key: &str, body: &[u8], content_type: &str, metadata: &[(&str, &str)]) {
        let meta = metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let object = S3Object {
            body: body.to_vec(),
            content_type: content_type.to_string(),
            metadata: meta,
            etag: etag_for(body),
        };
        self.state
            .lock()
            .expect("fake state lock")
            .objects
            .insert(key.to_string(), object);
    }

    pub fn object(&self, key: &str) -> Option<S3Object> {
        self.state
            .lock()
            .expect("fake state lock")
            .objects
            .get(key)
            .cloned()
    }

    pub fn keys(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("fake state lock")
            .objects
            .keys()
            .cloned()
            .collect()
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.lock().expect("fake state lock").requests.clone()
    }

    /// Every stored PutObject key and body, in request order.
    pub fn puts(&self) -> Vec<(String, Vec<u8>)> {
        self.state.lock().expect("fake state lock").puts.clone()
    }

    /// Every GetObject key and Range header, in request order.
    pub fn gets(&self) -> Vec<(String, Option<String>)> {
        self.state.lock().expect("fake state lock").gets.clone()
    }

    /// Replace one metadata value on a stored object. Tests use this to
    /// make a paste expired without waiting.
    pub fn set_metadata(&self, key: &str, name: &str, value: &str) {
        if let Some(object) = self
            .state
            .lock()
            .expect("fake state lock")
            .objects
            .get_mut(key)
        {
            object.metadata.insert(name.to_string(), value.to_string());
        }
    }

    pub fn aborted_uploads(&self) -> Vec<String> {
        self.state.lock().expect("fake state lock").aborted.clone()
    }

    pub fn set_missing_bucket(&self, missing: bool) {
        self.state.lock().expect("fake state lock").missing_bucket = missing;
    }

    /// Hold every matching operation until [`Self::release`] runs.
    pub fn hold(&self, gate: Gate) {
        self.gate.state.lock().expect("gate lock").held.push(gate);
    }

    /// Wait until a held operation is blocked inside the fake service.
    pub async fn wait_until_blocked(&self, gate: Gate) {
        for _ in 0..4_000 {
            if self
                .gate
                .state
                .lock()
                .expect("gate lock")
                .blocked
                .contains(&gate)
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("the fake service never blocked on {gate:?}");
    }

    /// Release every held operation.
    pub fn release(&self) {
        let mut state = self.gate.state.lock().expect("gate lock");
        state.held.clear();
        state.blocked.clear();
    }

    pub fn fail(&self, op: FailOp) {
        self.state
            .lock()
            .expect("fake state lock")
            .fail_ops
            .push(op);
    }

    pub fn fail_part(&self, part_number: i32) {
        self.state.lock().expect("fake state lock").fail_part_number = Some(part_number);
    }

    pub fn fail_head(&self, key: &str) {
        self.state
            .lock()
            .expect("fake state lock")
            .fail_head_keys
            .push(key.to_string());
    }

    /// Make every PutObject whose key starts with `prefix` fail.
    pub fn fail_put_prefix(&self, prefix: &str) {
        self.state
            .lock()
            .expect("fake state lock")
            .fail_put_prefixes
            .push(prefix.to_string());
    }

    pub fn drop_upload(&self, upload_id: &str) {
        self.state
            .lock()
            .expect("fake state lock")
            .uploads
            .remove(upload_id);
    }
}

fn etag_for(body: &[u8]) -> String {
    format!("\"etag-{}\"", body.len())
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn query_param(uri: &Uri, name: &str) -> Option<String> {
    let query = uri.query()?;
    for pair in query.split('&') {
        if pair == name {
            return Some(String::new());
        }
        if let Some((key, value)) = pair.split_once('=') {
            if key == name {
                return Some(percent_decode_str(value).decode_utf8_lossy().into_owned());
            }
        }
    }
    None
}

fn has_query_param(uri: &Uri, name: &str) -> bool {
    query_param(uri, name).is_some()
}

fn metadata_from_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    for (name, value) in headers.iter() {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-") {
            if let Ok(value) = value.to_str() {
                meta.insert(key.to_string(), value.to_string());
            }
        }
    }
    meta
}

fn content_type_from_headers(headers: &HeaderMap) -> String {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string()
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn s3_error(status: StatusCode, code: &str, message: &str) -> Response {
    xml_response(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><RequestId>fake</RequestId></Error>",
            xml_escape(code),
            xml_escape(message)
        ),
    )
}

fn split_path(uri: &Uri, bucket: &str) -> Option<String> {
    let path = uri.path().trim_start_matches('/');
    let prefix = format!("{bucket}/");
    let key = path.strip_prefix(&prefix)?;
    Some(key.to_string())
}

fn object_headers(object: &S3Object) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if !object.content_type.is_empty() {
        headers.insert(
            header::CONTENT_TYPE,
            object
                .content_type
                .parse()
                .unwrap_or_else(|_| "application/octet-stream".parse().expect("static header")),
        );
    }
    headers.insert(
        header::ETAG,
        object
            .etag
            .parse()
            .expect("generated etag must be a valid header"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        object
            .body
            .len()
            .to_string()
            .parse()
            .expect("length must be a valid header"),
    );
    for (key, value) in &object.metadata {
        if let (Ok(name), Ok(value)) = (
            format!("x-amz-meta-{key}").parse::<header::HeaderName>(),
            value.parse::<header::HeaderValue>(),
        ) {
            headers.insert(name, value);
        }
    }
    headers
}

async fn handle_request(State(app): State<FakeApp>, req: Request) -> Response {
    let state = Arc::clone(&app.state);
    let gate_control = Arc::clone(&app.gate);
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let (bucket, missing_bucket, configured_bucket) = {
        let state = state.lock().expect("fake state lock");
        let bucket = uri
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("")
            .to_string();
        (bucket, state.missing_bucket, state.bucket.clone())
    };
    {
        let headers = headers
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_string(), value.to_string()))
            })
            .collect();
        state
            .lock()
            .expect("fake state lock")
            .requests
            .push(RecordedRequest {
                method: method.as_str().to_string(),
                path: uri.path().to_string(),
                query: uri.query().unwrap_or("").to_string(),
                headers,
            });
    }
    // A held operation parks the client here, so a test can change a
    // file at a known point in the store call.
    let gate = match method.as_str() {
        "HEAD" => Some(Gate::HeadObject),
        "POST" if has_query_param(&uri, "uploads") => Some(Gate::CreateMultipartUpload),
        _ => None,
    };
    if let Some(gate) = gate {
        gate_control.pass(gate).await;
    }
    if missing_bucket || bucket != configured_bucket {
        if method == Method::HEAD {
            return (StatusCode::NOT_FOUND, HeaderMap::new(), Body::empty()).into_response();
        }
        return s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "the bucket does not exist",
        );
    }
    let key = split_path(&uri, &configured_bucket);
    match method {
        Method::PUT if headers.contains_key("x-amz-copy-source") => {
            copy_object(&state, key, &headers)
        }
        Method::PUT if has_query_param(&uri, "uploadId") => {
            upload_part(&state, key, &uri, &headers, body).await
        }
        Method::PUT => put_object(&state, key, &headers, body),
        Method::POST if has_query_param(&uri, "uploads") => create_multipart(&state, key, &headers),
        Method::POST if has_query_param(&uri, "uploadId") => complete_multipart(&state, key, &uri),
        Method::GET if query_param(&uri, "list-type").as_deref() == Some("2") => {
            list_objects(&state, &uri)
        }
        Method::GET => get_object(&state, key, &headers, false),
        Method::HEAD => get_object(&state, key, &headers, true),
        Method::DELETE if has_query_param(&uri, "uploadId") => abort_multipart(&state, &uri),
        Method::DELETE => delete_object(&state, key),
        _ => s3_error(
            StatusCode::NOT_IMPLEMENTED,
            "NotImplemented",
            "operation not supported by the fake service",
        ),
    }
}

fn put_object(
    state: &Arc<Mutex<FakeState>>,
    key: Option<String>,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let mut state = state.lock().expect("fake state lock");
    let Some(key) = key else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "missing object key",
        );
    };
    if state.fail_ops.contains(&FailOp::PutObject)
        || state
            .fail_put_prefixes
            .iter()
            .any(|prefix| key.starts_with(prefix))
    {
        return s3_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "injected PutObject failure",
        );
    }
    let object = S3Object {
        body: body.to_vec(),
        content_type: content_type_from_headers(headers),
        metadata: metadata_from_headers(headers),
        etag: etag_for(&body),
    };
    let etag = object.etag.clone();
    state.puts.push((key.clone(), body.to_vec()));
    state.objects.insert(key, object);
    (
        StatusCode::OK,
        [(header::ETAG, etag.as_str())],
        Body::empty(),
    )
        .into_response()
}

fn get_object(
    state: &Arc<Mutex<FakeState>>,
    key: Option<String>,
    headers: &HeaderMap,
    is_head: bool,
) -> Response {
    let mut state = state.lock().expect("fake state lock");
    let Some(key) = key else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "missing object key",
        );
    };
    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if !is_head {
        state.gets.push((key.clone(), range_header.clone()));
    }
    let Some(object) = state.objects.get(&key) else {
        if is_head {
            return (StatusCode::NOT_FOUND, HeaderMap::new(), Body::empty()).into_response();
        }
        return s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "the object does not exist",
        );
    };
    if is_head && state.fail_head_keys.contains(&key) {
        return (StatusCode::NOT_FOUND, HeaderMap::new(), Body::empty()).into_response();
    }
    let mut resp_headers = object_headers(object);
    if is_head {
        return (StatusCode::OK, resp_headers, Body::empty()).into_response();
    }
    let range = range_header.and_then(|value| parse_range(&value, object.body.len() as u64));
    match range {
        Some((start, end)) => {
            let slice = object.body[start as usize..=end as usize].to_vec();
            let length = slice.len();
            resp_headers.insert(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", object.body.len())
                    .parse()
                    .expect("content range must be a valid header"),
            );
            resp_headers.insert(
                header::CONTENT_LENGTH,
                length
                    .to_string()
                    .parse()
                    .expect("length must be a valid header"),
            );
            (StatusCode::PARTIAL_CONTENT, resp_headers, Body::from(slice)).into_response()
        }
        None => (
            StatusCode::OK,
            resp_headers,
            Body::from(object.body.clone()),
        )
            .into_response(),
    }
}

fn parse_range(value: &str, size: u64) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("bytes=")?;
    if rest.contains(',') || size == 0 {
        return None;
    }
    let (start, end) = rest.split_once('-')?;
    if start.is_empty() {
        let suffix: u64 = end.parse().ok()?;
        if suffix == 0 {
            return None;
        }
        return Some((size.saturating_sub(suffix), size - 1));
    }
    let start: u64 = start.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().ok()?.min(size - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

fn delete_object(state: &Arc<Mutex<FakeState>>, key: Option<String>) -> Response {
    let mut state = state.lock().expect("fake state lock");
    if let Some(key) = key {
        state.objects.remove(&key);
    }
    (StatusCode::NO_CONTENT, Body::empty()).into_response()
}

fn copy_object(
    state: &Arc<Mutex<FakeState>>,
    key: Option<String>,
    headers: &HeaderMap,
) -> Response {
    let mut state = state.lock().expect("fake state lock");
    if state.fail_ops.contains(&FailOp::CopyObject) {
        return s3_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "injected CopyObject failure",
        );
    }
    let Some(key) = key else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "missing object key",
        );
    };
    let Some(source) = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
    else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "missing copy source",
        );
    };
    let source = percent_decode_str(source.trim_start_matches('/'))
        .decode_utf8_lossy()
        .into_owned();
    let prefix = format!("{}/", state.bucket);
    let source = source.strip_prefix(&prefix).unwrap_or(&source).to_string();
    let Some(mut object) = state.objects.get(&source).cloned() else {
        return s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchKey",
            "the copy source does not exist",
        );
    };
    let replace = headers
        .get("x-amz-metadata-directive")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);
    if replace {
        object.content_type = content_type_from_headers(headers);
        object.metadata = metadata_from_headers(headers);
    }
    let etag = object.etag.clone();
    state.objects.insert(key, object);
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult><ETag>{}</ETag><LastModified>{}</LastModified></CopyObjectResult>",
            xml_escape(&etag),
            now_iso()
        ),
    )
}

fn create_multipart(
    state: &Arc<Mutex<FakeState>>,
    key: Option<String>,
    headers: &HeaderMap,
) -> Response {
    let mut state = state.lock().expect("fake state lock");
    let Some(key) = key else {
        return s3_error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "missing object key",
        );
    };
    state.next_upload += 1;
    let upload_id = format!("upload-{}", state.next_upload);
    let bucket = state.bucket.clone();
    state.uploads.insert(
        upload_id.clone(),
        Multipart {
            bucket,
            key: key.clone(),
            content_type: content_type_from_headers(headers),
            metadata: metadata_from_headers(headers),
            parts: BTreeMap::new(),
        },
    );
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
            xml_escape(&state.bucket),
            xml_escape(&key),
            xml_escape(&upload_id)
        ),
    )
}

async fn upload_part(
    state: &Arc<Mutex<FakeState>>,
    key: Option<String>,
    uri: &Uri,
    _headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let part_number = query_param(uri, "partNumber")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let upload_id = query_param(uri, "uploadId").unwrap_or_default();
    let mut state = state.lock().expect("fake state lock");
    if state.fail_ops.contains(&FailOp::UploadPart) || state.fail_part_number == Some(part_number) {
        return s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "injected UploadPart failure",
        );
    }
    let Some(upload) = state.uploads.get_mut(&upload_id) else {
        return s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            "the upload does not exist",
        );
    };
    if key.as_deref() != Some(upload.key.as_str()) {
        return s3_error(StatusCode::BAD_REQUEST, "InvalidRequest", "key mismatch");
    }
    let etag = etag_for(&body);
    upload
        .parts
        .insert(part_number, (etag.clone(), body.to_vec()));
    (
        StatusCode::OK,
        [(header::ETAG, etag.as_str())],
        Body::empty(),
    )
        .into_response()
}

fn complete_multipart(state: &Arc<Mutex<FakeState>>, key: Option<String>, uri: &Uri) -> Response {
    let upload_id = query_param(uri, "uploadId").unwrap_or_default();
    let mut state = state.lock().expect("fake state lock");
    if state.fail_ops.contains(&FailOp::CompleteMultipartUpload) {
        return s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "injected CompleteMultipartUpload failure",
        );
    }
    let Some(upload) = state.uploads.remove(&upload_id) else {
        return s3_error(
            StatusCode::NOT_FOUND,
            "NoSuchUpload",
            "the upload does not exist",
        );
    };
    if key.as_deref() != Some(upload.key.as_str()) {
        return s3_error(StatusCode::BAD_REQUEST, "InvalidRequest", "key mismatch");
    }
    let mut body = Vec::new();
    for (_, (_, part)) in upload.parts {
        body.extend_from_slice(&part);
    }
    let etag = etag_for(&body);
    let object = S3Object {
        body,
        content_type: upload.content_type,
        metadata: upload.metadata,
        etag: etag.clone(),
    };
    state.objects.insert(upload.key.clone(), object);
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Location>http://fake/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
            xml_escape(&state.bucket),
            xml_escape(&upload.key),
            xml_escape(&state.bucket),
            xml_escape(&upload.key),
            xml_escape(&etag)
        ),
    )
}

fn abort_multipart(state: &Arc<Mutex<FakeState>>, uri: &Uri) -> Response {
    let upload_id = query_param(uri, "uploadId").unwrap_or_default();
    let mut state = state.lock().expect("fake state lock");
    state.uploads.remove(&upload_id);
    state.aborted.push(upload_id);
    (StatusCode::NO_CONTENT, Body::empty()).into_response()
}

fn list_objects(state: &Arc<Mutex<FakeState>>, uri: &Uri) -> Response {
    let state = state.lock().expect("fake state lock");
    if state.fail_ops.contains(&FailOp::ListObjectsV2) {
        return s3_error(
            StatusCode::FORBIDDEN,
            "AccessDenied",
            "injected ListObjectsV2 failure",
        );
    }
    let prefix = query_param(uri, "prefix").unwrap_or_default();
    let max_keys = query_param(uri, "max-keys")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let token = query_param(uri, "continuation-token");
    let mut entries: Vec<(&String, &S3Object)> = state
        .objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .filter(|(key, _)| token.as_deref().is_none_or(|t| key.as_str() > t))
        .collect();
    let truncated = entries.len() > max_keys;
    entries.truncate(max_keys);
    let mut contents = String::new();
    for (key, object) in &entries {
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            now_iso(),
            xml_escape(&object.etag),
            object.body.len()
        ));
    }
    let next = if truncated {
        entries
            .last()
            .map(|(key, _)| {
                format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    xml_escape(key)
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>{}{}</ListBucketResult>",
            xml_escape(&state.bucket),
            xml_escape(&prefix),
            entries.len(),
            max_keys,
            truncated,
            next,
            contents
        ),
    )
}

/// Build a multipart/form-data body.
pub fn multipart_body(
    boundary: &str,
    filename: Option<&str>,
    content_type: Option<&str>,
    content: &[u8],
    extra_fields: &[(&str, &str)],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in extra_fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    let mut disposition = "Content-Disposition: form-data; name=\"file\"".to_string();
    if let Some(filename) = filename {
        disposition.push_str(&format!("; filename=\"{filename}\""));
    }
    body.extend_from_slice(disposition.as_bytes());
    body.extend_from_slice(b"\r\n");
    if let Some(content_type) = content_type {
        body.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    }
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(content);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

/// Build a multipart/form-data body with no file field.
pub fn multipart_fields_only(boundary: &str, fields: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

/// Make a store config with a custom bearer token.
pub fn config_with_token(mut config: Config, token: &str) -> Config {
    config.http_bearer_token = Some(token.to_string());
    config
}

/// A unique path under the system temporary directory.
pub fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dayfly-test-{}-{}-{name}",
        std::process::id(),
        rand::random::<u64>()
    ))
}

/// Write a temporary file and return its path.
pub fn write_temp_file(name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = temp_path(name);
    std::fs::write(&path, bytes).expect("temporary test file must be writable");
    path
}

/// Bytes with a repeating pattern, so multipart reassembly is not a run of
/// one byte and a missing part is visible.
pub fn pattern_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Metadata values written by [`Store::store_text`], for seeding objects
/// that look like dayfly wrote them.
pub fn stored_metadata(
    expires_at: &str,
    content_type: &str,
    size: usize,
    sha256: &str,
) -> Vec<(String, String)> {
    vec![
        ("sha256".to_string(), sha256.to_string()),
        ("size".to_string(), size.to_string()),
        ("ttl-days".to_string(), "1".to_string()),
        (
            "created-at".to_string(),
            (Utc::now() - chrono::Duration::days(1)).to_rfc3339(),
        ),
        ("expires-at".to_string(), expires_at.to_string()),
        ("content-type".to_string(), content_type.to_string()),
        ("detection-source".to_string(), "text".to_string()),
    ]
}

/// Convert owned metadata pairs to the slice form used by [`FakeS3::seed`].
pub fn metadata_refs(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect()
}

/// Build the full HTTP app: API routes plus the human UI routes.
pub fn app(store: Store) -> Router {
    let max_text_bytes = store.config.max_text_bytes;
    dayfly::http::api_router()
        .merge(dayfly::web::ui_router(max_text_bytes))
        .with_state(AppState { store })
}

/// Send one request to the router in process and collect the response.
pub async fn send(
    app: &Router,
    request: axum::http::Request<Body>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router call must complete");
    let status = response.status();
    let headers = response.headers().clone();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body must be readable")
        .to_vec();
    (status, headers, body)
}

/// Build a request with a body and the given headers.
pub fn request(
    method: &str,
    uri: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
) -> axum::http::Request<Body> {
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    builder.body(Body::from(body)).expect("request must build")
}

/// Decode a response body as JSON.
pub fn json_body(body: &[u8]) -> serde_json::Value {
    serde_json::from_slice(body).expect("response body must be JSON")
}

/// A small map of request headers for tests.
pub fn header_pairs(pairs: &[(&str, String)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}
