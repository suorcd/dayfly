//! A small S3-compatible fake for tests.
//!
//! This module is compiled for tests only. It implements the operations
//! that [`crate::store::Store`] uses: PutObject, GetObject (with Range),
//! HeadObject, ListObjectsV2 and DeleteObject. It does not check
//! signatures and it does not implement multipart upload.
//!
//! The fake records every PutObject key and body, and every GetObject
//! range. A test can therefore assert that an append writes only the new
//! bytes, and that a range read asks for only the bytes it needs.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use percent_encoding::percent_decode_str;

#[derive(Clone, Default)]
pub struct FakeS3 {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, FakeObject>,
    puts: Vec<(String, Vec<u8>)>,
    gets: Vec<(String, Option<String>)>,
    deletes: Vec<String>,
}

#[derive(Clone, Default)]
pub struct FakeObject {
    pub body: Vec<u8>,
    pub content_type: String,
    pub metadata: BTreeMap<String, String>,
}

impl FakeS3 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn object(&self, key: &str) -> Option<FakeObject> {
        self.inner.lock().unwrap().objects.get(key).cloned()
    }

    pub fn puts(&self) -> Vec<(String, Vec<u8>)> {
        self.inner.lock().unwrap().puts.clone()
    }

    pub fn gets(&self) -> Vec<(String, Option<String>)> {
        self.inner.lock().unwrap().gets.clone()
    }

    /// Replace one metadata value on a stored object. Tests use this to
    /// make a paste expired without waiting.
    pub fn set_metadata(&self, key: &str, name: &str, value: &str) {
        if let Some(object) = self.inner.lock().unwrap().objects.get_mut(key) {
            object.metadata.insert(name.to_string(), value.to_string());
        }
    }
}

/// Start the fake on a loopback port and return its state and endpoint.
pub async fn spawn() -> (FakeS3, String) {
    let fake = FakeS3::new();
    let app = Router::new().fallback(handle).with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake S3 must bind a loopback port");
    let addr = listener.local_addr().expect("fake S3 must have an address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("fake S3 server must run");
    });
    (fake, format!("http://{addr}"))
}

async fn handle(State(fake): State<FakeS3>, req: Request) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let path = uri.path().to_string();
    let (bucket, key) = split_path(&path);
    if bucket.is_empty() {
        return error_response(StatusCode::NOT_FOUND, "NoSuchBucket", "no bucket");
    }
    if method == Method::GET && key.is_empty() {
        return list_response(&fake, &uri);
    }
    if key.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "missing key");
    }
    match method {
        Method::PUT => put_response(&fake, &key, &headers, req).await,
        Method::HEAD => head_response(&fake, &key),
        Method::GET => get_response(&fake, &key, &headers),
        Method::DELETE => {
            fake.inner.lock().unwrap().deletes.push(key.clone());
            fake.inner.lock().unwrap().objects.remove(&key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "unsupported method",
        ),
    }
}

fn split_path(path: &str) -> (String, String) {
    let rest = path.strip_prefix('/').unwrap_or(path);
    let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
    (bucket.to_string(), key.to_string())
}

async fn put_response(fake: &FakeS3, key: &str, headers: &HeaderMap, req: Request) -> Response {
    let body = match axum::body::to_bytes(req.into_body(), usize::MAX).await {
        Ok(body) => body.to_vec(),
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", &e.to_string());
        }
    };
    let body = decode_aws_chunked(headers, body);
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let mut metadata = BTreeMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if let Some(meta_name) = name.strip_prefix("x-amz-meta-") {
            if let Ok(value) = value.to_str() {
                metadata.insert(meta_name.to_string(), value.to_string());
            }
        }
    }
    fake.inner
        .lock()
        .unwrap()
        .puts
        .push((key.to_string(), body.clone()));
    fake.inner.lock().unwrap().objects.insert(
        key.to_string(),
        FakeObject {
            body,
            content_type,
            metadata,
        },
    );
    (StatusCode::OK, [("etag", "\"fake-etag\"")]).into_response()
}

fn head_response(fake: &FakeS3, key: &str) -> Response {
    let Some(object) = fake.object(key) else {
        return error_response(StatusCode::NOT_FOUND, "NoSuchKey", "missing key");
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&object.body.len().to_string()).unwrap(),
    );
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_str(&object.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    for (name, value) in &object.metadata {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(format!("x-amz-meta-{name}").as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
    response
}

fn get_response(fake: &FakeS3, key: &str, headers: &HeaderMap) -> Response {
    let range = headers
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    fake.inner
        .lock()
        .unwrap()
        .gets
        .push((key.to_string(), range.clone()));
    let Some(object) = fake.object(key) else {
        return error_response(StatusCode::NOT_FOUND, "NoSuchKey", "missing key");
    };
    let total = object.body.len() as u64;
    let Some(range) = range else {
        let mut response = Response::new(Body::from(object.body));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert("content-length", total.to_string().parse().unwrap());
        return response;
    };
    let Some((start, end)) = parse_range(&range, total) else {
        return error_response(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "InvalidRange",
            "range not satisfiable",
        );
    };
    let body = object.body[start as usize..=end as usize].to_vec();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    let headers = response.headers_mut();
    headers.insert(
        "content-range",
        format!("bytes {start}-{end}/{total}").parse().unwrap(),
    );
    headers.insert(
        "content-length",
        (end - start + 1).to_string().parse().unwrap(),
    );
    response
}

fn parse_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("bytes=")?;
    let (start, end) = rest.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        total.checked_sub(1)?
    } else {
        end.parse().ok()?
    };
    if start >= total || end < start {
        return None;
    }
    Some((start, end.min(total - 1)))
}

fn list_response(fake: &FakeS3, uri: &Uri) -> Response {
    let prefix = query_param(uri, "prefix").unwrap_or_default();
    let objects: Vec<(String, u64)> = fake
        .inner
        .lock()
        .unwrap()
        .objects
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, object)| (key.clone(), object.body.len() as u64))
        .collect();
    let mut contents = String::new();
    for (key, size) in &objects {
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>2026-10-28T00:00:00.000Z</LastModified><ETag>\"fake-etag\"</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key)
        ));
    }
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test-bucket</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{}</ListBucketResult>",
        xml_escape(&prefix),
        objects.len(),
        contents
    );
    (StatusCode::OK, [("content-type", "application/xml")], xml).into_response()
}

fn query_param(uri: &Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == name {
            Some(percent_decode_str(value).decode_utf8_lossy().into_owned())
        } else {
            None
        }
    })
}

fn decode_aws_chunked(headers: &HeaderMap, body: Vec<u8>) -> Vec<u8> {
    let chunked = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("aws-chunked"))
        .unwrap_or(false)
        || headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.starts_with("STREAMING-"))
            .unwrap_or(false);
    if !chunked {
        return body;
    }
    let mut out = Vec::new();
    let mut rest = &body[..];
    while let Some(line_end) = rest.windows(2).position(|w| w == b"\r\n") {
        let line = &rest[..line_end];
        let size_text = line
            .split(|b| *b == b';')
            .next()
            .and_then(|s| std::str::from_utf8(s).ok());
        let Some(size) = size_text.and_then(|s| usize::from_str_radix(s.trim(), 16).ok()) else {
            break;
        };
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        if rest.len() < size {
            break;
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size..];
        if rest.starts_with(b"\r\n") {
            rest = &rest[2..];
        }
    }
    out
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message><RequestId>fake-request</RequestId><HostId>fake-host</HostId></Error>",
        xml_escape(code),
        xml_escape(message)
    );
    (status, [("content-type", "application/xml")], xml).into_response()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
