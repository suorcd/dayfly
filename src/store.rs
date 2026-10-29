//! S3 core for dayfly.
//!
//! Design rules enforced here, not just documented:
//!
//! * One S3 object per paste/file. No metadata sidecar, no database in v1.
//! * The binary is the only S3 credential holder. Agents never get S3
//!   credentials and no presigned URL is ever issued in v1.
//! * A store always creates a *new* random ID. Nothing in v1 overwrites an
//!   existing ID, and conditional writes (`If-None-Match`) are deliberately
//!   NOT relied on: some S3-compatible stores, including Garage, accept
//!   that header but ignore it and overwrite. See README.
//! * Expiry is day-granular and enforced on read by comparing stored
//!   `expires-at` metadata with now. S3 lifecycle deletion, if configured,
//!   is only a storage sweeper and may run after the TTL day.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use rand::Rng;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Config;
use crate::content_type::{detect_content_type, Detection};

pub const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const LIST_SCAN_MAX_PAGES: usize = 10;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("paste not found: {0}")]
    NotFound(String),
    #[error("bucket unavailable or misconfigured: {0}")]
    BucketUnavailable(String),
    #[error("paste expired: {0}")]
    Expired(String),
    #[error("payload too large: limit is {limit} bytes")]
    TooLarge { limit: u64 },
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("invalid date (expected YYYY-MM-DD): {0}")]
    InvalidDate(String),
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("range not satisfiable")]
    RangeNotSatisfiable,
    #[error("io error: {0}")]
    Io(String),
    #[error("s3 error: {0}")]
    S3(String),
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e.to_string())
    }
}

/// Map an AWS SDK error to a [`StoreError`] using the typed error metadata
/// rather than substring matching on the debug representation.
///
/// `NoSuchBucket` is deliberately kept distinct from `NoSuchKey`. A missing
/// bucket is an operator misconfiguration, and folding it into `NotFound` made
/// a wrong `S3_BUCKET` look like every paste had been deleted, while also
/// reporting a failed upload as "paste not found".
pub fn map_sdk_error<E, R>(err: SdkError<E, R>, id: &str) -> StoreError
where
    E: ProvideErrorMetadata + std::fmt::Debug,
    R: std::fmt::Debug,
{
    if let SdkError::ServiceError(ctx) = &err {
        match ctx.err().code() {
            Some("NoSuchKey") | Some("NoSuchVersion") | Some("NotFound") => {
                return StoreError::NotFound(id.to_string());
            }
            Some("NoSuchBucket") => {
                return StoreError::BucketUnavailable(id.to_string());
            }
            _ => {}
        }
    }
    StoreError::S3(format!("{err:?}"))
}

#[derive(Debug, Clone)]
pub struct StoredPaste {
    /// Full paste ID returned to clients: "<yyyy-mm-dd>/<random-id>".
    pub id: String,
    pub key: String,
    pub size: u64,
    pub sha256: String,
    pub content_type: String,
    pub filename: Option<String>,
    /// "client" | "sniffed" | "extension" | "text" | "fallback".
    pub detection_source: Option<String>,
    /// "explicit -> sniffed" when a sniffed signature overrode the client.
    pub detection_conflict: Option<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PasteMetadata {
    pub id: String,
    pub key: String,
    pub filename: Option<String>,
    pub content_type: String,
    pub detection_source: Option<String>,
    pub detection_conflict: Option<String>,
    pub size: u64,
    pub sha256: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub expired: bool,
}

#[derive(Debug, Clone)]
pub struct TextSlice {
    pub text: String,
    pub total_bytes: u64,
    pub returned_bytes: u64,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct PasteSummary {
    pub id: String,
    pub key: String,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
    /// From HeadObject, when available. `None` means the object has no
    /// expiry metadata (e.g. written by another tool).
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub filename: Option<String>,
}

pub const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Encode a u128 in base62. Pure function, unit-tested.
pub fn base62_encode(mut n: u128) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut out = Vec::new();
    while n > 0 {
        let idx = (n % 62) as usize;
        out.push(BASE62[idx]);
        n /= 62;
    }
    out.reverse();
    String::from_utf8(out).expect("base62 alphabet is ASCII")
}

/// At least 128 bits of entropy, encoded in base62 (at most 22 chars).
pub fn generate_random_id() -> String {
    let n: u128 = rand::rng().random();
    base62_encode(n)
}

/// Validate a client-supplied paste ID.
///
/// Accepts either a bare random ID (`abc123…`) or a full ID
/// (`2026-10-28/abc123…`). Segments must be non-empty, must not be `.` or
/// `..`, and may only contain ASCII alphanumerics, `-`, `_` and `.`.
/// The key itself is never taken from user input verbatim: it is always
/// rebuilt as `pastes/<validated-id>`.
pub fn validate_id(id: &str) -> Result<(), StoreError> {
    if id.is_empty() || id.len() > 128 {
        return Err(StoreError::InvalidId(
            "id must be non-empty and at most 128 characters".into(),
        ));
    }
    let stripped = id.strip_prefix("pastes/").unwrap_or(id);
    if stripped.is_empty() {
        return Err(StoreError::InvalidId("empty id".into()));
    }
    for segment in stripped.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(StoreError::InvalidId(format!("bad id segment in {id:?}")));
        }
        if !segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(StoreError::InvalidId(format!(
                "bad characters in id {id:?}"
            )));
        }
    }
    Ok(())
}

pub fn key_for(date: &str, random_id: &str) -> String {
    format!("pastes/{date}/{random_id}")
}

/// Parse an HTTP `Range` header value into an inclusive (start, end) pair
/// clamped to `size`. Only a single range is supported.
pub fn parse_range_header(header: &str, size: u64) -> Result<(u64, u64), StoreError> {
    let value = header.trim();
    let rest = value
        .strip_prefix("bytes=")
        .ok_or_else(|| StoreError::InvalidRange("range must start with bytes=".into()))?;
    if rest.contains(',') {
        return Err(StoreError::InvalidRange(
            "multiple ranges are not supported".into(),
        ));
    }
    let (start_s, end_s) = rest
        .split_once('-')
        .ok_or_else(|| StoreError::InvalidRange("range must contain '-'".into()))?;
    if size == 0 {
        return Err(StoreError::RangeNotSatisfiable);
    }
    let (start, end) = if start_s.is_empty() {
        // Suffix range: bytes=-N means the last N bytes.
        let suffix: u64 = end_s
            .parse()
            .map_err(|_| StoreError::InvalidRange("bad suffix length".into()))?;
        if suffix == 0 {
            return Err(StoreError::RangeNotSatisfiable);
        }
        (size.saturating_sub(suffix), size - 1)
    } else {
        let start: u64 = start_s
            .parse()
            .map_err(|_| StoreError::InvalidRange("bad range start".into()))?;
        if start >= size {
            return Err(StoreError::RangeNotSatisfiable);
        }
        let end = if end_s.is_empty() {
            size - 1
        } else {
            let e: u64 = end_s
                .parse()
                .map_err(|_| StoreError::InvalidRange("bad range end".into()))?;
            e.min(size - 1)
        };
        if end < start {
            return Err(StoreError::InvalidRange("range end before start".into()));
        }
        (start, end)
    };
    Ok((start, end))
}

/// Byte window for an offset/limit text fetch, clamped to `size`.
/// Returns None when offset is at or past the end.
pub fn offset_limit_window(offset: u64, limit: Option<u64>, size: u64) -> Option<(u64, u64)> {
    if offset >= size {
        return None;
    }
    let end = match limit {
        Some(l) => (offset.saturating_add(l)).min(size),
        None => size,
    };
    if end <= offset {
        return None;
    }
    Some((offset, end))
}

/// Longest valid UTF-8 prefix of `bytes`. Used when a byte range cuts a
/// multi-byte character at the end; the caller reports truncation.
pub fn valid_utf8_prefix(bytes: &[u8]) -> &str {
    let mut end = bytes.len();
    while end > 0 {
        if let Ok(s) = std::str::from_utf8(&bytes[..end]) {
            return s;
        }
        end -= 1;
    }
    ""
}

fn encode_filename(filename: &str) -> String {
    utf8_percent_encode(filename, NON_ALPHANUMERIC).to_string()
}

fn decode_filename(encoded: &str) -> String {
    percent_decode_str(encoded)
        .decode_utf8()
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| encoded.to_string())
}

/// Detection for in-memory / local-file uploads, where the first bytes
/// are available before the S3 call. Streaming HTTP uploads use
/// [`crate::content_type::detect_and_wrap`] instead, which peeks at the
/// live stream; both paths go through the same precedence rules.
fn detection_for(filename: Option<&str>, supplied: Option<&str>, prefix: &[u8]) -> Detection {
    detect_content_type(supplied, filename, prefix)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex_encode(&hasher.finalize())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn metadata_for(
    filename: Option<&str>,
    sha256: &str,
    size: u64,
    ttl_days: u32,
    created_at: DateTime<Utc>,
    detection: &Detection,
) -> (HashMap<String, String>, DateTime<Utc>) {
    let expires_at = created_at + chrono::Duration::days(i64::from(ttl_days));
    let mut meta = HashMap::new();
    if let Some(f) = filename {
        if !f.is_empty() {
            meta.insert("filename".to_string(), encode_filename(f));
        }
    }
    meta.insert("sha256".to_string(), sha256.to_string());
    meta.insert("size".to_string(), size.to_string());
    meta.insert("ttl-days".to_string(), ttl_days.to_string());
    meta.insert("created-at".to_string(), created_at.to_rfc3339());
    meta.insert("expires-at".to_string(), expires_at.to_rfc3339());
    // Final Content-Type is also stored in metadata (not just the S3
    // Content-Type header) so HeadObject alone recovers the full record.
    meta.insert("content-type".to_string(), detection.content_type.clone());
    meta.insert("detection-source".to_string(), detection.source.to_string());
    if let Some(conflict) = &detection.conflict {
        meta.insert("detection-conflict".to_string(), conflict.clone());
    }
    (meta, expires_at)
}

fn parse_rfc3339(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[derive(Clone)]
pub struct Store {
    client: Client,
    pub bucket: String,
    pub config: Config,
}

/// The S3 side of a streaming upload (see [`crate::stream::UploadSink`]).
/// Target backend is Garage (v1); the code stays S3-shaped, but no
/// AWS/MinIO-specific tuning or multi-backend abstraction is attempted.
struct AwsUploadSink {
    client: Client,
    bucket: String,
    key: String,
    content_type: String,
    metadata: HashMap<String, String>,
}

#[async_trait::async_trait]
impl crate::stream::UploadSink for AwsUploadSink {
    async fn put_small(&mut self, bytes: Vec<u8>, sha256: &str) -> Result<(), StoreError> {
        let mut meta = self.metadata.clone();
        meta.insert("sha256".to_string(), sha256.to_string());
        meta.insert("size".to_string(), bytes.len().to_string());
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(ByteStream::from(bytes))
            .content_type(self.content_type.clone())
            .set_metadata(Some(meta))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &self.key))?;
        Ok(())
    }

    async fn create_multipart(&mut self) -> Result<String, StoreError> {
        let out = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .content_type(self.content_type.clone())
            .set_metadata(Some(self.metadata.clone()))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &self.key))?;
        let id = out.upload_id().unwrap_or("").to_string();
        if id.is_empty() {
            return Err(StoreError::S3(
                "multipart upload returned no upload id".into(),
            ));
        }
        Ok(id)
    }

    async fn upload_part(
        &mut self,
        upload_id: &str,
        part_number: i32,
        bytes: Vec<u8>,
    ) -> Result<String, StoreError> {
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &self.key))?;
        Ok(out.e_tag().unwrap_or("").to_string())
    }

    async fn complete(
        &mut self,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), StoreError> {
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                parts
                    .into_iter()
                    .map(|(n, etag)| CompletedPart::builder().part_number(n).e_tag(etag).build())
                    .collect(),
            ))
            .build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &self.key))?;
        Ok(())
    }

    async fn abort(&mut self, upload_id: &str) {
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(upload_id)
            .send()
            .await;
    }
}

impl Store {
    pub async fn from_config(config: Config) -> Result<Self, StoreError> {
        // Credentials come from the environment variables named in Config
        // only. The AWS default chain is not used, so a stray AWS profile
        // or instance role can never silently supply credentials.
        let credentials = Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "dayfly-env",
        );
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(config.endpoint.clone())
            .region(Region::new(config.region.clone()))
            .credentials_provider(credentials)
            .load()
            .await;
        let s3_config = S3ConfigBuilder::from(&sdk_config)
            .force_path_style(true)
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
            bucket: config.bucket.clone(),
            config,
        })
    }

    /// Resolve a client ID to an S3 key.
    ///
    /// Full IDs (`yyyy-mm-dd/<random>`) resolve directly. Bare random IDs
    /// are resolved by scanning the `pastes/` prefix (at most
    /// [`LIST_SCAN_MAX_PAGES`] pages of 1,000 keys). That scan is the cost
    /// of not having an index: clients should keep the full ID returned
    /// at store time.
    pub async fn resolve_key(&self, id: &str) -> Result<String, StoreError> {
        validate_id(id)?;
        let stripped = id.strip_prefix("pastes/").unwrap_or(id);
        if stripped.contains('/') {
            return Ok(format!("pastes/{stripped}"));
        }
        let suffix = format!("/{stripped}");
        let mut token: Option<String> = None;
        for _ in 0..LIST_SCAN_MAX_PAGES {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix("pastes/")
                .max_keys(1000);
            if let Some(t) = token {
                req = req.continuation_token(t);
            }
            let out = req
                .send()
                .await
                .map_err(|e| map_sdk_error(e, id))?;
            for obj in out.contents() {
                if let Some(key) = obj.key() {
                    if key.ends_with(&suffix) || key == format!("pastes/{stripped}") {
                        return Ok(key.to_string());
                    }
                }
            }
            match out.next_continuation_token() {
                Some(t) if out.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
                _ => break,
            }
        }
        Err(StoreError::NotFound(id.to_string()))
    }

    pub async fn store_text(
        &self,
        text: &[u8],
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl_days: Option<u32>,
    ) -> Result<StoredPaste, StoreError> {
        if u64::try_from(text.len()).unwrap_or(u64::MAX) > self.config.max_text_bytes {
            return Err(StoreError::TooLarge {
                limit: self.config.max_text_bytes,
            });
        }
        let ttl = ttl_days.unwrap_or(self.config.default_ttl_days);
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let sha = sha256_hex(text);
        // Text with no filename and no explicit type is still detected
        // (it will come out as text/plain), so the stored type is never
        // a blind guess.
        let detection = detection_for(
            filename,
            content_type,
            &text[..text.len().min(crate::content_type::PEEK_BYTES)],
        );
        let ct = detection.content_type.clone();
        let (meta, expires_at) = metadata_for(
            filename,
            &sha,
            text.len() as u64,
            ttl,
            created_at,
            &detection,
        );
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(text.to_vec()))
            .content_type(ct.clone())
            .set_metadata(Some(meta))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))?;
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            size: text.len() as u64,
            sha256: sha,
            content_type: ct,
            filename: filename.map(|s| s.to_string()),
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
        })
    }

    /// Store a local file. Files up to one multipart chunk use a single
    /// PutObject; larger files use S3 multipart upload, reading the file
    /// in 8 MiB chunks so a large file is never loaded whole into memory.
    pub async fn store_file_path(
        &self,
        path: &Path,
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl_days: Option<u32>,
    ) -> Result<StoredPaste, StoreError> {
        let file_meta = tokio::fs::metadata(path).await?;
        if !file_meta.is_file() {
            return Err(StoreError::Io(format!(
                "not a regular file: {}",
                path.display()
            )));
        }
        let size = file_meta.len();
        if size > self.config.max_file_bytes {
            return Err(StoreError::TooLarge {
                limit: self.config.max_file_bytes,
            });
        }
        let ttl = ttl_days.unwrap_or(self.config.default_ttl_days);
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string()).or_else(|| {
            path.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        });
        // Sniff the first bytes for detection, then re-read the file
        // from the start for the actual upload, so no bytes are lost.
        let prefix: Vec<u8> = {
            let mut f = File::open(path).await?;
            let mut buf = vec![0u8; crate::content_type::PEEK_BYTES];
            let n = f.read(&mut buf).await?;
            buf.truncate(n);
            buf
        };
        let detection = detection_for(display_name.as_deref(), content_type, &prefix);
        let ct = detection.content_type.clone();

        if size as usize <= MULTIPART_CHUNK_BYTES {
            let bytes = tokio::fs::read(path).await?;
            let sha = sha256_hex(&bytes);
            let (meta, expires_at) = metadata_for(
                display_name.as_deref(),
                &sha,
                size,
                ttl,
                created_at,
                &detection,
            );
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(bytes))
                .content_type(ct.clone())
                .set_metadata(Some(meta))
                .send()
                .await
                .map_err(|e| map_sdk_error(e, &key))?;
            return Ok(StoredPaste {
                id: format!("{date}/{random_id}"),
                key,
                size,
                sha256: sha,
                content_type: ct,
                filename: display_name,
                detection_source: Some(detection.source.to_string()),
                detection_conflict: detection.conflict,
                expires_at,
            });
        }

        // Multipart path: single pass over the file, hashing as we go.
        let mut hasher = Sha256::new();
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .content_type(ct.clone())
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))?;
        let upload_id = create.upload_id().unwrap_or("").to_string();
        if upload_id.is_empty() {
            return Err(StoreError::S3(
                "multipart upload returned no upload id".into(),
            ));
        }
        let result: Result<Vec<CompletedPart>, StoreError> = async {
            let mut file = File::open(path).await?;
            let mut parts: Vec<CompletedPart> = Vec::new();
            let mut part_number: i32 = 1;
            let mut buf = vec![0u8; MULTIPART_CHUNK_BYTES];
            loop {
                let mut filled = 0usize;
                while filled < buf.len() {
                    let n = file.read(&mut buf[filled..]).await?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                if filled == 0 {
                    break;
                }
                hasher.update(&buf[..filled]);
                let chunk = buf[..filled].to_vec();
                let part = self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(ByteStream::from(chunk))
                    .send()
                    .await
                    .map_err(|e| map_sdk_error(e, &key))?;
                parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(part.e_tag().unwrap_or(""))
                        .build(),
                );
                part_number += 1;
                if filled < MULTIPART_CHUNK_BYTES {
                    break;
                }
            }
            Ok(parts)
        }
        .await;
        let parts = match result {
            Ok(p) => p,
            Err(e) => {
                let _ = self
                    .client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                return Err(e);
            }
        };
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        if let Err(e) = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
        {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(map_sdk_error(e, &key));
        }
        let sha = hex_encode(&hasher.finalize());
        // Metadata for a multipart object cannot be set at completion time
        // through the same call, so it is attached with a copy-in-place:
        // a CopyObject onto the same key with metadata replacement. If
        // the store does not support that, the object is still stored and
        // fetchable; expiry is then only visible via the returned record
        // and the lifecycle sweeper, not via HeadObject metadata.
        let (meta, expires_at) = metadata_for(
            display_name.as_deref(),
            &sha,
            size,
            ttl,
            created_at,
            &detection,
        );
        let copy_source = format!("{}/{}", self.bucket, key);
        let _ = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(&key)
            .copy_source(copy_source)
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .content_type(ct.clone())
            .set_metadata(Some(meta))
            .send()
            .await;
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            size,
            sha256: sha,
            content_type: ct,
            filename: display_name,
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
        })
    }

    /// Streaming upload: detect Content-Type from the first peeked chunk
    /// (via [`crate::content_type::detect_and_wrap`]), then drive the
    /// S3 sink. The peeked bytes are forwarded exactly once by the
    /// wrapper, so detection never loses or duplicates data.
    pub async fn store_stream<S>(
        &self,
        stream: S,
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl_days: Option<u32>,
        known_length: Option<u64>,
    ) -> Result<StoredPaste, StoreError>
    where
        S: futures::Stream<Item = Result<Bytes, StoreError>> + Send + Unpin,
    {
        let ttl = ttl_days.unwrap_or(self.config.default_ttl_days);
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string());
        let (detection, wrapped) =
            crate::content_type::detect_and_wrap(stream, content_type, filename).await;
        let ct = detection.content_type.clone();
        // Metadata for the initial upload: filename, TTL, expiry, final
        // type and detection record. sha256/size are added by put_small
        // or by the post-multipart copy-in-place (see below).
        let (initial_meta, expires_at) =
            metadata_for(display_name.as_deref(), "", 0, ttl, created_at, &detection);
        let mut sink = AwsUploadSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.clone(),
            content_type: ct.clone(),
            metadata: initial_meta,
        };
        let outcome = crate::stream::run_streaming_upload(
            wrapped,
            &mut sink,
            known_length,
            self.config.max_file_bytes,
        )
        .await?;
        if outcome.used_multipart {
            // Attach the full metadata (sha256, real size) by copy-in-
            // place, best effort, exactly as in store_file_path.
            let (meta, _) = metadata_for(
                display_name.as_deref(),
                &outcome.sha256,
                outcome.total_bytes,
                ttl,
                created_at,
                &detection,
            );
            let copy_source = format!("{}/{}", self.bucket, key);
            let _ = self
                .client
                .copy_object()
                .bucket(&self.bucket)
                .key(&key)
                .copy_source(copy_source)
                .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
                .content_type(ct.clone())
                .set_metadata(Some(meta))
                .send()
                .await;
        }
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            size: outcome.total_bytes,
            sha256: outcome.sha256,
            content_type: ct,
            filename: display_name,
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
        })
    }

    pub async fn head_metadata(&self, id: &str) -> Result<PasteMetadata, StoreError> {
        let key = self.resolve_key(id).await?;
        let out = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, id))?;
        let meta = out.metadata().cloned().unwrap_or_default();
        let created_at = meta.get("created-at").and_then(|s| parse_rfc3339(s));
        let expires_at = meta.get("expires-at").and_then(|s| parse_rfc3339(s));
        let expired = expires_at.map(|e| Utc::now() > e).unwrap_or(false);
        // Prefer the S3 Content-Type header; fall back to stored metadata.
        let content_type = out
            .content_type()
            .map(|s| s.to_string())
            .or_else(|| meta.get("content-type").cloned())
            .unwrap_or_else(|| "application/octet-stream".to_string());
        Ok(PasteMetadata {
            id: key.strip_prefix("pastes/").unwrap_or(&key).to_string(),
            key,
            filename: meta.get("filename").map(|s| decode_filename(s)),
            content_type,
            detection_source: meta.get("detection-source").cloned(),
            detection_conflict: meta.get("detection-conflict").cloned(),
            size: u64::try_from(out.content_length().unwrap_or(0)).unwrap_or(0),
            sha256: meta.get("sha256").cloned(),
            created_at,
            expires_at,
            expired,
        })
    }

    fn check_not_expired(meta: &PasteMetadata) -> Result<(), StoreError> {
        if meta.expired {
            return Err(StoreError::Expired(meta.id.clone()));
        }
        Ok(())
    }

    /// Fetch a byte range from an already-resolved object key.
    ///
    /// Callers that already hold metadata should use this rather than resolving
    /// the ID again, because resolution issues a `HeadObject` and, for a bare
    /// ID, a bucket scan. Downloading in chunks through the ID-based path
    /// re-ran that once per chunk, so a 64 MiB download cost nine prefix
    /// listings and nine heads.
    pub async fn fetch_range_by_key(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, StoreError> {
        let mut req = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some((start, end)) = range {
            req = req.range(format!("bytes={start}-{end}"));
        }
        let out = req.send().await.map_err(|e| map_sdk_error(e, key))?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?
            .to_vec();
        Ok(bytes)
    }

    pub async fn fetch_text(
        &self,
        id: &str,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<TextSlice, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let Some((start, end_exclusive)) = offset_limit_window(offset, limit, meta.size) else {
            return Ok(TextSlice {
                text: String::new(),
                total_bytes: meta.size,
                returned_bytes: 0,
                truncated: false,
            });
        };
        let bytes = self
            .fetch_range_by_key(&meta.key, Some((start, end_exclusive - 1)))
            .await?;
        let text = valid_utf8_prefix(&bytes).to_string();
        let returned = text.len() as u64;
        Ok(TextSlice {
            text,
            total_bytes: meta.size,
            returned_bytes: returned,
            truncated: offset + returned < meta.size,
        })
    }

    /// Download an object to a local file in 8 MiB range chunks, so the
    /// whole object is never held in memory. `range` is an inclusive
    /// (start, end) pair; None means the whole object.
    pub async fn download_file(
        &self,
        id: &str,
        dest: &Path,
        range: Option<(u64, u64)>,
    ) -> Result<PasteMetadata, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let (mut start, end) = match range {
            Some((s, e)) => {
                if s >= meta.size {
                    return Err(StoreError::RangeNotSatisfiable);
                }
                (s, e.min(meta.size.saturating_sub(1)))
            }
            None => (0, meta.size.saturating_sub(1)),
        };
        if meta.size == 0 {
            tokio::fs::write(dest, b"").await?;
            return Ok(meta);
        }
        let mut file = File::create(dest).await?;
        while start <= end {
            let chunk_end = (start + MULTIPART_CHUNK_BYTES as u64 - 1).min(end);
            let bytes = self
                .fetch_range_by_key(&meta.key, Some((start, chunk_end)))
                .await?;
            if bytes.is_empty() {
                break;
            }
            file.write_all(&bytes).await?;
            start = chunk_end + 1;
        }
        file.flush().await?;
        Ok(meta)
    }

    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        let key = self.resolve_key(id).await?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, id))?;
        Ok(())
    }

    /// List recent pastes for one date prefix only, by scanning the
    /// bucket directly: ListObjectsV2 (one page, up to 1,000 keys) for
    /// the `pastes/<date>/` prefix, then HeadObject per key to recover
    /// expiry/content-type/filename and to filter expired objects out.
    ///
    /// There is no database in v1 and no SQLite placeholder. This is
    /// deliberately O(n) in the objects scanned (plus one Head per key):
    /// it is fast for a small internal bucket and gets slower as the
    /// bucket grows. That slowdown is exactly the trigger for adding a
    /// database later — it is documented in README, not hidden, and no
    /// database schema is invented in v1. Results are in S3 key order,
    /// not sorted by anything else, and this is not search.
    pub async fn list_recent(
        &self,
        date: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PasteSummary>, StoreError> {
        let date_str = match date {
            Some(d) => {
                NaiveDate::parse_from_str(d, "%Y-%m-%d")
                    .map_err(|_| StoreError::InvalidDate(d.to_string()))?;
                d.to_string()
            }
            None => Utc::now().format("%Y-%m-%d").to_string(),
        };
        let limit = limit.clamp(1, 1000) as i32;
        let out = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(format!("pastes/{date_str}/"))
            .max_keys(limit)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &date_str))?;
        let mut summaries = Vec::new();
        for obj in out.contents() {
            let Some(key) = obj.key() else { continue };
            let id = key.strip_prefix("pastes/").unwrap_or(key).to_string();
            // HeadObject per key: ListObjectsV2 returns no user metadata.
            // A head failure (e.g. deleted between list and head) drops
            // the entry instead of failing the whole listing.
            let meta = self.head_metadata(&id).await.ok();
            if let Some(m) = &meta {
                if m.expired {
                    continue;
                }
            }
            summaries.push(PasteSummary {
                id,
                key: key.to_string(),
                size: u64::try_from(obj.size().unwrap_or(0)).unwrap_or(0),
                last_modified: obj
                    .last_modified()
                    .and_then(|dt| DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())),
                expires_at: meta.as_ref().and_then(|m| m.expires_at),
                created_at: meta.as_ref().and_then(|m| m.created_at),
                content_type: meta.as_ref().map(|m| m.content_type.clone()),
                filename: meta.as_ref().and_then(|m| m.filename.clone()),
            });
        }
        Ok(summaries)
    }
}

/// Parse a `DateTime` from an S3 `LastModified` smithy date without
/// depending on its exact type in tests. Kept small: production code uses
/// the `DateTime::from_timestamp` call in `list_recent`.
#[allow(dead_code)]
fn _unused(_: SystemTime, _: Duration) {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn base62_roundtrip_shape() {
        assert_eq!(base62_encode(0), "0");
        assert_eq!(base62_encode(61), "z");
        assert_eq!(base62_encode(62), "10");
        let id = base62_encode(u128::MAX);
        assert!(!id.is_empty() && id.len() <= 22);
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn random_id_has_high_entropy_shape() {
        let id = generate_random_id();
        assert!(!id.is_empty() && id.len() <= 22);
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn id_validation_rejects_traversal() {
        assert!(validate_id("../etc").is_err());
        assert!(validate_id("").is_err());
        assert!(validate_id("a/b/../../c").is_err());
        assert!(validate_id("2026-10-28/abcXYZ0123").is_ok());
        assert!(validate_id("abcXYZ0123").is_ok());
        assert!(validate_id("bad id!").is_err());
    }

    #[test]
    fn key_layout() {
        assert_eq!(key_for("2026-10-28", "abc"), "pastes/2026-10-28/abc");
    }

    #[test]
    fn range_parsing() {
        assert_eq!(parse_range_header("bytes=0-99", 1000).unwrap(), (0, 99));
        assert_eq!(parse_range_header("bytes=900-", 1000).unwrap(), (900, 999));
        assert_eq!(parse_range_header("bytes=-10", 1000).unwrap(), (990, 999));
        assert_eq!(parse_range_header("bytes=0-9999", 1000).unwrap(), (0, 999));
        assert!(parse_range_header("bytes=1000-1001", 1000).is_err());
        assert!(parse_range_header("bytes=0-10,20-30", 1000).is_err());
        assert!(parse_range_header("garbage", 1000).is_err());
    }

    #[test]
    fn offset_limit_windows() {
        assert_eq!(offset_limit_window(0, Some(10), 100), Some((0, 10)));
        assert_eq!(offset_limit_window(90, Some(20), 100), Some((90, 100)));
        assert_eq!(offset_limit_window(100, Some(10), 100), None);
        assert_eq!(offset_limit_window(0, None, 5), Some((0, 5)));
    }

    #[test]
    fn utf8_prefix_truncation() {
        // "é" is two bytes; cutting after the first byte must not panic
        // and must not produce a replacement character in the middle.
        let bytes = "aé".as_bytes();
        assert_eq!(valid_utf8_prefix(&bytes[..2]), "a");
        assert_eq!(valid_utf8_prefix("hello".as_bytes()), "hello");
    }

    #[test]
    fn expiry_metadata_roundtrip_shape() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
        let (meta, expires) = metadata_for(Some("a b.txt"), "deadbeef", 3, 7, created, &detection);
        assert_eq!(meta.get("detection-source").unwrap(), "text");
        assert_eq!(
            meta.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(meta.get("ttl-days").unwrap(), "7");
        // NON_ALPHANUMERIC encodes '.' too; decode still round-trips.
        assert_eq!(meta.get("filename").unwrap(), "a%20b%2Etxt");
        assert_eq!(decode_filename(meta.get("filename").unwrap()), "a b.txt");
        assert!(expires > created);
    }
}
