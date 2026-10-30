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
//! * Expiry is enforced on read by comparing stored `expires-at` metadata
//!   with now. A TTL can be shorter than one day. S3 lifecycle deletion,
//!   if configured, is only a day-rounded storage sweeper and may run
//!   after expiry.

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
use futures::StreamExt;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use rand::Rng;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Config;
use crate::content_type::{detect_content_type, detect_content_type_sample, Detection};

pub const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const LIST_SCAN_MAX_PAGES: usize = 10;
/// Longest accepted listing window, in days. This cap bounds the number of
/// date prefixes that one request can scan.
pub const LIST_WINDOW_MAX_DAYS: usize = 366;

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

/// A time-to-live duration for a stored object.
///
/// A TTL can be shorter than one day. The stored `expires-at` metadata
/// uses the exact duration, not a rounded day count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ttl {
    duration: chrono::Duration,
}

/// An invalid TTL value from a header, query parameter, form field, or
/// MCP parameter. It also reports a request that supplies both TTL forms.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub struct TtlParseError {
    value: Option<String>,
    reason: String,
    hint: &'static str,
}

impl std::fmt::Display for TtlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            Some(value) => write!(f, "invalid TTL {value:?}: {}. {}", self.reason, self.hint),
            None => write!(f, "invalid TTL: {}. {}", self.reason, self.hint),
        }
    }
}

const TTL_FORMS_HINT: &str =
    "Use seconds (\"3600\") or number and unit pairs (\"90m\", \"1h30m\", \"2d\", \"1d12h\").";
const TTL_DAYS_HINT: &str = "Use a positive integer number of whole days.";

impl TtlParseError {
    fn new(value: &str, reason: impl Into<String>) -> Self {
        Self {
            value: Some(value.to_string()),
            reason: reason.into(),
            hint: TTL_FORMS_HINT,
        }
    }

    /// An invalid value for the legacy `ttl_days` form.
    pub fn invalid_days(value: &str) -> Self {
        Self {
            value: Some(value.to_string()),
            reason: "the value must be a positive integer".to_string(),
            hint: TTL_DAYS_HINT,
        }
    }

    /// A request supplied both a duration form and a day form.
    pub fn mixed_forms() -> Self {
        Self {
            value: None,
            reason: "use one TTL form, not both".to_string(),
            hint: "Remove either the duration form or the day form.",
        }
    }
}

impl Ttl {
    /// A TTL in whole days. This constructor serves the legacy
    /// `ttl_days` inputs and `DAYFLY_DEFAULT_TTL_DAYS`.
    pub fn days(days: u32) -> Self {
        Self {
            // A u32 day count always fits a chrono Duration. Keep the
            // fallback so an invalid value can never panic.
            duration: chrono::Duration::try_days(i64::from(days)).unwrap_or(chrono::Duration::MAX),
        }
    }

    /// A positive whole-day TTL. Zero is rejected because a paste with a
    /// zero TTL is expired at creation.
    pub fn checked_days(days: u32) -> Result<Self, TtlParseError> {
        if days == 0 {
            return Err(TtlParseError::invalid_days("0"));
        }
        Ok(Self::days(days))
    }

    /// Parse a positive whole-day count for the legacy `ttl_days` forms.
    pub fn parse_days(value: &str) -> Result<Self, TtlParseError> {
        let days: u32 = value
            .trim()
            .parse()
            .map_err(|_| TtlParseError::invalid_days(value))?;
        if days == 0 {
            return Err(TtlParseError::invalid_days(value));
        }
        Ok(Self::days(days))
    }

    /// Parse seconds or a compound duration.
    ///
    /// Accepted forms are a bare integer as seconds (`3600`) and a
    /// sequence of number and unit pairs with `s`, `m`, `h`, and `d`
    /// (`90m`, `1h30m`, `2d`, `1d12h`). Empty, zero, negative, unknown
    /// unit, and overflow values are rejected.
    pub fn parse(value: &str) -> Result<Self, TtlParseError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(TtlParseError::new(value, "the value is empty"));
        }
        if value.starts_with('-') {
            return Err(TtlParseError::new(value, "a negative value is not allowed"));
        }
        if value.bytes().all(|b| b.is_ascii_digit()) {
            let seconds: i64 = value
                .parse()
                .map_err(|_| TtlParseError::new(value, "the number is too large"))?;
            if seconds == 0 {
                return Err(TtlParseError::new(value, "zero is not allowed"));
            }
            let duration = chrono::Duration::try_seconds(seconds)
                .ok_or_else(|| TtlParseError::new(value, "the duration is too large"))?;
            return Ok(Self { duration });
        }
        let mut total_seconds: i64 = 0;
        let mut rest = value;
        while !rest.is_empty() {
            let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
            if digits == 0 {
                return Err(TtlParseError::new(
                    value,
                    "a number must come before each unit",
                ));
            }
            let (number_str, after_number) = rest.split_at(digits);
            let number: i64 = number_str
                .parse()
                .map_err(|_| TtlParseError::new(value, "a number is too large"))?;
            if number == 0 {
                return Err(TtlParseError::new(value, "a number is zero"));
            }
            let unit = after_number
                .chars()
                .next()
                .ok_or_else(|| TtlParseError::new(value, "a unit is missing after a number"))?;
            let multiplier = match unit {
                's' => 1,
                'm' => 60,
                'h' => 3_600,
                'd' => 86_400,
                other => return Err(TtlParseError::new(value, format!("unknown unit {other:?}"))),
            };
            let part = number
                .checked_mul(multiplier)
                .ok_or_else(|| TtlParseError::new(value, "the duration is too large"))?;
            total_seconds = total_seconds
                .checked_add(part)
                .ok_or_else(|| TtlParseError::new(value, "the duration is too large"))?;
            rest = &after_number[unit.len_utf8()..];
        }
        if total_seconds == 0 {
            return Err(TtlParseError::new(value, "zero is not allowed"));
        }
        let duration = chrono::Duration::try_seconds(total_seconds)
            .ok_or_else(|| TtlParseError::new(value, "the duration is too large"))?;
        Ok(Self { duration })
    }

    /// The exact TTL in seconds.
    pub fn num_seconds(&self) -> i64 {
        self.duration.num_seconds()
    }

    fn duration(&self) -> chrono::Duration {
        self.duration
    }

    /// Whole days, when the duration has no smaller part. This decides
    /// whether the legacy `ttl-days` metadata is still accurate.
    fn whole_days(&self) -> Option<u32> {
        if self.duration.num_seconds() % 86_400 == 0 {
            u32::try_from(self.duration.num_days()).ok()
        } else {
            None
        }
    }
}

/// One validated listing request from HTTP, MCP, or the web frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListWindowRequest {
    /// A single UTC date.
    Date(String),
    /// Inclusive start and end dates.
    Range { start: String, end: String },
    /// The default window: yesterday and today in UTC.
    Default,
}

/// Parse a date, a range, or a default request into a
/// [`ListWindowRequest`].
///
/// A date takes precedence over a range. A range must supply both dates and
/// the start date must not be after the end date. A malformed or inverted
/// range returns [`StoreError::InvalidDate`].
pub fn parse_list_window_request(
    date: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<ListWindowRequest, StoreError> {
    if let Some(date) = date {
        parse_list_date(date)?;
        return Ok(ListWindowRequest::Date(date.to_string()));
    }
    match (start, end) {
        (None, None) => Ok(ListWindowRequest::Default),
        (Some(start), Some(end)) => {
            let start_date = parse_list_date(start)?;
            let end_date = parse_list_date(end)?;
            if start_date > end_date {
                return Err(StoreError::InvalidDate(format!(
                    "start {start} is after end {end}"
                )));
            }
            Ok(ListWindowRequest::Range {
                start: start.to_string(),
                end: end.to_string(),
            })
        }
        _ => Err(StoreError::InvalidDate(
            "supply both start and end, or neither".to_string(),
        )),
    }
}

fn parse_list_date(value: &str) -> Result<NaiveDate, StoreError> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| StoreError::InvalidDate(value.to_string()))
}

/// Resolve a request into an inclusive window. This function also applies
/// [`LIST_WINDOW_MAX_DAYS`].
fn resolve_list_window(
    request: &ListWindowRequest,
    now: DateTime<Utc>,
) -> Result<(NaiveDate, NaiveDate), StoreError> {
    let (start, end) = match request {
        ListWindowRequest::Date(date) => {
            let day = parse_list_date(date)?;
            (day, day)
        }
        ListWindowRequest::Range { start, end } => (parse_list_date(start)?, parse_list_date(end)?),
        ListWindowRequest::Default => default_list_window(now),
    };
    let days = (end - start).num_days() + 1;
    if days > LIST_WINDOW_MAX_DAYS as i64 {
        return Err(StoreError::InvalidDate(format!(
            "window has {days} days; the limit is {LIST_WINDOW_MAX_DAYS}"
        )));
    }
    Ok((start, end))
}

fn inclusive_dates(start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = NaiveDate> {
    std::iter::successors(Some(start), move |date| {
        date.succ_opt().filter(|next| *next <= end)
    })
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

/// Detection for buffered uploads, where the whole payload is available
/// before the S3 call. Streaming HTTP uploads use
/// [`crate::content_type::detect_and_wrap`] instead, which peeks at the
/// live stream; both paths go through the same precedence rules.
fn detection_for(filename: Option<&str>, supplied: Option<&str>, payload: &[u8]) -> Detection {
    detect_content_type(supplied, filename, payload)
}

/// Collect a stream into memory and stop with `TooLarge` as soon as the
/// body exceeds `max_bytes`. A read error returns before any S3 call.
async fn collect_stream_body<S>(mut stream: S, max_bytes: u64) -> Result<Vec<u8>, StoreError>
where
    S: futures::Stream<Item = Result<Bytes, StoreError>> + Unpin,
{
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let new_len = body.len() as u64 + chunk.len() as u64;
        if new_len > max_bytes {
            return Err(StoreError::TooLarge { limit: max_bytes });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex_encode(&hasher.finalize())
}

/// Hash a file in fixed-size reads, so a large file is never held whole
/// in memory. S3 takes object metadata only at CreateMultipartUpload, so
/// the hash must exist before that call.
async fn sha256_file(path: &Path) -> Result<String, StoreError> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MULTIPART_CHUNK_BYTES];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(&hasher.finalize()))
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
    sha256: Option<&str>,
    size: Option<u64>,
    ttl: Ttl,
    created_at: DateTime<Utc>,
    detection: &Detection,
) -> (HashMap<String, String>, DateTime<Utc>) {
    let expires_at = created_at
        .checked_add_signed(ttl.duration())
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    let mut meta = HashMap::new();
    if let Some(f) = filename {
        if !f.is_empty() {
            meta.insert("filename".to_string(), encode_filename(f));
        }
    }
    // sha256 and size are unknown until the upload completes. Omit both
    // keys then, so HeadObject does not report an empty hash or a zero
    // size as a real value.
    if let Some(hash) = sha256 {
        meta.insert("sha256".to_string(), hash.to_string());
    }
    if let Some(bytes) = size {
        meta.insert("size".to_string(), bytes.to_string());
    }
    meta.insert("ttl-seconds".to_string(), ttl.num_seconds().to_string());
    if let Some(days) = ttl.whole_days() {
        meta.insert("ttl-days".to_string(), days.to_string());
    }
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

/// Read-time expiry rule. An object with no `expires-at` metadata never
/// expires on read; the lifecycle sweeper is a separate backstop.
fn is_expired_at(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    expires_at.map(|expires| now > expires).unwrap_or(false)
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
        // Credentials come from the environment or the credential files
        // named in Config only. The AWS default chain is not used, so a
        // stray AWS profile or instance role can never supply credentials.
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
            let out = req.send().await.map_err(|e| map_sdk_error(e, id))?;
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
        ttl: Option<Ttl>,
    ) -> Result<StoredPaste, StoreError> {
        if u64::try_from(text.len()).unwrap_or(u64::MAX) > self.config.max_text_bytes {
            return Err(StoreError::TooLarge {
                limit: self.config.max_text_bytes,
            });
        }
        let ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let sha = sha256_hex(text);
        // Text with no filename and no explicit type is still detected
        // (it will come out as text/plain), so the stored type is never
        // a blind guess. The whole payload is checked, so a NUL byte
        // after the first 8 KiB still makes the payload binary.
        let detection = detection_for(filename, content_type, text);
        let ct = detection.content_type.clone();
        let (meta, expires_at) = metadata_for(
            filename,
            Some(&sha),
            Some(text.len() as u64),
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
        ttl: Option<Ttl>,
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
        let ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string()).or_else(|| {
            path.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        });
        if size as usize <= MULTIPART_CHUNK_BYTES {
            // One read gives the whole payload for both detection and
            // upload, so text detection sees every byte.
            let bytes = tokio::fs::read(path).await?;
            let detection = detection_for(display_name.as_deref(), content_type, &bytes);
            let ct = detection.content_type.clone();
            let sha = sha256_hex(&bytes);
            let (meta, expires_at) = metadata_for(
                display_name.as_deref(),
                Some(&sha),
                Some(size),
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

        // Multipart path: S3 takes the object metadata on
        // CreateMultipartUpload. Hash the file first, because the hash is
        // part of that metadata, then upload the parts. A later CopyObject
        // would cost a second put and could fail after the object exists.
        // The prefetch is only a sample, so plain-text detection cannot
        // prove that the whole file is text. A specific explicit type
        // or a specific filename extension still wins.
        let prefix: Vec<u8> = {
            let mut f = File::open(path).await?;
            let mut buf = vec![0u8; crate::content_type::PEEK_BYTES];
            let n = f.read(&mut buf).await?;
            buf.truncate(n);
            buf
        };
        let detection = detect_content_type_sample(
            content_type,
            display_name.as_deref(),
            &prefix,
            size <= crate::content_type::PEEK_BYTES as u64,
        );
        let ct = detection.content_type.clone();
        let sha = sha256_file(path).await?;
        let (meta, expires_at) = metadata_for(
            display_name.as_deref(),
            Some(&sha),
            Some(size),
            ttl,
            created_at,
            &detection,
        );
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .content_type(ct.clone())
            .set_metadata(Some(meta))
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

    /// Store a streamed upload.
    ///
    /// A text paste (no filename) is buffered up to `max_bytes`. The
    /// text cap bounds that buffer, so detection sees the whole body and
    /// an oversize body is rejected before any S3 write. The buffered
    /// body goes through [`Store::store_text`], which keeps the text
    /// cap, TTL, metadata, and detection rules.
    ///
    /// A file upload streams. Detection peeks at the first chunk (via
    /// [`crate::content_type::detect_and_wrap`]), then drives the S3
    /// sink. The peeked bytes are forwarded exactly once by the
    /// wrapper, so detection never loses or duplicates data. A plain
    /// text candidate needs the whole body. When the peek is a
    /// truncated sample, the candidate falls back to
    /// `application/octet-stream` unless `known_length` shows that the
    /// whole body fits in the sample.
    ///
    /// `max_bytes` is chosen by the caller before streaming: the text cap
    /// for a text paste (no filename), or the file cap for a file upload.
    /// A known `Content-Length` over that cap is rejected before detection
    /// peeks, so an oversized body is not read at all.
    pub async fn store_stream<S>(
        &self,
        stream: S,
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl: Option<Ttl>,
        known_length: Option<u64>,
        max_bytes: u64,
    ) -> Result<StoredPaste, StoreError>
    where
        S: futures::Stream<Item = Result<Bytes, StoreError>> + Send + Unpin,
    {
        if let Some(len) = known_length {
            if len > max_bytes {
                return Err(StoreError::TooLarge { limit: max_bytes });
            }
        }
        let ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        // A text paste has no filename. Buffer the whole body, bounded
        // by the text cap, so detection sees every byte and text is
        // served inline. A file upload keeps the streaming path, because
        // its cap can be much larger.
        if filename.is_none_or(|name| name.trim().is_empty()) {
            let body = collect_stream_body(stream, max_bytes).await?;
            return self.store_text(&body, None, content_type, Some(ttl)).await;
        }
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string());
        let (detection, wrapped) =
            crate::content_type::detect_and_wrap(stream, content_type, filename, known_length)
                .await;
        let ct = detection.content_type.clone();
        // Metadata for the initial upload: filename, TTL, expiry, final
        // type and detection record. The hash and the final size are not
        // known yet. put_small adds both; for multipart, S3 stores the
        // size in ContentLength. S3 takes metadata only at
        // CreateMultipartUpload, so no later copy updates it.
        let (initial_meta, expires_at) = metadata_for(
            display_name.as_deref(),
            None,
            None,
            ttl,
            created_at,
            &detection,
        );
        let mut sink = AwsUploadSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.clone(),
            content_type: ct.clone(),
            metadata: initial_meta,
        };
        let outcome =
            crate::stream::run_streaming_upload(wrapped, &mut sink, known_length, max_bytes)
                .await?;
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
        let expired = is_expired_at(expires_at, Utc::now());
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

    /// List current pastes for an inclusive date window.
    ///
    /// No dates select the default window: yesterday and today in UTC. A
    /// date for [`list_recent`](Self::list_recent) means one day.
    /// [`parse_list_window_request`] documents the range rules.
    ///
    /// The scan starts at the newest date and moves to older dates. The
    /// `limit` applies to the newest objects in the window, so an older
    /// date can be omitted when the limit is reached. The returned
    /// summaries are sorted by date ascending, then by key.
    ///
    /// The function scans the bucket directly: `ListObjectsV2` for each
    /// `pastes/<date>/` prefix, then one `HeadObject` per key to recover
    /// expiry/content-type/filename and to filter expired objects. Each
    /// prefix follows continuation tokens for up to [`LIST_SCAN_MAX_PAGES`]
    /// pages. A window can span at most [`LIST_WINDOW_MAX_DAYS`] dates. The
    /// listing is O(n) in the objects scanned (plus one Head per
    /// key): it is fast for a small internal bucket and gets slower as the
    /// bucket grows. That slowdown is exactly the trigger for adding a
    /// database later — it is documented in README, not hidden, and no
    /// database schema is invented in v1. This is not search.
    pub async fn list_window(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PasteSummary>, StoreError> {
        let request = parse_list_window_request(None, start, end)?;
        let (start_date, end_date) = resolve_list_window(&request, Utc::now())?;
        let limit = limit.clamp(1, 1000);
        let mut summaries = Vec::new();
        let dates: Vec<NaiveDate> = inclusive_dates(start_date, end_date).collect();
        for date in dates.into_iter().rev() {
            if summaries.len() >= limit {
                break;
            }
            let date_str = date.format("%Y-%m-%d").to_string();
            let prefix = format!("pastes/{date_str}/");
            let mut token: Option<String> = None;
            for _ in 0..LIST_SCAN_MAX_PAGES {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.bucket)
                    .prefix(&prefix)
                    .max_keys(1000);
                if let Some(t) = token.as_deref() {
                    req = req.continuation_token(t);
                }
                let out = req.send().await.map_err(|e| map_sdk_error(e, &date_str))?;
                for obj in out.contents() {
                    if summaries.len() >= limit {
                        break;
                    }
                    let Some(key) = obj.key() else { continue };
                    let id = key.strip_prefix("pastes/").unwrap_or(key).to_string();
                    // HeadObject per key: ListObjectsV2 returns no user
                    // metadata. A head failure (e.g. deleted between list
                    // and head) drops the entry instead of failing the
                    // whole listing.
                    let Some(meta) = self.head_metadata(&id).await.ok() else {
                        continue;
                    };
                    if meta.expired {
                        continue;
                    }
                    summaries.push(PasteSummary {
                        id,
                        key: key.to_string(),
                        size: u64::try_from(obj.size().unwrap_or(0)).unwrap_or(0),
                        last_modified: obj
                            .last_modified()
                            .and_then(|dt| DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())),
                        expires_at: meta.expires_at,
                        created_at: meta.created_at,
                        content_type: Some(meta.content_type),
                        filename: meta.filename,
                    });
                }
                let truncated = out.is_truncated().unwrap_or(false);
                token = match (truncated, out.next_continuation_token()) {
                    (true, Some(t)) => Some(t.to_string()),
                    _ => None,
                };
                if token.is_none() || summaries.len() >= limit {
                    break;
                }
            }
        }
        summaries.sort_by(|a, b| {
            let a_date = a.id.split_once('/').map(|(date, _)| date).unwrap_or("");
            let b_date = b.id.split_once('/').map(|(date, _)| date).unwrap_or("");
            a_date.cmp(b_date).then_with(|| a.key.cmp(&b.key))
        });
        Ok(summaries)
    }

    /// Compatibility wrapper. A date means one day. No date delegates to the
    /// default window of [`list_window`](Self::list_window).
    pub async fn list_recent(
        &self,
        date: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PasteSummary>, StoreError> {
        match date {
            Some(date) => self.list_window(Some(date), Some(date), limit).await,
            None => self.list_window(None, None, limit).await,
        }
    }
}

/// Default listing window: yesterday and today in UTC.
///
/// The key layout uses UTC dates. At 20:00 Pacific the UTC clock is already
/// on the next day, so the pastes written during that Pacific evening use
/// two UTC date prefixes. Yesterday and today cover the full Pacific day.
fn default_list_window(now: DateTime<Utc>) -> (NaiveDate, NaiveDate) {
    let today = now.date_naive();
    (today - chrono::Duration::days(1), today)
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
    fn default_list_window_covers_the_pacific_evening() {
        // At 20:00 Pacific on 2026-10-28 the UTC clock is already
        // 2026-10-29T03:00Z. The default window must cover both UTC dates,
        // because the Pacific day spans the UTC midnight boundary.
        let now = Utc.with_ymd_and_hms(2026, 10, 29, 3, 0, 0).unwrap();
        let (start, end) = default_list_window(now);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 10, 28).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 10, 29).unwrap());
    }

    #[test]
    fn list_recent_date_stays_one_day() {
        let request = parse_list_window_request(Some("2026-10-28"), None, None).unwrap();
        assert_eq!(request, ListWindowRequest::Date("2026-10-28".to_string()));
        let (start, end) = resolve_list_window(&request, Utc::now()).unwrap();
        let days: Vec<_> = inclusive_dates(start, end).collect();
        assert_eq!(days, vec![NaiveDate::from_ymd_opt(2026, 10, 28).unwrap()]);
    }

    #[test]
    fn inclusive_range_spans_two_dates_in_order() {
        let request =
            parse_list_window_request(None, Some("2026-10-28"), Some("2026-10-29")).unwrap();
        let (start, end) = resolve_list_window(&request, Utc::now()).unwrap();
        let days: Vec<_> = inclusive_dates(start, end)
            .map(|date| date.format("%Y-%m-%d").to_string())
            .collect();
        assert_eq!(days, vec!["2026-10-28", "2026-10-29"]);
    }

    #[test]
    fn inverted_range_is_rejected() {
        let err =
            parse_list_window_request(None, Some("2026-10-29"), Some("2026-10-28")).unwrap_err();
        assert!(matches!(err, StoreError::InvalidDate(_)), "got {err:?}");
    }

    #[test]
    fn malformed_date_is_rejected() {
        let err =
            parse_list_window_request(None, Some("2026-13-01"), Some("2026-12-01")).unwrap_err();
        assert!(matches!(err, StoreError::InvalidDate(_)), "got {err:?}");
    }

    #[test]
    fn overlong_range_is_rejected() {
        let request =
            parse_list_window_request(None, Some("2025-01-01"), Some("2026-10-29")).unwrap();
        assert!(matches!(
            resolve_list_window(&request, Utc::now()),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn one_sided_range_is_rejected() {
        assert!(matches!(
            parse_list_window_request(None, Some("2026-10-28"), None),
            Err(StoreError::InvalidDate(_))
        ));
        assert!(matches!(
            parse_list_window_request(None, None, Some("2026-10-28")),
            Err(StoreError::InvalidDate(_))
        ));
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
        let (meta, expires) = metadata_for(
            Some("a b.txt"),
            Some("deadbeef"),
            Some(3),
            Ttl::days(7),
            created,
            &detection,
        );
        assert_eq!(meta.get("detection-source").unwrap(), "text");
        assert_eq!(
            meta.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(meta.get("ttl-days").unwrap(), "7");
        assert_eq!(meta.get("ttl-seconds").unwrap(), "604800");
        // NON_ALPHANUMERIC encodes '.' too; decode still round-trips.
        assert_eq!(meta.get("filename").unwrap(), "a%20b%2Etxt");
        assert_eq!(decode_filename(meta.get("filename").unwrap()), "a b.txt");
        assert_eq!(expires, created + chrono::Duration::days(7));
    }

    #[test]
    fn ttl_parser_accepts_seconds_and_compound_forms() {
        for (value, seconds) in [
            ("3600", 3_600),
            ("90m", 5_400),
            ("1h30m", 5_400),
            ("2d", 172_800),
            ("1d12h", 129_600),
            ("1h30m10s", 5_410),
        ] {
            let ttl = Ttl::parse(value).unwrap_or_else(|e| panic!("{value:?}: {e}"));
            assert_eq!(ttl.num_seconds(), seconds, "value {value:?}");
        }
    }

    #[test]
    fn ttl_parser_rejects_empty_zero_negative_unknown_and_overflow() {
        for value in [
            "",
            "   ",
            "0",
            "0s",
            "0d",
            "0h30m",
            "-1",
            "-1h",
            "1x",
            "1h30",
            "h",
            "1.5h",
            "1 h",
            "1H",
            "999999999999999999999999999999",
            "99999999999999d",
        ] {
            assert!(Ttl::parse(value).is_err(), "value {value:?} must fail");
        }
    }

    #[test]
    fn ttl_days_zero_is_rejected() {
        assert!(Ttl::parse_days("0").is_err());
        assert!(Ttl::parse_days("0 ").is_err());
        assert!(Ttl::checked_days(0).is_err());
        assert_eq!(Ttl::parse_days("7").unwrap().num_seconds(), 7 * 86_400);
    }

    #[test]
    fn metadata_uses_exact_subday_expiry() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
        let (meta, expires) = metadata_for(
            None,
            Some("deadbeef"),
            Some(3),
            Ttl::parse("1h").unwrap(),
            created,
            &detection,
        );
        assert_eq!(expires, created + chrono::Duration::hours(1));
        assert_eq!(meta.get("ttl-seconds").unwrap(), "3600");
        assert!(
            !meta.contains_key("ttl-days"),
            "a sub-day TTL must not store a rounded ttl-days value"
        );
    }

    /// The read path marks a one-hour paste readable before expiry and
    /// expired after it, without sleeping. `created_at` is in the past.
    #[test]
    fn one_hour_ttl_is_readable_before_expiry_and_expired_after() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
        let (_, expires) = metadata_for(
            None,
            Some("deadbeef"),
            Some(3),
            Ttl::parse("1h").unwrap(),
            created,
            &detection,
        );
        let paste = |now: DateTime<Utc>| PasteMetadata {
            id: "2026-10-28/abc".to_string(),
            key: "pastes/2026-10-28/abc".to_string(),
            filename: None,
            content_type: "text/plain".to_string(),
            detection_source: Some("text".to_string()),
            detection_conflict: None,
            size: 3,
            sha256: Some("deadbeef".to_string()),
            created_at: Some(created),
            expires_at: Some(expires),
            expired: is_expired_at(Some(expires), now),
        };
        assert!(
            Store::check_not_expired(&paste(created + chrono::Duration::minutes(59))).is_ok(),
            "the paste must be readable before the one-hour expiry"
        );
        assert!(
            matches!(
                Store::check_not_expired(&paste(created + chrono::Duration::minutes(61))),
                Err(StoreError::Expired(_))
            ),
            "the paste must be expired after the one-hour expiry"
        );
    }

    #[tokio::test]
    async fn sha256_file_hashes_the_whole_file() {
        let dir = std::env::temp_dir().join(format!("dayfly-sha256-{}", generate_random_id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("blob.bin");
        // More than one read, so the loop boundary is exercised.
        let data = vec![0x5au8; MULTIPART_CHUNK_BYTES + 17];
        tokio::fs::write(&path, &data).await.unwrap();
        assert_eq!(sha256_file(&path).await.unwrap(), sha256_hex(&data));
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[test]
    fn metadata_map_for_known_hash_is_complete() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
        let (meta, expires) = metadata_for(
            Some("note.txt"),
            Some("abc123"),
            Some(42),
            Ttl::days(3),
            created,
            &detection,
        );
        assert_eq!(meta.get("expires-at").unwrap(), "2026-10-31T00:00:00+00:00");
        assert_eq!(meta.get("created-at").unwrap(), "2026-10-28T00:00:00+00:00");
        assert_eq!(meta.get("ttl-days").unwrap(), "3");
        assert_eq!(
            meta.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(meta.get("sha256").unwrap(), "abc123");
        assert_eq!(meta.get("size").unwrap(), "42");
        assert_eq!(expires, created + chrono::Duration::days(3));
    }

    #[test]
    fn metadata_map_for_unknown_hash_omits_hash_and_size() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "application/octet-stream".to_string(),
            source: "fallback",
            conflict: None,
        };
        // The streaming multipart path builds this map before the hash
        // and the final size are known. An empty string or a zero must
        // not appear, because HeadObject would report them as real.
        let (meta, _) = metadata_for(Some("a.bin"), None, None, Ttl::days(7), created, &detection);
        assert!(
            !meta.contains_key("sha256"),
            "unknown sha256 must be absent, not empty"
        );
        assert!(!meta.contains_key("size"), "unknown size must be absent");
        assert_eq!(meta.get("expires-at").unwrap(), "2026-11-04T00:00:00+00:00");
    }
}
