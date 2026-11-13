//! S3 core for dayfly.
//!
//! Design rules enforced here, not just documented:
//!
//! * One base S3 object per paste/file, plus one segment object per
//!   append. No metadata sidecar, no database in v1.
//! * The binary is the only S3 credential holder. Agents never get S3
//!   credentials and no presigned URL is ever issued in v1.
//! * A store creates a *new* random ID. An append adds a segment under the
//!   existing ID and never rewrites the base object. Conditional writes
//!   (`If-None-Match`) are deliberately NOT relied on: some S3-compatible
//!   stores, including Garage, accept that header but ignore it and
//!   overwrite. Two concurrent appends can therefore choose the same
//!   sequence. Their random suffixes keep the object keys distinct, so
//!   both appends survive. See the README for the read order.
//! * Expiry is enforced on read by comparing stored `expires-at` metadata
//!   with now. A TTL can be shorter than one day. An append inherits the
//!   base expiry and does not extend it. With `retention_by_size` on, an
//!   append can shorten the expiry. S3 lifecycle deletion, if configured,
//!   is only a day-rounded storage sweeper and may run after expiry.

use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, LifecycleRule, MetadataDirective,
};
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use futures::StreamExt;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};
use rand::Rng;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::config::{
    Config, DEFAULT_ALIAS_WORDS, MAX_ALIAS_WORDS, MAX_TTL_DAYS, MAX_TTL_SECONDS, MIN_ALIAS_WORDS,
};
use crate::content_type::{detect_content_type, detect_content_type_sample, Detection, PEEK_BYTES};
use crate::metrics::Metrics;

/// Await one S3 request and record its operation and outcome.
macro_rules! s3_call {
    ($metrics:expr, $operation:literal, $call:expr) => {{
        let result = $call.await;
        if let Some(metrics) = $metrics.as_ref() {
            metrics.observe_s3($operation, if result.is_ok() { "ok" } else { "error" });
        }
        result
    }};
}

pub const MULTIPART_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const LIST_SCAN_MAX_PAGES: usize = 10;
/// Maximum number of concurrent `HeadObject` requests during a listing.
const LIST_HEAD_CONCURRENCY: usize = 16;
/// Longest accepted listing window, in days. This cap bounds the number of
/// date prefixes that one request can scan.
pub const LIST_WINDOW_MAX_DAYS: usize = 366;
/// Maximum number of appended segments for one paste.
pub const MAX_SEGMENTS_PER_PASTE: usize = 4096;
/// Attempts for one object in a multi-object expiry update.
const MULTI_STEP_ATTEMPTS: usize = 3;
/// Extra segment-prefix passes after a delete removes the base object.
const DELETE_SEGMENT_PASSES: usize = 2;
/// Attempts for one multipart abort.
const ABORT_ATTEMPTS: usize = 3;
/// Backoff between multipart abort attempts.
const ABORT_BACKOFF: Duration = Duration::from_millis(50);

/// Extra segments the reader tolerates above the append cap.
///
/// Two concurrent appends can pass the pre-write check and overshoot by
/// one. The reader must still serve and delete that paste.
const SEGMENT_READ_HEADROOM: usize = 64;
/// Hard bound for reading a segment listing.
const MAX_SEGMENTS_READ: usize = MAX_SEGMENTS_PER_PASTE + SEGMENT_READ_HEADROOM;

/// Maximum bytes returned by one `fetch_text` call. A caller that sends
/// no limit gets this bound. The caller can page with `offset` to read
/// the rest.
pub const MAX_FETCH_TEXT_BYTES: u64 = 8 * 1024 * 1024;

/// Maximum bytes for one S3 `CopyObject` request.
///
/// S3 caps one atomic copy at 5 GB. A larger object needs a multipart copy.
/// Dayfly does not implement one, so a TTL change on a larger object fails
/// before any rewrite.
pub const COPY_OBJECT_MAX_BYTES: u64 = 5_000_000_000;

/// True when one `CopyObject` request cannot carry this object.
fn copy_object_too_large(size: u64) -> bool {
    size > COPY_OBJECT_MAX_BYTES
}

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
    #[error("upload length mismatch: expected {expected} bytes, received {actual} bytes")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("invalid date (expected YYYY-MM-DD): {0}")]
    InvalidDate(String),
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("invalid list cursor: {0}")]
    InvalidCursor(String),
    #[error("invalid title: {0}")]
    InvalidTitle(String),
    #[error("invalid tags: {0}")]
    InvalidTags(String),
    #[error("invalid search: {0}")]
    InvalidSearch(String),
    #[error("paste is not text: {0}")]
    NotText(String),
    #[error("range not satisfiable")]
    RangeNotSatisfiable,
    #[error("{0}")]
    TtlOutOfRange(String),
    #[error("invalid lifecycle configuration: {0}")]
    InvalidLifecycle(String),
    #[error("could not allocate a word alias after {attempts} attempts")]
    AliasUnavailable { attempts: usize },
    #[error("paste has more than {limit} appended segments")]
    TooManySegments { limit: usize },
    #[error("local file access is disabled on this transport: set DAYFLY_MCP_LOCAL_ROOT on the server to allow a directory")]
    LocalFilesDisabled,
    #[error("path outside the allowed root: {0}")]
    PathOutsideRoot(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("s3 error: {0}")]
    S3(String),
    #[error("bucket storage quota is reached: try again after a paste expires or is deleted")]
    QuotaExceeded,
    #[error("object is larger than the {limit} byte copy limit")]
    CopyLimit { limit: u64 },
    #[error("multi-object update failed at the {step} step for {key}")]
    PartialUpdate { step: &'static str, key: String },
}

impl StoreError {
    /// A stable machine-readable code for this error.
    ///
    /// Clients use the code to choose a recovery path. Do not change an
    /// existing code; add a new one when a new error class appears.
    pub fn code(&self) -> &'static str {
        match self {
            StoreError::NotFound(_) => "not_found",
            StoreError::BucketUnavailable(_) => "bucket_unavailable",
            StoreError::Expired(_) => "expired",
            StoreError::TooLarge { .. } => "too_large",
            StoreError::LengthMismatch { .. } => "length_mismatch",
            StoreError::InvalidId(_) => "invalid_id",
            StoreError::InvalidDate(_) => "invalid_date",
            StoreError::InvalidRange(_) => "invalid_range",
            StoreError::InvalidCursor(_) => "invalid_cursor",
            StoreError::InvalidTitle(_) => "invalid_title",
            StoreError::InvalidTags(_) => "invalid_tags",
            StoreError::InvalidSearch(_) => "invalid_search",
            StoreError::NotText(_) => "not_text",
            StoreError::RangeNotSatisfiable => "range_not_satisfiable",
            StoreError::TtlOutOfRange(_) => "invalid_ttl",
            StoreError::InvalidLifecycle(_) => "invalid_lifecycle",
            StoreError::AliasUnavailable { .. } => "alias_unavailable",
            StoreError::TooManySegments { .. } => "too_many_segments",
            StoreError::LocalFilesDisabled => "local_files_disabled",
            StoreError::PathOutsideRoot(_) => "path_outside_root",
            StoreError::InvalidPath(_) => "invalid_path",
            StoreError::Io(_) => "io_error",
            StoreError::S3(_) => "s3_error",
            StoreError::QuotaExceeded => "quota_exceeded",
            StoreError::CopyLimit { .. } => "copy_limit",
            StoreError::PartialUpdate { .. } => "partial_update",
        }
    }
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

    /// A TTL longer than the configured maximum.
    fn above_max(value: &str, max_days: u32) -> Self {
        Self {
            value: Some(value.to_string()),
            reason: format!("the maximum TTL is {max_days} days"),
            hint: "Use a shorter TTL.",
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

    /// A TTL in seconds for a value that the server computes.
    ///
    /// The retention curve uses this constructor. A negative value is
    /// clamped to zero, so this function cannot panic.
    pub(crate) fn seconds(seconds: i64) -> Self {
        Self {
            duration: chrono::Duration::try_seconds(seconds.max(0))
                .unwrap_or(chrono::Duration::MAX),
        }
    }

    /// A positive whole-day TTL. Zero is rejected because a paste with a
    /// zero TTL is expired at creation.
    pub fn checked_days(days: u32) -> Result<Self, TtlParseError> {
        if days == 0 {
            return Err(TtlParseError::invalid_days("0"));
        }
        if days > MAX_TTL_DAYS {
            return Err(TtlParseError::above_max(&days.to_string(), MAX_TTL_DAYS));
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
        if days > MAX_TTL_DAYS {
            return Err(TtlParseError::above_max(value, MAX_TTL_DAYS));
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
            if seconds > MAX_TTL_SECONDS {
                return Err(TtlParseError::above_max(value, MAX_TTL_DAYS));
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
        if total_seconds > MAX_TTL_SECONDS {
            return Err(TtlParseError::above_max(value, MAX_TTL_DAYS));
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
    /// The default window: the last `DAYFLY_LIST_DEFAULT_DAYS` days,
    /// ending today in UTC.
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
    default_days: u32,
) -> Result<(NaiveDate, NaiveDate), StoreError> {
    let (start, end) = match request {
        ListWindowRequest::Date(date) => {
            let day = parse_list_date(date)?;
            (day, day)
        }
        ListWindowRequest::Range { start, end } => (parse_list_date(start)?, parse_list_date(end)?),
        ListWindowRequest::Default => default_list_window(now, default_days),
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
        if is_quota_error(ctx.err().code(), ctx.err().message()) {
            tracing::warn!(bucket_object = id, "bucket storage quota is reached");
            return StoreError::QuotaExceeded;
        }
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
    s3_failure("an object store request failed", err)
}

/// Whether an S3 error reports a bucket quota limit.
///
/// The `QuotaExceeded` code is a quota error with any message. Garage
/// rejects a write with `403 Forbidden` and a message that contains
/// "Bucket size quota is reached" or "Object quota is reached". The S3
/// error code is `Forbidden`, so the message is the stable signal.
fn is_quota_error(code: Option<&str>, message: Option<&str>) -> bool {
    if code == Some("QuotaExceeded") {
        return true;
    }
    code == Some("Forbidden") && message.is_some_and(|message| message.contains("quota is reached"))
}

/// Log an object store failure and return a client-safe error.
///
/// The AWS SDK debug text can contain the endpoint, the response headers,
/// and the response body. Keep that text in the server log. The client
/// gets one fixed message, so a bucket name, an error code, or an internal
/// host does not reach the response.
fn s3_failure(message: &str, error: impl std::fmt::Debug) -> StoreError {
    tracing::warn!("{message}: {error:?}");
    StoreError::S3(message.to_string())
}

fn partial_update_error(step: &'static str, key: &str, error: StoreError) -> StoreError {
    match error {
        // A missing bucket is a configuration fault, not a partial write.
        StoreError::BucketUnavailable(_) => error,
        _ => StoreError::PartialUpdate {
            step,
            key: key.to_string(),
        },
    }
}

/// Optional fields for a new paste.
///
/// The store normalizes the title and tags. A caller can pass raw values
/// from a header, a form, or an MCP argument.
#[derive(Clone, Default)]
pub struct PasteFields {
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub title: Option<String>,
    pub tags: Vec<String>,
    /// Canonical text form of the resolved client address.
    ///
    /// The HTTP and HTTP MCP transports set this. The stdio transport
    /// leaves it `None`, so no `created-by` key is written.
    pub created_by: Option<String>,
}

impl std::fmt::Debug for PasteFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasteFields")
            .field("filename", &self.filename)
            .field("content_type", &self.content_type)
            .field("title", &self.title)
            .field("tags", &self.tags)
            .field("created_by", &self.created_by.as_ref().map(|_| "<set>"))
            .finish()
    }
}

/// Maximum title length in bytes.
pub const MAX_TITLE_BYTES: usize = 200;
/// Maximum number of tags on one paste.
pub const MAX_TAGS: usize = 8;
/// Maximum total tag length in bytes, including separators.
pub const MAX_TAGS_BYTES: usize = 512;

#[derive(Debug, Clone)]
pub struct StoredPaste {
    /// Full paste ID returned to clients: "<yyyy-mm-dd>/<random-id>".
    pub id: String,
    pub key: String,
    /// Word alias, when the store wrote one.
    pub words: Option<String>,
    pub size: u64,
    pub sha256: String,
    pub content_type: String,
    pub filename: Option<String>,
    /// Human-readable name for the paste.
    pub title: Option<String>,
    /// Normalized tags.
    pub tags: Vec<String>,
    /// "client" | "sniffed" | "extension" | "text" | "fallback".
    pub detection_source: Option<String>,
    /// "explicit -> sniffed" when a sniffed signature overrode the client.
    pub detection_conflict: Option<String>,
    pub expires_at: DateTime<Utc>,
    /// Management token for this paste, when `paste_tokens` is on.
    ///
    /// The store returns the token once, at creation. Only its SHA-256
    /// is stored, in the base object's metadata.
    pub paste_token: Option<String>,
}

#[derive(Clone)]
pub struct PasteMetadata {
    pub id: String,
    pub key: String,
    /// Word alias, when the base object records one.
    pub words: Option<String>,
    pub filename: Option<String>,
    /// Human-readable name for the paste.
    pub title: Option<String>,
    /// Normalized tags.
    pub tags: Vec<String>,
    pub content_type: String,
    pub detection_source: Option<String>,
    pub detection_conflict: Option<String>,
    /// Total size: the base object plus every appended segment.
    pub size: u64,
    /// Size of the base object alone. The first segment starts at this
    /// offset in the paste.
    pub base_size: u64,
    /// Appended segments in sequence then suffix order.
    pub segments: Vec<Segment>,
    /// SHA-256 of the base object. None when segments exist, because no
    /// stored value describes the concatenated paste.
    pub sha256: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    /// Resolved creator address, when the base object records one.
    ///
    /// The API layer returns this only when `DAYFLY_EXPOSE_CREATOR_ADDR`
    /// is on.
    pub created_by: Option<String>,
    pub expired: bool,
}

impl std::fmt::Debug for PasteMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PasteMetadata")
            .field("id", &self.id)
            .field("key", &self.key)
            .field("words", &self.words)
            .field("filename", &self.filename)
            .field("title", &self.title)
            .field("tags", &self.tags)
            .field("content_type", &self.content_type)
            .field("detection_source", &self.detection_source)
            .field("detection_conflict", &self.detection_conflict)
            .field("size", &self.size)
            .field("base_size", &self.base_size)
            .field("segments", &self.segments)
            .field("sha256", &self.sha256)
            .field("created_at", &self.created_at)
            .field("expires_at", &self.expires_at)
            .field("created_by", &self.created_by.as_ref().map(|_| "<set>"))
            .field("expired", &self.expired)
            .finish()
    }
}

impl PasteMetadata {
    /// Build the range plan for this paste without another S3 call.
    pub fn plan(&self) -> SegmentPlan {
        SegmentPlan::new(self.key.clone(), self.base_size, self.segments.clone())
    }

    /// A stable validator for the joined paste.
    ///
    /// The stored `sha256` covers the base object only, so use it only when
    /// there are no segments. With segments, the total size and the last
    /// segment key change on every append, and both come from stored state.
    pub fn etag(&self) -> String {
        if self.segments.is_empty() {
            if let Some(sha256) = &self.sha256 {
                return format!("\"{sha256}\"");
            }
            return format!(
                "\"{}\"",
                sha256_hex(format!("{}:{}", self.size, self.key).as_bytes())
            );
        }
        let last_segment = self
            .segments
            .last()
            .map(|segment| segment.key.as_str())
            .unwrap_or("");
        format!(
            "\"{}\"",
            sha256_hex(format!("{}:{}", self.size, last_segment).as_bytes())
        )
    }
}

#[derive(Debug, Clone)]
pub struct TextSlice {
    pub text: String,
    pub total_bytes: u64,
    pub returned_bytes: u64,
    pub truncated: bool,
}

/// Options for one text search.
#[derive(Debug, Clone, Copy)]
pub struct SearchOptions {
    /// Treat the query as a regular expression.
    pub regex: bool,
    /// Match without regard to case.
    pub ignore_case: bool,
    /// Lines of context before and after a match. Maximum 5.
    pub context_lines: usize,
    /// Maximum number of hits. Maximum 500.
    pub max_matches: usize,
    /// Start the scan at this byte offset, from the previous `next_offset`.
    pub after_offset: u64,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            regex: false,
            ignore_case: false,
            context_lines: 0,
            max_matches: 50,
            after_offset: 0,
        }
    }
}

/// One search hit. Line and offset are 1-based and absolute.
#[derive(Debug, Clone)]
pub struct SearchHit {
    /// 1-based line number of the matching line.
    pub line: u64,
    /// Byte offset of the start of the matching line in the joined paste.
    pub byte_offset: u64,
    /// Matched line with the requested context, at most 4 KiB.
    pub preview: String,
    /// Match byte ranges within `preview`.
    pub ranges: Vec<(usize, usize)>,
}

/// The result of one text search.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    /// Bytes read from `after_offset` to the end of the scan.
    pub scanned_bytes: u64,
    /// The scan stopped at the match limit or the byte cap.
    pub truncated: bool,
    /// Resume here for the next page.
    pub next_offset: Option<u64>,
}

/// Maximum bytes for one search query.
pub const MAX_SEARCH_QUERY_BYTES: usize = 4_096;
/// Maximum hits in one search result.
pub const MAX_SEARCH_MATCHES: usize = 500;
/// Maximum context lines on each side of a hit.
pub const MAX_SEARCH_CONTEXT_LINES: usize = 5;
/// Maximum preview length in bytes.
pub const MAX_SEARCH_PREVIEW_BYTES: usize = 4_096;

/// Whether a content type can be searched as text.
pub(crate) fn is_text_content_type(content_type: &str) -> bool {
    let main = content_type.split(';').next().unwrap_or("").trim();
    main.starts_with("text/")
        || matches!(
            main,
            "application/json" | "application/xml" | "application/javascript" | "image/svg+xml"
        )
}

/// One line in the searched byte buffer.
struct SearchLine<'a> {
    /// Absolute start offset in the joined paste.
    start: u64,
    /// Absolute offset just after the newline, or the end of the paste.
    end: u64,
    text: &'a [u8],
}

/// Split a byte buffer into lines and keep absolute offsets.
fn split_search_lines(bytes: &[u8], base: u64) -> Vec<SearchLine<'_>> {
    let mut lines = Vec::new();
    let mut start = base;
    let mut cursor = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let mut text = &bytes[cursor..index];
        if text.ends_with(b"\r") {
            text = &text[..text.len() - 1];
        }
        lines.push(SearchLine {
            start,
            end: base + index as u64 + 1,
            text,
        });
        start = base + index as u64 + 1;
        cursor = index + 1;
    }
    if cursor < bytes.len() {
        let mut text = &bytes[cursor..];
        if text.ends_with(b"\r") {
            text = &text[..text.len() - 1];
        }
        lines.push(SearchLine {
            start,
            end: base + bytes.len() as u64,
            text,
        });
    }
    lines
}

/// Build the preview for one match, with context lines and capped size.
fn build_search_preview(
    lines: &[SearchLine<'_>],
    index: usize,
    match_start: usize,
    match_end: usize,
    context: usize,
) -> (String, Vec<(usize, usize)>) {
    let first = index.saturating_sub(context);
    let last = (index + context + 1).min(lines.len());
    let mut preview = Vec::new();
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    for (position, line) in lines[first..last].iter().enumerate() {
        if position > 0 {
            preview.push(b'\n');
            offset += 1;
        }
        if first + position == index {
            ranges.push((offset + match_start, offset + match_end));
        }
        preview.extend_from_slice(line.text);
        offset += line.text.len();
    }
    if preview.len() > MAX_SEARCH_PREVIEW_BYTES {
        preview.truncate(MAX_SEARCH_PREVIEW_BYTES);
        ranges.retain(|(_, end)| *end <= MAX_SEARCH_PREVIEW_BYTES);
    }
    (String::from_utf8_lossy(&preview).to_string(), ranges)
}

/// Result of one append. `meta` describes the whole paste after the
/// append, including the new total size.
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub appended_bytes: u64,
    /// None when the appended text was empty and no object was written.
    pub segment_key: Option<String>,
    pub meta: PasteMetadata,
}

#[derive(Debug, Clone)]
pub struct PasteSummary {
    pub id: String,
    pub key: String,
    /// Word alias, when the base object records one.
    pub words: Option<String>,
    /// Human-readable name for the paste.
    pub title: Option<String>,
    /// Normalized tags.
    pub tags: Vec<String>,
    pub size: u64,
    pub last_modified: Option<DateTime<Utc>>,
    /// From HeadObject, when available. `None` means the object has no
    /// expiry metadata (e.g. written by another tool).
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub content_type: Option<String>,
    pub filename: Option<String>,
}

/// A stateless cursor for the next listing page.
///
/// `date` is the UTC date prefix to resume. `start_after` is the last
/// S3 key that the previous page scanned. The next scan starts after
/// that key on that date, then continues to older dates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCursor {
    pub date: String,
    pub start_after: String,
}

/// One listing page and its cursor state.
#[derive(Debug, Clone)]
pub struct ListPage {
    pub items: Vec<PasteSummary>,
    /// The limit was reached. Another page may exist.
    pub truncated: bool,
    pub next: Option<ListCursor>,
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

/// A 128-bit management token in lowercase hex.
fn generate_paste_token() -> String {
    format!("{:032x}", rand::rng().random::<u128>())
}

/// Validate a client-supplied paste ID.
///
/// Accepts either a bare random ID (`abc123…`) or a full ID
/// (`2026-10-28/abc123…`). An ID has at most two components after
/// `pastes/`, so a segment key is not a valid paste ID. Components must
/// be non-empty, must not be `.` or `..`, and may only contain ASCII
/// alphanumerics, `-`, `_` and `.`. The key itself is never taken from
/// user input verbatim: it is always rebuilt as `pastes/<validated-id>`.
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
    let parts: Vec<&str> = stripped.split('/').collect();
    if parts.len() > 2 {
        return Err(StoreError::InvalidId(format!(
            "id must be <random> or <date>/<random>: {id:?}"
        )));
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

/// Default number of words in an alias.
pub const WORDS_IN_PHRASE: usize = DEFAULT_ALIAS_WORDS as usize;

/// Number of alias candidates to try before the store reports failure.
pub const ALIAS_ATTEMPTS: usize = 8;

/// Prefix for alias objects. An alias object holds one canonical key.
pub const ALIAS_PREFIX: &str = "words/";

static WORDLIST: OnceLock<Vec<&'static str>> = OnceLock::new();
static WORD_SET: OnceLock<HashSet<&'static str>> = OnceLock::new();

/// The bundled EFF large wordlist, parsed once.
fn wordlist() -> &'static [&'static str] {
    WORDLIST.get_or_init(|| {
        include_str!("wordlist.txt")
            .lines()
            .map(str::trim)
            .filter(|word| !word.is_empty())
            .collect()
    })
}

fn word_set() -> &'static HashSet<&'static str> {
    WORD_SET.get_or_init(|| wordlist().iter().copied().collect())
}

/// Normalize a default three-word alias to the lowercase hyphen form.
///
/// This keeps the public helper on the default word count. Store
/// resolution uses [`normalize_words_up_to`] with the configured count.
pub fn normalize_words(input: &str) -> Option<String> {
    normalize_words_up_to(input, DEFAULT_ALIAS_WORDS)
}

/// Normalize an alias to the lowercase hyphen form.
///
/// The function accepts spaces or hyphens between words and trims the
/// input. It accepts 3 known words up to `max_words`. Four wordlist
/// entries contain a hyphen, so the parser tests every split of the input
/// into wordlist entries.
pub fn normalize_words_up_to(input: &str, max_words: u8) -> Option<String> {
    let max_words = max_words.clamp(MIN_ALIAS_WORDS, MAX_ALIAS_WORDS);
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let chunks: Vec<&str> = lower
        .split(|c: char| c == '-' || c.is_whitespace())
        .filter(|chunk| !chunk.is_empty())
        .collect();
    if chunks.len() < usize::from(MIN_ALIAS_WORDS)
        || !is_words(
            &chunks,
            usize::from(MIN_ALIAS_WORDS),
            usize::from(max_words),
        )
    {
        return None;
    }
    Some(chunks.join("-"))
}

fn is_words(chunks: &[&str], min_words: usize, max_words: usize) -> bool {
    (min_words..=max_words).any(|words| has_word_count(chunks, words))
}

fn has_word_count(chunks: &[&str], words: usize) -> bool {
    fn search(chunks: &[&str], index: usize, words_left: usize) -> bool {
        if words_left == 0 {
            return index == chunks.len();
        }
        if chunks.len().saturating_sub(index) < words_left {
            return false;
        }
        let mut candidate = String::new();
        for end in index..chunks.len() {
            if !candidate.is_empty() {
                candidate.push('-');
            }
            candidate.push_str(chunks[end]);
            if word_set().contains(candidate.as_str()) && search(chunks, end + 1, words_left - 1) {
                return true;
            }
        }
        false
    }
    search(chunks, 0, words)
}

/// True when the input is a valid default three-word alias.
pub fn is_word_phrase(input: &str) -> bool {
    normalize_words(input).is_some()
}

/// Generate one random default three-word alias in the hyphen form.
pub fn generate_words() -> String {
    generate_words_with_count(DEFAULT_ALIAS_WORDS)
}

/// Generate one random alias with `count` words in the hyphen form.
///
/// The count is clamped to the allowed range, so this function cannot
/// generate an alias that the parser rejects.
pub fn generate_words_with_count(count: u8) -> String {
    let count = count.clamp(MIN_ALIAS_WORDS, MAX_ALIAS_WORDS);
    let words = wordlist();
    let mut rng = rand::rng();
    let mut phrase = String::new();
    for index in 0..usize::from(count) {
        if index > 0 {
            phrase.push('-');
        }
        phrase.push_str(words[rng.random_range(0..words.len())]);
    }
    phrase
}

/// Key for the alias object of a normalized phrase.
pub fn alias_key(phrase: &str) -> String {
    format!("{ALIAS_PREFIX}{phrase}")
}

/// Number of decimal digits in a segment sequence number.
///
/// The width is the number of digits in `u64::MAX`, so numeric order and
/// lexicographic key order are the same for every possible sequence.
pub const SEGMENT_SEQUENCE_DIGITS: usize = 20;

/// Number of characters in the random suffix of a segment name.
pub const SEGMENT_SUFFIX_LEN: usize = 12;

/// Generate a random URL-safe suffix for a segment name.
///
/// The suffix has 12 base62 characters. Two appends that choose the same
/// sequence get different object keys.
pub fn generate_segment_suffix() -> String {
    rand::rng()
        .sample_iter(&rand::distr::Alphanumeric)
        .take(SEGMENT_SUFFIX_LEN)
        .map(char::from)
        .collect()
}

/// Key for one appended segment. The base object is segment 0, so the
/// first appended segment has sequence 0.
///
/// The name is `<20-digit sequence>-<suffix>`. The suffix makes the key
/// unique when two appends choose the same sequence.
pub fn segment_key(base_key: &str, sequence: u64, suffix: &str) -> String {
    format!(
        "{base_key}/{sequence:0width$}-{suffix}",
        width = SEGMENT_SEQUENCE_DIGITS
    )
}

/// Prefix that holds every appended segment for a base key.
pub fn segment_prefix(base_key: &str) -> String {
    format!("{base_key}/")
}

/// Parse a segment key that belongs directly to `base_key`.
///
/// Returns the sequence and the suffix. Returns `None` for a nested key,
/// a short sequence, a non-digit sequence, or an empty or non-base62
/// suffix. Such keys are not part of the paste and must not change its
/// size.
pub fn parse_segment_key<'a>(base_key: &str, key: &'a str) -> Option<(u64, &'a str)> {
    let name = key.strip_prefix(&segment_prefix(base_key))?;
    let (sequence, suffix) = name.split_once('-')?;
    if sequence.len() != SEGMENT_SEQUENCE_DIGITS || !sequence.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return None;
    }
    let sequence = sequence.parse::<u64>().ok()?;
    Some((sequence, suffix))
}

/// True when the key has the paste shape `pastes/<date>/<id>`.
///
/// A segment key adds a third component after `pastes/`, so it is not a
/// paste. Listing and bare-id resolution use this predicate to skip it.
pub fn is_paste_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("pastes/") else {
        return false;
    };
    let mut parts = rest.split('/');
    let date = parts.next().unwrap_or("");
    let id = parts.next().unwrap_or("");
    !date.is_empty() && !id.is_empty() && parts.next().is_none()
}

/// One appended segment of a paste.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub sequence: u64,
    /// Random part of the segment name. It orders segments that share a
    /// sequence.
    pub suffix: String,
    pub key: String,
    pub size: u64,
}

/// Order segments by sequence, then by suffix.
fn compare_segments(a: &Segment, b: &Segment) -> std::cmp::Ordering {
    a.sequence
        .cmp(&b.sequence)
        .then_with(|| a.suffix.cmp(&b.suffix))
}

/// Base object plus appended segments, in sequence then suffix order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentPlan {
    pub base_key: String,
    pub base_size: u64,
    pub segments: Vec<Segment>,
}

impl SegmentPlan {
    pub fn new(base_key: impl Into<String>, base_size: u64, mut segments: Vec<Segment>) -> Self {
        segments.sort_by(compare_segments);
        Self {
            base_key: base_key.into(),
            base_size,
            segments,
        }
    }

    /// Sum of the base object and every segment.
    pub fn total_size(&self) -> u64 {
        self.base_size
            + self
                .segments
                .iter()
                .fold(0u64, |sum, s| sum.saturating_add(s.size))
    }

    /// Map an inclusive global byte range to object-local inclusive
    /// ranges. Each tuple is `(key, local_start, local_end)`.
    ///
    /// Empty objects are skipped. A range that starts past the end
    /// returns an empty list, so the caller issues no read.
    pub fn object_ranges(&self, start: u64, end: u64) -> Vec<(String, u64, u64)> {
        if start > end {
            return Vec::new();
        }
        let end_exclusive = end.saturating_add(1);
        let mut out = Vec::new();
        let mut offset = 0u64;
        let objects = std::iter::once((self.base_key.clone(), self.base_size))
            .chain(self.segments.iter().map(|s| (s.key.clone(), s.size)));
        for (key, size) in objects {
            let object_start = offset;
            let object_end = offset.saturating_add(size);
            offset = object_end;
            if size == 0 {
                continue;
            }
            let overlap_start = start.max(object_start);
            let overlap_end = end_exclusive.min(object_end);
            if overlap_start < overlap_end {
                out.push((
                    key,
                    overlap_start - object_start,
                    overlap_end - object_start - 1,
                ));
            }
        }
        out
    }
}

/// Keep only well-formed segment keys for `base_key`, in sequence then
/// suffix order.
pub fn segments_from_listing(base_key: &str, entries: &[(String, u64)]) -> Vec<Segment> {
    let mut segments: Vec<Segment> = entries
        .iter()
        .filter_map(|(key, size)| {
            parse_segment_key(base_key, key).map(|(sequence, suffix)| Segment {
                sequence,
                suffix: suffix.to_string(),
                key: key.clone(),
                size: *size,
            })
        })
        .collect();
    segments.sort_by(compare_segments);
    segments
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

/// Hash and sample one file without reading past `len + 1` bytes.
///
/// S3 takes object metadata only at CreateMultipartUpload, so the hash must
/// exist before that call. The first [`PEEK_BYTES`] come from this same read,
/// so content detection describes the bytes that are hashed and uploaded. A
/// file that grew past `len` is rejected before the upload starts.
async fn hash_file_prefix<R>(reader: &mut R, len: u64) -> Result<(String, Vec<u8>), StoreError>
where
    R: AsyncRead + Unpin,
{
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MULTIPART_CHUNK_BYTES];
    let mut hashed: u64 = 0;
    let mut prefix = Vec::with_capacity(PEEK_BYTES);
    let mut limited = (&mut *reader).take(len.saturating_add(1));
    loop {
        let n = limited.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        if prefix.len() < PEEK_BYTES {
            let take = (PEEK_BYTES - prefix.len()).min(n);
            prefix.extend_from_slice(&buf[..take]);
        }
        hasher.update(&buf[..n]);
        hashed += n as u64;
    }
    if hashed != len {
        return Err(StoreError::Io(format!(
            "the file changed while it was read: expected {len} bytes, read {hashed}"
        )));
    }
    Ok((hex_encode(&hasher.finalize()), prefix))
}

/// Read one open file as a byte stream, for
/// [`crate::stream::run_streaming_upload`].
///
/// The handle is the caller's, so the stream reads the same file that the
/// hash and the size describe.
fn file_stream(file: File) -> impl futures::Stream<Item = Result<Bytes, StoreError>> {
    futures::stream::unfold(file, |mut file| async move {
        let mut buf = vec![0u8; MULTIPART_CHUNK_BYTES];
        match file.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok(Bytes::from(buf)), file))
            }
            Err(e) => Some((Err(StoreError::Io(e.to_string())), file)),
        }
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// "1 day" or "N days", for a TTL error message.
fn days_label(days: u32) -> String {
    if days == 1 {
        "1 day".to_string()
    } else {
        format!("{days} days")
    }
}

fn expires_at_for(created_at: DateTime<Utc>, ttl: Ttl) -> DateTime<Utc> {
    created_at
        .checked_add_signed(ttl.duration())
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

/// Trim a title and enforce its byte limit.
fn normalize_title(title: Option<&str>) -> Result<Option<String>, StoreError> {
    let Some(title) = title else {
        return Ok(None);
    };
    let title = title.trim();
    if title.is_empty() {
        return Ok(None);
    }
    if title.len() > MAX_TITLE_BYTES {
        return Err(StoreError::InvalidTitle(format!(
            "title is longer than {MAX_TITLE_BYTES} bytes"
        )));
    }
    Ok(Some(title.to_string()))
}

/// Lowercase, trim, and deduplicate tags, then enforce the limits.
fn normalize_tags(tags: &[String]) -> Result<Vec<String>, StoreError> {
    let mut out: Vec<String> = Vec::new();
    for tag in tags {
        let tag = tag.trim().to_lowercase();
        if tag.is_empty() {
            continue;
        }
        if !out.contains(&tag) {
            out.push(tag);
        }
    }
    if out.len() > MAX_TAGS {
        return Err(StoreError::InvalidTags(format!(
            "at most {MAX_TAGS} tags are allowed"
        )));
    }
    let total = out.iter().map(String::len).sum::<usize>() + out.len().saturating_sub(1);
    if total > MAX_TAGS_BYTES {
        return Err(StoreError::InvalidTags(format!(
            "tags are longer than {MAX_TAGS_BYTES} bytes"
        )));
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn metadata_for(
    filename: Option<&str>,
    sha256: Option<&str>,
    size: Option<u64>,
    words: Option<&str>,
    paste_token_hash: Option<&str>,
    ttl: Ttl,
    created_at: DateTime<Utc>,
    created_by: Option<&str>,
    detection: &Detection,
    title: Option<&str>,
    tags: &[String],
) -> (HashMap<String, String>, DateTime<Utc>) {
    let expires_at = expires_at_for(created_at, ttl);
    let mut meta = HashMap::new();
    if let Some(f) = filename {
        if !f.is_empty() {
            meta.insert("filename".to_string(), encode_filename(f));
        }
    }
    if let Some(title) = title {
        meta.insert("title".to_string(), encode_filename(title));
    }
    if !tags.is_empty() {
        meta.insert(
            "tags".to_string(),
            serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string()),
        );
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
    if let Some(words) = words {
        meta.insert("words".to_string(), words.to_string());
    }
    if let Some(hash) = paste_token_hash {
        meta.insert("paste-token-sha256".to_string(), hash.to_string());
    }
    meta.insert("ttl-seconds".to_string(), ttl.num_seconds().to_string());
    if let Some(days) = ttl.whole_days() {
        meta.insert("ttl-days".to_string(), days.to_string());
    }
    meta.insert("created-at".to_string(), created_at.to_rfc3339());
    if let Some(address) = created_by {
        meta.insert("created-by".to_string(), address.to_string());
    }
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

/// One incomplete multipart upload, named by key and upload ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadRef {
    pub key: String,
    pub upload_id: String,
}

/// One multipart upload whose abort failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartAbortFailure {
    pub key: String,
    pub upload_id: String,
    pub message: String,
}

/// The result of one stale multipart upload sweep.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MultipartAbortReport {
    /// Uploads that the sweep aborted in live mode.
    pub aborted: Vec<MultipartUploadRef>,
    /// Uploads that a dry run would abort.
    pub would_abort: Vec<MultipartUploadRef>,
    /// Uploads whose abort failed.
    pub failed: Vec<MultipartAbortFailure>,
}

/// One failed cleanup item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupFailure {
    pub key: String,
    pub message: String,
}

/// Options for one manual cleanup pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupOptions {
    /// Report the items without changing the bucket.
    pub dry_run: bool,
    /// Abort multipart uploads older than this duration.
    pub multipart_older_than: Duration,
}

/// The result of one manual cleanup pass.
///
/// Each list holds the named items that the pass changed, would change,
/// or failed to change. The command uses the lengths as machine-readable
/// counts.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CleanupReport {
    pub multipart: MultipartAbortReport,
    pub pastes_deleted: Vec<String>,
    pub pastes_would_delete: Vec<String>,
    pub pastes_failed: Vec<CleanupFailure>,
    pub aliases_deleted: Vec<String>,
    pub aliases_would_delete: Vec<String>,
    pub aliases_failed: Vec<CleanupFailure>,
}

impl CleanupReport {
    /// The number of items that the pass failed to change.
    pub fn failed(&self) -> usize {
        self.multipart.failed.len() + self.pastes_failed.len() + self.aliases_failed.len()
    }
}

#[derive(Clone)]
pub struct Store {
    client: Client,
    pub bucket: String,
    pub config: Config,
    alias_lock: Arc<AsyncMutex<()>>,
    alias_candidates: Option<Arc<Vec<String>>>,
    metrics: Option<Arc<Metrics>>,
    /// Multipart uploads that this process is still driving. The stale
    /// sweep skips these, so a slow but live upload is not aborted.
    active_uploads: Arc<Mutex<HashSet<String>>>,
}

/// A shared handle to the current [`Store`].
///
/// An operation takes one snapshot with [`Self::get`] and uses that
/// snapshot for its full duration. A reload replaces the snapshot, so the
/// next request sees the new configuration and an in-flight request does
/// not.
#[derive(Clone)]
pub struct StoreHandle {
    current: Arc<RwLock<Arc<Store>>>,
}

impl StoreHandle {
    pub fn new(store: Store) -> Self {
        Self {
            current: Arc::new(RwLock::new(Arc::new(store))),
        }
    }

    /// The current snapshot.
    pub fn get(&self) -> Arc<Store> {
        self.current
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Replace the snapshot after a successful reload.
    pub fn replace(&self, store: Store) {
        *self
            .current
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(store);
    }
}

/// Abort one multipart upload with retries.
///
/// The function tries [`ABORT_ATTEMPTS`] times. It logs the key and the
/// upload ID when every attempt fails, so an orphan is visible in the
/// server log. A missing upload counts as success. The caller keeps its
/// original upload error.
async fn abort_multipart_with_retry(
    client: &Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    metrics: Option<&Arc<Metrics>>,
) -> Result<(), StoreError> {
    let mut last_error = None;
    for attempt in 1..=ABORT_ATTEMPTS {
        let result = s3_call!(
            metrics,
            "abort_multipart_upload",
            client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
        );
        match result {
            Ok(_) => return Ok(()),
            Err(error) => {
                if let SdkError::ServiceError(ctx) = &error {
                    if ctx.err().code() == Some("NoSuchUpload") {
                        return Ok(());
                    }
                }
                last_error = Some(map_sdk_error(error, key));
                if attempt < ABORT_ATTEMPTS {
                    tokio::time::sleep(ABORT_BACKOFF * attempt as u32).await;
                }
            }
        }
    }
    let error = last_error.unwrap_or_else(|| StoreError::S3("multipart abort failed".into()));
    tracing::error!(
        key = %key,
        upload_id = %upload_id,
        error = %error,
        "cannot abort multipart upload after {ABORT_ATTEMPTS} attempts"
    );
    Err(error)
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
    metrics: Option<Arc<Metrics>>,
    active_uploads: Arc<Mutex<HashSet<String>>>,
    current_upload: Option<String>,
}

impl Drop for AwsUploadSink {
    fn drop(&mut self) {
        // A cancelled request drops this sink without an abort. Remove the
        // upload from the live set, so the next sweep can clean it.
        if let Some(upload_id) = self.current_upload.take() {
            self.active_uploads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&upload_id);
        }
    }
}

#[async_trait::async_trait]
impl crate::stream::UploadSink for AwsUploadSink {
    async fn put_small(&mut self, bytes: Vec<u8>, sha256: &str) -> Result<(), StoreError> {
        let mut meta = self.metadata.clone();
        meta.insert("sha256".to_string(), sha256.to_string());
        meta.insert("size".to_string(), bytes.len().to_string());
        s3_call!(
            self.metrics,
            "put_object",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&self.key)
                .body(ByteStream::from(bytes))
                .content_type(self.content_type.clone())
                .set_metadata(Some(meta))
                .send()
        )
        .map_err(|e| map_sdk_error(e, &self.key))?;
        Ok(())
    }

    async fn create_multipart(&mut self) -> Result<String, StoreError> {
        let out = s3_call!(
            self.metrics,
            "create_multipart_upload",
            self.client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .content_type(self.content_type.clone())
                .set_metadata(Some(self.metadata.clone()))
                .send()
        )
        .map_err(|e| map_sdk_error(e, &self.key))?;
        let id = out.upload_id().unwrap_or("").to_string();
        if id.is_empty() {
            return Err(StoreError::S3(
                "multipart upload returned no upload id".into(),
            ));
        }
        self.active_uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id.clone());
        self.current_upload = Some(id.clone());
        Ok(id)
    }

    async fn upload_part(
        &mut self,
        upload_id: &str,
        part_number: i32,
        bytes: Vec<u8>,
    ) -> Result<String, StoreError> {
        let out = s3_call!(
            self.metrics,
            "upload_part",
            self.client
                .upload_part()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(bytes))
                .send()
        )
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
        s3_call!(
            self.metrics,
            "complete_multipart_upload",
            self.client
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .upload_id(upload_id)
                .multipart_upload(completed)
                .send()
        )
        .map_err(|e| map_sdk_error(e, &self.key))?;
        self.current_upload = None;
        self.active_uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(upload_id);
        Ok(())
    }

    async fn abort(&mut self, upload_id: &str) {
        self.current_upload = None;
        self.active_uploads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(upload_id);
        let _ = abort_multipart_with_retry(
            &self.client,
            &self.bucket,
            &self.key,
            upload_id,
            self.metrics.as_ref(),
        )
        .await;
    }
}

impl Store {
    pub async fn from_config(config: Config) -> Result<Self, StoreError> {
        Self::from_config_with_metrics(config, None).await
    }

    /// Build a store that records S3 metrics in the given registry.
    pub async fn from_config_with_metrics(
        config: Config,
        metrics: Option<Arc<Metrics>>,
    ) -> Result<Self, StoreError> {
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
            alias_lock: Arc::new(AsyncMutex::new(())),
            alias_candidates: None,
            metrics,
            active_uploads: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// The metrics registry that this store snapshot records to.
    pub fn metrics_handle(&self) -> Option<Arc<Metrics>> {
        self.metrics.clone()
    }

    /// Carry the live upload set across a reload.
    ///
    /// A reload builds a new `Store`. The new stale-upload sweep must not
    /// abort an upload that the old store is still driving.
    pub(crate) fn adopt_active_uploads(&mut self, other: &Store) {
        self.active_uploads = Arc::clone(&other.active_uploads);
    }

    /// The bucket lifecycle rules, or an empty set when none is set.
    ///
    /// The call needs read permission on the bucket.
    pub async fn lifecycle_rules(&self) -> Result<Vec<LifecycleRule>, StoreError> {
        let result = s3_call!(
            self.metrics,
            "get_bucket_lifecycle_configuration",
            self.client
                .get_bucket_lifecycle_configuration()
                .bucket(self.bucket.clone())
                .send()
        );
        match result {
            Ok(output) => Ok(output.rules().to_vec()),
            Err(error) => {
                let absent = match &error {
                    SdkError::ServiceError(ctx) => {
                        ctx.err().code() == Some("NoSuchLifecycleConfiguration")
                            || ctx.raw().status().as_u16() == 404
                    }
                    _ => false,
                };
                if absent {
                    Ok(Vec::new())
                } else {
                    Err(map_sdk_error(error, &self.bucket))
                }
            }
        }
    }

    /// Replace the bucket lifecycle rules.
    ///
    /// The call needs write permission on the bucket. Garage does not
    /// require owner permission for this endpoint.
    pub async fn put_lifecycle_rules(&self, rules: Vec<LifecycleRule>) -> Result<(), StoreError> {
        let configuration = crate::lifecycle::configuration(rules)
            .map_err(|error| StoreError::InvalidLifecycle(error.to_string()))?;
        s3_call!(
            self.metrics,
            "put_bucket_lifecycle_configuration",
            self.client
                .put_bucket_lifecycle_configuration()
                .bucket(self.bucket.clone())
                .lifecycle_configuration(configuration)
                .send()
        )
        .map_err(|error| map_sdk_error(error, &self.bucket))?;
        Ok(())
    }

    /// Log the bucket lifecycle rules, best effort.
    ///
    /// The call needs read permission on the bucket. A missing rule or an
    /// S3 error logs one warning. Startup and serving continue.
    pub async fn log_lifecycle_rules(&self) {
        match self.lifecycle_rules().await {
            Ok(rules) if rules.is_empty() => {
                tracing::warn!(
                    bucket = %self.bucket,
                    "bucket has no lifecycle rules; expired objects stay in storage until a client or an operator deletes them"
                );
            }
            Ok(rules) => {
                for rule in &rules {
                    tracing::info!(
                        bucket = %self.bucket,
                        rule = rule.id().unwrap_or(""),
                        status = ?rule.status(),
                        prefix = ?rule.filter().and_then(|filter| filter.prefix()),
                        expire_days = ?rule.expiration().and_then(|expiration| expiration.days()),
                        abort_after_days = ?rule.abort_incomplete_multipart_upload().and_then(|abort| abort.days_after_initiation()),
                        "bucket lifecycle rule"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    bucket = %self.bucket,
                    "cannot read the bucket lifecycle configuration: {error:?}"
                );
            }
        }
    }

    /// Abort incomplete multipart uploads older than `older_than`.
    ///
    /// The automatic sweep uses this wrapper. It returns the count of
    /// aborted uploads and logs each failed abort. A manual cleanup calls
    /// [`Self::abort_stale_multipart_uploads_with_report`] instead.
    pub async fn abort_stale_multipart_uploads(
        &self,
        older_than: Duration,
    ) -> Result<usize, StoreError> {
        Ok(self
            .abort_stale_multipart_uploads_with_report(older_than, false)
            .await?
            .aborted
            .len())
    }

    /// Abort incomplete multipart uploads older than `older_than`.
    ///
    /// The sweep lists the `pastes/` prefix and follows pagination. It
    /// aborts each stale upload with retries and skips an upload that this
    /// process still drives. One failed abort logs and the sweep
    /// continues, so one bad object cannot stop the rest. With `dry_run`,
    /// the report lists each upload that the sweep would abort and changes
    /// nothing.
    pub async fn abort_stale_multipart_uploads_with_report(
        &self,
        older_than: Duration,
        dry_run: bool,
    ) -> Result<MultipartAbortReport, StoreError> {
        let cutoff = chrono::Duration::from_std(older_than)
            .ok()
            .and_then(|older_than| Utc::now().checked_sub_signed(older_than));
        let Some(cutoff) = cutoff else {
            // The age is too large to represent. No upload is that old.
            return Ok(MultipartAbortReport::default());
        };
        let mut key_marker: Option<String> = None;
        let mut upload_id_marker: Option<String> = None;
        let mut report = MultipartAbortReport::default();
        loop {
            let mut request = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket)
                .prefix("pastes/");
            if let Some(marker) = &key_marker {
                request = request.key_marker(marker);
            }
            if let Some(marker) = &upload_id_marker {
                request = request.upload_id_marker(marker);
            }
            let output = s3_call!(self.metrics, "list_multipart_uploads", request.send())
                .map_err(|error| map_sdk_error(error, "pastes/"))?;
            for upload in output.uploads() {
                let Some(key) = upload.key() else { continue };
                let Some(upload_id) = upload.upload_id() else {
                    continue;
                };
                let initiated = upload
                    .initiated()
                    .and_then(|date| DateTime::from_timestamp(date.secs(), date.subsec_nanos()));
                if !initiated.is_some_and(|date| date < cutoff) {
                    continue;
                }
                let is_active = self
                    .active_uploads
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .contains(upload_id);
                if is_active {
                    continue;
                }
                let named = MultipartUploadRef {
                    key: key.to_string(),
                    upload_id: upload_id.to_string(),
                };
                if dry_run {
                    report.would_abort.push(named);
                    continue;
                }
                match abort_multipart_with_retry(
                    &self.client,
                    &self.bucket,
                    key,
                    upload_id,
                    self.metrics.as_ref(),
                )
                .await
                {
                    Ok(()) => {
                        report.aborted.push(named);
                        if let Some(metrics) = &self.metrics {
                            metrics.count_multipart_orphan();
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            key = %key,
                            upload_id = %upload_id,
                            "cannot abort a stale multipart upload: {error}"
                        );
                        report.failed.push(MultipartAbortFailure {
                            key: key.to_string(),
                            upload_id: upload_id.to_string(),
                            message: error.to_string(),
                        });
                    }
                }
            }
            if !output.is_truncated().unwrap_or(false) {
                break;
            }
            let next_key = output.next_key_marker().map(str::to_string);
            let next_upload_id = output.next_upload_id_marker().map(str::to_string);
            if next_key.is_none() && next_upload_id.is_none() {
                break;
            }
            key_marker = next_key;
            upload_id_marker = next_upload_id;
        }
        Ok(report)
    }

    /// Run one manual cleanup pass.
    ///
    /// The pass aborts stale multipart uploads, deletes expired paste
    /// objects through [`Self::delete`], then deletes expired alias
    /// objects under `words/`. A per-item failure is collected and the
    /// pass continues. With `dry_run`, the pass collects the items that it
    /// would delete and changes nothing. The pass does not read the bucket
    /// lifecycle configuration.
    pub async fn cleanup(&self, options: CleanupOptions) -> Result<CleanupReport, StoreError> {
        let mut report = CleanupReport {
            multipart: self
                .abort_stale_multipart_uploads_with_report(
                    options.multipart_older_than,
                    options.dry_run,
                )
                .await?,
            ..CleanupReport::default()
        };
        let now = Utc::now();
        let paste_keys = match self.list_prefix_keys("pastes/").await {
            Ok(keys) => keys,
            Err(error) => {
                report.pastes_failed.push(CleanupFailure {
                    key: "pastes/".to_string(),
                    message: error.to_string(),
                });
                Vec::new()
            }
        };
        for key in paste_keys {
            if !is_paste_key(&key) {
                continue;
            }
            let expires_at = match self.head_expiry(&key).await {
                Ok(expires_at) => expires_at,
                Err(error) => {
                    report.pastes_failed.push(CleanupFailure {
                        key: key.clone(),
                        message: error.to_string(),
                    });
                    continue;
                }
            };
            if !is_expired_at(expires_at, now) {
                continue;
            }
            if options.dry_run {
                report.pastes_would_delete.push(key);
                continue;
            }
            match self.delete(&key).await {
                Ok(()) => report.pastes_deleted.push(key),
                Err(error) => report.pastes_failed.push(CleanupFailure {
                    key: key.clone(),
                    message: error.to_string(),
                }),
            }
        }
        let alias_keys = match self.list_prefix_keys(ALIAS_PREFIX).await {
            Ok(keys) => keys,
            Err(error) => {
                report.aliases_failed.push(CleanupFailure {
                    key: ALIAS_PREFIX.to_string(),
                    message: error.to_string(),
                });
                Vec::new()
            }
        };
        for key in alias_keys {
            let expires_at = match self.head_expiry(&key).await {
                Ok(expires_at) => expires_at,
                Err(error) => {
                    report.aliases_failed.push(CleanupFailure {
                        key: key.clone(),
                        message: error.to_string(),
                    });
                    continue;
                }
            };
            if !is_expired_at(expires_at, now) {
                continue;
            }
            if options.dry_run {
                report.aliases_would_delete.push(key);
                continue;
            }
            match self.delete_object_key(&key).await {
                Ok(()) => report.aliases_deleted.push(key),
                Err(error) => report.aliases_failed.push(CleanupFailure {
                    key: key.clone(),
                    message: error.to_string(),
                }),
            }
        }
        Ok(report)
    }

    /// List every object key under `prefix`, across all pages.
    ///
    /// The cleanup command scans the full bucket, so the listing follows
    /// continuation tokens until S3 reports the last page. The bucket is
    /// small in v1. The scan is O(n) in stored objects.
    async fn list_prefix_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(1000);
            if let Some(token) = token.as_deref() {
                request = request.continuation_token(token);
            }
            let output = s3_call!(self.metrics, "list_objects_v2", request.send())
                .map_err(|error| map_sdk_error(error, prefix))?;
            keys.extend(
                output
                    .contents()
                    .iter()
                    .filter_map(|object| object.key().map(str::to_string)),
            );
            match (
                output.is_truncated().unwrap_or(false),
                output.next_continuation_token(),
            ) {
                (true, Some(next)) => token = Some(next.to_string()),
                _ => return Ok(keys),
            }
        }
    }

    /// Read the `expires-at` metadata value from one object.
    ///
    /// A missing object returns `Ok(None)`. The cleanup scan treats a
    /// missing object or a missing expiry value as an object to keep.
    async fn head_expiry(&self, key: &str) -> Result<Option<DateTime<Utc>>, StoreError> {
        match s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
        .map_err(|error| map_sdk_error(error, key))
        {
            Ok(output) => Ok(output
                .metadata()
                .and_then(|meta| meta.get("expires-at"))
                .and_then(|value| parse_rfc3339(value))),
            Err(StoreError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Normalize an alias with the configured maximum word count.
    fn normalize_alias(&self, input: &str) -> Option<String> {
        normalize_words_up_to(input, self.config.alias_words)
    }

    /// Use a fixed candidate order for alias allocation.
    ///
    /// Integration tests use this to force two stores to compete for one
    /// phrase. Production code uses the random word generator.
    #[doc(hidden)]
    pub fn with_alias_candidates(mut self, candidates: Vec<String>) -> Self {
        self.alias_candidates = Some(Arc::new(candidates));
        self
    }

    /// True when an alias object already holds this phrase.
    ///
    /// An alias object with a valid past `expires-at` value is free to
    /// reuse. A missing or malformed value keeps the alias taken.
    async fn alias_object_exists(&self, phrase: &str) -> Result<bool, StoreError> {
        let key = alias_key(phrase);
        match s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, &key))
        {
            Ok(out) => {
                let expires_at = out
                    .metadata()
                    .and_then(|meta| meta.get("expires-at"))
                    .and_then(|value| parse_rfc3339(value));
                Ok(!expires_at.is_some_and(|expires| Utc::now() > expires))
            }
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Choose an unused alias from a small set of random candidates.
    async fn choose_alias(&self) -> Result<String, StoreError> {
        match &self.alias_candidates {
            Some(candidates) => self.choose_alias_from(candidates.iter().cloned()).await,
            None => {
                let count = self.config.alias_words;
                self.choose_alias_from(
                    std::iter::repeat_with(move || generate_words_with_count(count))
                        .take(ALIAS_ATTEMPTS),
                )
                .await
            }
        }
    }

    /// Choose the first alias in `candidates` that no alias object holds.
    ///
    /// Production code calls [`Self::choose_alias`]. The collision test
    /// uses this method to supply a fixed candidate order.
    #[doc(hidden)]
    pub async fn choose_alias_from<I>(&self, candidates: I) -> Result<String, StoreError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut attempts = 0;
        for candidate in candidates {
            attempts += 1;
            let Some(phrase) = self.normalize_alias(&candidate) else {
                continue;
            };
            if !self.alias_object_exists(&phrase).await? {
                return Ok(phrase);
            }
        }
        Err(StoreError::AliasUnavailable { attempts })
    }

    /// Write the alias object that maps a phrase to a canonical key.
    async fn write_alias(
        &self,
        phrase: &str,
        target_key: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let key = alias_key(phrase);
        let mut meta = HashMap::new();
        meta.insert("target".to_string(), target_key.to_string());
        meta.insert("expires-at".to_string(), expires_at.to_rfc3339());
        meta.insert("kind".to_string(), "alias".to_string());
        s3_call!(
            self.metrics,
            "put_object",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(target_key.as_bytes().to_vec()))
                .content_type("text/plain")
                .set_metadata(Some(meta))
                .send()
        )
        .map_err(|e| map_sdk_error(e, &key))?;
        Ok(())
    }

    /// Claim an alias for `key` before the base object is written.
    ///
    /// One lock on this `Store` instance covers the check and the write,
    /// so two callers that share the instance cannot choose the same
    /// phrase. Garage ignores conditional writes, so two stores can still
    /// race; [`Self::verify_alias`] detects that case before a paste
    /// returns.
    async fn claim_alias(
        &self,
        key: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<String, StoreError> {
        let _guard = self.alias_lock.lock().await;
        let words = self.choose_alias().await?;
        if let Err(e) = self.write_alias(&words, key, expires_at).await {
            // The write may have reached the bucket even when the SDK
            // reports an error. Remove the alias only if it points here.
            let _ = self.delete_alias_if_target(&words, key).await;
            return Err(e);
        }
        Ok(words)
    }

    async fn delete_alias_object(&self, phrase: &str) -> Result<(), StoreError> {
        let key = alias_key(phrase);
        s3_call!(
            self.metrics,
            "delete_object",
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, &key))?;
        Ok(())
    }

    /// Remove the base object and segments after a failed store.
    async fn cleanup_failed_store(&self, base_key: &str, phrase: &str) {
        if let Err(error) = self.delete_alias_if_target(phrase, base_key).await {
            tracing::warn!(
                key = %alias_key(phrase),
                error = ?error,
                "cannot delete the alias object of a failed store"
            );
        }
        let _ = self.delete_base_and_segments(base_key).await;
    }

    /// Read the canonical key from an alias object.
    ///
    /// The body must equal the stored `target` metadata, and the target
    /// must be a valid paste key. A stale or corrupt alias is not trusted.
    async fn read_alias(&self, phrase: &str) -> Result<String, StoreError> {
        let key = alias_key(phrase);
        let out = s3_call!(
            self.metrics,
            "get_object",
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
        )
        .map_err(|e| match map_sdk_error(e, &key) {
            StoreError::NotFound(_) => StoreError::NotFound(phrase.to_string()),
            other => other,
        })?;
        let meta = out.metadata().cloned().unwrap_or_default();
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| s3_failure("cannot read the alias object body", e))?
            .to_vec();
        let target =
            String::from_utf8(bytes).map_err(|_| StoreError::NotFound(phrase.to_string()))?;
        if meta.get("kind").map(String::as_str) != Some("alias")
            || meta.get("target").map(String::as_str) != Some(target.as_str())
            || !target.starts_with("pastes/")
            || validate_id(&target).is_err()
        {
            return Err(StoreError::NotFound(phrase.to_string()));
        }
        Ok(target)
    }

    /// Require that `phrase` still points to `key`.
    ///
    /// A second process can overwrite an alias because Garage ignores
    /// conditional writes. Check the binding after the upload, before
    /// the paste is returned.
    async fn verify_alias(&self, phrase: &str, key: &str) -> Result<(), StoreError> {
        match self.read_alias(phrase).await {
            Ok(target) if target == key => Ok(()),
            Ok(_) => Err(StoreError::AliasUnavailable { attempts: 1 }),
            Err(e) => Err(e),
        }
    }

    /// True when an alias check shows that the object has no alias.
    ///
    /// A different target or a missing alias object means the object lost
    /// its name. Any other error is transient, so the caller keeps the
    /// object and returns the error.
    fn alias_is_lost(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::AliasUnavailable { .. } | StoreError::NotFound(_)
        )
    }

    /// Delete an alias only when it still points to `target`.
    ///
    /// Cleanup and delete must not remove a phrase that another store
    /// has rebound to a different paste.
    async fn delete_alias_if_target(&self, phrase: &str, target: &str) -> Result<(), StoreError> {
        match self.read_alias(phrase).await {
            Ok(current) if current == target => self.delete_alias_object(phrase).await,
            Ok(_) | Err(StoreError::NotFound(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Resolve a phrase to a live canonical key.
    async fn resolve_alias(&self, phrase: &str) -> Result<String, StoreError> {
        let target = self.read_alias(phrase).await?;
        let meta = match self.head_metadata_by_key(&target, phrase).await {
            Ok(meta) => meta,
            Err(StoreError::NotFound(_)) => return Err(StoreError::NotFound(phrase.to_string())),
            Err(e) => return Err(e),
        };
        if meta.expired {
            return Err(StoreError::Expired(phrase.to_string()));
        }
        Ok(meta.key)
    }

    /// Read the `words` value from a base object, when it has one.
    async fn base_words(&self, key: &str) -> Result<Option<String>, StoreError> {
        let out = match s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, key))
        {
            Ok(out) => out,
            Err(StoreError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(out
            .metadata()
            .and_then(|meta| meta.get("words").cloned())
            .and_then(|words| self.normalize_alias(&words)))
    }

    /// Delete one object. DeleteObject is idempotent, so a missing key is
    /// not an error.
    async fn delete_object_key(&self, key: &str) -> Result<(), StoreError> {
        s3_call!(
            self.metrics,
            "delete_object",
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, key))?;
        Ok(())
    }

    /// Delete the base object and every object under its segment prefix.
    ///
    /// The first listing runs before the base delete. A listing failure
    /// therefore leaves the paste intact. After the base is gone, the
    /// function lists the prefix again and removes anything that arrived
    /// during the delete. An append that is still in flight removes its
    /// own segment when it sees that the base is gone.
    async fn delete_base_and_segments(&self, base_key: &str) -> Result<(), StoreError> {
        let segments = match self.list_segment_entries(base_key).await {
            Ok(segments) => segments,
            Err(error) => {
                tracing::warn!(
                    key = %base_key,
                    error = ?error,
                    "cannot list segments for delete"
                );
                return Err(partial_update_error("segment list", base_key, error));
            }
        };
        if let Err(error) = self.delete_object_key(base_key).await {
            tracing::warn!(
                key = %base_key,
                error = ?error,
                "cannot delete object"
            );
            return Err(partial_update_error("base delete", base_key, error));
        }
        let mut first_error = None;
        for (key, _) in segments {
            if let Err(error) = self.delete_object_key(&key).await {
                tracing::warn!(
                    key = %key,
                    error = ?error,
                    "cannot delete object"
                );
                first_error.get_or_insert(partial_update_error("segment delete", &key, error));
            }
        }
        for _ in 0..DELETE_SEGMENT_PASSES {
            let remaining = match self.list_segment_entries(base_key).await {
                Ok(remaining) => remaining,
                Err(error) => {
                    tracing::warn!(
                        key = %base_key,
                        error = ?error,
                        "cannot list segments for delete"
                    );
                    first_error.get_or_insert(partial_update_error(
                        "segment list",
                        base_key,
                        error,
                    ));
                    break;
                }
            };
            if remaining.is_empty() {
                break;
            }
            for (key, _) in remaining {
                if let Err(error) = self.delete_object_key(&key).await {
                    tracing::warn!(
                        key = %key,
                        error = ?error,
                        "cannot delete object"
                    );
                    first_error.get_or_insert(partial_update_error("segment delete", &key, error));
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn delete_by_words(&self, phrase: &str) -> Result<(), StoreError> {
        let target = self.read_alias(phrase).await?;
        self.delete_base_and_segments(&target).await?;
        self.delete_alias_if_target(phrase, &target)
            .await
            .map_err(|error| partial_update_error("alias delete", &alias_key(phrase), error))
    }

    /// Resolve a client ID to an S3 key.
    ///
    /// Full IDs (`yyyy-mm-dd/<random>`) resolve directly. Bare random IDs
    /// are resolved by scanning the `pastes/` prefix (at most
    /// [`LIST_SCAN_MAX_PAGES`] pages of 1,000 keys). A valid word
    /// alias resolves through its `words/` object. That scan is the cost
    /// of not having an index: clients should keep the full ID returned
    /// at store time.
    pub async fn resolve_key(&self, id: &str) -> Result<String, StoreError> {
        if let Some(phrase) = self.normalize_alias(id) {
            return self.resolve_alias(&phrase).await;
        }
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
            let out = s3_call!(self.metrics, "list_objects_v2", req.send())
                .map_err(|e| map_sdk_error(e, id))?;
            for obj in out.contents() {
                if let Some(key) = obj.key() {
                    if !is_paste_key(key) {
                        continue;
                    }
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

    /// Clamp a TTL to the size curve when `retention_by_size` is on.
    ///
    /// With the setting off, the original TTL comes back unchanged, so
    /// every existing TTL form keeps its exact expiry. The clamp never
    /// goes below the configured minimum TTL.
    fn clamp_ttl_to_size(&self, ttl: Ttl, size: u64) -> Ttl {
        if !self.config.retention_by_size {
            return ttl;
        }
        let floor = i64::from(self.config.min_ttl_days) * 86_400;
        let limit = self.config.retention_seconds(size).max(floor);
        if ttl.num_seconds() > limit {
            Ttl::seconds(limit)
        } else {
            ttl
        }
    }

    /// Reject a TTL outside the configured range.
    fn validate_ttl(&self, ttl: Ttl) -> Result<Ttl, StoreError> {
        let seconds = ttl.num_seconds();
        let min_seconds = i64::from(self.config.min_ttl_days) * 86_400;
        if min_seconds > 0 && seconds < min_seconds {
            return Err(StoreError::TtlOutOfRange(format!(
                "the minimum TTL is {}",
                days_label(self.config.min_ttl_days)
            )));
        }
        let max_seconds = i64::from(self.config.max_ttl_days) * 86_400;
        if seconds > max_seconds {
            return Err(StoreError::TtlOutOfRange(format!(
                "the maximum TTL is {}",
                days_label(self.config.max_ttl_days)
            )));
        }
        Ok(ttl)
    }

    pub async fn store_text(
        &self,
        text: &[u8],
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl: Option<Ttl>,
    ) -> Result<StoredPaste, StoreError> {
        let fields = PasteFields {
            filename: filename.map(str::to_string),
            content_type: content_type.map(str::to_string),
            ..PasteFields::default()
        };
        self.store_text_with_fields(text, &fields, ttl).await
    }

    /// Store text with optional title and tags.
    pub async fn store_text_with_fields(
        &self,
        text: &[u8],
        fields: &PasteFields,
        ttl: Option<Ttl>,
    ) -> Result<StoredPaste, StoreError> {
        let filename = fields.filename.as_deref();
        let content_type = fields.content_type.as_deref();
        let title = normalize_title(fields.title.as_deref())?;
        let tags = normalize_tags(&fields.tags)?;
        let max_bytes = self.config.max_text_paste_bytes();
        if u64::try_from(text.len()).unwrap_or(u64::MAX) > max_bytes {
            return Err(StoreError::TooLarge { limit: max_bytes });
        }
        let ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        let ttl = self.validate_ttl(ttl)?;
        let ttl = self.clamp_ttl_to_size(ttl, text.len() as u64);
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
        let expires_at = expires_at_for(created_at, ttl);
        let paste_token = self.config.paste_tokens.then(generate_paste_token);
        let paste_token_hash = paste_token
            .as_deref()
            .map(|token| sha256_hex(token.as_bytes()));
        let words = self.claim_alias(&key, expires_at).await?;
        let (meta, _) = metadata_for(
            filename,
            Some(&sha),
            Some(text.len() as u64),
            Some(&words),
            paste_token_hash.as_deref(),
            ttl,
            created_at,
            fields.created_by.as_deref(),
            &detection,
            title.as_deref(),
            &tags,
        );
        if let Err(e) = s3_call!(
            self.metrics,
            "put_object",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(text.to_vec()))
                .content_type(ct.clone())
                .set_metadata(Some(meta))
                .send()
        ) {
            self.cleanup_failed_store(&key, &words).await;
            return Err(map_sdk_error(e, &key));
        }
        if let Err(e) = self.verify_alias(&words, &key).await {
            if Self::alias_is_lost(&e) {
                self.cleanup_failed_store(&key, &words).await;
            }
            return Err(e);
        }
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            words: Some(words),
            size: text.len() as u64,
            sha256: sha,
            content_type: ct,
            filename: filename.map(|s| s.to_string()),
            title,
            tags,
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
            paste_token,
        })
    }

    /// Store a local file. Files up to one multipart chunk use a single
    /// PutObject; larger files use S3 multipart upload, reading the file
    /// in chunks so a large file is never loaded whole into memory.
    ///
    /// One open handle serves every read. The size, the hash, and the
    /// uploaded bytes therefore describe the same file: a path-based
    /// second read could see a file that changed, and the stored metadata
    /// would then describe bytes that were never uploaded.
    pub async fn store_file_path(
        &self,
        path: &Path,
        filename: Option<&str>,
        content_type: Option<&str>,
        ttl: Option<Ttl>,
    ) -> Result<StoredPaste, StoreError> {
        let fields = PasteFields {
            filename: filename.map(str::to_string),
            content_type: content_type.map(str::to_string),
            ..PasteFields::default()
        };
        self.store_file_path_with_fields(path, &fields, ttl).await
    }

    /// Store a local file with optional title and tags.
    pub async fn store_file_path_with_fields(
        &self,
        path: &Path,
        fields: &PasteFields,
        ttl: Option<Ttl>,
    ) -> Result<StoredPaste, StoreError> {
        let filename = fields.filename.as_deref();
        let content_type = fields.content_type.as_deref();
        let title = normalize_title(fields.title.as_deref())?;
        let tags = normalize_tags(&fields.tags)?;
        let mut file = File::open(path).await?;
        // Metadata from the handle, not the path: one file, one view.
        let file_meta = file.metadata().await?;
        if !file_meta.is_file() {
            return Err(StoreError::Io(format!(
                "not a regular file: {}",
                path.display()
            )));
        }
        let mut len = file_meta.len();
        // The total paste cap also bounds creation. Use the lower value
        // for every size check and for the streaming upload.
        let max_file_bytes = self.config.max_file_paste_bytes();
        if len > max_file_bytes {
            return Err(StoreError::TooLarge {
                limit: max_file_bytes,
            });
        }
        let mut ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        ttl = self.validate_ttl(ttl)?;
        ttl = self.clamp_ttl_to_size(ttl, len);
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string()).or_else(|| {
            path.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        });
        let paste_token = self.config.paste_tokens.then(generate_paste_token);
        let paste_token_hash = paste_token
            .as_deref()
            .map(|token| sha256_hex(token.as_bytes()));
        let mut expires_at = expires_at_for(created_at, ttl);
        let mut words: Option<String> = None;
        if len as usize <= MULTIPART_CHUNK_BYTES {
            // One bounded read gives the body, the hash, and the size, so
            // text detection sees every byte. The read stops one byte past
            // the smaller of the file cap and one chunk. A file that grew
            // past one chunk is re-read on the multipart path. A file that
            // grew past the cap is rejected before any object is written.
            let read_cap = max_file_bytes
                .min(MULTIPART_CHUNK_BYTES as u64)
                .saturating_add(1);
            let mut bytes: Vec<u8> = Vec::with_capacity(len as usize);
            let claimed = self.claim_alias(&key, expires_at).await?;
            let read = match (&mut file).take(read_cap).read_to_end(&mut bytes).await {
                Ok(read) => read,
                Err(e) => {
                    self.cleanup_failed_store(&key, &claimed).await;
                    return Err(e.into());
                }
            };
            let size = read as u64;
            if size > max_file_bytes {
                self.cleanup_failed_store(&key, &claimed).await;
                return Err(StoreError::TooLarge {
                    limit: max_file_bytes,
                });
            }
            if size <= MULTIPART_CHUNK_BYTES as u64 {
                let ttl = self.clamp_ttl_to_size(ttl, size);
                let expires_at = expires_at_for(created_at, ttl);
                let detection = detection_for(display_name.as_deref(), content_type, &bytes);
                let ct = detection.content_type.clone();
                let sha = sha256_hex(&bytes);
                let (meta, _) = metadata_for(
                    display_name.as_deref(),
                    Some(&sha),
                    Some(size),
                    Some(&claimed),
                    paste_token_hash.as_deref(),
                    ttl,
                    created_at,
                    fields.created_by.as_deref(),
                    &detection,
                    title.as_deref(),
                    &tags,
                );
                if let Err(e) = s3_call!(
                    self.metrics,
                    "put_object",
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .key(&key)
                        .body(ByteStream::from(bytes))
                        .content_type(ct.clone())
                        .set_metadata(Some(meta))
                        .send()
                ) {
                    self.cleanup_failed_store(&key, &claimed).await;
                    return Err(map_sdk_error(e, &key));
                }
                if let Err(e) = self.verify_alias(&claimed, &key).await {
                    if Self::alias_is_lost(&e) {
                        self.cleanup_failed_store(&key, &claimed).await;
                    }
                    return Err(e);
                }
                return Ok(StoredPaste {
                    id: format!("{date}/{random_id}"),
                    key,
                    words: Some(claimed),
                    size,
                    sha256: sha,
                    content_type: ct,
                    filename: display_name,
                    title,
                    tags,
                    detection_source: Some(detection.source.to_string()),
                    detection_conflict: detection.conflict,
                    expires_at,
                    paste_token,
                });
            }

            // The file grew past one chunk. Re-read it from the start on
            // the multipart path. The alias was claimed before the read.
            drop(bytes);
            words = Some(claimed);
            if let Err(e) = file.seek(SeekFrom::Start(0)).await {
                if let Some(words) = words.as_deref() {
                    self.cleanup_failed_store(&key, words).await;
                }
                return Err(e.into());
            }
            len = match file.metadata().await {
                Ok(meta) => meta.len(),
                Err(e) => {
                    if let Some(words) = words.as_deref() {
                        self.cleanup_failed_store(&key, words).await;
                    }
                    return Err(e.into());
                }
            };
            if len > max_file_bytes {
                if let Some(words) = words.as_deref() {
                    self.cleanup_failed_store(&key, words).await;
                }
                return Err(StoreError::TooLarge {
                    limit: max_file_bytes,
                });
            }
            // The file grew. A larger paste gets a shorter retention.
            // The alias still holds the earlier, longer expiry.
            ttl = self.clamp_ttl_to_size(ttl, len);
            expires_at = expires_at_for(created_at, ttl);
        }

        // Multipart path. S3 takes the object metadata at
        // CreateMultipartUpload, so the hash must be known first: hash the
        // bytes from this handle, then upload from the same handle and
        // require the uploaded bytes to match that hash. A file that
        // changes in between fails the comparison, and the object and its
        // alias are deleted before any paste is returned.
        //
        // The hash read stops at len + 1 bytes. A file that grew is rejected
        // without reading the whole file. The first PEEK_BYTES also come from
        // this read, so the recorded type describes the bytes that are
        // hashed and uploaded.
        let (sha, prefix) = match hash_file_prefix(&mut file, len).await {
            Ok(value) => value,
            Err(e) => {
                if let Some(words) = words.as_deref() {
                    self.cleanup_failed_store(&key, words).await;
                }
                // The hash read stops at len + 1. If the file is now past
                // the cap, report the cap rather than a generic change.
                if let Ok(meta) = file.metadata().await {
                    if meta.len() > max_file_bytes {
                        return Err(StoreError::TooLarge {
                            limit: max_file_bytes,
                        });
                    }
                }
                return Err(e);
            }
        };
        let detection = detect_content_type_sample(
            content_type,
            display_name.as_deref(),
            &prefix,
            len <= PEEK_BYTES as u64,
        );
        let ct = detection.content_type.clone();
        if let Err(e) = file.seek(SeekFrom::Start(0)).await {
            if let Some(words) = words.as_deref() {
                self.cleanup_failed_store(&key, words).await;
            }
            return Err(e.into());
        }
        let words = match words {
            Some(words) => words,
            None => self.claim_alias(&key, expires_at).await?,
        };
        let (meta, _) = metadata_for(
            display_name.as_deref(),
            Some(&sha),
            Some(len),
            Some(&words),
            paste_token_hash.as_deref(),
            ttl,
            created_at,
            fields.created_by.as_deref(),
            &detection,
            title.as_deref(),
            &tags,
        );
        let mut sink = AwsUploadSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.clone(),
            content_type: ct.clone(),
            metadata: meta,
            metrics: self.metrics.clone(),
            active_uploads: Arc::clone(&self.active_uploads),
            current_upload: None,
        };
        let outcome = match crate::stream::run_streaming_upload(
            Box::pin(file_stream(file)),
            &mut sink,
            Some(len),
            max_file_bytes,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(e) => {
                self.cleanup_failed_store(&key, &words).await;
                return Err(e);
            }
        };
        if outcome.total_bytes != len || outcome.sha256 != sha {
            self.cleanup_failed_store(&key, &words).await;
            return Err(StoreError::Io(format!(
                "the file changed during the upload: expected {len} bytes with sha256 {sha}, uploaded {} bytes with sha256 {}",
                outcome.total_bytes, outcome.sha256
            )));
        }
        if let Err(e) = self.verify_alias(&words, &key).await {
            if Self::alias_is_lost(&e) {
                self.cleanup_failed_store(&key, &words).await;
            }
            return Err(e);
        }
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            words: Some(words),
            size: outcome.total_bytes,
            sha256: outcome.sha256,
            content_type: ct,
            filename: display_name,
            title,
            tags,
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
            paste_token,
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
    /// The total paste cap always applies, so the store clamps `max_bytes`
    /// to [`Config::max_paste_bytes`]. A known `Content-Length` over that
    /// cap is rejected before detection peeks, so an oversized body is not
    /// read at all.
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
        let fields = PasteFields {
            filename: filename.map(str::to_string),
            content_type: content_type.map(str::to_string),
            ..PasteFields::default()
        };
        self.store_stream_with_fields(stream, &fields, ttl, known_length, max_bytes)
            .await
    }

    /// Store a streamed upload with optional title and tags.
    pub async fn store_stream_with_fields<S>(
        &self,
        stream: S,
        fields: &PasteFields,
        ttl: Option<Ttl>,
        known_length: Option<u64>,
        max_bytes: u64,
    ) -> Result<StoredPaste, StoreError>
    where
        S: futures::Stream<Item = Result<Bytes, StoreError>> + Send + Unpin,
    {
        let filename = fields.filename.as_deref();
        let content_type = fields.content_type.as_deref();
        let title = normalize_title(fields.title.as_deref())?;
        let tags = normalize_tags(&fields.tags)?;
        // The total paste cap also bounds creation. Clamp here so a direct
        // caller cannot create a paste above it.
        let max_bytes = max_bytes.min(self.config.max_paste_bytes);
        if let Some(len) = known_length {
            if len > max_bytes {
                return Err(StoreError::TooLarge { limit: max_bytes });
            }
        }
        let ttl = ttl.unwrap_or_else(|| Ttl::days(self.config.default_ttl_days));
        let ttl = self.validate_ttl(ttl)?;
        // A text paste has no filename. Buffer the whole body, bounded
        // by the text cap, so detection sees every byte and text is
        // served inline. A file upload keeps the streaming path, because
        // its cap can be much larger.
        if filename.is_none_or(|name| name.trim().is_empty()) {
            let body = collect_stream_body(stream, max_bytes).await?;
            if let Some(expected) = known_length {
                let actual = body.len() as u64;
                if actual != expected {
                    return Err(StoreError::LengthMismatch { expected, actual });
                }
            }
            let text_fields = PasteFields {
                filename: None,
                content_type: fields.content_type.clone(),
                title: fields.title.clone(),
                tags: fields.tags.clone(),
                created_by: fields.created_by.clone(),
            };
            return self
                .store_text_with_fields(&body, &text_fields, Some(ttl))
                .await;
        }
        // A known length gives the exact size. Without one, use the
        // request cap: the final size cannot exceed it, so the shorter
        // retention is safe.
        let ttl = self.clamp_ttl_to_size(ttl, known_length.unwrap_or(max_bytes));
        let created_at = Utc::now();
        let random_id = generate_random_id();
        let date = created_at.format("%Y-%m-%d").to_string();
        let key = key_for(&date, &random_id);
        let display_name = filename.map(|s| s.to_string());
        let (detection, wrapped) =
            crate::content_type::detect_and_wrap(stream, content_type, filename, known_length)
                .await;
        let ct = detection.content_type.clone();
        let expires_at = expires_at_for(created_at, ttl);
        let paste_token = self.config.paste_tokens.then(generate_paste_token);
        let paste_token_hash = paste_token
            .as_deref()
            .map(|token| sha256_hex(token.as_bytes()));
        let words = self.claim_alias(&key, expires_at).await?;
        // Metadata for the initial upload: filename, TTL, expiry, final
        // type and detection record. The hash and the final size are not
        // known yet. put_small adds both; for multipart, S3 stores the
        // size in ContentLength. S3 takes metadata only at
        // CreateMultipartUpload, so no later copy updates it.
        let (initial_meta, _) = metadata_for(
            display_name.as_deref(),
            None,
            None,
            Some(&words),
            paste_token_hash.as_deref(),
            ttl,
            created_at,
            fields.created_by.as_deref(),
            &detection,
            title.as_deref(),
            &tags,
        );
        let mut sink = AwsUploadSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.clone(),
            content_type: ct.clone(),
            metadata: initial_meta,
            metrics: self.metrics.clone(),
            active_uploads: Arc::clone(&self.active_uploads),
            current_upload: None,
        };
        let outcome =
            match crate::stream::run_streaming_upload(wrapped, &mut sink, known_length, max_bytes)
                .await
            {
                Ok(outcome) => outcome,
                Err(e) => {
                    self.cleanup_failed_store(&key, &words).await;
                    return Err(e);
                }
            };
        if let Err(e) = self.verify_alias(&words, &key).await {
            if Self::alias_is_lost(&e) {
                self.cleanup_failed_store(&key, &words).await;
            }
            return Err(e);
        }
        Ok(StoredPaste {
            id: format!("{date}/{random_id}"),
            key,
            words: Some(words),
            size: outcome.total_bytes,
            sha256: outcome.sha256,
            content_type: ct,
            filename: display_name,
            title,
            tags,
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
            paste_token,
        })
    }

    /// List every object under the segment prefix of `base_key`.
    ///
    /// The list is paginated. Keys that do not have the exact suffixed
    /// segment shape stay in the result: `delete` removes them too, so a
    /// stray key cannot keep storage forever. The read path uses
    /// [`segments_from_listing`], which ignores them.
    async fn list_segment_entries(&self, base_key: &str) -> Result<Vec<(String, u64)>, StoreError> {
        let prefix = segment_prefix(base_key);
        let mut entries = Vec::new();
        let mut token: Option<String> = None;
        let max_pages = MAX_SEGMENTS_READ.div_ceil(1000) + 1;
        for _ in 0..max_pages {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .max_keys(1000);
            if let Some(t) = token.take() {
                req = req.continuation_token(t);
            }
            let out = s3_call!(self.metrics, "list_objects_v2", req.send())
                .map_err(|e| map_sdk_error(e, base_key))?;
            for obj in out.contents() {
                if let Some(key) = obj.key() {
                    entries.push((
                        key.to_string(),
                        u64::try_from(obj.size().unwrap_or(0)).unwrap_or(0),
                    ));
                }
            }
            if entries.len() > MAX_SEGMENTS_READ {
                return Err(StoreError::TooManySegments {
                    limit: MAX_SEGMENTS_READ,
                });
            }
            match out.next_continuation_token() {
                Some(t) if out.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
                _ => return Ok(entries),
            }
        }
        // The page limit is reached while the listing is still truncated.
        Err(StoreError::TooManySegments {
            limit: MAX_SEGMENTS_READ,
        })
    }

    /// Build the base-plus-segments plan for a known base key and size.
    async fn load_plan(&self, base_key: &str, base_size: u64) -> Result<SegmentPlan, StoreError> {
        let entries = self.list_segment_entries(base_key).await?;
        let segments = segments_from_listing(base_key, &entries);
        Ok(SegmentPlan::new(base_key.to_string(), base_size, segments))
    }

    /// Append text to an existing paste as one new segment object.
    ///
    /// The text cap applies to this append only, not to the total size.
    /// The expiry check happens before the write, so an expired or
    /// missing id creates no object. With `retention_by_size` on, an
    /// append that crosses a curve boundary shortens the expiry. The base
    /// object and the earlier segments are then rewritten with the new
    /// expiry.
    ///
    /// Sequence assignment lists the segment prefix and uses max + 1.
    /// Two concurrent appends to the same id can choose the same
    /// sequence. A random suffix in the segment name keeps the object
    /// keys distinct, so both appends survive. The store does not rely on
    /// a conditional write, because Garage ignores `If-None-Match`.
    pub async fn append_text(&self, id: &str, text: &[u8]) -> Result<AppendResult, StoreError> {
        if u64::try_from(text.len()).unwrap_or(u64::MAX) > self.config.max_text_bytes {
            return Err(StoreError::TooLarge {
                limit: self.config.max_text_bytes,
            });
        }
        let mut meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        if text.is_empty() {
            return Ok(AppendResult {
                appended_bytes: 0,
                segment_key: None,
                meta,
            });
        }
        let appended_bytes = text.len() as u64;
        let new_size = meta.size.saturating_add(appended_bytes);
        if new_size > self.config.max_paste_bytes {
            return Err(StoreError::TooLarge {
                limit: self.config.max_paste_bytes,
            });
        }
        if meta.segments.len() >= MAX_SEGMENTS_PER_PASTE {
            return Err(StoreError::TooManySegments {
                limit: MAX_SEGMENTS_PER_PASTE,
            });
        }
        let previous_segments = meta.segments.len();
        let now = Utc::now();
        let mut expires_at = meta.expires_at;
        let mut shortened_ttl = None;
        if self.config.retention_by_size {
            if let (Some(created_at), Some(current)) = (meta.created_at, meta.expires_at) {
                let curve_ttl = Ttl::seconds(self.config.retention_seconds(new_size));
                let curve_expires = expires_at_for(created_at, curve_ttl);
                if curve_expires < current {
                    if curve_expires <= now {
                        return Err(StoreError::Expired(meta.id.clone()));
                    }
                    expires_at = Some(curve_expires);
                    shortened_ttl = Some(curve_ttl);
                }
            }
        }
        let sequence = match meta.segments.iter().map(|s| s.sequence).max() {
            Some(max) => max
                .checked_add(1)
                .ok_or_else(|| StoreError::S3("segment sequence exhausted".into()))?,
            None => 0,
        };
        let suffix = generate_segment_suffix();
        let key = segment_key(&meta.key, sequence, &suffix);
        let mut object_meta = HashMap::new();
        object_meta.insert("sequence".to_string(), sequence.to_string());
        object_meta.insert("size".to_string(), text.len().to_string());
        object_meta.insert("created-at".to_string(), now.to_rfc3339());
        if let Some(expires_at) = expires_at {
            // The append writes the current effective expiry. It does not
            // extend it.
            object_meta.insert("expires-at".to_string(), expires_at.to_rfc3339());
        }
        // A shortening append writes the base first, then the earlier
        // segments, and the new segment last. A failure before the last
        // write leaves no new content, so a retry does not duplicate the
        // text. The base is already at the shorter expiry, so a partial
        // failure cannot leave the paste readable under the old expiry.
        if let (Some(ttl), Some(new_expiry)) = (shortened_ttl, expires_at) {
            self.rewrite_expiry_step("base", &meta.key, new_expiry, ttl)
                .await?;
            let mut first_error = None;
            for segment in &meta.segments[..previous_segments] {
                if let Err(error) = self
                    .rewrite_expiry_step("segment", &segment.key, new_expiry, ttl)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
        }
        if let Err(error) = s3_call!(
            self.metrics,
            "put_object",
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(text.to_vec()))
                .content_type(meta.content_type.clone())
                .set_metadata(Some(object_meta))
                .send()
        ) {
            tracing::warn!(key = %key, error = ?error, "append segment write failed");
            return Err(StoreError::PartialUpdate {
                step: "new segment",
                key,
            });
        }
        meta.segments.push(Segment {
            sequence,
            suffix,
            key: key.clone(),
            size: appended_bytes,
        });
        meta.size = new_size;
        meta.sha256 = None;
        // A delete can remove the base after the first head and before
        // this segment write. Check the base again. If it is gone, remove
        // the segment, so an in-flight delete does not leave an orphan.
        if let Err(error) = s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&meta.key)
                .send()
        ) {
            let mapped = map_sdk_error(error, &meta.key);
            let mut removed = false;
            for _ in 0..MULTI_STEP_ATTEMPTS {
                if self.delete_object_key(&key).await.is_ok() {
                    removed = true;
                    break;
                }
            }
            if !removed {
                tracing::warn!(
                    key = %key,
                    "cannot remove the segment after the base disappeared"
                );
            }
            return Err(mapped);
        }
        meta.expires_at = expires_at;
        meta.expired = false;
        Ok(AppendResult {
            appended_bytes,
            segment_key: Some(key),
            meta,
        })
    }

    pub async fn head_metadata(&self, id: &str) -> Result<PasteMetadata, StoreError> {
        let key = self.resolve_key(id).await?;
        self.head_metadata_by_key(&key, id).await
    }

    /// Read metadata and reject an expired paste.
    ///
    /// The HTTP metadata route and the MCP `fetch_metadata` tool share
    /// this method, so both report the same error class for an expired
    /// paste. [`Self::head_metadata`] stays available for callers that
    /// need to inspect an expired object, such as set-TTL.
    pub async fn head_metadata_live(&self, id: &str) -> Result<PasteMetadata, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        Ok(meta)
    }

    /// True when `token` matches the stored SHA-256 for `id`.
    ///
    /// A missing paste, a paste without a token, or an S3 error returns
    /// false. The caller decides the HTTP status. This method never
    /// returns or logs the stored hash.
    pub async fn verify_paste_token(&self, id: &str, token: &str) -> bool {
        let Ok(key) = self.resolve_key(id).await else {
            return false;
        };
        let Ok(out) = s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
        ) else {
            return false;
        };
        let Some(expected) = out
            .metadata()
            .and_then(|meta| meta.get("paste-token-sha256"))
        else {
            return false;
        };
        let actual = sha256_hex(token.as_bytes());
        expected.as_bytes().ct_eq(actual.as_bytes()).into()
    }

    /// Read metadata for a canonical key without another alias lookup.
    async fn head_metadata_by_key(&self, key: &str, id: &str) -> Result<PasteMetadata, StoreError> {
        self.head_metadata_by_key_with_segments(key, id, None).await
    }

    /// Read metadata for a canonical key with segment entries supplied by
    /// the caller.
    ///
    /// `None` loads the segment prefix. `Some` uses the given entries, so a
    /// listing can reuse the keys it already scanned instead of issuing one
    /// prefix listing per paste.
    async fn head_metadata_by_key_with_segments(
        &self,
        key: &str,
        id: &str,
        segments: Option<&[(String, u64)]>,
    ) -> Result<PasteMetadata, StoreError> {
        let out = s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
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
        let base_size = u64::try_from(out.content_length().unwrap_or(0)).unwrap_or(0);
        let plan = match segments {
            Some(entries) => {
                if entries.len() > MAX_SEGMENTS_READ {
                    return Err(StoreError::TooManySegments {
                        limit: MAX_SEGMENTS_READ,
                    });
                }
                SegmentPlan::new(
                    key.to_string(),
                    base_size,
                    segments_from_listing(key, entries),
                )
            }
            None => self.load_plan(key, base_size).await?,
        };
        let has_segments = !plan.segments.is_empty();
        Ok(PasteMetadata {
            id: key.strip_prefix("pastes/").unwrap_or(key).to_string(),
            key: key.to_string(),
            words: meta
                .get("words")
                .and_then(|words| self.normalize_alias(words)),
            filename: meta.get("filename").map(|s| decode_filename(s)),
            title: meta.get("title").map(|s| decode_filename(s)),
            tags: meta
                .get("tags")
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default(),
            content_type,
            detection_source: meta.get("detection-source").cloned(),
            detection_conflict: meta.get("detection-conflict").cloned(),
            size: plan.total_size(),
            base_size,
            segments: plan.segments,
            // A stored hash covers the base object only. Do not report it
            // as if it covered the concatenated paste.
            sha256: if has_segments {
                None
            } else {
                meta.get("sha256").cloned()
            },
            created_at,
            expires_at,
            created_by: meta.get("created-by").cloned(),
            expired,
        })
    }

    fn check_not_expired(meta: &PasteMetadata) -> Result<(), StoreError> {
        if meta.expired {
            return Err(StoreError::Expired(meta.id.clone()));
        }
        Ok(())
    }

    /// Reject a paste that has one object above the `CopyObject` limit.
    ///
    /// The check runs before any rewrite, so a failed TTL change changes
    /// no object.
    fn check_copy_limit(meta: &PasteMetadata) -> Result<(), StoreError> {
        let oversized = std::iter::once(meta.base_size)
            .chain(meta.segments.iter().map(|segment| segment.size))
            .any(copy_object_too_large);
        if oversized {
            return Err(StoreError::CopyLimit {
                limit: COPY_OBJECT_MAX_BYTES,
            });
        }
        Ok(())
    }

    /// Replace the expiry metadata on one object, preserving its body and
    /// all other metadata. A server-side copy avoids downloading the object.
    async fn rewrite_expiry(
        &self,
        key: &str,
        expires_at: DateTime<Utc>,
        ttl: Ttl,
    ) -> Result<(), StoreError> {
        let out = s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, key))?;
        let size = u64::try_from(out.content_length().unwrap_or(0)).unwrap_or(0);
        if copy_object_too_large(size) {
            return Err(StoreError::CopyLimit {
                limit: COPY_OBJECT_MAX_BYTES,
            });
        }
        let mut meta = out.metadata().cloned().unwrap_or_default();
        meta.insert("expires-at".to_string(), expires_at.to_rfc3339());
        meta.insert("ttl-seconds".to_string(), ttl.num_seconds().to_string());
        if let Some(days) = ttl.whole_days() {
            meta.insert("ttl-days".to_string(), days.to_string());
        } else {
            meta.remove("ttl-days");
        }
        let content_type = out
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_string();
        s3_call!(
            self.metrics,
            "copy_object",
            self.client
                .copy_object()
                .bucket(&self.bucket)
                .key(key)
                .copy_source(format!("{}/{}", self.bucket, key))
                .metadata_directive(MetadataDirective::Replace)
                .content_type(content_type)
                .set_metadata(Some(meta))
                .send()
        )
        .map_err(|e| map_sdk_error(e, key))?;
        Ok(())
    }

    /// Rewrite one object's expiry and retry a transient failure.
    ///
    /// The function returns [`StoreError::PartialUpdate`] when every
    /// attempt fails. The message names the step and the object key. The
    /// base object is the read-time authority, so the caller writes it in
    /// the order that keeps the safe expiry.
    async fn rewrite_expiry_step(
        &self,
        step: &'static str,
        key: &str,
        expires_at: DateTime<Utc>,
        ttl: Ttl,
    ) -> Result<(), StoreError> {
        let mut last_error = None;
        for _ in 0..MULTI_STEP_ATTEMPTS {
            match self.rewrite_expiry(key, expires_at, ttl).await {
                Ok(()) => return Ok(()),
                Err(error) => last_error = Some(error),
            }
        }
        Err(partial_update_error(
            step,
            key,
            last_error.unwrap_or_else(|| StoreError::S3("multi-object update failed".to_string())),
        ))
    }

    /// Change the expiry of an existing paste, every segment, and the alias.
    ///
    /// The new expiry is computed from now, so an extension is real. With
    /// `retention_by_size` on, the new expiry cannot go past the curve
    /// value for the current total size. An expired paste is rejected
    /// before any object is rewritten.
    pub async fn set_ttl(&self, id: &str, ttl: Ttl) -> Result<PasteMetadata, StoreError> {
        let ttl = self.validate_ttl(ttl)?;
        let mut meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        Self::check_copy_limit(&meta)?;
        let now = Utc::now();
        let mut expires_at = expires_at_for(now, ttl);
        if self.config.retention_by_size {
            if let Some(created_at) = meta.created_at {
                let curve_expires = expires_at_for(
                    created_at,
                    Ttl::seconds(self.config.retention_seconds(meta.size)),
                );
                if curve_expires < expires_at {
                    expires_at = curve_expires;
                }
            }
        }
        if expires_at <= now {
            return Err(StoreError::Expired(meta.id.clone()));
        }
        let ttl = Ttl::seconds((expires_at - now).num_seconds());
        let shortening = meta.expires_at.is_some_and(|current| expires_at < current);
        let mut first_error = None;
        if shortening {
            // The base is the read-time authority. Write it first, so a
            // later failure cannot leave the old, longer expiry in force.
            self.rewrite_expiry_step("base", &meta.key, expires_at, ttl)
                .await?;
            for segment in &meta.segments {
                if let Err(error) = self
                    .rewrite_expiry_step("segment", &segment.key, expires_at, ttl)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
            if let Some(words) = &meta.words {
                let key = alias_key(words);
                if let Err(error) = self
                    .rewrite_expiry_step("alias", &key, expires_at, ttl)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
        } else {
            // Keep the old base expiry until every other object is new.
            for segment in &meta.segments {
                if let Err(error) = self
                    .rewrite_expiry_step("segment", &segment.key, expires_at, ttl)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
            if let Some(words) = &meta.words {
                let key = alias_key(words);
                if let Err(error) = self
                    .rewrite_expiry_step("alias", &key, expires_at, ttl)
                    .await
                {
                    first_error.get_or_insert(error);
                }
            }
            if first_error.is_none() {
                self.rewrite_expiry_step("base", &meta.key, expires_at, ttl)
                    .await?;
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        meta.expires_at = Some(expires_at);
        meta.expired = false;
        Ok(meta)
    }

    /// Fetch one object's byte range. This is the only method that issues
    /// a GET against a single key.
    async fn fetch_object_range(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, StoreError> {
        let mut req = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some((start, end)) = range {
            req = req.range(format!("bytes={start}-{end}"));
        }
        let out =
            s3_call!(self.metrics, "get_object", req.send()).map_err(|e| map_sdk_error(e, key))?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| s3_failure("cannot read the object body", e))?
            .to_vec();
        Ok(bytes)
    }

    /// Fetch an inclusive global byte range from a paste.
    ///
    /// The plan maps the range to the base object and to the segments
    /// that hold it. A segment that the range does not touch is not
    /// read, and a touched object gets one range request for the overlap
    /// only.
    pub async fn fetch_range_with_plan(
        &self,
        plan: &SegmentPlan,
        start: u64,
        end: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let mut bytes = Vec::new();
        for (key, local_start, local_end) in plan.object_ranges(start, end) {
            let part = self
                .fetch_object_range(&key, Some((local_start, local_end)))
                .await?;
            bytes.extend_from_slice(&part);
        }
        Ok(bytes)
    }

    /// Fetch a byte range from an already-resolved base key.
    ///
    /// Callers that already hold metadata should use
    /// [`Self::fetch_range_with_plan`] rather than resolving the key
    /// again, because this method issues a `HeadObject` and a segment
    /// listing before the read. Downloading in chunks through the ID-based
    /// path re-ran that once per chunk, so a 64 MiB download cost nine
    /// prefix listings and nine heads.
    ///
    /// The binary keeps this method for callers that hold a key but not
    /// metadata. The HTTP path uses the plan form to avoid the extra
    /// calls. The store tests use this method to check that a range is
    /// concatenated across the base object and its segments.
    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn fetch_range_by_key(
        &self,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Vec<u8>, StoreError> {
        let out = s3_call!(
            self.metrics,
            "head_object",
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
        )
        .map_err(|e| map_sdk_error(e, key))?;
        let base_size = u64::try_from(out.content_length().unwrap_or(0)).unwrap_or(0);
        let plan = self.load_plan(key, base_size).await?;
        match range {
            Some((start, end)) => self.fetch_range_with_plan(&plan, start, end).await,
            None => {
                self.fetch_range_with_plan(&plan, 0, plan.total_size().saturating_sub(1))
                    .await
            }
        }
    }

    pub async fn fetch_text(
        &self,
        id: &str,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<TextSlice, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let limit = Some(
            limit
                .unwrap_or(MAX_FETCH_TEXT_BYTES)
                .min(MAX_FETCH_TEXT_BYTES),
        );
        let Some((start, end_exclusive)) = offset_limit_window(offset, limit, meta.size) else {
            return Ok(TextSlice {
                text: String::new(),
                total_bytes: meta.size,
                returned_bytes: 0,
                truncated: false,
            });
        };
        let bytes = self
            .fetch_range_with_plan(&meta.plan(), start, end_exclusive - 1)
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

    /// Search one text paste for a literal string or a regular expression.
    ///
    /// The search reads at most [`Config::search_max_bytes`] from
    /// `after_offset`. It scans the base object and every segment through
    /// the existing segment plan. A line is the unit of a hit. The scan is
    /// O(bytes read), and it uses no index.
    pub async fn search_text(
        &self,
        id: &str,
        query: &str,
        options: SearchOptions,
    ) -> Result<SearchResult, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        if !is_text_content_type(&meta.content_type) {
            return Err(StoreError::NotText(meta.content_type));
        }
        if query.is_empty() {
            return Err(StoreError::InvalidSearch("query is empty".to_string()));
        }
        if query.len() > MAX_SEARCH_QUERY_BYTES {
            return Err(StoreError::InvalidSearch(format!(
                "query is longer than {MAX_SEARCH_QUERY_BYTES} bytes"
            )));
        }
        let pattern = if options.regex {
            query.to_string()
        } else {
            regex::escape(query)
        };
        let re = regex::bytes::RegexBuilder::new(&pattern)
            .case_insensitive(options.ignore_case)
            .size_limit(1 << 20)
            .build()
            .map_err(|e| StoreError::InvalidSearch(e.to_string()))?;
        let context_lines = options.context_lines.min(MAX_SEARCH_CONTEXT_LINES);
        let max_matches = options.max_matches.clamp(1, MAX_SEARCH_MATCHES);
        let total = meta.size;
        let after = options.after_offset.min(total);
        let scan_end = total.min(after.saturating_add(self.config.search_max_bytes));
        let prefix_newlines = self.count_newlines(&meta.plan(), 0, after).await?;
        let bytes = if after < scan_end {
            self.fetch_range_with_plan(&meta.plan(), after, scan_end - 1)
                .await?
        } else {
            Vec::new()
        };
        let lines = split_search_lines(&bytes, after);
        let mut hits = Vec::new();
        let mut truncated = false;
        let mut last_line_end = scan_end;
        'lines: for (index, line) in lines.iter().enumerate() {
            if hits.len() >= max_matches {
                truncated = true;
                break;
            }
            let matches: Vec<(usize, usize)> = re
                .find_iter(line.text)
                .map(|m| (m.start(), m.end()))
                .collect();
            for (start, end) in matches {
                if hits.len() >= max_matches {
                    truncated = true;
                    break 'lines;
                }
                let (preview, ranges) =
                    build_search_preview(&lines, index, start, end, context_lines);
                hits.push(SearchHit {
                    line: prefix_newlines + 1 + index as u64,
                    byte_offset: line.start,
                    preview,
                    ranges,
                });
                last_line_end = line.end;
            }
        }
        if scan_end < total {
            truncated = true;
        }
        let next_offset = if truncated {
            Some(if hits.is_empty() {
                scan_end
            } else {
                last_line_end
            })
        } else {
            None
        };
        Ok(SearchResult {
            hits,
            scanned_bytes: scan_end.saturating_sub(after),
            truncated,
            next_offset,
        })
    }

    /// Read the last `tail_bytes` of a paste.
    pub async fn fetch_text_tail(
        &self,
        id: &str,
        tail_bytes: u64,
    ) -> Result<TextSlice, StoreError> {
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let start = meta.size.saturating_sub(tail_bytes);
        self.fetch_text(id, start, Some(tail_bytes)).await
    }

    /// Read a 1-based inclusive line range from a paste.
    ///
    /// The method scans the paste to find the byte offsets of the lines.
    /// The returned slice is still capped by [`MAX_FETCH_TEXT_BYTES`].
    pub async fn fetch_text_lines(
        &self,
        id: &str,
        line_start: u64,
        line_end: u64,
    ) -> Result<TextSlice, StoreError> {
        if line_start == 0 || line_end < line_start {
            return Err(StoreError::InvalidSearch(
                "line_start must be at least 1 and line_end must be at least line_start"
                    .to_string(),
            ));
        }
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let (start, end) = self
            .line_range_offsets(&meta.plan(), meta.size, line_start, line_end)
            .await?;
        self.fetch_text(id, start, Some(end.saturating_sub(start)))
            .await
    }

    /// Count newlines in `start..end`.
    async fn count_newlines(
        &self,
        plan: &SegmentPlan,
        start: u64,
        end: u64,
    ) -> Result<u64, StoreError> {
        let mut count = 0u64;
        let mut cur = start;
        while cur < end {
            let chunk_end = (cur + MULTIPART_CHUNK_BYTES as u64 - 1).min(end - 1);
            let bytes = self.fetch_range_with_plan(plan, cur, chunk_end).await?;
            count += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
            cur = chunk_end + 1;
        }
        Ok(count)
    }

    /// Find the byte offsets of an inclusive line range.
    async fn line_range_offsets(
        &self,
        plan: &SegmentPlan,
        total: u64,
        line_start: u64,
        line_end: u64,
    ) -> Result<(u64, u64), StoreError> {
        let mut line = 1u64;
        let mut start = 0u64;
        let mut wanted_start = None;
        let mut cur = 0u64;
        while cur < total {
            let chunk_end = (cur + MULTIPART_CHUNK_BYTES as u64 - 1).min(total - 1);
            let bytes = self.fetch_range_with_plan(plan, cur, chunk_end).await?;
            for (index, byte) in bytes.iter().enumerate() {
                if *byte != b'\n' {
                    continue;
                }
                let end = cur + index as u64 + 1;
                if line == line_start {
                    wanted_start = Some(start);
                }
                if line == line_end {
                    return Ok((wanted_start.unwrap_or(start), end));
                }
                line += 1;
                start = end;
            }
            cur = chunk_end + 1;
        }
        if line == line_start {
            wanted_start = Some(start);
        }
        if let Some(wanted) = wanted_start {
            if line_end >= line {
                return Ok((wanted, total));
            }
        }
        Ok((total, total))
    }

    /// Download a paste to a local file in 8 MiB range chunks, so the
    /// whole paste is never held in memory. `range` is an inclusive
    /// (start, end) pair; None means the whole paste.
    ///
    /// The plan keeps each read inside one object, so a segment is never
    /// fetched as part of the base object.
    pub async fn download_file(
        &self,
        id: &str,
        dest: &Path,
        range: Option<(u64, u64)>,
    ) -> Result<PasteMetadata, StoreError> {
        if let Some((start, end)) = range {
            if start > end {
                return Err(StoreError::InvalidRange(
                    "range end before start".to_string(),
                ));
            }
        }
        let meta = self.head_metadata(id).await?;
        Self::check_not_expired(&meta)?;
        let (start, end) = match range {
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
        let plan = meta.plan();
        let mut file = File::create(dest).await?;
        for (key, local_start, local_end) in plan.object_ranges(start, end) {
            let mut cur = local_start;
            while cur <= local_end {
                let chunk_end = (cur + MULTIPART_CHUNK_BYTES as u64 - 1).min(local_end);
                let bytes = self
                    .fetch_object_range(&key, Some((cur, chunk_end)))
                    .await?;
                if bytes.is_empty() {
                    break;
                }
                file.write_all(&bytes).await?;
                cur = chunk_end + 1;
            }
        }
        file.flush().await?;
        Ok(meta)
    }

    /// Delete the base object, every object under its segment prefix, and
    /// the alias object.
    ///
    /// The input can be a canonical ID or a word alias. The prefix
    /// listing runs first, so a listing failure leaves the paste intact
    /// rather than deleting only the base object.
    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        if let Some(phrase) = self.normalize_alias(id) {
            return self.delete_by_words(&phrase).await;
        }
        let key = self.resolve_key(id).await?;
        let words = self.base_words(&key).await?;
        self.delete_base_and_segments(&key).await?;
        if let Some(words) = words {
            self.delete_alias_if_target(&words, &key)
                .await
                .map_err(|error| partial_update_error("alias delete", &alias_key(&words), error))?;
        }
        Ok(())
    }

    /// List one page of current pastes for an inclusive date window.
    ///
    /// No dates select the default window: the last
    /// `DAYFLY_LIST_DEFAULT_DAYS` days, ending today in UTC. A
    /// date for [`list_recent`](Self::list_recent) means one day.
    /// [`parse_list_window_request`] documents the range rules.
    ///
    /// The scan starts at the newest date and moves to older dates. The
    /// `limit` applies to the newest dates. Within one date, the limit
    /// applies in S3 key order. Paste IDs are random, so key order is not
    /// creation order. An older date can be omitted when the limit is
    /// reached. The returned summaries are sorted by date ascending, then
    /// by key.
    ///
    /// The function scans the bucket directly: one `ListObjectsV2` for each
    /// `pastes/<date>/` prefix. That scan returns base keys and segment
    /// keys, so the function groups segments by base key without another
    /// prefix listing. One `HeadObject` per paste recovers expiry,
    /// content-type, filename, and metadata. The head calls use bounded
    /// concurrency, and the result keeps key order. A date scan stops when
    /// the page holds enough base keys and all their segments. Segment
    /// keys are skipped, so a segment never appears as a paste. Each
    /// prefix follows continuation tokens for up to
    /// [`LIST_SCAN_MAX_PAGES`] pages. A window can span at most
    /// [`LIST_WINDOW_MAX_DAYS`] dates. The listing is O(n) in the objects
    /// scanned (plus one head per paste): it is fast for a small internal
    /// bucket and gets slower as the bucket grows. That slowdown is
    /// exactly the trigger for adding a database later — it is documented
    /// in README, not hidden, and no database schema is invented in v1.
    /// This is not search.
    ///
    /// A cursor resumes after the last key of the previous page. The
    /// cursor is stateless: `date` names the prefix, and `start_after`
    /// is passed to `ListObjectsV2`. The next page may be empty when the
    /// previous page ended exactly at the end of the bucket.
    pub async fn list_window_page(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
        cursor: Option<&ListCursor>,
    ) -> Result<ListPage, StoreError> {
        let request = parse_list_window_request(None, start, end)?;
        let (start_date, end_date) =
            resolve_list_window(&request, Utc::now(), self.config.list_default_days)?;
        let limit = limit.clamp(1, 1000);
        let mut cursor_date = None;
        if let Some(cursor) = cursor {
            let date = NaiveDate::parse_from_str(&cursor.date, "%Y-%m-%d").map_err(|_| {
                StoreError::InvalidCursor(format!("date must be YYYY-MM-DD: {}", cursor.date))
            })?;
            if date < start_date || date > end_date {
                return Err(StoreError::InvalidCursor(format!(
                    "date {} is outside the listing window",
                    cursor.date
                )));
            }
            let prefix = format!("pastes/{}/", cursor.date);
            if !cursor.start_after.starts_with(&prefix) {
                return Err(StoreError::InvalidCursor(format!(
                    "key {} does not belong to date {}",
                    cursor.start_after, cursor.date
                )));
            }
            cursor_date = Some(date);
        }
        let mut summaries = Vec::new();
        let mut last_included: Option<ListCursor> = None;
        let dates: Vec<NaiveDate> = inclusive_dates(start_date, end_date).collect();
        for date in dates.into_iter().rev() {
            if summaries.len() >= limit {
                break;
            }
            if cursor_date.is_some_and(|cursor| date > cursor) {
                continue;
            }
            let date_str = date.format("%Y-%m-%d").to_string();
            let prefix = format!("pastes/{date_str}/");
            let mut token: Option<String> = None;
            let mut start_after = cursor
                .filter(|cursor| cursor.date == date_str)
                .map(|cursor| cursor.start_after.clone());
            let mut bases: Vec<(String, Option<DateTime<Utc>>)> = Vec::new();
            let mut segment_map: HashMap<String, Vec<(String, u64)>> = HashMap::new();
            let mut hydrated = 0usize;
            let mut open_base: Option<String> = None;
            let mut incomplete_base: Option<String> = None;
            let mut pages = 0usize;
            let mut listing_done = false;
            loop {
                let needed = limit - summaries.len();
                // Collect base keys and their segment keys. A later base
                // closes the segment group of the previous base. The scan
                // stops as soon as the page has enough closed groups.
                while !listing_done {
                    let needed_end = hydrated + needed;
                    if bases.len() > needed_end
                        || (bases.len() == needed_end && open_base.is_none())
                    {
                        break;
                    }
                    if pages >= LIST_SCAN_MAX_PAGES {
                        listing_done = true;
                        // A prefix cap can cut a segment group in the middle.
                        // Let the head fall back to a segment-prefix listing.
                        incomplete_base = open_base.clone();
                        break;
                    }
                    let mut req = self
                        .client
                        .list_objects_v2()
                        .bucket(&self.bucket)
                        .prefix(&prefix)
                        .max_keys(1000);
                    if let Some(t) = token.as_deref() {
                        req = req.continuation_token(t);
                    } else if let Some(key) = start_after.take() {
                        req = req.start_after(key);
                    }
                    let out = s3_call!(self.metrics, "list_objects_v2", req.send())
                        .map_err(|e| map_sdk_error(e, &date_str))?;
                    pages += 1;
                    for obj in out.contents() {
                        let Some(key) = obj.key() else { continue };
                        if is_paste_key(key) {
                            let last_modified = obj.last_modified().and_then(|dt| {
                                DateTime::from_timestamp(dt.secs(), dt.subsec_nanos())
                            });
                            open_base = Some(key.to_string());
                            bases.push((key.to_string(), last_modified));
                            continue;
                        }
                        if !key.starts_with("pastes/") {
                            continue;
                        }
                        if let Some((base, _)) = key.rsplit_once('/') {
                            let entries = segment_map.entry(base.to_string()).or_default();
                            entries.push((
                                key.to_string(),
                                u64::try_from(obj.size().unwrap_or(0)).unwrap_or(0),
                            ));
                            if entries.len() > MAX_SEGMENTS_READ {
                                return Err(StoreError::TooManySegments {
                                    limit: MAX_SEGMENTS_READ,
                                });
                            }
                        }
                        let closes_open_base = open_base.as_deref().is_some_and(|base| {
                            !key.starts_with(base) || key.as_bytes().get(base.len()) != Some(&b'/')
                        });
                        if closes_open_base {
                            open_base = None;
                        }
                    }
                    let page_truncated = out.is_truncated().unwrap_or(false);
                    token = match (page_truncated, out.next_continuation_token()) {
                        (true, Some(t)) => Some(t.to_string()),
                        _ => None,
                    };
                    if token.is_none() {
                        listing_done = true;
                    }
                }
                // Read the next batch of base metadata. `buffered` keeps
                // the key order while it runs up to the concurrency limit.
                let chunk_end = (hydrated + needed).min(bases.len());
                if chunk_end > hydrated {
                    let chunk = bases[hydrated..chunk_end].to_vec();
                    hydrated = chunk_end;
                    let incomplete_base = incomplete_base.as_deref();
                    let hydrated: Vec<Result<(String, Option<PasteSummary>), StoreError>> =
                        futures::stream::iter(chunk)
                            .map(|(key, last_modified)| {
                                let segment_map = &segment_map;
                                async move {
                                    let id =
                                        key.strip_prefix("pastes/").unwrap_or(&key).to_string();
                                    let entries = if incomplete_base == Some(key.as_str()) {
                                        None
                                    } else {
                                        Some(
                                            segment_map.get(&key).map(Vec::as_slice).unwrap_or(&[]),
                                        )
                                    };
                                    // HeadObject per key: ListObjectsV2 returns
                                    // no user metadata. An object deleted
                                    // between list and head is skipped. Any
                                    // other error fails the listing, so an S3
                                    // outage does not look like an empty bucket.
                                    let meta = match self
                                        .head_metadata_by_key_with_segments(&key, &id, entries)
                                        .await
                                    {
                                        Ok(meta) => meta,
                                        Err(StoreError::NotFound(_)) => {
                                            return Ok((key, None));
                                        }
                                        Err(e) => return Err(e),
                                    };
                                    if meta.expired {
                                        return Ok((key, None));
                                    }
                                    Ok((
                                        key,
                                        Some(PasteSummary {
                                            id,
                                            key: meta.key,
                                            words: meta.words,
                                            title: meta.title,
                                            tags: meta.tags,
                                            size: meta.size,
                                            last_modified,
                                            expires_at: meta.expires_at,
                                            created_at: meta.created_at,
                                            content_type: Some(meta.content_type),
                                            filename: meta.filename,
                                        }),
                                    ))
                                }
                            })
                            .buffered(LIST_HEAD_CONCURRENCY)
                            .collect()
                            .await;
                    for result in hydrated {
                        let (key, summary) = result?;
                        let Some(summary) = summary else { continue };
                        summaries.push(summary);
                        last_included = Some(ListCursor {
                            date: date_str.clone(),
                            start_after: key,
                        });
                        if summaries.len() >= limit {
                            break;
                        }
                    }
                }
                if summaries.len() >= limit {
                    break;
                }
                if listing_done && hydrated >= bases.len() {
                    break;
                }
            }
        }
        summaries.sort_by(|a, b| {
            let a_date = a.id.split_once('/').map(|(date, _)| date).unwrap_or("");
            let b_date = b.id.split_once('/').map(|(date, _)| date).unwrap_or("");
            a_date.cmp(b_date).then_with(|| a.key.cmp(&b.key))
        });
        let truncated = summaries.len() >= limit;
        let next = if truncated { last_included } else { None };
        Ok(ListPage {
            items: summaries,
            truncated,
            next,
        })
    }

    /// Compatibility wrapper that returns only the page items.
    pub async fn list_window(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PasteSummary>, StoreError> {
        Ok(self.list_window_page(start, end, limit, None).await?.items)
    }

    /// One page of recent pastes. A date means one day. No date
    /// delegates to the default window of
    /// [`list_window_page`](Self::list_window_page).
    pub async fn list_recent_page(
        &self,
        date: Option<&str>,
        limit: usize,
        cursor: Option<&ListCursor>,
    ) -> Result<ListPage, StoreError> {
        match date {
            Some(date) => {
                self.list_window_page(Some(date), Some(date), limit, cursor)
                    .await
            }
            None => self.list_window_page(None, None, limit, cursor).await,
        }
    }

    /// Compatibility wrapper. A date means one day. No date delegates to the
    /// default window of [`list_window`](Self::list_window).
    pub async fn list_recent(
        &self,
        date: Option<&str>,
        limit: usize,
    ) -> Result<Vec<PasteSummary>, StoreError> {
        Ok(self.list_recent_page(date, limit, None).await?.items)
    }
}

/// Default listing window: `days` UTC dates that end today.
///
/// The key layout uses UTC dates. At 20:00 Pacific the UTC clock is already
/// on the next day, so the pastes written during that Pacific evening use
/// two UTC date prefixes. The default of two days covers the full Pacific
/// day.
pub fn default_list_window(now: DateTime<Utc>, days: u32) -> (NaiveDate, NaiveDate) {
    let today = now.date_naive();
    let days = i64::from(days.max(1));
    (today - chrono::Duration::days(days - 1), today)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn garage_quota_errors_are_recognised() {
        assert!(is_quota_error(
            Some("Forbidden"),
            Some(
                "Bucket size quota is reached, maximum total size of objects for this bucket: 1000. \
                 The bucket is already 900 bytes, and this object would add 200 bytes."
            )
        ));
        assert!(is_quota_error(
            Some("Forbidden"),
            Some("Object quota is reached, maximum objects for this bucket: 10")
        ));
    }

    #[test]
    fn other_forbidden_errors_are_not_quota_errors() {
        assert!(!is_quota_error(
            Some("Forbidden"),
            Some("Operation is not allowed for this key.")
        ));
        assert!(!is_quota_error(
            Some("NoSuchKey"),
            Some("The specified key does not exist.")
        ));
        assert!(!is_quota_error(None, None));
    }

    #[test]
    fn a_quota_exceeded_code_is_quota_with_any_message() {
        assert!(is_quota_error(
            Some("QuotaExceeded"),
            Some("the bucket is full")
        ));
        assert!(is_quota_error(Some("QuotaExceeded"), None));
    }

    #[test]
    fn the_copy_object_limit_matches_the_s3_limit() {
        assert!(!copy_object_too_large(COPY_OBJECT_MAX_BYTES));
        assert!(copy_object_too_large(COPY_OBJECT_MAX_BYTES + 1));
    }

    #[tokio::test]
    async fn hash_file_prefix_stops_one_byte_past_the_expected_length() {
        let data = vec![b'x'; 64];
        let mut input: &[u8] = &data;

        let error = hash_file_prefix(&mut input, 32)
            .await
            .expect_err("a grown file must be rejected");
        assert!(
            error.to_string().contains("expected 32 bytes, read 33"),
            "got: {error}"
        );
        assert_eq!(
            64 - input.len(),
            33,
            "the hash must stop at len + 1, not read the whole file"
        );
    }

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
        assert!(
            validate_id("2026-10-28/abcXYZ0123/00000000000000000000").is_err(),
            "a segment key must not be a paste id"
        );
    }

    #[test]
    fn wordlist_has_7776_lowercase_words() {
        let words = wordlist();
        assert_eq!(words.len(), 7_776);
        let unique: HashSet<&str> = words.iter().copied().collect();
        assert_eq!(unique.len(), 7_776, "wordlist entries must be unique");
        for word in words {
            assert!(!word.is_empty(), "wordlist entries must not be empty");
            assert!(
                word.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
                "wordlist entry must be lowercase ASCII: {word:?}"
            );
        }
    }

    #[test]
    fn word_phrase_normalization_accepts_case_spaces_and_hyphens() {
        assert_eq!(
            normalize_words(" Abacus Abdomen-Zoology ").as_deref(),
            Some("abacus-abdomen-zoology")
        );
        assert!(is_word_phrase("abacus abdomen zoology"));
        assert!(is_word_phrase("ABACUS-ABDOMEN-ZOOLOGY"));
        assert!(is_word_phrase("drop-down abacus zoology"));
        assert_eq!(
            normalize_words("drop-down abacus zoology").as_deref(),
            Some("drop-down-abacus-zoology")
        );
    }

    #[test]
    fn word_phrase_normalization_rejects_wrong_count_and_unknown_words() {
        assert!(!is_word_phrase(""));
        assert!(!is_word_phrase("abacus"));
        assert!(!is_word_phrase("abacus-abdomen"));
        assert!(!is_word_phrase("abacus-abdomen-zoology-extra"));
        assert!(!is_word_phrase("abacus-abdomen-notaword"));
        assert!(!is_word_phrase("abacus/abdomen/zoology"));
    }

    #[test]
    fn generated_words_have_three_known_lowercase_words() {
        for _ in 0..100 {
            let phrase = generate_words();
            assert!(
                is_word_phrase(&phrase),
                "generated phrase must parse: {phrase}"
            );
            assert_eq!(normalize_words(&phrase).as_deref(), Some(phrase.as_str()));
        }
    }

    #[test]
    fn generated_aliases_have_the_requested_word_count() {
        for count in MIN_ALIAS_WORDS..=MAX_ALIAS_WORDS {
            let phrase = generate_words_with_count(count);
            let chunks: Vec<&str> = phrase.split('-').collect();
            assert!(
                has_word_count(&chunks, usize::from(count)),
                "{count} words expected: {phrase}"
            );
            assert!(
                normalize_words_up_to(&phrase, count).is_some(),
                "{count} words must resolve: {phrase}"
            );
        }
    }

    #[test]
    fn alias_normalization_accepts_up_to_the_maximum() {
        let four = "abacus-abdomen-zoology-acorn";
        assert!(normalize_words_up_to(four, 3).is_none());
        assert!(normalize_words_up_to(four, 4).is_some());
        let six = "abacus-abdomen-zoology-acorn-aerosol-album";
        assert!(normalize_words_up_to(six, 5).is_none());
        assert!(normalize_words_up_to(six, 6).is_some());
    }

    #[test]
    fn alias_key_uses_the_words_prefix() {
        assert_eq!(
            alias_key("abacus-abdomen-zoology"),
            "words/abacus-abdomen-zoology"
        );
    }

    #[test]
    fn default_list_window_covers_the_pacific_evening() {
        // At 20:00 Pacific on 2026-10-28 the UTC clock is already
        // 2026-10-29T03:00Z. The default window must cover both UTC dates,
        // because the Pacific day spans the UTC midnight boundary.
        let now = Utc.with_ymd_and_hms(2026, 10, 29, 3, 0, 0).unwrap();
        let (start, end) = default_list_window(now, 2);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 10, 28).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 10, 29).unwrap());
    }

    #[test]
    fn default_list_window_follows_the_configured_days() {
        let now = Utc.with_ymd_and_hms(2026, 10, 29, 3, 0, 0).unwrap();
        let (start, end) = default_list_window(now, 3);
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 10, 27).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 10, 29).unwrap());
        assert_eq!(inclusive_dates(start, end).count(), 3);
    }

    #[test]
    fn list_recent_date_stays_one_day() {
        let request = parse_list_window_request(Some("2026-10-28"), None, None).unwrap();
        assert_eq!(request, ListWindowRequest::Date("2026-10-28".to_string()));
        let (start, end) = resolve_list_window(&request, Utc::now(), 2).unwrap();
        let days: Vec<_> = inclusive_dates(start, end).collect();
        assert_eq!(days, vec![NaiveDate::from_ymd_opt(2026, 10, 28).unwrap()]);
    }

    #[test]
    fn inclusive_range_spans_two_dates_in_order() {
        let request =
            parse_list_window_request(None, Some("2026-10-28"), Some("2026-10-29")).unwrap();
        let (start, end) = resolve_list_window(&request, Utc::now(), 2).unwrap();
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
            resolve_list_window(&request, Utc::now(), 2),
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
    fn segment_suffix_has_a_fixed_base62_shape() {
        for _ in 0..32 {
            let suffix = generate_segment_suffix();
            assert_eq!(suffix.len(), SEGMENT_SUFFIX_LEN);
            assert!(
                suffix.bytes().all(|b| b.is_ascii_alphanumeric()),
                "the suffix must be URL-safe base62: {suffix:?}"
            );
        }
    }

    #[test]
    fn segment_key_format() {
        let base = "pastes/2026-10-28/abcXYZ";
        let suffix = "Ab3dE6gH9jK2";
        assert_eq!(
            segment_key(base, 0, suffix),
            "pastes/2026-10-28/abcXYZ/00000000000000000000-Ab3dE6gH9jK2"
        );
        assert_eq!(
            segment_key(base, 7, suffix),
            "pastes/2026-10-28/abcXYZ/00000000000000000007-Ab3dE6gH9jK2"
        );
        assert_eq!(
            parse_segment_key(base, &segment_key(base, 7, suffix)),
            Some((7, suffix))
        );
        assert!(is_paste_key(base));
        assert!(!is_paste_key(&segment_key(base, 0, "abc")));
        assert!(!is_paste_key("pastes/2026-10-28"));
        assert!(!is_paste_key("other/2026-10-28/abc"));
    }

    #[test]
    fn segment_key_order_uses_sequence_then_suffix() {
        let base = "pastes/2026-10-28/abcXYZ";
        assert!(segment_key(base, 2, "zzz") < segment_key(base, 10, "aaa"));
        assert!(segment_key(base, 0, "aaa") < segment_key(base, 0, "zzz"));
    }

    #[test]
    fn parse_segment_key_rejects_malformed_names() {
        let base = "pastes/2026-10-28/abcXYZ";
        assert_eq!(parse_segment_key(base, &format!("{base}/short-abc")), None);
        assert_eq!(
            parse_segment_key(base, &format!("{base}/0000000000000000000x-abc")),
            None
        );
        assert_eq!(
            parse_segment_key(base, &format!("{base}/00000000000000000000")),
            None
        );
        assert_eq!(
            parse_segment_key(base, &format!("{base}/00000000000000000000-")),
            None
        );
        assert_eq!(
            parse_segment_key(base, &format!("{base}/00000000000000000000-ab/cd")),
            None
        );
        assert_eq!(
            parse_segment_key(base, &format!("{base}/00000000000000000000-abc/extra")),
            None
        );
    }

    #[test]
    fn segment_plan_totals_and_maps_ranges_across_base_and_segments() {
        let base = "pastes/2026-10-28/abc";
        let segment_0 = segment_key(base, 0, "aaa");
        let segment_1 = segment_key(base, 1, "aaa");
        let plan = SegmentPlan::new(
            base,
            10,
            vec![
                Segment {
                    sequence: 1,
                    suffix: "aaa".into(),
                    key: segment_1.clone(),
                    size: 7,
                },
                Segment {
                    sequence: 0,
                    suffix: "aaa".into(),
                    key: segment_0.clone(),
                    size: 5,
                },
            ],
        );
        assert_eq!(plan.total_size(), 22);
        assert_eq!(plan.object_ranges(0, 9), vec![(base.to_string(), 0, 9)]);
        assert_eq!(plan.object_ranges(10, 14), vec![(segment_0.clone(), 0, 4)]);
        assert_eq!(
            plan.object_ranges(8, 12),
            vec![(base.to_string(), 8, 9), (segment_0.clone(), 0, 2),]
        );
        assert_eq!(plan.object_ranges(16, 21), vec![(segment_1.clone(), 1, 6)]);
        assert!(plan.object_ranges(22, 30).is_empty());
    }

    #[test]
    fn segment_plan_orders_same_sequence_segments_by_suffix() {
        let base = "pastes/2026-10-28/abc";
        let first = segment_key(base, 0, "aaa");
        let second = segment_key(base, 0, "zzz");
        let plan = SegmentPlan::new(
            base,
            4,
            vec![
                Segment {
                    sequence: 0,
                    suffix: "zzz".into(),
                    key: second.clone(),
                    size: 2,
                },
                Segment {
                    sequence: 0,
                    suffix: "aaa".into(),
                    key: first.clone(),
                    size: 3,
                },
            ],
        );
        assert_eq!(
            plan.segments
                .iter()
                .map(|s| s.suffix.as_str())
                .collect::<Vec<_>>(),
            vec!["aaa", "zzz"]
        );
        assert_eq!(
            plan.object_ranges(4, 8),
            vec![(first, 0, 2), (second, 0, 1)]
        );
    }

    #[test]
    fn segment_listing_keeps_only_direct_well_formed_keys_in_order() {
        let base = "pastes/2026-10-28/abc";
        let entries = vec![
            (segment_key(base, 2, "aaa"), 3),
            (segment_key(base, 0, "zzz"), 5),
            (segment_key(base, 0, "aaa"), 4),
            (format!("{base}/not-a-sequence"), 9),
            (format!("{base}/00000000000000000000"), 8),
            (format!("{base}/00000000000000000000-"), 8),
            (format!("{base}/00000000000000000000-aaa/nested"), 8),
        ];
        let segments = segments_from_listing(base, &entries);
        assert_eq!(
            segments
                .iter()
                .map(|s| (s.sequence, s.suffix.as_str(), s.size))
                .collect::<Vec<_>>(),
            vec![(0, "aaa", 4), (0, "zzz", 5), (2, "aaa", 3)]
        );
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
            None,
            None,
            Ttl::days(7),
            created,
            None,
            &detection,
            None,
            &[],
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
    fn ttl_above_the_maximum_is_rejected() {
        assert!(Ttl::checked_days(MAX_TTL_DAYS + 1).is_err());
        assert!(Ttl::parse_days(&(MAX_TTL_DAYS + 1).to_string()).is_err());
        assert!(Ttl::parse(&(MAX_TTL_SECONDS + 1).to_string()).is_err());
        assert!(Ttl::parse(&format!("{}d", MAX_TTL_DAYS + 1)).is_err());
        assert_eq!(
            Ttl::checked_days(MAX_TTL_DAYS)
                .expect("the maximum must be accepted")
                .num_seconds(),
            MAX_TTL_SECONDS
        );
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
            None,
            None,
            Ttl::parse("1h").unwrap(),
            created,
            None,
            &detection,
            None,
            &[],
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
            None,
            None,
            Ttl::parse("1h").unwrap(),
            created,
            None,
            &detection,
            None,
            &[],
        );
        let paste = |now: DateTime<Utc>| PasteMetadata {
            id: "2026-10-28/abc".to_string(),
            key: "pastes/2026-10-28/abc".to_string(),
            words: None,
            filename: None,
            title: None,
            tags: Vec::new(),
            content_type: "text/plain".to_string(),
            detection_source: Some("text".to_string()),
            detection_conflict: None,
            size: 3,
            base_size: 3,
            segments: Vec::new(),
            sha256: Some("deadbeef".to_string()),
            created_at: Some(created),
            expires_at: Some(expires),
            created_by: None,
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
            None,
            None,
            Ttl::days(3),
            created,
            None,
            &detection,
            None,
            &[],
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
    fn metadata_map_records_the_creator_address_when_present() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain".to_string(),
            source: "text",
            conflict: None,
        };
        let (meta, _) = metadata_for(
            None,
            None,
            None,
            None,
            None,
            Ttl::days(1),
            created,
            Some("203.0.113.7"),
            &detection,
            None,
            &[],
        );
        assert_eq!(meta.get("created-by").unwrap(), "203.0.113.7");

        let (meta, _) = metadata_for(
            None,
            None,
            None,
            None,
            None,
            Ttl::days(1),
            created,
            None,
            &detection,
            None,
            &[],
        );
        assert!(
            !meta.contains_key("created-by"),
            "an absent address must not write the key"
        );
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
        let (meta, _) = metadata_for(
            Some("a.bin"),
            None,
            None,
            None,
            None,
            Ttl::days(7),
            created,
            None,
            &detection,
            None,
            &[],
        );
        assert!(
            !meta.contains_key("sha256"),
            "unknown sha256 must be absent, not empty"
        );
        assert!(!meta.contains_key("size"), "unknown size must be absent");
        assert_eq!(meta.get("expires-at").unwrap(), "2026-11-04T00:00:00+00:00");
    }
}

#[cfg(test)]
mod token_tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn paste_token_has_128_bits_of_hex() {
        let token = generate_paste_token();
        assert_eq!(token.len(), 32);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(token, generate_paste_token());
    }

    #[test]
    fn metadata_map_stores_the_token_hash_only_when_present() {
        let created = Utc.with_ymd_and_hms(2026, 10, 28, 0, 0, 0).unwrap();
        let detection = Detection {
            content_type: "text/plain; charset=utf-8".to_string(),
            source: "text",
            conflict: None,
        };
        let (meta, _) = metadata_for(
            None,
            None,
            None,
            None,
            Some("abc123"),
            Ttl::days(1),
            created,
            None,
            &detection,
            None,
            &[],
        );
        assert_eq!(meta.get("paste-token-sha256").unwrap(), "abc123");

        let (meta, _) = metadata_for(
            None,
            None,
            None,
            None,
            None,
            Ttl::days(1),
            created,
            None,
            &detection,
            None,
            &[],
        );
        assert!(!meta.contains_key("paste-token-sha256"));
    }
}
