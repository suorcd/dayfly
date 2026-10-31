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
//!   base expiry and does not extend it. S3 lifecycle deletion, if
//!   configured, is only a day-rounded storage sweeper and may run after
//!   expiry.

use std::collections::{HashMap, HashSet};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Arc, OnceLock};
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
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

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
    #[error("could not allocate a three-word alias after {attempts} attempts")]
    AliasUnavailable { attempts: usize },
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
    /// Three-word alias, when the store wrote one.
    pub words: Option<String>,
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
    /// Three-word alias, when the base object records one.
    pub words: Option<String>,
    pub filename: Option<String>,
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
    pub expired: bool,
}

impl PasteMetadata {
    /// Build the range plan for this paste without another S3 call.
    pub fn plan(&self) -> SegmentPlan {
        SegmentPlan::new(self.key.clone(), self.base_size, self.segments.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TextSlice {
    pub text: String,
    pub total_bytes: u64,
    pub returned_bytes: u64,
    pub truncated: bool,
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
    /// Three-word alias, when the base object records one.
    pub words: Option<String>,
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

/// Number of words in a three-word alias.
pub const WORDS_IN_PHRASE: usize = 3;

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

/// Normalize a three-word alias to the lowercase hyphen form.
///
/// The function accepts spaces or hyphens between words and trims the
/// input. It returns `None` when the input is not exactly three known
/// words. Four wordlist entries contain a hyphen, so the parser tests
/// every split of the input into three wordlist entries.
pub fn normalize_words(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    let chunks: Vec<&str> = lower
        .split(|c: char| c == '-' || c.is_whitespace())
        .filter(|chunk| !chunk.is_empty())
        .collect();
    if chunks.len() < WORDS_IN_PHRASE || !is_three_words(&chunks) {
        return None;
    }
    Some(chunks.join("-"))
}

fn is_three_words(chunks: &[&str]) -> bool {
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
    search(chunks, 0, WORDS_IN_PHRASE)
}

/// True when the input is a valid three-word alias.
pub fn is_word_phrase(input: &str) -> bool {
    normalize_words(input).is_some()
}

/// Generate one random three-word alias in the hyphen form.
pub fn generate_words() -> String {
    let words = wordlist();
    let mut rng = rand::rng();
    let mut phrase = String::new();
    for index in 0..WORDS_IN_PHRASE {
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

/// Hash a file in fixed-size reads, so a large file is never held whole
/// in memory. S3 takes object metadata only at CreateMultipartUpload, so
/// the hash must exist before that call.
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

fn expires_at_for(created_at: DateTime<Utc>, ttl: Ttl) -> DateTime<Utc> {
    created_at
        .checked_add_signed(ttl.duration())
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn metadata_for(
    filename: Option<&str>,
    sha256: Option<&str>,
    size: Option<u64>,
    words: Option<&str>,
    ttl: Ttl,
    created_at: DateTime<Utc>,
    detection: &Detection,
) -> (HashMap<String, String>, DateTime<Utc>) {
    let expires_at = expires_at_for(created_at, ttl);
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
    if let Some(words) = words {
        meta.insert("words".to_string(), words.to_string());
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
    alias_lock: Arc<AsyncMutex<()>>,
    alias_candidates: Option<Arc<Vec<String>>>,
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
            alias_lock: Arc::new(AsyncMutex::new(())),
            alias_candidates: None,
        })
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
    async fn alias_object_exists(&self, phrase: &str) -> Result<bool, StoreError> {
        let key = alias_key(phrase);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))
        {
            Ok(_) => Ok(true),
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Choose an unused alias from a small set of random candidates.
    async fn choose_alias(&self) -> Result<String, StoreError> {
        match &self.alias_candidates {
            Some(candidates) => self.choose_alias_from(candidates.iter().cloned()).await,
            None => {
                self.choose_alias_from(std::iter::repeat_with(generate_words).take(ALIAS_ATTEMPTS))
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
            let Some(phrase) = normalize_words(&candidate) else {
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
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(target_key.as_bytes().to_vec()))
            .content_type("text/plain")
            .set_metadata(Some(meta))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))?;
        Ok(())
    }

    /// Claim an alias for `key` before the base object is written.
    ///
    /// One process-wide lock covers the check and the write, so two
    /// stores in one process cannot choose the same phrase. Garage
    /// ignores conditional writes, so two processes can still race;
    /// [`Self::verify_alias`] detects that case before a paste returns.
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
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))?;
        Ok(())
    }

    /// Remove the base object and segments after a failed store.
    async fn cleanup_failed_store(&self, base_key: &str, phrase: &str) {
        let _ = self.delete_alias_if_target(phrase, base_key).await;
        let _ = self.delete_base_and_segments(base_key).await;
    }

    /// Read the canonical key from an alias object.
    ///
    /// The body must equal the stored `target` metadata, and the target
    /// must be a valid paste key. A stale or corrupt alias is not trusted.
    async fn read_alias(&self, phrase: &str) -> Result<String, StoreError> {
        let key = alias_key(phrase);
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| match map_sdk_error(e, &key) {
                StoreError::NotFound(_) => StoreError::NotFound(phrase.to_string()),
                other => other,
            })?;
        let meta = out.metadata().cloned().unwrap_or_default();
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?
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
        let out = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, key))
        {
            Ok(out) => out,
            Err(StoreError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        Ok(out
            .metadata()
            .and_then(|meta| meta.get("words").cloned())
            .and_then(|words| normalize_words(&words)))
    }

    /// Delete the base object and every object under its segment prefix.
    async fn delete_base_and_segments(&self, base_key: &str) -> Result<(), StoreError> {
        let segments = self.list_segment_entries(base_key).await?;
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(base_key)
            .send()
            .await
            .map_err(|e| map_sdk_error(e, base_key))?;
        for (key, _) in segments {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| map_sdk_error(e, &key))?;
        }
        Ok(())
    }

    async fn delete_by_words(&self, phrase: &str) -> Result<(), StoreError> {
        let target = self.read_alias(phrase).await?;
        self.delete_base_and_segments(&target).await?;
        self.delete_alias_if_target(phrase, &target).await
    }

    /// Resolve a client ID to an S3 key.
    ///
    /// Full IDs (`yyyy-mm-dd/<random>`) resolve directly. Bare random IDs
    /// are resolved by scanning the `pastes/` prefix (at most
    /// [`LIST_SCAN_MAX_PAGES`] pages of 1,000 keys). A valid three-word
    /// alias resolves through its `words/` object. That scan is the cost
    /// of not having an index: clients should keep the full ID returned
    /// at store time.
    pub async fn resolve_key(&self, id: &str) -> Result<String, StoreError> {
        if let Some(phrase) = normalize_words(id) {
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
            let out = req.send().await.map_err(|e| map_sdk_error(e, id))?;
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
        let expires_at = expires_at_for(created_at, ttl);
        let words = self.claim_alias(&key, expires_at).await?;
        let (meta, _) = metadata_for(
            filename,
            Some(&sha),
            Some(text.len() as u64),
            Some(&words),
            ttl,
            created_at,
            &detection,
        );
        if let Err(e) = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(text.to_vec()))
            .content_type(ct.clone())
            .set_metadata(Some(meta))
            .send()
            .await
        {
            self.cleanup_failed_store(&key, &words).await;
            return Err(map_sdk_error(e, &key));
        }
        if let Err(e) = self.verify_alias(&words, &key).await {
            self.cleanup_failed_store(&key, &words).await;
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
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
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
        let mut file = File::open(path).await?;
        // Metadata from the handle, not the path: one file, one view.
        let file_meta = file.metadata().await?;
        if !file_meta.is_file() {
            return Err(StoreError::Io(format!(
                "not a regular file: {}",
                path.display()
            )));
        }
        let len = file_meta.len();
        if len > self.config.max_file_bytes {
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
        let expires_at = expires_at_for(created_at, ttl);
        if len as usize <= MULTIPART_CHUNK_BYTES {
            // One bounded read gives the body, the hash, and the size, so
            // text detection sees every byte. The read stops one byte past
            // the cap: a file that grew after the metadata call is
            // rejected instead of loaded whole.
            let mut bytes: Vec<u8> = Vec::with_capacity(len as usize);
            let words = self.claim_alias(&key, expires_at).await?;
            let read = match file
                .take(self.config.max_file_bytes.saturating_add(1))
                .read_to_end(&mut bytes)
                .await
            {
                Ok(read) => read,
                Err(e) => {
                    self.cleanup_failed_store(&key, &words).await;
                    return Err(e.into());
                }
            };
            let size = read as u64;
            if size > self.config.max_file_bytes {
                self.cleanup_failed_store(&key, &words).await;
                return Err(StoreError::TooLarge {
                    limit: self.config.max_file_bytes,
                });
            }
            let detection = detection_for(display_name.as_deref(), content_type, &bytes);
            let ct = detection.content_type.clone();
            let sha = sha256_hex(&bytes);
            let (meta, _) = metadata_for(
                display_name.as_deref(),
                Some(&sha),
                Some(size),
                Some(&words),
                ttl,
                created_at,
                &detection,
            );
            if let Err(e) = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(bytes))
                .content_type(ct.clone())
                .set_metadata(Some(meta))
                .send()
                .await
            {
                self.cleanup_failed_store(&key, &words).await;
                return Err(map_sdk_error(e, &key));
            }
            if let Err(e) = self.verify_alias(&words, &key).await {
                self.cleanup_failed_store(&key, &words).await;
                return Err(e);
            }
            return Ok(StoredPaste {
                id: format!("{date}/{random_id}"),
                key,
                words: Some(words),
                size,
                sha256: sha,
                content_type: ct,
                filename: display_name,
                detection_source: Some(detection.source.to_string()),
                detection_conflict: detection.conflict,
                expires_at,
            });
        }

        // Multipart path. S3 takes the object metadata at
        // CreateMultipartUpload, so the hash must be known first: hash the
        // bytes from this handle, then upload from the same handle and
        // require the uploaded bytes to match that hash. A file that
        // changes in between fails the comparison, and the object and its
        // alias are deleted before any paste is returned.
        let sha = {
            let mut hasher = Sha256::new();
            let mut buf = vec![0u8; MULTIPART_CHUNK_BYTES];
            let mut hashed: u64 = 0;
            loop {
                let n = file.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                hashed += n as u64;
            }
            if hashed != len {
                return Err(StoreError::Io(format!(
                    "the file changed while it was read: expected {len} bytes, read {hashed}"
                )));
            }
            hex_encode(&hasher.finalize())
        };
        // The detection sample comes from the same handle, so the recorded
        // type describes the bytes that are uploaded.
        file.seek(SeekFrom::Start(0)).await?;
        let prefix = {
            let mut buf = vec![0u8; crate::content_type::PEEK_BYTES];
            let n = file.read(&mut buf).await?;
            buf.truncate(n);
            buf
        };
        let detection = detect_content_type_sample(
            content_type,
            display_name.as_deref(),
            &prefix,
            len <= crate::content_type::PEEK_BYTES as u64,
        );
        let ct = detection.content_type.clone();
        file.seek(SeekFrom::Start(0)).await?;
        let words = self.claim_alias(&key, expires_at).await?;
        let (meta, _) = metadata_for(
            display_name.as_deref(),
            Some(&sha),
            Some(len),
            Some(&words),
            ttl,
            created_at,
            &detection,
        );
        let mut sink = AwsUploadSink {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.clone(),
            content_type: ct.clone(),
            metadata: meta,
        };
        let outcome = match crate::stream::run_streaming_upload(
            Box::pin(file_stream(file)),
            &mut sink,
            Some(len),
            self.config.max_file_bytes,
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
            self.cleanup_failed_store(&key, &words).await;
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
        let expires_at = expires_at_for(created_at, ttl);
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
            self.cleanup_failed_store(&key, &words).await;
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
            detection_source: Some(detection.source.to_string()),
            detection_conflict: detection.conflict,
            expires_at,
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
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .max_keys(1000);
            if let Some(t) = token {
                req = req.continuation_token(t);
            }
            let out = req.send().await.map_err(|e| map_sdk_error(e, base_key))?;
            for obj in out.contents() {
                if let Some(key) = obj.key() {
                    entries.push((
                        key.to_string(),
                        u64::try_from(obj.size().unwrap_or(0)).unwrap_or(0),
                    ));
                }
            }
            match out.next_continuation_token() {
                Some(t) if out.is_truncated().unwrap_or(false) => token = Some(t.to_string()),
                _ => break,
            }
        }
        Ok(entries)
    }

    /// Build the base-plus-segments plan for a known base key and size.
    async fn load_plan(&self, base_key: &str, base_size: u64) -> Result<SegmentPlan, StoreError> {
        let entries = self.list_segment_entries(base_key).await?;
        let segments = segments_from_listing(base_key, &entries);
        Ok(SegmentPlan::new(base_key.to_string(), base_size, segments))
    }

    /// Append text to an existing paste as one new segment object.
    ///
    /// The base object is never rewritten or copied. The text cap applies
    /// to this append only, not to the total size. The expiry check
    /// happens before the write, so an expired or missing id creates no
    /// object.
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
        object_meta.insert("created-at".to_string(), Utc::now().to_rfc3339());
        if let Some(expires_at) = meta.expires_at {
            // The append inherits the base expiry. It does not extend it.
            object_meta.insert("expires-at".to_string(), expires_at.to_rfc3339());
        }
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(text.to_vec()))
            .content_type(meta.content_type.clone())
            .set_metadata(Some(object_meta))
            .send()
            .await
            .map_err(|e| map_sdk_error(e, &key))?;
        let appended_bytes = text.len() as u64;
        meta.segments.push(Segment {
            sequence,
            suffix,
            key: key.clone(),
            size: appended_bytes,
        });
        meta.size = meta.size.saturating_add(appended_bytes);
        meta.sha256 = None;
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

    /// Read metadata for a canonical key without another alias lookup.
    async fn head_metadata_by_key(&self, key: &str, id: &str) -> Result<PasteMetadata, StoreError> {
        let out = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
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
        let base_size = u64::try_from(out.content_length().unwrap_or(0)).unwrap_or(0);
        let plan = self.load_plan(key, base_size).await?;
        let has_segments = !plan.segments.is_empty();
        Ok(PasteMetadata {
            id: key.strip_prefix("pastes/").unwrap_or(key).to_string(),
            key: key.to_string(),
            words: meta.get("words").and_then(|words| normalize_words(words)),
            filename: meta.get("filename").map(|s| decode_filename(s)),
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
            expired,
        })
    }

    fn check_not_expired(meta: &PasteMetadata) -> Result<(), StoreError> {
        if meta.expired {
            return Err(StoreError::Expired(meta.id.clone()));
        }
        Ok(())
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
        let out = req.send().await.map_err(|e| map_sdk_error(e, key))?;
        let bytes = out
            .body
            .collect()
            .await
            .map_err(|e| StoreError::S3(e.to_string()))?
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
        let out = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
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
    /// The input can be a canonical ID or a three-word alias. The prefix
    /// listing runs first, so a listing failure leaves the paste intact
    /// rather than deleting only the base object.
    pub async fn delete(&self, id: &str) -> Result<(), StoreError> {
        if let Some(phrase) = normalize_words(id) {
            return self.delete_by_words(&phrase).await;
        }
        let key = self.resolve_key(id).await?;
        let words = self.base_words(&key).await?;
        self.delete_base_and_segments(&key).await?;
        if let Some(words) = words {
            self.delete_alias_if_target(&words, &key).await?;
        }
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
    /// `pastes/<date>/` prefix, then one `HeadObject` and a segment-prefix
    /// listing per paste to recover expiry, content-type, filename, and
    /// total size, and to filter expired objects. Segment keys are skipped,
    /// so a segment never appears as a paste. Each prefix follows
    /// continuation tokens for up to [`LIST_SCAN_MAX_PAGES`] pages. A
    /// window can span at most [`LIST_WINDOW_MAX_DAYS`] dates. The listing
    /// is O(n) in the objects scanned (plus a head and a list per paste):
    /// it is fast for a small internal bucket and gets slower as the bucket
    /// grows. That slowdown is exactly the trigger for adding a database
    /// later — it is documented in README, not hidden, and no database
    /// schema is invented in v1. This is not search.
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
                    // A segment key has a third component after `pastes/`.
                    // It is part of a paste, not a paste.
                    if !is_paste_key(key) {
                        continue;
                    }
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
                        words: meta.words,
                        size: meta.size,
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
            None,
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
            None,
            Ttl::parse("1h").unwrap(),
            created,
            &detection,
        );
        let paste = |now: DateTime<Utc>| PasteMetadata {
            id: "2026-10-28/abc".to_string(),
            key: "pastes/2026-10-28/abc".to_string(),
            words: None,
            filename: None,
            content_type: "text/plain".to_string(),
            detection_source: Some("text".to_string()),
            detection_conflict: None,
            size: 3,
            base_size: 3,
            segments: Vec::new(),
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
        let (meta, _) = metadata_for(
            Some("a.bin"),
            None,
            None,
            None,
            Ttl::days(7),
            created,
            &detection,
        );
        assert!(
            !meta.contains_key("sha256"),
            "unknown sha256 must be absent, not empty"
        );
        assert!(!meta.contains_key("size"), "unknown size must be absent");
        assert_eq!(meta.get("expires-at").unwrap(), "2026-11-04T00:00:00+00:00");
    }
}
