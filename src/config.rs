use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::client_addr::Cidr;
use crate::stream::STREAM_PART_BYTES;

/// Maximum number of parts in one S3 multipart upload.
pub const MAX_MULTIPART_PARTS: u64 = 10_000;

/// Largest file that one multipart upload can hold at the current part size.
pub const MAX_MULTIPART_BYTES: u64 = MAX_MULTIPART_PARTS * STREAM_PART_BYTES as u64;

/// Maximum TTL accepted from an API client, in days.
///
/// Ten years is a safety bound, not a storage policy. It keeps a mistyped
/// duration from producing a far-future `expires-at` and keeps the service
/// description's limit finite.
pub const MAX_TTL_DAYS: u32 = 3650;
/// Maximum TTL accepted from an API client, in seconds.
pub const MAX_TTL_SECONDS: i64 = MAX_TTL_DAYS as i64 * 86_400;

/// Message returned by every listing route when listing is off.
pub const LISTING_DISABLED_MESSAGE: &str = "Listing is turned off on this instance.";

/// Minimum number of words in a generated alias.
pub const MIN_ALIAS_WORDS: u8 = 3;

/// Maximum number of words in a generated alias.
pub const MAX_ALIAS_WORDS: u8 = 6;

/// Default number of words in a generated alias.
pub const DEFAULT_ALIAS_WORDS: u8 = 3;

/// The log output format.
///
/// `Text` is the default and keeps the existing output. `Json` writes one
/// JSON object per line, for a log collector such as Loki.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    /// Parse the `DAYFLY_LOG_FORMAT` value.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// The value used in the configuration summary.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

/// Configuration loaded from the environment.
///
/// The S3 credentials come from a direct environment variable or from a
/// file named by the `_FILE` alternative. No S3 credential is ever logged.
/// [`Config`] deliberately has no `Debug` implementation that prints secret
/// fields; callers that log configuration must use
/// [`Config::safe_summary`].
#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Maximum size, in bytes, for a text paste. Enforced by the MCP
    /// `store_text` tool, the human text form, and HTTP uploads with no
    /// filename. For an HTTP upload, a filename selects
    /// [`Self::max_file_bytes`] instead. [`Self::max_paste_bytes`] also
    /// limits a new paste, so the lower value wins. Default: 10 MiB.
    pub max_text_bytes: u64,
    /// Maximum size, in bytes, for a file paste. [`Self::max_paste_bytes`]
    /// also limits a new paste, so the lower value wins. Default: 1 GiB.
    pub max_file_bytes: u64,
    /// Maximum total size, in bytes, for one paste, including the base
    /// object and every appended segment. A new paste above this cap
    /// fails with `TooLarge` and writes no object. Default: 1 GiB.
    pub max_paste_bytes: u64,
    /// Maximum bytes that one text search reads. Default: 64 MiB.
    ///
    /// The search returns `truncated` when it reaches this cap. The
    /// caller resumes with `next_offset`.
    pub search_max_bytes: u64,
    /// Number of words in a new alias. The allowed range is 3 to 6.
    /// Resolution accepts 3 words up to this value. Default: 3.
    pub alias_words: u8,
    /// Default TTL in positive whole days when a store call does not
    /// specify one. Zero is invalid because it would create a paste that
    /// is already expired.
    pub default_ttl_days: u32,
    /// Minimum TTL in whole days for an explicit TTL.
    ///
    /// Default: `0`, so a TTL can be shorter than one day. When this is
    /// above zero, a request below the minimum is rejected.
    pub min_ttl_days: u32,
    /// Maximum TTL in whole days.
    ///
    /// Default: [`MAX_TTL_DAYS`]. A request above the maximum is rejected.
    /// The value must be from 1 to [`MAX_TTL_DAYS`].
    pub max_ttl_days: u32,
    /// Whole-day choices for the browser TTL selector.
    ///
    /// Default: `1,3,7,14,30,90`. Every choice must be from
    /// [`Self::min_ttl_days`] to [`Self::max_ttl_days`]. The default TTL
    /// is added when it is absent.
    pub ttl_choices: Vec<u32>,
    /// Number of days in the default paste list window, ending today in
    /// UTC.
    ///
    /// Default: 2, so the window covers yesterday and today. The value
    /// must be from 1 to [`crate::store::LIST_WINDOW_MAX_DAYS`].
    pub list_default_days: u32,
    /// Whether to clamp the expiry of a paste by its size.
    ///
    /// Default: `false`. With the default, the requested TTL is used
    /// exactly. When this is `true`, a larger paste gets a shorter
    /// retention, down to [`Self::retention_min_days`] at
    /// [`Self::max_paste_bytes`]. The clamp only shortens the expiry.
    pub retention_by_size: bool,
    /// Minimum retention in whole days at [`Self::max_paste_bytes`].
    ///
    /// Default: 30. The value must be from 1 to 3650 and must not
    /// exceed [`Self::retention_max_days`].
    pub retention_min_days: u32,
    /// Maximum retention in whole days at zero bytes.
    ///
    /// Default: 365. The value must be from 1 to 3650 and must be at
    /// least [`Self::retention_min_days`].
    pub retention_max_days: u32,
    /// Default bind address for HTTP mode.
    pub http_bind: String,
    /// Log output format.
    ///
    /// Default: [`LogFormat::Text`]. A JSON value writes one object per
    /// line to stderr. A change needs a restart, because the subscriber
    /// is built at startup.
    pub log_format: LogFormat,
    /// Optional bind address for the private Prometheus metrics listener.
    ///
    /// Default: unset, so no metrics listener exists. When set, the
    /// listener serves only `GET /metrics`. The public HTTP listener
    /// never serves that route. The address must be an IP address and a
    /// port, for example `127.0.0.1:9091` or `10.0.0.5:9091`.
    pub metrics_bind: Option<SocketAddr>,
    /// CIDR blocks allowed to scrape the metrics listener.
    ///
    /// Default: empty. A loopback [`Self::metrics_bind`] needs no list.
    /// A non-loopback bind is refused at startup when this list is
    /// empty. The listener checks the socket peer and never reads
    /// `X-Forwarded-For`.
    pub metrics_allowed_networks: Vec<Cidr>,
    /// CIDR blocks of reverse proxies whose `X-Forwarded-For` header is
    /// believed.
    ///
    /// Default: empty. With the default, the socket peer is the client and
    /// forwarding headers are ignored. When the peer is listed here, the
    /// forwarding chain is walked from the right and the first address
    /// that is not a listed proxy is the client. See "Access model" in the
    /// README.
    pub trusted_proxies: Vec<Cidr>,
    /// Whether a non-loopback bind may run without a bearer token.
    ///
    /// Default: `false`. With the default, `serve` refuses a bind address
    /// that other hosts can reach when `DAYFLY_HTTP_BEARER_TOKEN` is unset.
    /// Set this to `true` only on a network you already trust, or behind a
    /// proxy that authenticates. The service then logs one warning at
    /// startup. See "Access model" in the README.
    pub allow_unauthenticated: bool,
    /// Whether listing requires the bearer token.
    ///
    /// Default: `false`. With the default, listing follows the global
    /// bearer setting: gated when a token is set, open when none is. When
    /// this is `true`, `GET /v1/pastes` and the human `/pastes` page
    /// require the bearer token even if reads are otherwise anonymous.
    /// With no token configured, both routes refuse: there is nothing to
    /// check against, and an open list is what this setting prevents.
    pub list_requires_token: bool,
    /// Whether any listing route works.
    ///
    /// Default: `true`. When this is `false`, the human `/pastes` page,
    /// `GET /v1/pastes`, and the MCP `list_recent` tool refuse with one
    /// message. This setting wins over [`Self::list_requires_token`].
    pub list_enabled: bool,
    /// Whether a new paste gets a management token.
    ///
    /// Default: `false`. When this is `true`, creation returns a random
    /// token once. Only its SHA-256 is stored. The token authorizes
    /// `DELETE`, `PATCH`, and `append` for that paste, as an alternative to
    /// the bearer token. The bearer token remains a universal key.
    pub paste_tokens: bool,
    /// Whether API responses return the stored creator address.
    ///
    /// Default: `false`. The base object always records `created-by` when
    /// the transport has a client address. The meta route, the `HEAD`
    /// response, and the MCP `fetch_metadata` tool include it only when
    /// this is `true`. The access log also includes the client address
    /// only when this is `true`. An address is personal data.
    pub expose_creator_addr: bool,
    /// Maximum requests per client per minute. Zero disables the limiter.
    ///
    /// Default: `0`. The limit is a token bucket per resolved client
    /// address, refilled at this rate, with a burst of the same size. The
    /// limiter is per process and resets on restart. See "Access model" in
    /// the README.
    pub rate_limit_per_minute: u32,
    /// Whether the human web UI shows the generated aliases.
    ///
    /// Default: `true`. This is a display setting only: aliases are still
    /// generated and stored, and the JSON/MCP responses still include the
    /// `words` field. It is not a secrecy control.
    pub show_words: bool,
    /// Whether the paste list shows the internal date and ID path.
    ///
    /// Default: `false`. The list keeps the alias as the link and hides
    /// the canonical `date/id` text. Set this to `true` to show the ID
    /// under the alias. This is a display setting only: the view URLs
    /// and API responses still accept the canonical ID.
    pub show_internal_paths: bool,
    /// Whether a PDF download is served inline.
    ///
    /// Default: `true`. Set this to `false` on a public instance so a PDF
    /// is served as an attachment. HTML and SVG are always attachments.
    pub inline_pdf: bool,
    /// Whether the view page numbers the lines of a text paste.
    ///
    /// Default: `false`. The numbers are presentation only: a CSS
    /// counter renders them, they are not selectable, and they never
    /// enter the copied text.
    pub view_line_numbers: bool,
    /// Optional bearer token for the binary's own HTTP endpoint.
    /// This is NOT the S3/Garage credential and is not a replacement for
    /// it: the Garage key/secret stay in S3_ACCESS_KEY_ID /
    /// S3_SECRET_ACCESS_KEY and are held only by this binary. When this
    /// is set (non-empty), HTTP mode rejects requests without
    /// `Authorization: Bearer <token>` with 401. When unset, HTTP mode
    /// relies on private-network / localhost deployment (see README).
    /// The MCP stdio transport does not use this token: spawning the
    /// binary locally is the trust boundary there.
    pub http_bearer_token: Option<String>,
    /// Optional directory that limits the MCP local-path tools.
    ///
    /// When set, `store_file` and `fetch_file` resolve their path and
    /// reject anything outside this directory, including a symlink that
    /// points away from it. The path is canonical at startup.
    ///
    /// When unset, the stdio transport allows any path the process can
    /// reach, because spawning the binary locally is the trust boundary.
    /// The HTTP transport has no such boundary, so it refuses both tools
    /// until this is set.
    pub mcp_local_root: Option<PathBuf>,
    /// Optional favicon served at `/favicon.ico`.
    ///
    /// The path comes from `DAYFLY_FAVICON_PATH`. The file is read once
    /// at startup and must be 1 MiB or smaller. When unset, the route
    /// returns 404.
    pub favicon: Option<Favicon>,
    /// Optional `robots.txt` served at `/robots.txt`.
    ///
    /// The path comes from `DAYFLY_ROBOTS_TXT_PATH`. The file is read
    /// once at startup. When unset, the route returns 404.
    pub robots_txt: Option<TextAsset>,
    /// Optional `security.txt` served at `/.well-known/security.txt`.
    ///
    /// The path comes from `DAYFLY_SECURITY_TXT_PATH`. The file is read
    /// once at startup. When unset, the route returns 404.
    pub security_txt: Option<TextAsset>,
    /// Optional site name for the human UI footer.
    ///
    /// The value comes from `DAYFLY_SITE_NAME`. When unset, the footer
    /// keeps the default `dayfly — internal use only` text. When set, the
    /// site name replaces that text.
    pub site_name: Option<String>,
    /// Optional takedown contact for the human UI footer.
    ///
    /// The value comes from `DAYFLY_CONTACT`. An `http://` or `https://`
    /// value becomes a link. A value with `@` becomes a `mailto:` link.
    /// Any other value is plain text.
    pub contact: Option<String>,
    /// Optional acceptable-use URL for the human UI footer.
    ///
    /// The value comes from `DAYFLY_AUP_URL` and must use `http://` or
    /// `https://`. The footer links the text "acceptable use" to it.
    pub aup_url: Option<String>,
}

/// Maximum characters in `DAYFLY_SITE_NAME`.
const MAX_SITE_NAME_CHARS: usize = 80;
/// Maximum characters in `DAYFLY_CONTACT`.
const MAX_CONTACT_CHARS: usize = 200;
/// Maximum characters in `DAYFLY_AUP_URL`.
const MAX_AUP_URL_CHARS: usize = 500;

/// A favicon image loaded at startup.
#[derive(Clone, PartialEq, Eq)]
pub struct Favicon {
    /// Raw image bytes.
    pub bytes: Arc<[u8]>,
    /// Response content type for the bytes.
    pub content_type: &'static str,
}

/// A UTF-8 text file served at a fixed path.
#[derive(Clone, PartialEq, Eq)]
pub struct TextAsset {
    /// Raw file bytes.
    pub bytes: Arc<[u8]>,
}

/// One setting that the environment file template and the parser share.
pub struct ConfigField {
    pub name: &'static str,
    /// The default value, or an example value for a required field.
    pub value: Option<&'static str>,
    pub required: bool,
    pub help: &'static str,
}

/// The documented settings, in template order.
pub const CONFIG_FIELDS: &[ConfigField] = &[
    ConfigField {
        name: "S3_ENDPOINT",
        value: Some("https://s3.example.invalid:3900"),
        required: true,
        help: "Garage endpoint URL, http:// or https://",
    },
    ConfigField {
        name: "S3_BUCKET",
        value: Some("pastebin-test"),
        required: true,
        help: "Private bucket name",
    },
    ConfigField {
        name: "S3_ACCESS_KEY_ID",
        value: Some("EXAMPLE-ACCESS-KEY"),
        required: true,
        help: "Garage access key",
    },
    ConfigField {
        name: "S3_SECRET_ACCESS_KEY",
        value: Some("EXAMPLE-SECRET-KEY"),
        required: true,
        help: "Garage secret key",
    },
    ConfigField {
        name: "S3_REGION",
        value: Some("us-east-1"),
        required: false,
        help: "Signing region for Garage",
    },
    ConfigField {
        name: "DAYFLY_MAX_TEXT_BYTES",
        value: Some("104857600"),
        required: false,
        help: "Maximum text paste size in bytes",
    },
    ConfigField {
        name: "DAYFLY_MAX_FILE_BYTES",
        value: Some("1073741824"),
        required: false,
        help: "Maximum file paste size in bytes",
    },
    ConfigField {
        name: "DAYFLY_MAX_PASTE_BYTES",
        value: Some("1073741824"),
        required: false,
        help: "Maximum total paste size in bytes, including appends",
    },
    ConfigField {
        name: "DAYFLY_SEARCH_MAX_BYTES",
        value: Some("67108864"),
        required: false,
        help: "Maximum bytes that one text search reads",
    },
    ConfigField {
        name: "DAYFLY_ALIAS_WORDS",
        value: Some("3"),
        required: false,
        help: "Number of words in a new alias, 3 to 6",
    },
    ConfigField {
        name: "DAYFLY_DEFAULT_TTL_DAYS",
        value: Some("7"),
        required: false,
        help: "Default TTL in whole days",
    },
    ConfigField {
        name: "DAYFLY_MIN_TTL_DAYS",
        value: Some("0"),
        required: false,
        help: "Minimum TTL in whole days; 0 allows a shorter TTL",
    },
    ConfigField {
        name: "DAYFLY_MAX_TTL_DAYS",
        value: Some("3650"),
        required: false,
        help: "Maximum TTL in whole days",
    },
    ConfigField {
        name: "DAYFLY_TTL_CHOICES",
        value: Some("1,3,7,14,30,90"),
        required: false,
        help: "Comma-separated whole-day choices for the browser TTL selector",
    },
    ConfigField {
        name: "DAYFLY_LIST_DEFAULT_DAYS",
        value: Some("2"),
        required: false,
        help: "Default window for the paste list, in whole days",
    },
    ConfigField {
        name: "DAYFLY_RETENTION_BY_SIZE",
        value: Some("false"),
        required: false,
        help: "Clamp retention by the paste size",
    },
    ConfigField {
        name: "DAYFLY_RETENTION_MIN_DAYS",
        value: Some("30"),
        required: false,
        help: "Minimum retention in days at the paste size cap",
    },
    ConfigField {
        name: "DAYFLY_RETENTION_MAX_DAYS",
        value: Some("365"),
        required: false,
        help: "Maximum retention in days for a small paste",
    },
    ConfigField {
        name: "DAYFLY_HTTP_BIND",
        value: Some("127.0.0.1:8080"),
        required: false,
        help: "HTTP bind address",
    },
    ConfigField {
        name: "DAYFLY_LOG_FORMAT",
        value: Some("text"),
        required: false,
        help: "Log format: text or json",
    },
    ConfigField {
        name: "DAYFLY_METRICS_BIND",
        value: None,
        required: false,
        help: "Optional private bind address for the Prometheus metrics listener, for example 127.0.0.1:9091",
    },
    ConfigField {
        name: "DAYFLY_METRICS_ALLOWED_NETWORKS",
        value: None,
        required: false,
        help: "Comma-separated CIDRs allowed to scrape the metrics listener; required for a non-loopback bind",
    },
    ConfigField {
        name: "DAYFLY_TRUSTED_PROXIES",
        value: Some(""),
        required: false,
        help: "Comma-separated CIDRs of proxies whose X-Forwarded-For is trusted, for example 10.0.0.0/8",
    },
    ConfigField {
        name: "DAYFLY_ALLOW_UNAUTHENTICATED",
        value: Some("false"),
        required: false,
        help: "Start a non-loopback bind with no bearer token",
    },
    ConfigField {
        name: "DAYFLY_LIST_ENABLED",
        value: Some("true"),
        required: false,
        help: "Allow any listing route",
    },
    ConfigField {
        name: "DAYFLY_LIST_REQUIRES_TOKEN",
        value: Some("false"),
        required: false,
        help: "Require the bearer token to list pastes",
    },
    ConfigField {
        name: "DAYFLY_PASTE_TOKENS",
        value: Some("false"),
        required: false,
        help: "Return a per-paste management token at creation",
    },
    ConfigField {
        name: "DAYFLY_EXPOSE_CREATOR_ADDR",
        value: Some("false"),
        required: false,
        help: "Return the stored creator address in metadata responses and access logs",
    },
    ConfigField {
        name: "DAYFLY_RATE_LIMIT_PER_MINUTE",
        value: Some("0"),
        required: false,
        help: "Maximum requests per client per minute; 0 disables the limiter",
    },
    ConfigField {
        name: "DAYFLY_SHOW_WORDS",
        value: Some("true"),
        required: false,
        help: "Show generated aliases in the human UI",
    },
    ConfigField {
        name: "DAYFLY_SHOW_INTERNAL_PATHS",
        value: Some("false"),
        required: false,
        help: "Show the internal date and ID path in the paste list",
    },
    ConfigField {
        name: "DAYFLY_INLINE_PDF",
        value: Some("true"),
        required: false,
        help: "Serve PDF downloads inline",
    },
    ConfigField {
        name: "DAYFLY_VIEW_LINE_NUMBERS",
        value: Some("false"),
        required: false,
        help: "Number the lines of a text paste on the view page",
    },
    ConfigField {
        name: "DAYFLY_HTTP_BEARER_TOKEN",
        value: None,
        required: false,
        help: "Optional bearer token for the HTTP endpoint",
    },
    ConfigField {
        name: "DAYFLY_MCP_LOCAL_ROOT",
        value: None,
        required: false,
        help: "Optional directory for the MCP local-path tools",
    },
    ConfigField {
        name: "DAYFLY_FAVICON_PATH",
        value: None,
        required: false,
        help: "Optional path to a favicon served at /favicon.ico",
    },
    ConfigField {
        name: "DAYFLY_ROBOTS_TXT_PATH",
        value: None,
        required: false,
        help: "Optional path to a robots.txt file served at /robots.txt",
    },
    ConfigField {
        name: "DAYFLY_SECURITY_TXT_PATH",
        value: None,
        required: false,
        help: "Optional path to a security.txt file served at /.well-known/security.txt",
    },
    ConfigField {
        name: "DAYFLY_SITE_NAME",
        value: None,
        required: false,
        help: "Optional site name for the human UI footer; replaces the default internal-use text",
    },
    ConfigField {
        name: "DAYFLY_CONTACT",
        value: None,
        required: false,
        help: "Optional takedown contact for the human UI footer, an email address or an http(s) URL",
    },
    ConfigField {
        name: "DAYFLY_AUP_URL",
        value: None,
        required: false,
        help: "Optional acceptable-use URL for the human UI footer",
    },
];

fn field_default(name: &str) -> Option<&'static str> {
    CONFIG_FIELDS
        .iter()
        .find(|field| field.name == name)
        .filter(|field| !field.required)
        .and_then(|field| field.value)
}

fn field_u64(name: &str, fallback: u64) -> u64 {
    field_default(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn field_u32(name: &str, fallback: u32) -> u32 {
    field_default(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    Missing(String),
    #[error("invalid value for {0}: {1}")]
    Invalid(String, String),
    #[error("S3_BUCKET must not be empty")]
    EmptyBucket,
    #[error("S3_ENDPOINT must be an http:// or https:// URL")]
    BadEndpoint,
    #[error("{variable} and {file_variable} are both set; set only one of them")]
    ConflictingCredentialSources {
        variable: String,
        file_variable: String,
    },
    #[error("cannot read {file_variable} file {path}: {source}")]
    CredentialFileRead {
        file_variable: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{file_variable} file {path} is empty")]
    EmptyCredentialFile { file_variable: String, path: String },
    #[error("cannot read config file {path}: {source}")]
    ConfigFileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config file {path} at line {line}: {message}")]
    ConfigFileParse {
        path: String,
        line: usize,
        message: String,
    },
}

fn required<F>(lookup: &F, name: &str) -> Result<String, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
        _ => Err(ConfigError::Missing(name.to_string())),
    }
}

fn optional_string<F>(lookup: &F, name: &str, default: &str) -> String
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(v) if !v.trim().is_empty() => v.trim().to_string(),
        _ => default.to_string(),
    }
}

/// Read an optional free-text setting.
///
/// An empty value means unset. A value above `max_chars` or one with a
/// control character is an error.
fn optional_text<F>(lookup: &F, name: &str, max_chars: usize) -> Result<Option<String>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.chars().count() > max_chars {
        return Err(ConfigError::Invalid(
            name.to_string(),
            format!("expected at most {max_chars} characters"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(ConfigError::Invalid(
            name.to_string(),
            "control characters are not allowed".to_string(),
        ));
    }
    Ok(Some(value.to_string()))
}

/// Read an optional absolute URL that uses HTTP or HTTPS.
fn optional_url<F>(lookup: &F, name: &str, max_chars: usize) -> Result<Option<String>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(value) = optional_text(lookup, name, max_chars)? else {
        return Ok(None);
    };
    if !(value.starts_with("http://") || value.starts_with("https://")) {
        return Err(ConfigError::Invalid(
            name.to_string(),
            "expected an http:// or https:// URL".to_string(),
        ));
    }
    Ok(Some(value))
}

fn optional_u64<F>(lookup: &F, name: &str, default: u64) -> Result<u64, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(v) if !v.trim().is_empty() => v.trim().parse::<u64>().map_err(|_| {
            ConfigError::Invalid(name.to_string(), "expected a non-negative integer".into())
        }),
        _ => Ok(default),
    }
}

fn optional_u32<F>(lookup: &F, name: &str, default: u32) -> Result<u32, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(v) if !v.trim().is_empty() => v.trim().parse::<u32>().map_err(|_| {
            ConfigError::Invalid(name.to_string(), "expected a non-negative integer".into())
        }),
        _ => Ok(default),
    }
}

/// Parse and check `DAYFLY_TTL_CHOICES`.
///
/// Every choice must be from `min_ttl_days` (at least 1) to `max_ttl_days`.
/// The default TTL is added when it is absent. The result is sorted and
/// has no duplicates.
fn ttl_choices<F>(
    lookup: &F,
    default_ttl_days: u32,
    min_ttl_days: u32,
    max_ttl_days: u32,
) -> Result<Vec<u32>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let min = min_ttl_days.max(1);
    let raw = lookup("DAYFLY_TTL_CHOICES")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "1,3,7,14,30,90".to_string());
    let mut choices = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(ConfigError::Invalid(
                "DAYFLY_TTL_CHOICES".to_string(),
                "expected a comma-separated list of whole days".to_string(),
            ));
        }
        let days: u32 = part.parse().map_err(|_| {
            ConfigError::Invalid(
                "DAYFLY_TTL_CHOICES".to_string(),
                "expected a comma-separated list of whole days".to_string(),
            )
        })?;
        if days < min || days > max_ttl_days {
            return Err(ConfigError::Invalid(
                "DAYFLY_TTL_CHOICES".to_string(),
                format!("each value must be from {min} to {max_ttl_days}"),
            ));
        }
        choices.push(days);
    }
    if choices.is_empty() {
        return Err(ConfigError::Invalid(
            "DAYFLY_TTL_CHOICES".to_string(),
            "expected at least one value".to_string(),
        ));
    }
    if !choices.contains(&default_ttl_days) {
        choices.push(default_ttl_days);
    }
    choices.sort_unstable();
    choices.dedup();
    Ok(choices)
}

/// Parse a boolean setting. Accepts the usual spellings so an env file can
/// use `true`/`false`, `1`/`0`, `yes`/`no`, or `on`/`off`.
pub fn parse_bool_flag(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn optional_bool<F>(lookup: &F, name: &str, default: bool) -> Result<bool, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(v) if !v.trim().is_empty() => parse_bool_flag(&v)
            .ok_or_else(|| ConfigError::Invalid(name.to_string(), "expected true or false".into())),
        _ => Ok(default),
    }
}

/// Parse the trusted-proxy list.
///
/// An unset or empty value means no proxy is trusted, which keeps the
/// socket peer as the client. Each entry is one CIDR block or one bare
/// address.
fn trusted_proxies<F>(lookup: &F) -> Result<Vec<Cidr>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    cidr_list(lookup, "DAYFLY_TRUSTED_PROXIES")
}

/// Parse a comma-separated CIDR list.
///
/// An unset or empty value means an empty list. Each entry is one CIDR
/// block or one bare address.
fn cidr_list<F>(lookup: &F, name: &str) -> Result<Vec<Cidr>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(value) = lookup(name) else {
        return Ok(Vec::new());
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut networks = Vec::new();
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(ConfigError::Invalid(
                name.to_string(),
                "expected a comma-separated list of CIDR blocks".to_string(),
            ));
        }
        let cidr = part.parse::<Cidr>().map_err(|error| {
            ConfigError::Invalid(name.to_string(), format!("`{part}`: {error}"))
        })?;
        networks.push(cidr);
    }
    Ok(networks)
}

/// Parse `DAYFLY_LOG_FORMAT`.
fn log_format<F>(lookup: &F) -> Result<LogFormat, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup("DAYFLY_LOG_FORMAT") {
        Some(value) if !value.trim().is_empty() => LogFormat::parse(&value).ok_or_else(|| {
            ConfigError::Invalid(
                "DAYFLY_LOG_FORMAT".to_string(),
                "expected text or json".to_string(),
            )
        }),
        _ => Ok(LogFormat::Text),
    }
}

/// Read the log format before the subscriber starts.
///
/// The value comes from the process environment or the environment file.
/// The full configuration is loaded after the subscriber exists, so the
/// format can be applied to the first log line.
pub fn log_format_from_env_file(path: Option<&Path>) -> Result<LogFormat, ConfigError> {
    let file_values = match path {
        Some(path) if path.exists() => Some(read_env_file(path)?),
        _ => None,
    };
    let lookup = |name: &str| {
        env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                file_values
                    .as_ref()
                    .and_then(|values| values.get(name).cloned())
            })
    };
    log_format(&lookup)
}

/// Parse an optional IP address and port.
fn optional_socket_addr<F>(lookup: &F, name: &str) -> Result<Option<SocketAddr>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(name) {
        Some(value) if !value.trim().is_empty() => {
            value.trim().parse::<SocketAddr>().map(Some).map_err(|_| {
                ConfigError::Invalid(
                    name.to_string(),
                    "expected an IP address and port, for example 127.0.0.1:9091".to_string(),
                )
            })
        }
        _ => Ok(None),
    }
}

/// Read an S3 credential from a direct variable or from a file.
///
/// A credential file read error names the variable and the path only. The
/// error never contains the file contents.
fn credential<F>(lookup: &F, name: &str) -> Result<String, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let file_variable = format!("{name}_FILE");
    let direct = lookup(name).filter(|value| !value.trim().is_empty());
    let file_path = lookup(&file_variable).filter(|value| !value.trim().is_empty());
    match (direct, file_path) {
        (Some(_), Some(_)) => Err(ConfigError::ConflictingCredentialSources {
            variable: name.to_string(),
            file_variable,
        }),
        (Some(value), None) => Ok(value.trim().to_string()),
        (None, Some(path)) => read_credential_file(&file_variable, path.trim()),
        (None, None) => Err(ConfigError::Missing(format!("{name} or {file_variable}"))),
    }
}

/// Read an optional credential from a direct value or a file.
///
/// A direct value is returned even when it is blank. The caller decides
/// whether a set-but-blank value is an error; for the bearer token it is.
fn optional_credential<F>(lookup: &F, name: &str) -> Result<Option<String>, ConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let file_variable = format!("{name}_FILE");
    let direct = lookup(name);
    let file_path = lookup(&file_variable).filter(|value| !value.trim().is_empty());
    match (direct, file_path) {
        (Some(_), Some(_)) => Err(ConfigError::ConflictingCredentialSources {
            variable: name.to_string(),
            file_variable,
        }),
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => read_credential_file(&file_variable, path.trim()).map(Some),
        (None, None) => Ok(None),
    }
}

fn read_credential_file(file_variable: &str, path: &str) -> Result<String, ConfigError> {
    let map_error = |source| ConfigError::CredentialFileRead {
        file_variable: file_variable.to_string(),
        path: path.to_string(),
        source,
    };
    let mut file = std::fs::File::open(path).map_err(map_error)?;
    let metadata = file.metadata().map_err(map_error)?;
    if let Some(warning) = credential_file_warning(path, &metadata) {
        tracing::warn!("{warning}");
    }
    warn_on_loose_credential_directory(path);
    let mut contents = String::new();
    file.read_to_string(&mut contents).map_err(map_error)?;
    let value = contents.trim();
    if value.is_empty() {
        return Err(ConfigError::EmptyCredentialFile {
            file_variable: file_variable.to_string(),
            path: path.to_string(),
        });
    }
    Ok(value.to_string())
}

/// Resolve relative secret-file paths against the env file directory.
fn resolve_secret_paths(values: &mut HashMap<String, String>, base: Option<&Path>) {
    let Some(base) = base else {
        return;
    };
    let base = if base.as_os_str().is_empty() {
        Path::new(".")
    } else {
        base
    };
    for name in [
        "S3_ACCESS_KEY_ID_FILE",
        "S3_SECRET_ACCESS_KEY_FILE",
        "DAYFLY_HTTP_BEARER_TOKEN_FILE",
    ] {
        if let Some(value) = values.get_mut(name) {
            let trimmed = value.trim();
            if !trimmed.is_empty() && Path::new(trimmed).is_relative() {
                *value = base.join(trimmed).to_string_lossy().into_owned();
            }
        }
    }
}

/// Read one environment file.
pub fn load_env_file(path: &Path) -> Result<HashMap<String, String>, ConfigError> {
    read_env_file(path)
}

/// Read one environment file.
fn read_env_file(path: &Path) -> Result<HashMap<String, String>, ConfigError> {
    let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::ConfigFileRead {
        path: path.display().to_string(),
        source,
    })?;
    parse_env_file(&contents, path)
}

/// Parse `NAME=VALUE` lines into a map.
fn parse_env_file(contents: &str, path: &Path) -> Result<HashMap<String, String>, ConfigError> {
    let mut values = HashMap::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let Some((name, value)) = line.split_once('=') else {
            return Err(ConfigError::ConfigFileParse {
                path: path.display().to_string(),
                line: index + 1,
                message: "expected NAME=VALUE".to_string(),
            });
        };
        let name = name.trim();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(ConfigError::ConfigFileParse {
                path: path.display().to_string(),
                line: index + 1,
                message: format!("invalid variable name {name:?}"),
            });
        }
        let mut value = value.trim();
        if value.len() >= 2 {
            let bytes = value.as_bytes();
            let quoted = (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
                || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'');
            if quoted {
                value = &value[1..value.len() - 1];
            }
        }
        values.insert(name.to_string(), value.to_string());
    }
    Ok(values)
}

/// Build the mode and owner warnings for one environment file.
///
/// The environment file can hold the S3 credentials and the bearer token,
/// so it gets the same mode and owner check as a credential file. The
/// warnings never include the file contents.
pub fn environment_file_warnings(path: &Path) -> Vec<String> {
    credential_warnings(&path.display().to_string())
}

/// Build the mode and owner warnings for one credential file.
///
/// The warnings never include the file contents. A missing file returns no
/// warning; the caller reports the read error separately.
#[cfg(unix)]
pub fn credential_warnings(path: &str) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Ok(metadata) = std::fs::metadata(path) {
        if let Some(warning) = credential_file_warning(path, &metadata) {
            warnings.push(warning);
        }
    }
    let parent = std::path::Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    if let Some(warning) = credential_directory_warning(parent) {
        warnings.push(warning);
    }
    warnings
}

#[cfg(not(unix))]
pub fn credential_warnings(_path: &str) -> Vec<String> {
    Vec::new()
}

/// Return true when the bind address accepts connections from this host only.
///
/// The check reads the host part of `host:port`. It does not resolve names,
/// so only the literal `localhost` and a loopback IP address count as local.
/// Any other value counts as exposed, which is the safe answer for a failed
/// parse.
pub fn bind_is_loopback(bind: &str) -> bool {
    let Some((host, _port)) = bind.rsplit_once(':') else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

/// Refuse an exposed bind that has no authentication in front of it.
///
/// Three states. A loopback bind needs no token. A non-loopback bind needs a
/// token. `DAYFLY_ALLOW_UNAUTHENTICATED` accepts an exposed bind with no
/// token on purpose, for a deployment behind an authenticating proxy.
///
/// Without the token check, one environment change turns every paste in the
/// bucket into a read, append, and delete target for any host that can reach
/// the port. The service would not warn.
pub fn ensure_bind_is_safe(
    bind: &str,
    has_token: bool,
    allow_unauthenticated: bool,
) -> Result<(), ConfigError> {
    if bind_is_loopback(bind) || has_token || allow_unauthenticated {
        return Ok(());
    }
    Err(ConfigError::Invalid(
        bind.to_string(),
        "this bind address is reachable from other hosts and \
         DAYFLY_HTTP_BEARER_TOKEN is unset. Set the token, bind to 127.0.0.1, \
         or set DAYFLY_ALLOW_UNAUTHENTICATED=1 to accept an unauthenticated \
         service on this network"
            .to_string(),
    ))
}

/// Parse the optional HTTP bearer token.
///
/// An unset value means "no token". A set value with only ASCII
/// whitespace is invalid: the variable is present, but it would leave
/// the service unauthenticated while it looks protected. A non-empty
/// value that contains ASCII whitespace is also invalid: the
/// `Authorization` parser rejects such a credential, so startup must
/// fail instead of locking out every request at runtime. The value is
/// not trimmed into a different token.
fn bearer_token(value: Option<String>) -> Result<Option<String>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "DAYFLY_HTTP_BEARER_TOKEN".to_string(),
            "the variable is set but empty; unset it to disable authentication".to_string(),
        ));
    }
    if value.contains(|c: char| c.is_ascii_whitespace()) {
        return Err(ConfigError::Invalid(
            "DAYFLY_HTTP_BEARER_TOKEN".to_string(),
            "expected a token with no ASCII whitespace".to_string(),
        ));
    }
    Ok(Some(value))
}

/// Parse the optional MCP local-file root.
///
/// The value must name an existing directory. The result is canonical,
/// because the path check compares real paths after resolving symlinks.
/// A relative value resolves against the working directory.
fn local_root(value: Option<String>) -> Result<Option<PathBuf>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let resolved = std::fs::canonicalize(value).map_err(|e| {
        ConfigError::Invalid(
            "DAYFLY_MCP_LOCAL_ROOT".to_string(),
            format!("expected an existing directory: {e}"),
        )
    })?;
    if !resolved.is_dir() {
        return Err(ConfigError::Invalid(
            "DAYFLY_MCP_LOCAL_ROOT".to_string(),
            "expected an existing directory".to_string(),
        ));
    }
    Ok(Some(resolved))
}

/// Largest favicon accepted at startup.
const MAX_FAVICON_BYTES: usize = 1024 * 1024;

/// Largest text asset accepted at startup.
const MAX_TEXT_ASSET_BYTES: usize = 1024 * 1024;

/// Load an optional UTF-8 text file served at a fixed path.
fn text_asset(value: Option<String>, name: &str) -> Result<Option<TextAsset>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let path = Path::new(value);
    let bytes = std::fs::read(path)
        .map_err(|e| ConfigError::Invalid(name.to_string(), format!("cannot read {value}: {e}")))?;
    if bytes.is_empty() {
        return Err(ConfigError::Invalid(
            name.to_string(),
            "the file is empty".to_string(),
        ));
    }
    if bytes.len() > MAX_TEXT_ASSET_BYTES {
        return Err(ConfigError::Invalid(
            name.to_string(),
            format!("the file is larger than {MAX_TEXT_ASSET_BYTES} bytes"),
        ));
    }
    std::str::from_utf8(&bytes)
        .map_err(|_| ConfigError::Invalid(name.to_string(), "the file is not UTF-8".to_string()))?;
    Ok(Some(TextAsset {
        bytes: Arc::from(bytes.into_boxed_slice()),
    }))
}

/// Load the optional favicon file and pick its response content type.
fn favicon(value: Option<String>) -> Result<Option<Favicon>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let path = Path::new(value);
    let bytes = std::fs::read(path).map_err(|e| {
        ConfigError::Invalid(
            "DAYFLY_FAVICON_PATH".to_string(),
            format!("cannot read {value}: {e}"),
        )
    })?;
    if bytes.is_empty() {
        return Err(ConfigError::Invalid(
            "DAYFLY_FAVICON_PATH".to_string(),
            "the file is empty".to_string(),
        ));
    }
    if bytes.len() > MAX_FAVICON_BYTES {
        return Err(ConfigError::Invalid(
            "DAYFLY_FAVICON_PATH".to_string(),
            format!("the file is larger than {MAX_FAVICON_BYTES} bytes"),
        ));
    }
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    let content_type = match extension.as_deref() {
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("gif") => "image/gif",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        _ => "image/x-icon",
    };
    Ok(Some(Favicon {
        bytes: Arc::from(bytes.into_boxed_slice()),
        content_type,
    }))
}

/// Build a warning for unsafe credential file metadata.
///
/// The warning names the path, the mode, and the owner IDs. It never
/// contains the file contents. A group or world read or write bit is
/// unsafe. A file owner other than the effective user is unsafe.
#[cfg(unix)]
fn credential_file_warning(path: &str, metadata: &std::fs::Metadata) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    credential_file_warning_parts(path, metadata.mode(), metadata.uid(), effective_uid())
}

#[cfg(unix)]
fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn credential_file_warning_parts(
    path: &str,
    mode: u32,
    owner: u32,
    effective_uid: u32,
) -> Option<String> {
    let mode = mode & 0o777;
    let loose_mode = mode & 0o066 != 0;
    let foreign_owner = owner != effective_uid;
    match (loose_mode, foreign_owner) {
        (false, false) => None,
        (true, false) => Some(format!(
            "credential file {path} has mode {mode:04o}. Group or world read or write access can expose or replace the key. Set mode 0600."
        )),
        (false, true) => Some(format!(
            "credential file {path} has mode {mode:04o} and is owned by uid {owner}. The effective uid is {effective_uid}. Make the effective user the owner."
        )),
        (true, true) => Some(format!(
            "credential file {path} has mode {mode:04o} and owner uid {owner}. The effective uid is {effective_uid}. Group or world read or write access can expose or replace the key. Set mode 0600 and make the effective user the owner."
        )),
    }
}

#[cfg(not(unix))]
fn credential_file_warning(_path: &str, _metadata: &std::fs::Metadata) -> Option<String> {
    None
}

/// Warn on an unsafe parent directory of a credential file.
///
/// A group or other user with access to the directory can replace the
/// credential file. A parent directory that cannot be read is skipped.
#[cfg(unix)]
fn warn_on_loose_credential_directory(path: &str) {
    let Some(parent) = std::path::Path::new(path).parent() else {
        return;
    };
    // A bare relative path has an empty parent. Check the current directory.
    let parent = if parent.as_os_str().is_empty() {
        std::path::Path::new(".")
    } else {
        parent
    };
    if let Some(warning) = credential_directory_warning(parent) {
        tracing::warn!("{warning}");
    }
}

#[cfg(not(unix))]
fn warn_on_loose_credential_directory(_path: &str) {}

/// Build a warning for unsafe credential directory metadata.
///
/// The warning names the path, the mode, and the owner IDs. A group or
/// world permission can let another user replace the credential file.
#[cfg(unix)]
fn credential_directory_warning(path: &std::path::Path) -> Option<String> {
    use std::os::unix::fs::MetadataExt;

    let metadata = std::fs::metadata(path).ok()?;
    credential_directory_warning_parts(
        &path.display().to_string(),
        metadata.mode(),
        metadata.uid(),
        effective_uid(),
    )
}

#[cfg(unix)]
fn credential_directory_warning_parts(
    path: &str,
    mode: u32,
    owner: u32,
    effective_uid: u32,
) -> Option<String> {
    let mode = mode & 0o777;
    let loose_mode = mode & 0o077 != 0;
    let foreign_owner = owner != effective_uid;
    match (loose_mode, foreign_owner) {
        (false, false) => None,
        (true, false) => Some(format!(
            "credential directory {path} has mode {mode:04o}. Group or world access to the directory can expose or replace the credential file. Set mode 0700."
        )),
        (false, true) => Some(format!(
            "credential directory {path} has mode {mode:04o} and is owned by uid {owner}. The effective uid is {effective_uid}. Make the effective user the owner."
        )),
        (true, true) => Some(format!(
            "credential directory {path} has mode {mode:04o} and owner uid {owner}. The effective uid is {effective_uid}. Group or world access to the directory can expose or replace the credential file. Set mode 0700 and make the effective user the owner."
        )),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    /// Build a configuration from the process environment and an optional
    /// environment file.
    ///
    /// A process environment variable wins over the same name in the file.
    /// The file format is one `NAME=VALUE` pair per line. Blank lines and
    /// lines that start with `#` are ignored. A leading `export` and one
    /// pair of matching single or double quotes are accepted. A relative
    /// secret-file path in the file resolves against the file directory.
    pub fn from_env_file(path: Option<&Path>) -> Result<Self, ConfigError> {
        let file_values = match path {
            Some(path) => {
                let mut values = read_env_file(path)?;
                resolve_secret_paths(&mut values, path.parent());
                Some(values)
            }
            None => None,
        };
        Self::from_lookup(|name| {
            env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    file_values
                        .as_ref()
                        .and_then(|values| values.get(name).cloned())
                })
        })
    }

    /// Files that a reload watch must follow.
    ///
    /// The result starts with `env_file`. It adds each secret file that
    /// the process environment or the file selects. A relative secret
    /// path in the file resolves against the file directory, as at
    /// startup.
    pub fn watch_paths(env_file: &Path) -> Vec<PathBuf> {
        let mut paths = vec![env_file.to_path_buf()];
        let Ok(mut values) = read_env_file(env_file) else {
            return paths;
        };
        resolve_secret_paths(&mut values, env_file.parent());
        let lookup = |name: &str| {
            env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| values.get(name).cloned())
        };
        for name in [
            "S3_ACCESS_KEY_ID_FILE",
            "S3_SECRET_ACCESS_KEY_FILE",
            "DAYFLY_HTTP_BEARER_TOKEN_FILE",
        ] {
            if let Some(path) = lookup(name) {
                let path = PathBuf::from(path.trim());
                if !path.as_os_str().is_empty() && !paths.contains(&path) {
                    paths.push(path);
                }
            }
        }
        paths
    }

    /// Build a configuration from a name-to-value lookup.
    ///
    /// `from_env` passes the process environment. Unit tests pass a plain
    /// map, so they do not change global process state.
    fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let endpoint = required(&lookup, "S3_ENDPOINT")?;
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(ConfigError::BadEndpoint);
        }
        let bucket = required(&lookup, "S3_BUCKET")?;
        if bucket.is_empty() {
            return Err(ConfigError::EmptyBucket);
        }
        let log_format = log_format(&lookup)?;
        let metrics_bind = optional_socket_addr(&lookup, "DAYFLY_METRICS_BIND")?;
        let metrics_allowed_networks = cidr_list(&lookup, "DAYFLY_METRICS_ALLOWED_NETWORKS")?;
        if let Some(bind) = metrics_bind {
            if !bind.ip().is_loopback() && metrics_allowed_networks.is_empty() {
                return Err(ConfigError::Invalid(
                    "DAYFLY_METRICS_ALLOWED_NETWORKS".to_string(),
                    format!("required when DAYFLY_METRICS_BIND ({bind}) is not a loopback address"),
                ));
            }
        }
        let min_ttl_days = optional_u32(&lookup, "DAYFLY_MIN_TTL_DAYS", 0)?;
        let max_ttl_days = optional_u32(&lookup, "DAYFLY_MAX_TTL_DAYS", MAX_TTL_DAYS)?;
        if max_ttl_days == 0 || max_ttl_days > MAX_TTL_DAYS {
            return Err(ConfigError::Invalid(
                "DAYFLY_MAX_TTL_DAYS".to_string(),
                format!("expected an integer from 1 to {MAX_TTL_DAYS}"),
            ));
        }
        if min_ttl_days > max_ttl_days {
            return Err(ConfigError::Invalid(
                "DAYFLY_MIN_TTL_DAYS".to_string(),
                "must not exceed DAYFLY_MAX_TTL_DAYS".to_string(),
            ));
        }
        let default_ttl_days = optional_u32(
            &lookup,
            "DAYFLY_DEFAULT_TTL_DAYS",
            field_u32("DAYFLY_DEFAULT_TTL_DAYS", 7),
        )?;
        if default_ttl_days == 0 || default_ttl_days < min_ttl_days.max(1) {
            return Err(ConfigError::Invalid(
                "DAYFLY_DEFAULT_TTL_DAYS".to_string(),
                format!(
                    "expected an integer from {} to {max_ttl_days}",
                    min_ttl_days.max(1)
                ),
            ));
        }
        if default_ttl_days > max_ttl_days {
            return Err(ConfigError::Invalid(
                "DAYFLY_DEFAULT_TTL_DAYS".to_string(),
                format!("expected at most {max_ttl_days} days"),
            ));
        }
        let ttl_choices = ttl_choices(&lookup, default_ttl_days, min_ttl_days, max_ttl_days)?;
        let list_default_days = optional_u32(
            &lookup,
            "DAYFLY_LIST_DEFAULT_DAYS",
            field_u32("DAYFLY_LIST_DEFAULT_DAYS", 2),
        )?;
        if list_default_days == 0 || list_default_days as usize > crate::store::LIST_WINDOW_MAX_DAYS
        {
            return Err(ConfigError::Invalid(
                "DAYFLY_LIST_DEFAULT_DAYS".to_string(),
                format!(
                    "expected an integer from 1 to {}",
                    crate::store::LIST_WINDOW_MAX_DAYS
                ),
            ));
        }
        let retention_min_days = optional_u32(
            &lookup,
            "DAYFLY_RETENTION_MIN_DAYS",
            field_u32("DAYFLY_RETENTION_MIN_DAYS", 30),
        )?;
        if retention_min_days == 0 || retention_min_days > MAX_TTL_DAYS {
            return Err(ConfigError::Invalid(
                "DAYFLY_RETENTION_MIN_DAYS".to_string(),
                format!("expected an integer from 1 to {MAX_TTL_DAYS}"),
            ));
        }
        let retention_max_days = optional_u32(
            &lookup,
            "DAYFLY_RETENTION_MAX_DAYS",
            field_u32("DAYFLY_RETENTION_MAX_DAYS", 365),
        )?;
        if retention_max_days == 0 || retention_max_days > MAX_TTL_DAYS {
            return Err(ConfigError::Invalid(
                "DAYFLY_RETENTION_MAX_DAYS".to_string(),
                format!("expected an integer from 1 to {MAX_TTL_DAYS}"),
            ));
        }
        if retention_min_days > retention_max_days {
            return Err(ConfigError::Invalid(
                "DAYFLY_RETENTION_MIN_DAYS".to_string(),
                "must not exceed DAYFLY_RETENTION_MAX_DAYS".to_string(),
            ));
        }
        let max_file_bytes = optional_u64(
            &lookup,
            "DAYFLY_MAX_FILE_BYTES",
            field_u64("DAYFLY_MAX_FILE_BYTES", 1024 * 1024 * 1024),
        )?;
        if max_file_bytes > MAX_MULTIPART_BYTES {
            return Err(ConfigError::Invalid(
                "DAYFLY_MAX_FILE_BYTES".to_string(),
                format!("expected at most {MAX_MULTIPART_BYTES} bytes: 10,000 parts of 5 MiB"),
            ));
        }
        let search_max_bytes = optional_u64(
            &lookup,
            "DAYFLY_SEARCH_MAX_BYTES",
            field_u64("DAYFLY_SEARCH_MAX_BYTES", 64 * 1024 * 1024),
        )?;
        if search_max_bytes == 0 {
            return Err(ConfigError::Invalid(
                "DAYFLY_SEARCH_MAX_BYTES".to_string(),
                "expected a positive number of bytes".to_string(),
            ));
        }
        let alias_words = optional_u32(
            &lookup,
            "DAYFLY_ALIAS_WORDS",
            u32::from(DEFAULT_ALIAS_WORDS),
        )?;
        if !(u32::from(MIN_ALIAS_WORDS)..=u32::from(MAX_ALIAS_WORDS)).contains(&alias_words) {
            return Err(ConfigError::Invalid(
                "DAYFLY_ALIAS_WORDS".to_string(),
                format!("expected an integer from {MIN_ALIAS_WORDS} to {MAX_ALIAS_WORDS}"),
            ));
        }
        // The range check above keeps this conversion in bounds.
        let alias_words = u8::try_from(alias_words).unwrap_or(DEFAULT_ALIAS_WORDS);
        Ok(Self {
            endpoint,
            bucket,
            // Region is a signing parameter for S3-compatible stores such
            // as Garage. It is not hardcoded: the operator supplies it.
            region: optional_string(
                &lookup,
                "S3_REGION",
                field_default("S3_REGION").unwrap_or("us-east-1"),
            ),
            access_key_id: credential(&lookup, "S3_ACCESS_KEY_ID")?,
            secret_access_key: credential(&lookup, "S3_SECRET_ACCESS_KEY")?,
            max_text_bytes: optional_u64(
                &lookup,
                "DAYFLY_MAX_TEXT_BYTES",
                field_u64("DAYFLY_MAX_TEXT_BYTES", 100 * 1024 * 1024),
            )?,
            max_file_bytes,
            max_paste_bytes: optional_u64(
                &lookup,
                "DAYFLY_MAX_PASTE_BYTES",
                field_u64("DAYFLY_MAX_PASTE_BYTES", 1024 * 1024 * 1024),
            )?,
            search_max_bytes,
            alias_words,
            default_ttl_days,
            min_ttl_days,
            max_ttl_days,
            ttl_choices,
            list_default_days,
            retention_by_size: optional_bool(
                &lookup,
                "DAYFLY_RETENTION_BY_SIZE",
                field_default("DAYFLY_RETENTION_BY_SIZE") == Some("true"),
            )?,
            retention_min_days,
            retention_max_days,
            http_bind: optional_string(
                &lookup,
                "DAYFLY_HTTP_BIND",
                field_default("DAYFLY_HTTP_BIND").unwrap_or("127.0.0.1:8080"),
            ),
            log_format,
            metrics_bind,
            metrics_allowed_networks,
            trusted_proxies: trusted_proxies(&lookup)?,
            show_words: optional_bool(
                &lookup,
                "DAYFLY_SHOW_WORDS",
                field_default("DAYFLY_SHOW_WORDS") == Some("true"),
            )?,
            show_internal_paths: optional_bool(
                &lookup,
                "DAYFLY_SHOW_INTERNAL_PATHS",
                field_default("DAYFLY_SHOW_INTERNAL_PATHS") == Some("true"),
            )?,
            inline_pdf: optional_bool(
                &lookup,
                "DAYFLY_INLINE_PDF",
                field_default("DAYFLY_INLINE_PDF") == Some("true"),
            )?,
            view_line_numbers: optional_bool(
                &lookup,
                "DAYFLY_VIEW_LINE_NUMBERS",
                field_default("DAYFLY_VIEW_LINE_NUMBERS") == Some("true"),
            )?,
            allow_unauthenticated: optional_bool(
                &lookup,
                "DAYFLY_ALLOW_UNAUTHENTICATED",
                field_default("DAYFLY_ALLOW_UNAUTHENTICATED") == Some("true"),
            )?,
            list_requires_token: optional_bool(
                &lookup,
                "DAYFLY_LIST_REQUIRES_TOKEN",
                field_default("DAYFLY_LIST_REQUIRES_TOKEN") == Some("true"),
            )?,
            list_enabled: optional_bool(
                &lookup,
                "DAYFLY_LIST_ENABLED",
                field_default("DAYFLY_LIST_ENABLED") != Some("false"),
            )?,
            paste_tokens: optional_bool(
                &lookup,
                "DAYFLY_PASTE_TOKENS",
                field_default("DAYFLY_PASTE_TOKENS") == Some("true"),
            )?,
            expose_creator_addr: optional_bool(
                &lookup,
                "DAYFLY_EXPOSE_CREATOR_ADDR",
                field_default("DAYFLY_EXPOSE_CREATOR_ADDR") == Some("true"),
            )?,
            rate_limit_per_minute: optional_u32(
                &lookup,
                "DAYFLY_RATE_LIMIT_PER_MINUTE",
                field_u32("DAYFLY_RATE_LIMIT_PER_MINUTE", 0),
            )?,
            http_bearer_token: bearer_token(optional_credential(
                &lookup,
                "DAYFLY_HTTP_BEARER_TOKEN",
            )?)?,
            mcp_local_root: local_root(lookup("DAYFLY_MCP_LOCAL_ROOT"))?,
            favicon: favicon(lookup("DAYFLY_FAVICON_PATH"))?,
            robots_txt: text_asset(lookup("DAYFLY_ROBOTS_TXT_PATH"), "DAYFLY_ROBOTS_TXT_PATH")?,
            security_txt: text_asset(
                lookup("DAYFLY_SECURITY_TXT_PATH"),
                "DAYFLY_SECURITY_TXT_PATH",
            )?,
            site_name: optional_text(&lookup, "DAYFLY_SITE_NAME", MAX_SITE_NAME_CHARS)?,
            contact: optional_text(&lookup, "DAYFLY_CONTACT", MAX_CONTACT_CHARS)?,
            aup_url: optional_url(&lookup, "DAYFLY_AUP_URL", MAX_AUP_URL_CHARS)?,
        })
    }

    /// Effective text cap when this config creates a paste.
    ///
    /// The result is the lower of [`Self::max_text_bytes`] and
    /// [`Self::max_paste_bytes`]. A total cap below the text cap also
    /// applies at creation.
    pub fn max_text_paste_bytes(&self) -> u64 {
        self.max_text_bytes.min(self.max_paste_bytes)
    }

    /// Effective file cap when this config creates a paste.
    ///
    /// The result is the lower of [`Self::max_file_bytes`] and
    /// [`Self::max_paste_bytes`]. A total cap below the file cap also
    /// applies at creation.
    pub fn max_file_paste_bytes(&self) -> u64 {
        self.max_file_bytes.min(self.max_paste_bytes)
    }

    /// Cap for the first write of an HTTP upload, decided before the body
    /// is streamed.
    ///
    /// A filename (`X-Filename`, `?filename=`, or a multipart filename)
    /// means a file upload and gets [`Self::max_file_paste_bytes`]. No
    /// filename means a text paste and gets [`Self::max_text_paste_bytes`].
    /// Content-Type is deliberately not used here: detection needs bytes,
    /// and a file named `report.txt` is still a file upload even though its
    /// bytes look like text.
    pub fn max_bytes_for_upload(&self, filename: Option<&str>) -> u64 {
        match filename {
            Some(name) if !name.trim().is_empty() => self.max_file_paste_bytes(),
            _ => self.max_text_paste_bytes(),
        }
    }

    /// The longest retention, in seconds, that the size curve allows.
    ///
    /// The curve is `min + (max - min) * (1 - size / cap)^3`. The size
    /// and the cap are in bytes. A size at or above the cap gives the
    /// minimum. This function does not check `retention_by_size`; the
    /// caller decides whether the clamp applies.
    pub fn retention_seconds(&self, size: u64) -> i64 {
        let min_days = i64::from(self.retention_min_days);
        let min_seconds = min_days * 86_400;
        if self.max_paste_bytes == 0 {
            return min_seconds;
        }
        let span_days = i64::from(
            self.retention_max_days
                .saturating_sub(self.retention_min_days),
        );
        let fraction = (size.min(self.max_paste_bytes) as f64) / (self.max_paste_bytes as f64);
        let days = min_days as f64 + span_days as f64 * (1.0 - fraction).powi(3);
        (days * 86_400.0).floor() as i64
    }

    /// A log-safe summary: endpoint host scheme and bucket only, never keys.
    pub fn safe_summary(&self) -> String {
        format!(
            "bucket={} endpoint_scheme={} region={} max_text_bytes={} max_file_bytes={} max_paste_bytes={} search_max_bytes={} alias_words={} default_ttl_days={} list_default_days={} retention_by_size={} retention_min_days={} retention_max_days={} show_words={} show_internal_paths={} inline_pdf={} view_line_numbers={} trusted_proxies={} list_requires_token={} paste_tokens={} expose_creator_addr={} rate_limit_per_minute={} log_format={} metrics_bind={} metrics_allowed_networks={} mcp_local_root={} favicon={} robots_txt={} security_txt={}",
            self.bucket,
            if self.endpoint.starts_with("https://") {
                "https"
            } else {
                "http"
            },
            self.region,
            self.max_text_bytes,
            self.max_file_bytes,
            self.max_paste_bytes,
            self.search_max_bytes,
            self.alias_words,
            self.default_ttl_days,
            self.list_default_days,
            self.retention_by_size,
            self.retention_min_days,
            self.retention_max_days,
            self.show_words,
            self.show_internal_paths,
            self.inline_pdf,
            self.view_line_numbers,
            self.trusted_proxies.len(),
            self.list_requires_token,
            self.paste_tokens,
            self.expose_creator_addr,
            self.rate_limit_per_minute,
            self.log_format.as_str(),
            self.metrics_bind
                .as_ref()
                .map(|bind| bind.to_string())
                .unwrap_or_else(|| "unset".to_string()),
            self.metrics_allowed_networks.len(),
            if self.mcp_local_root.is_some() {
                "set"
            } else {
                "unset"
            },
            if self.favicon.is_some() {
                "set"
            } else {
                "unset"
            },
            if self.robots_txt.is_some() {
                "set"
            } else {
                "unset"
            },
            if self.security_txt.is_some() {
                "set"
            } else {
                "unset"
            }
        )
        // Deliberately no bearer-token presence flag here either: even
        // "auth is on" is not logged, to keep the summary boring.
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn lookup_from(pairs: &[(String, String)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs.iter().cloned().collect();
        move |name| map.get(name).cloned()
    }

    fn base_env() -> Vec<(String, String)> {
        [
            ("S3_ENDPOINT", "http://127.0.0.1:3900"),
            ("S3_BUCKET", "test-bucket"),
            ("S3_ACCESS_KEY_ID", "test-access-key"),
            ("S3_SECRET_ACCESS_KEY", "test-secret-key"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
    }

    fn with_credential_file(
        mut pairs: Vec<(String, String)>,
        direct: &str,
        file: &str,
        path: &str,
    ) -> Vec<(String, String)> {
        pairs.retain(|(name, _)| name != direct);
        pairs.push((file.to_string(), path.to_string()));
        pairs
    }

    fn expect_error(result: Result<Config, ConfigError>) -> ConfigError {
        match result {
            Ok(_) => panic!("expected a configuration error"),
            Err(error) => error,
        }
    }

    fn unique_temp_path() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dayfly-config-test-{}-{count}.txt",
            std::process::id()
        ))
    }

    struct TempCredentialFile {
        path: PathBuf,
    }

    impl TempCredentialFile {
        fn new(contents: &str) -> Self {
            let path = unique_temp_path();
            std::fs::write(&path, contents).expect("write temp credential file");
            Self { path }
        }

        fn path(&self) -> &str {
            self.path.to_str().expect("temp path is UTF-8")
        }

        #[cfg(unix)]
        fn set_mode(&self, mode: u32) {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(mode))
                .expect("set temp credential file mode");
        }
    }

    impl Drop for TempCredentialFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn unique_temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "dayfly-config-test-dir-{}-{count}",
            std::process::id()
        ))
    }

    #[cfg(unix)]
    struct TempEnvFile {
        dir: PathBuf,
        path: PathBuf,
    }

    #[cfg(unix)]
    impl TempEnvFile {
        fn new(contents: &str, mode: u32) -> Self {
            use std::os::unix::fs::PermissionsExt;

            let dir = unique_temp_dir();
            std::fs::create_dir(&dir).expect("create temp config directory");
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .expect("set temp config directory mode");
            let path = dir.join("dayfly.env");
            std::fs::write(&path, contents).expect("write temp env file");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                .expect("set temp env file mode");
            Self { dir, path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    #[cfg(unix)]
    impl Drop for TempEnvFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[cfg(unix)]
    fn credential_metadata(file: &TempCredentialFile) -> std::fs::Metadata {
        std::fs::metadata(file.path()).expect("read temp credential file metadata")
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_with_mode_0600_produces_no_warning() {
        let file = TempCredentialFile::new("private-secret\n");
        file.set_mode(0o600);
        assert_eq!(
            credential_file_warning(file.path(), &credential_metadata(&file)),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn env_file_with_mode_0600_produces_no_warning() {
        let file = TempEnvFile::new("DAYFLY_HTTP_BEARER_TOKEN=secret\n", 0o600);
        assert_eq!(environment_file_warnings(file.path()), Vec::<String>::new());
    }

    #[cfg(unix)]
    #[test]
    fn env_file_with_mode_0644_produces_a_warning() {
        let file = TempEnvFile::new("DAYFLY_HTTP_BEARER_TOKEN=secret\n", 0o644);
        let warnings = environment_file_warnings(file.path());
        assert_eq!(warnings.len(), 1, "warnings: {warnings:?}");
        let warning = &warnings[0];
        assert!(
            warning.contains(file.path().to_str().expect("temp path is UTF-8")),
            "warning: {warning}"
        );
        assert!(warning.contains("0644"), "warning: {warning}");
        assert!(!warning.contains("secret"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_world_readable_file() {
        let file = TempCredentialFile::new("world-readable-secret\n");
        file.set_mode(0o644);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a world-readable credential file");
        assert!(warning.contains(file.path()), "warning: {warning}");
        assert!(warning.contains("0644"), "warning: {warning}");
        assert!(
            !warning.contains("world-readable-secret"),
            "warning: {warning}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_group_readable_file() {
        let file = TempCredentialFile::new("group-readable-secret\n");
        file.set_mode(0o640);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a group-readable credential file");
        assert!(warning.contains("0640"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_world_readable_bit() {
        let file = TempCredentialFile::new("world-readable-secret\n");
        file.set_mode(0o604);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a world-readable credential file");
        assert!(warning.contains("0604"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_world_writable_file() {
        let file = TempCredentialFile::new("world-writable-secret\n");
        file.set_mode(0o622);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a world-writable credential file");
        assert!(warning.contains(file.path()), "warning: {warning}");
        assert!(warning.contains("0622"), "warning: {warning}");
        assert!(
            !warning.contains("world-writable-secret"),
            "warning: {warning}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_group_writable_file() {
        let file = TempCredentialFile::new("group-writable-secret\n");
        file.set_mode(0o660);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a group-writable credential file");
        assert!(warning.contains("0660"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_warning_for_world_writable_bit() {
        let file = TempCredentialFile::new("world-writable-secret\n");
        file.set_mode(0o602);
        let warning = credential_file_warning(file.path(), &credential_metadata(&file))
            .expect("a warning for a world-writable credential file");
        assert!(warning.contains("0602"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_with_execute_bits_only_produces_no_warning() {
        let file = TempCredentialFile::new("private-secret\n");
        file.set_mode(0o700);
        assert_eq!(
            credential_file_warning(file.path(), &credential_metadata(&file)),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_file_owned_by_another_user_produces_a_warning() {
        let warning = credential_file_warning_parts("/tmp/credential", 0o600, 1000, 1001)
            .expect("a warning for a foreign owner");
        assert!(warning.contains("/tmp/credential"), "warning: {warning}");
        assert!(warning.contains("0600"), "warning: {warning}");
        assert!(warning.contains("1000"), "warning: {warning}");
        assert!(warning.contains("1001"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_directory_with_mode_0700_produces_no_warning() {
        assert_eq!(
            credential_directory_warning_parts("/home/you/.config/dayfly", 0o700, 1000, 1000),
            None
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_directory_warning_for_group_or_world_accessible_directory() {
        for mode in [0o755, 0o770, 0o777] {
            let warning =
                credential_directory_warning_parts("/home/you/.config/dayfly", mode, 1000, 1000)
                    .unwrap_or_else(|| panic!("a warning for mode {mode:04o}"));
            assert!(
                warning.contains("/home/you/.config/dayfly"),
                "warning: {warning}"
            );
            assert!(
                warning.contains(&format!("{mode:04o}")),
                "warning: {warning}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn credential_directory_owned_by_another_user_produces_a_warning() {
        let warning =
            credential_directory_warning_parts("/home/you/.config/dayfly", 0o700, 1000, 1001)
                .expect("a warning for a foreign directory owner");
        assert!(warning.contains("0700"), "warning: {warning}");
        assert!(warning.contains("1000"), "warning: {warning}");
        assert!(warning.contains("1001"), "warning: {warning}");
    }

    #[cfg(unix)]
    #[test]
    fn credential_directory_warning_skips_a_missing_parent() {
        let missing_parent = unique_temp_path();
        assert!(!missing_parent.exists());
        let file = missing_parent.join("access-key-id");
        assert_eq!(credential_directory_warning(&file), None);
    }

    fn test_config(max_text_bytes: u64, max_file_bytes: u64) -> Config {
        Config {
            endpoint: "http://127.0.0.1:1".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            max_text_bytes,
            max_file_bytes,
            max_paste_bytes: 1024 * 1024 * 1024,
            search_max_bytes: 64 * 1024 * 1024,
            alias_words: 3,
            default_ttl_days: 7,
            min_ttl_days: 0,
            max_ttl_days: 3650,
            ttl_choices: vec![1, 3, 7, 14, 30, 90],
            list_default_days: 2,
            retention_by_size: false,
            retention_min_days: 30,
            retention_max_days: 365,
            http_bind: "127.0.0.1:0".to_string(),
            log_format: LogFormat::Text,
            metrics_bind: None,
            metrics_allowed_networks: Vec::new(),
            trusted_proxies: Vec::new(),
            show_words: true,
            show_internal_paths: false,
            inline_pdf: true,
            view_line_numbers: false,
            allow_unauthenticated: false,
            list_requires_token: false,
            list_enabled: true,
            paste_tokens: false,
            expose_creator_addr: false,
            rate_limit_per_minute: 0,
            http_bearer_token: None,
            mcp_local_root: None,
            favicon: None,
            robots_txt: None,
            security_txt: None,
            site_name: None,
            contact: None,
            aup_url: None,
        }
    }

    #[test]
    fn upload_cap_follows_filename_presence() {
        let config = test_config(10, 1_000);
        assert_eq!(config.max_bytes_for_upload(None), 10);
        assert_eq!(config.max_bytes_for_upload(Some("")), 10);
        assert_eq!(config.max_bytes_for_upload(Some("   ")), 10);
        assert_eq!(config.max_bytes_for_upload(Some("report.txt")), 1_000);
    }

    #[test]
    fn endpoint_must_be_http_url() {
        let mut pairs = base_env();
        pairs[0].1 = "ftp://127.0.0.1:3900".to_string();
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        assert!(matches!(error, ConfigError::BadEndpoint));
    }

    #[test]
    fn region_and_http_bind_values_are_trimmed() {
        let mut pairs = base_env();
        pairs.push(("S3_REGION".to_string(), " eu-west-1 ".to_string()));
        pairs.push((
            "DAYFLY_HTTP_BIND".to_string(),
            " 127.0.0.1:9090 ".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("trimmed config");
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(config.http_bind, "127.0.0.1:9090");
    }

    #[test]
    fn whitespace_only_region_and_http_bind_use_defaults() {
        let mut pairs = base_env();
        pairs.push(("S3_REGION".to_string(), "   ".to_string()));
        pairs.push(("DAYFLY_HTTP_BIND".to_string(), "\t".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("default config");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.http_bind, "127.0.0.1:8080");
    }

    #[test]
    fn bind_is_loopback_classifies_common_addresses() {
        assert!(bind_is_loopback("127.0.0.1:8080"));
        assert!(bind_is_loopback("[::1]:8080"));
        assert!(bind_is_loopback("localhost:8080"));
        assert!(!bind_is_loopback("0.0.0.0:8080"));
        assert!(!bind_is_loopback("[::]:8080"));
        assert!(!bind_is_loopback("10.11.111.7:8080"));
        assert!(!bind_is_loopback("dayfly.k.ser.ink:8080"));
        assert!(!bind_is_loopback("not an address"));
    }

    #[test]
    fn non_loopback_bind_without_a_token_is_refused() {
        let error = ensure_bind_is_safe("0.0.0.0:8080", false, false)
            .expect_err("a non-loopback bind with no token must be refused");
        let message = error.to_string();
        assert!(
            message.contains("DAYFLY_ALLOW_UNAUTHENTICATED"),
            "the error must name the opt-in: {message}"
        );
        assert!(
            message.contains("0.0.0.0:8080"),
            "the error must name the bind address: {message}"
        );
    }

    #[test]
    fn loopback_bind_without_a_token_is_allowed() {
        ensure_bind_is_safe("127.0.0.1:8080", false, false)
            .expect("a loopback bind needs no token and no opt-in");
    }

    #[test]
    fn non_loopback_bind_with_a_token_is_allowed() {
        ensure_bind_is_safe("0.0.0.0:8080", true, false)
            .expect("a bearer token covers a non-loopback bind");
    }

    #[test]
    fn non_loopback_bind_with_the_opt_in_is_allowed() {
        ensure_bind_is_safe("[::]:8080", false, true)
            .expect("the opt-in accepts an exposed bind on purpose");
    }

    #[test]
    fn allow_unauthenticated_defaults_to_false() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(!config.allow_unauthenticated);
    }

    #[test]
    fn rate_limit_defaults_to_zero() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert_eq!(config.rate_limit_per_minute, 0);
    }

    #[test]
    fn rate_limit_accepts_a_value() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_RATE_LIMIT_PER_MINUTE".to_string(),
            "120".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("rate limit config");
        assert_eq!(config.rate_limit_per_minute, 120);
    }

    #[test]
    fn expose_creator_addr_defaults_to_false_and_accepts_true() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(!config.expose_creator_addr);

        let mut pairs = base_env();
        pairs.push(("DAYFLY_EXPOSE_CREATOR_ADDR".to_string(), "1".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("exposure config");
        assert!(config.expose_creator_addr);
    }

    #[test]
    fn expose_creator_addr_rejects_a_non_boolean() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_EXPOSE_CREATOR_ADDR".to_string(),
            "sometimes".to_string(),
        ));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_EXPOSE_CREATOR_ADDR");
                assert!(message.contains("true or false"), "message: {message}");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn log_format_defaults_to_text_and_accepts_json() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert_eq!(config.log_format, LogFormat::Text);

        let mut pairs = base_env();
        pairs.push(("DAYFLY_LOG_FORMAT".to_string(), "json".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("JSON log config");
        assert_eq!(config.log_format, LogFormat::Json);
    }

    #[test]
    fn log_format_rejects_an_unknown_value() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_LOG_FORMAT".to_string(), "xml".to_string()));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_LOG_FORMAT");
                assert!(message.contains("text or json"), "message: {message}");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn metrics_listener_defaults_to_off() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert_eq!(config.metrics_bind, None);
        assert!(config.metrics_allowed_networks.is_empty());
    }

    #[test]
    fn a_loopback_metrics_bind_needs_no_allowlist() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_METRICS_BIND".to_string(),
            "127.0.0.1:9091".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("loopback metrics config");
        assert_eq!(
            config.metrics_bind,
            Some("127.0.0.1:9091".parse().expect("socket address"))
        );
    }

    #[test]
    fn a_non_loopback_metrics_bind_requires_an_allowlist() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_METRICS_BIND".to_string(),
            "0.0.0.0:9091".to_string(),
        ));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_METRICS_ALLOWED_NETWORKS");
                assert!(
                    message.contains("0.0.0.0:9091"),
                    "message must name the bind: {message}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn metrics_allowlist_accepts_cidrs() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_METRICS_BIND".to_string(),
            "0.0.0.0:9091".to_string(),
        ));
        pairs.push((
            "DAYFLY_METRICS_ALLOWED_NETWORKS".to_string(),
            "10.0.0.0/8, 2001:db8::/32".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("metrics allowlist config");
        let expected: Vec<Cidr> = vec![
            "10.0.0.0/8".parse().expect("IPv4 CIDR"),
            "2001:db8::/32".parse().expect("IPv6 CIDR"),
        ];
        assert_eq!(config.metrics_allowed_networks, expected);
    }

    #[test]
    fn retention_defaults_to_off_with_documented_days() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(!config.retention_by_size);
        assert_eq!(config.retention_min_days, 30);
        assert_eq!(config.retention_max_days, 365);
    }

    #[test]
    fn retention_accepts_enabled_values() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_RETENTION_BY_SIZE".to_string(), "true".to_string()));
        pairs.push(("DAYFLY_RETENTION_MIN_DAYS".to_string(), "10".to_string()));
        pairs.push(("DAYFLY_RETENTION_MAX_DAYS".to_string(), "90".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("retention config");
        assert!(config.retention_by_size);
        assert_eq!(config.retention_min_days, 10);
        assert_eq!(config.retention_max_days, 90);
    }

    #[test]
    fn retention_rejects_zero_and_values_above_the_maximum() {
        for setting in ["DAYFLY_RETENTION_MIN_DAYS", "DAYFLY_RETENTION_MAX_DAYS"] {
            for value in ["0".to_string(), (MAX_TTL_DAYS + 1).to_string()] {
                let mut pairs = base_env();
                pairs.push((setting.to_string(), value));
                let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
                match error {
                    ConfigError::Invalid(name, message) => {
                        assert_eq!(name, setting);
                        assert!(
                            message.contains(&MAX_TTL_DAYS.to_string()),
                            "message must name the maximum: {message}"
                        );
                    }
                    other => panic!("unexpected error: {other}"),
                }
            }
        }
    }

    #[test]
    fn retention_rejects_a_minimum_above_the_maximum() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_RETENTION_MIN_DAYS".to_string(), "90".to_string()));
        pairs.push(("DAYFLY_RETENTION_MAX_DAYS".to_string(), "30".to_string()));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_RETENTION_MIN_DAYS");
                assert!(
                    message.contains("DAYFLY_RETENTION_MAX_DAYS"),
                    "message must name the other setting: {message}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn retention_curve_clamps_at_zero_and_at_the_size_cap() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_RETENTION_MIN_DAYS".to_string(), "30".to_string()));
        pairs.push(("DAYFLY_RETENTION_MAX_DAYS".to_string(), "365".to_string()));
        pairs.push(("DAYFLY_MAX_PASTE_BYTES".to_string(), "1000".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("curve config");

        assert_eq!(config.retention_seconds(0), 365 * 86_400);
        assert_eq!(config.retention_seconds(500), 71 * 86_400 + 75_600);
        assert_eq!(config.retention_seconds(1000), 30 * 86_400);
        assert_eq!(
            config.retention_seconds(5000),
            30 * 86_400,
            "a paste above the cap must get the minimum"
        );
    }

    #[test]
    fn trusted_proxies_default_to_empty() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(config.trusted_proxies.is_empty());
    }

    #[test]
    fn trusted_proxies_accept_a_comma_separated_list() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_TRUSTED_PROXIES".to_string(),
            "10.0.0.0/8, 2001:db8::1".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("proxy list config");
        let expected: Vec<Cidr> = vec![
            "10.0.0.0/8".parse().expect("IPv4 CIDR"),
            "2001:db8::1".parse().expect("IPv6 host"),
        ];
        assert_eq!(config.trusted_proxies, expected);
    }

    #[test]
    fn an_empty_trusted_proxy_list_means_no_proxies() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_TRUSTED_PROXIES".to_string(), "   ".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("empty proxy list");
        assert!(config.trusted_proxies.is_empty());
    }

    #[test]
    fn an_invalid_trusted_proxy_names_the_setting() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_TRUSTED_PROXIES".to_string(),
            "10.0.0.0/33".to_string(),
        ));
        let error = Config::from_lookup(lookup_from(&pairs))
            .err()
            .expect("an invalid prefix must be refused");
        let message = error.to_string();
        assert!(
            message.contains("DAYFLY_TRUSTED_PROXIES"),
            "the error must name the setting: {message}"
        );
        assert!(
            message.contains("10.0.0.0/33"),
            "the error must name the entry: {message}"
        );
    }

    #[test]
    fn list_requires_token_defaults_to_false() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(!config.list_requires_token);
    }

    #[test]
    fn list_requires_token_accepts_true() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_LIST_REQUIRES_TOKEN".to_string(), "true".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("gated listing config");
        assert!(config.list_requires_token);
    }

    #[test]
    fn paste_tokens_default_to_false() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(!config.paste_tokens);
    }

    #[test]
    fn paste_tokens_accept_true() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_PASTE_TOKENS".to_string(), "true".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("token config");
        assert!(config.paste_tokens);
    }

    #[test]
    fn show_words_defaults_to_true() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("default config");
        assert!(config.show_words);
    }

    #[test]
    fn show_words_accepts_false() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_SHOW_WORDS".to_string(), "false".to_string()));
        let config = Config::from_lookup(lookup_from(&pairs)).expect("false config");
        assert!(!config.show_words);
    }

    #[test]
    fn show_words_accepts_common_bool_spellings() {
        for (value, expected) in [("1", true), ("0", false), ("yes", true), ("off", false)] {
            let mut pairs = base_env();
            pairs.push(("DAYFLY_SHOW_WORDS".to_string(), value.to_string()));
            let config = Config::from_lookup(lookup_from(&pairs))
                .unwrap_or_else(|error| panic!("{value}: {error}"));
            assert_eq!(config.show_words, expected, "value {value}");
        }
    }

    #[test]
    fn show_words_rejects_an_unknown_value() {
        let mut pairs = base_env();
        pairs.push(("DAYFLY_SHOW_WORDS".to_string(), "maybe".to_string()));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_SHOW_WORDS");
                assert!(message.contains("true or false"), "{message}");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn default_ttl_above_the_maximum_is_rejected() {
        let mut pairs = base_env();
        pairs.push((
            "DAYFLY_DEFAULT_TTL_DAYS".to_string(),
            (MAX_TTL_DAYS + 1).to_string(),
        ));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match error {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_DEFAULT_TTL_DAYS");
                assert!(
                    message.contains(&MAX_TTL_DAYS.to_string()),
                    "message must name the maximum: {message}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn direct_credentials_still_work() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("direct config");
        assert_eq!(config.access_key_id, "test-access-key");
        assert_eq!(config.secret_access_key, "test-secret-key");
    }

    #[test]
    fn credential_file_value_is_trimmed() {
        let access = TempCredentialFile::new("file-access-key\n");
        let secret = TempCredentialFile::new("file-secret-key\r\n");
        let mut pairs = base_env();
        pairs = with_credential_file(
            pairs,
            "S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY_ID_FILE",
            access.path(),
        );
        pairs = with_credential_file(
            pairs,
            "S3_SECRET_ACCESS_KEY",
            "S3_SECRET_ACCESS_KEY_FILE",
            secret.path(),
        );
        let config = Config::from_lookup(lookup_from(&pairs)).expect("file config");
        assert_eq!(config.access_key_id, "file-access-key");
        assert_eq!(config.secret_access_key, "file-secret-key");
    }

    #[test]
    fn empty_credential_file_is_rejected() {
        let file = TempCredentialFile::new("\n");
        let pairs = with_credential_file(
            base_env(),
            "S3_ACCESS_KEY_ID",
            "S3_ACCESS_KEY_ID_FILE",
            file.path(),
        );
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match &error {
            ConfigError::EmptyCredentialFile {
                file_variable,
                path,
            } => {
                assert_eq!(file_variable, "S3_ACCESS_KEY_ID_FILE");
                assert_eq!(path, file.path());
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn missing_credential_file_is_rejected() {
        let path = unique_temp_path();
        assert!(!path.exists());
        let path = path.to_str().expect("temp path is UTF-8");
        let pairs = with_credential_file(
            base_env(),
            "S3_SECRET_ACCESS_KEY",
            "S3_SECRET_ACCESS_KEY_FILE",
            path,
        );
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match &error {
            ConfigError::CredentialFileRead {
                file_variable,
                path: error_path,
                source,
            } => {
                assert_eq!(file_variable, "S3_SECRET_ACCESS_KEY_FILE");
                assert_eq!(error_path, path);
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn direct_and_file_credentials_conflict() {
        let file = TempCredentialFile::new("file-access-key\n");
        let mut pairs = base_env();
        pairs.push(("S3_ACCESS_KEY_ID_FILE".to_string(), file.path().to_string()));
        let error = expect_error(Config::from_lookup(lookup_from(&pairs)));
        match &error {
            ConfigError::ConflictingCredentialSources {
                variable,
                file_variable,
            } => {
                assert_eq!(variable, "S3_ACCESS_KEY_ID");
                assert_eq!(file_variable, "S3_ACCESS_KEY_ID_FILE");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert!(
            !error.to_string().contains("test-access-key"),
            "message: {error}"
        );
    }

    const BASE_ENV: &[(&str, &str)] = &[
        ("S3_ENDPOINT", "http://127.0.0.1:1"),
        ("S3_BUCKET", "test-bucket"),
        ("S3_ACCESS_KEY_ID", "test-access-key"),
        ("S3_SECRET_ACCESS_KEY", "test-secret-key"),
    ];

    fn lookup<'a>(entries: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            entries
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string())
        }
    }

    fn load_with_token(token: Option<&str>) -> Result<Config, ConfigError> {
        let mut entries = BASE_ENV.to_vec();
        if let Some(token) = token {
            entries.push(("DAYFLY_HTTP_BEARER_TOKEN", token));
        }
        Config::from_lookup(lookup(&entries))
    }

    #[test]
    fn token_with_whitespace_is_rejected() {
        for token in [
            "sekrit token",
            "sekrit\ttoken",
            "sekrit\ntoken",
            " sekrit",
            "sekrit ",
        ] {
            let err = match load_with_token(Some(token)) {
                Ok(_) => panic!("token with ASCII whitespace must be rejected: {token:?}"),
                Err(e) => e,
            };
            match err {
                ConfigError::Invalid(name, message) => {
                    assert_eq!(name, "DAYFLY_HTTP_BEARER_TOKEN");
                    assert!(
                        !message.contains("sekrit"),
                        "error message must not contain the token"
                    );
                }
                other => panic!("expected ConfigError::Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn token_without_whitespace_is_preserved() {
        let config = match load_with_token(Some("sekrit-token")) {
            Ok(config) => config,
            Err(e) => panic!("a normal token must load: {e}"),
        };
        assert_eq!(config.http_bearer_token.as_deref(), Some("sekrit-token"));
    }

    #[test]
    fn absent_token_disables_auth() {
        let config = match load_with_token(None) {
            Ok(config) => config,
            Err(e) => panic!("an unset token must mean no auth: {e}"),
        };
        assert_eq!(config.http_bearer_token, None);
    }

    #[test]
    fn set_but_blank_token_is_rejected() {
        for token in ["", "   ", "\t"] {
            let err = expect_error(load_with_token(Some(token)));
            match err {
                ConfigError::Invalid(name, message) => {
                    assert_eq!(name, "DAYFLY_HTTP_BEARER_TOKEN");
                    assert!(
                        message.contains("set") && message.contains("empty"),
                        "the error must say the variable is set but empty: {message}"
                    );
                }
                other => panic!("expected ConfigError::Invalid, got {other:?}"),
            }
        }
    }

    #[test]
    fn env_file_blank_token_is_rejected() {
        let values = parse_env_file("DAYFLY_HTTP_BEARER_TOKEN=\"\"\n", Path::new("test.env"))
            .expect("the env file line must parse");
        let mut entries = base_env();
        entries.extend(values);
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_HTTP_BEARER_TOKEN");
                assert!(message.contains("empty"), "got: {message}");
            }
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn footer_settings_are_optional() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("base env must parse");
        assert_eq!(config.site_name, None);
        assert_eq!(config.contact, None);
        assert_eq!(config.aup_url, None);
    }

    #[test]
    fn footer_settings_parse() {
        let mut entries = base_env();
        entries.push(("DAYFLY_SITE_NAME".to_string(), " dayfly ".to_string()));
        entries.push((
            "DAYFLY_CONTACT".to_string(),
            "abuse@example.com".to_string(),
        ));
        entries.push((
            "DAYFLY_AUP_URL".to_string(),
            "https://example.com/aup".to_string(),
        ));
        let config = Config::from_lookup(lookup_from(&entries)).expect("footer env must parse");
        assert_eq!(config.site_name.as_deref(), Some("dayfly"));
        assert_eq!(config.contact.as_deref(), Some("abuse@example.com"));
        assert_eq!(config.aup_url.as_deref(), Some("https://example.com/aup"));
    }

    #[test]
    fn ttl_range_defaults_keep_the_wide_range() {
        let config = Config::from_lookup(lookup_from(&base_env())).expect("base env must parse");
        assert_eq!(config.min_ttl_days, 0);
        assert_eq!(config.max_ttl_days, MAX_TTL_DAYS);
        assert_eq!(config.ttl_choices, vec![1, 3, 7, 14, 30, 90]);
        assert!(config.list_enabled);
    }

    #[test]
    fn ttl_range_parses_a_public_profile() {
        let mut entries = base_env();
        entries.push(("DAYFLY_MIN_TTL_DAYS".to_string(), "1".to_string()));
        entries.push(("DAYFLY_MAX_TTL_DAYS".to_string(), "3".to_string()));
        entries.push(("DAYFLY_TTL_CHOICES".to_string(), "1,2,3".to_string()));
        entries.push(("DAYFLY_DEFAULT_TTL_DAYS".to_string(), "1".to_string()));
        entries.push(("DAYFLY_LIST_ENABLED".to_string(), "false".to_string()));
        let config = Config::from_lookup(lookup_from(&entries)).expect("public profile must parse");
        assert_eq!(config.min_ttl_days, 1);
        assert_eq!(config.max_ttl_days, 3);
        assert_eq!(config.ttl_choices, vec![1, 2, 3]);
        assert_eq!(config.default_ttl_days, 1);
        assert!(!config.list_enabled);
    }

    #[test]
    fn default_ttl_must_fit_the_range() {
        let mut entries = base_env();
        entries.push(("DAYFLY_MIN_TTL_DAYS".to_string(), "2".to_string()));
        entries.push(("DAYFLY_MAX_TTL_DAYS".to_string(), "3".to_string()));
        entries.push(("DAYFLY_DEFAULT_TTL_DAYS".to_string(), "1".to_string()));
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, _) => assert_eq!(name, "DAYFLY_DEFAULT_TTL_DAYS"),
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn minimum_ttl_must_not_exceed_the_maximum() {
        let mut entries = base_env();
        entries.push(("DAYFLY_MIN_TTL_DAYS".to_string(), "4".to_string()));
        entries.push(("DAYFLY_MAX_TTL_DAYS".to_string(), "3".to_string()));
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, _) => assert_eq!(name, "DAYFLY_MIN_TTL_DAYS"),
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn ttl_choices_must_fit_the_range() {
        let mut entries = base_env();
        entries.push(("DAYFLY_MAX_TTL_DAYS".to_string(), "3".to_string()));
        entries.push(("DAYFLY_DEFAULT_TTL_DAYS".to_string(), "1".to_string()));
        entries.push(("DAYFLY_TTL_CHOICES".to_string(), "1,7".to_string()));
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, _) => assert_eq!(name, "DAYFLY_TTL_CHOICES"),
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn acceptable_use_url_must_use_http() {
        let mut entries = base_env();
        entries.push((
            "DAYFLY_AUP_URL".to_string(),
            "javascript:alert(1)".to_string(),
        ));
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, message) => {
                assert_eq!(name, "DAYFLY_AUP_URL");
                assert!(message.contains("http"), "got: {message}");
            }
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    #[test]
    fn site_name_rejects_control_characters() {
        let mut entries = base_env();
        entries.push(("DAYFLY_SITE_NAME".to_string(), "day\nfly".to_string()));
        let err = expect_error(Config::from_lookup(lookup_from(&entries)));
        match err {
            ConfigError::Invalid(name, _) => assert_eq!(name, "DAYFLY_SITE_NAME"),
            other => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }
}
