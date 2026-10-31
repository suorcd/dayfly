use std::collections::HashMap;
use std::env;
use std::io::Read;
use std::path::{Path, PathBuf};

use thiserror::Error;

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
    /// filename. A filename selects [`Self::max_file_bytes`] instead.
    /// Default: 10 MiB.
    pub max_text_bytes: u64,
    /// Maximum size, in bytes, for a file paste. Default: 1 GiB.
    pub max_file_bytes: u64,
    /// Default TTL in positive whole days when a store call does not
    /// specify one. Zero is invalid because it would create a paste that
    /// is already expired.
    pub default_ttl_days: u32,
    /// Default bind address for HTTP mode.
    pub http_bind: String,
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
}

/// One setting that the environment file template and the parser share.
pub struct ConfigField {
    pub name: &'static str,
    /// The default value, or the placeholder for a required field.
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
        help: "Signing placeholder for Garage",
    },
    ConfigField {
        name: "DAYFLY_MAX_TEXT_BYTES",
        value: Some("10485760"),
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
        name: "DAYFLY_DEFAULT_TTL_DAYS",
        value: Some("7"),
        required: false,
        help: "Default TTL in whole days",
    },
    ConfigField {
        name: "DAYFLY_HTTP_BIND",
        value: Some("127.0.0.1:8080"),
        required: false,
        help: "HTTP bind address",
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
fn optional_credential<F>(lookup: &F, name: &str) -> Result<Option<String>, ConfigError>
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

/// Parse the optional HTTP bearer token.
///
/// An unset value or a value with only ASCII whitespace means "no token".
/// A non-empty value that contains ASCII whitespace is invalid: the
/// `Authorization` parser rejects such a credential, so startup must fail
/// instead of locking out every request at runtime. The value is not
/// trimmed into a different token.
fn bearer_token(value: Option<String>) -> Result<Option<String>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
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
        let default_ttl_days = optional_u32(
            &lookup,
            "DAYFLY_DEFAULT_TTL_DAYS",
            field_u32("DAYFLY_DEFAULT_TTL_DAYS", 7),
        )?;
        if default_ttl_days == 0 {
            return Err(ConfigError::Invalid(
                "DAYFLY_DEFAULT_TTL_DAYS".to_string(),
                "expected a positive integer".to_string(),
            ));
        }
        Ok(Self {
            endpoint,
            bucket,
            // Region is a signing placeholder for S3-compatible stores such
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
                field_u64("DAYFLY_MAX_TEXT_BYTES", 10 * 1024 * 1024),
            )?,
            max_file_bytes: optional_u64(
                &lookup,
                "DAYFLY_MAX_FILE_BYTES",
                field_u64("DAYFLY_MAX_FILE_BYTES", 1024 * 1024 * 1024),
            )?,
            default_ttl_days,
            http_bind: optional_string(
                &lookup,
                "DAYFLY_HTTP_BIND",
                field_default("DAYFLY_HTTP_BIND").unwrap_or("127.0.0.1:8080"),
            ),
            http_bearer_token: bearer_token(optional_credential(
                &lookup,
                "DAYFLY_HTTP_BEARER_TOKEN",
            )?)?,
            mcp_local_root: local_root(lookup("DAYFLY_MCP_LOCAL_ROOT"))?,
        })
    }

    /// Cap for an HTTP upload, decided before the body is streamed.
    ///
    /// A filename (`X-Filename`, `?filename=`, or a multipart filename)
    /// means a file upload and gets [`Self::max_file_bytes`]. No filename
    /// means a text paste and gets [`Self::max_text_bytes`]. Content-Type
    /// is deliberately not used here: detection needs bytes, and a file
    /// named `report.txt` is still a file upload even though its bytes
    /// look like text.
    pub fn max_bytes_for_upload(&self, filename: Option<&str>) -> u64 {
        match filename {
            Some(name) if !name.trim().is_empty() => self.max_file_bytes,
            _ => self.max_text_bytes,
        }
    }

    /// A log-safe summary: endpoint host scheme and bucket only, never keys.
    pub fn safe_summary(&self) -> String {
        format!(
            "bucket={} endpoint_scheme={} region={} max_text_bytes={} max_file_bytes={} default_ttl_days={} mcp_local_root={}",
            self.bucket,
            if self.endpoint.starts_with("https://") {
                "https"
            } else {
                "http"
            },
            self.region,
            self.max_text_bytes,
            self.max_file_bytes,
            self.default_ttl_days,
            if self.mcp_local_root.is_some() {
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
            default_ttl_days: 7,
            http_bind: "127.0.0.1:0".to_string(),
            http_bearer_token: None,
            mcp_local_root: None,
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
    fn absent_or_empty_token_disables_auth() {
        for token in [None, Some(""), Some("   "), Some("\t")] {
            let config = match load_with_token(token) {
                Ok(config) => config,
                Err(e) => panic!("token {token:?} must mean no auth: {e}"),
            };
            assert_eq!(
                config.http_bearer_token, None,
                "token {token:?} must mean no auth"
            );
        }
    }
}
