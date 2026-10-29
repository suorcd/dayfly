use std::env;

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
    /// Default TTL in whole days when a store call does not specify one.
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

fn read_credential_file(file_variable: &str, path: &str) -> Result<String, ConfigError> {
    let contents =
        std::fs::read_to_string(path).map_err(|source| ConfigError::CredentialFileRead {
            file_variable: file_variable.to_string(),
            path: path.to_string(),
            source,
        })?;
    let value = contents.trim();
    if value.is_empty() {
        return Err(ConfigError::EmptyCredentialFile {
            file_variable: file_variable.to_string(),
            path: path.to_string(),
        });
    }
    Ok(value.to_string())
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
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
        Ok(Self {
            endpoint,
            bucket,
            // Region is a signing placeholder for S3-compatible stores such
            // as Garage. It is not hardcoded: the operator supplies it.
            region: lookup("S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            access_key_id: credential(&lookup, "S3_ACCESS_KEY_ID")?,
            secret_access_key: credential(&lookup, "S3_SECRET_ACCESS_KEY")?,
            max_text_bytes: optional_u64(&lookup, "DAYFLY_MAX_TEXT_BYTES", 10 * 1024 * 1024)?,
            max_file_bytes: optional_u64(&lookup, "DAYFLY_MAX_FILE_BYTES", 1024 * 1024 * 1024)?,
            default_ttl_days: optional_u32(&lookup, "DAYFLY_DEFAULT_TTL_DAYS", 7)?,
            http_bind: lookup("DAYFLY_HTTP_BIND").unwrap_or_else(|| "127.0.0.1:8080".to_string()),
            http_bearer_token: match lookup("DAYFLY_HTTP_BEARER_TOKEN") {
                Some(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
                _ => None,
            },
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
            "bucket={} endpoint_scheme={} region={} max_text_bytes={} max_file_bytes={} default_ttl_days={}",
            self.bucket,
            if self.endpoint.starts_with("https://") {
                "https"
            } else {
                "http"
            },
            self.region,
            self.max_text_bytes,
            self.max_file_bytes,
            self.default_ttl_days
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
    }

    impl Drop for TempCredentialFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
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
}
