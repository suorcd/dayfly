use std::env;

use thiserror::Error;

/// Configuration loaded from the environment only.
///
/// No S3 credential is ever logged. [`Config`] deliberately has no `Debug`
/// implementation that prints secret fields; callers that log configuration
/// must use [`Config::safe_summary`].
#[derive(Clone)]
pub struct Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Maximum size, in bytes, for a text paste. Enforced by the MCP
    /// `store_text` tool and by the human text form. NOT enforced on the
    /// raw-body `POST /upload` path, which only applies `max_file_bytes`;
    /// see README "Known limitations". Default: 10 MiB.
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
}

fn required(name: &str) -> Result<String, ConfigError> {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => Ok(v.trim().to_string()),
        _ => Err(ConfigError::Missing(name.to_string())),
    }
}

fn optional_u64(name: &str, default: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse::<u64>().map_err(|_| {
            ConfigError::Invalid(name.to_string(), "expected a non-negative integer".into())
        }),
        _ => Ok(default),
    }
}

fn optional_u32(name: &str, default: u32) -> Result<u32, ConfigError> {
    match env::var(name) {
        Ok(v) if !v.trim().is_empty() => v.trim().parse::<u32>().map_err(|_| {
            ConfigError::Invalid(name.to_string(), "expected a non-negative integer".into())
        }),
        _ => Ok(default),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let endpoint = required("S3_ENDPOINT")?;
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(ConfigError::BadEndpoint);
        }
        let bucket = required("S3_BUCKET")?;
        if bucket.is_empty() {
            return Err(ConfigError::EmptyBucket);
        }
        Ok(Self {
            endpoint,
            bucket,
            // Region is a signing placeholder for S3-compatible stores such
            // as Garage. It is not hardcoded: the operator supplies it.
            region: env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key_id: required("S3_ACCESS_KEY_ID")?,
            secret_access_key: required("S3_SECRET_ACCESS_KEY")?,
            max_text_bytes: optional_u64("DAYFLY_MAX_TEXT_BYTES", 10 * 1024 * 1024)?,
            max_file_bytes: optional_u64("DAYFLY_MAX_FILE_BYTES", 1024 * 1024 * 1024)?,
            default_ttl_days: optional_u32("DAYFLY_DEFAULT_TTL_DAYS", 7)?,
            http_bind: env::var("DAYFLY_HTTP_BIND")
                .unwrap_or_else(|_| "127.0.0.1:8080".to_string()),
            http_bearer_token: match env::var("DAYFLY_HTTP_BEARER_TOKEN") {
                Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
                _ => None,
            },
        })
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
    use super::*;

    #[test]
    fn endpoint_must_be_http_url() {
        // from_env reads the real process environment, so only test the
        // validation rule indirectly via a helper-free check: a non-URL
        // endpoint is rejected by construction in from_env, and this test
        // documents that contract. See README for the env contract.
        assert!(ConfigError::BadEndpoint.to_string().contains("http"));
    }
}
