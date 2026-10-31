//! Real `Config::from_env` tests.
//!
//! The environment is process-global, so every test in this file takes the
//! same lock and sets the full variable set it needs.

use std::env;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use dayfly::config::{Config, ConfigError};

const VARS: [&str; 12] = [
    "S3_ENDPOINT",
    "S3_BUCKET",
    "S3_REGION",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_ACCESS_KEY_ID_FILE",
    "S3_SECRET_ACCESS_KEY_FILE",
    "DAYFLY_MAX_TEXT_BYTES",
    "DAYFLY_MAX_FILE_BYTES",
    "DAYFLY_DEFAULT_TTL_DAYS",
    "DAYFLY_HTTP_BIND",
    "DAYFLY_MCP_LOCAL_ROOT",
];

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn clear_env() {
    for name in VARS {
        env::remove_var(name);
    }
    env::remove_var("DAYFLY_HTTP_BEARER_TOKEN");
}

fn set_required() {
    env::set_var("S3_ENDPOINT", "https://s3.example.invalid:3900");
    env::set_var("S3_BUCKET", "dayfly-test");
    env::set_var("S3_ACCESS_KEY_ID", "test-access");
    env::set_var("S3_SECRET_ACCESS_KEY", "test-secret");
}

fn minimal_config() -> Config {
    clear_env();
    set_required();
    Config::from_env().expect("minimal environment must parse")
}

fn temp_env_file(name: &str) -> PathBuf {
    let path = env::temp_dir().join(format!(
        "dayfly-config-file-{}-{name}.env",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn minimal_environment_applies_documented_defaults() {
    let _guard = lock_env();
    let config = minimal_config();
    assert_eq!(config.endpoint, "https://s3.example.invalid:3900");
    assert_eq!(config.bucket, "dayfly-test");
    assert_eq!(config.region, "us-east-1");
    assert_eq!(config.access_key_id, "test-access");
    assert_eq!(config.secret_access_key, "test-secret");
    assert_eq!(config.max_text_bytes, 10 * 1024 * 1024);
    assert_eq!(config.max_file_bytes, 1024 * 1024 * 1024);
    assert_eq!(config.default_ttl_days, 7);
    assert_eq!(config.http_bind, "127.0.0.1:8080");
    assert_eq!(config.http_bearer_token, None);
    assert_eq!(config.mcp_local_root, None);
}

#[test]
fn env_file_supplies_values_and_the_process_environment_wins() {
    let _guard = lock_env();
    clear_env();
    let path = temp_env_file("precedence");
    std::fs::write(
        &path,
        "# comment\nexport S3_ENDPOINT=\"https://file.example:3900\"\nS3_BUCKET=from-file\nS3_ACCESS_KEY_ID=file-key\nS3_SECRET_ACCESS_KEY='file-secret'\nS3_REGION=from-file\n",
    )
    .expect("temporary config file must be writable");
    env::set_var("S3_BUCKET", "from-env");
    env::set_var("S3_REGION", "   ");

    let config = Config::from_env_file(Some(&path)).expect("the env file must parse");
    assert_eq!(config.endpoint, "https://file.example:3900");
    assert_eq!(config.bucket, "from-env");
    assert_eq!(config.access_key_id, "file-key");
    assert_eq!(config.secret_access_key, "file-secret");
    assert_eq!(
        config.region, "from-file",
        "an empty process value must fall back to the file"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_named_env_file_must_exist() {
    let _guard = lock_env();
    clear_env();
    let path = temp_env_file("missing");
    let error = match Config::from_env_file(Some(&path)) {
        Ok(_) => panic!("a named config file must exist"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains(path.to_str().expect("temporary config path must be UTF-8")),
        "the error must name the path: {error}"
    );
}

#[test]
fn malformed_env_file_line_names_the_path_and_line() {
    let _guard = lock_env();
    clear_env();
    let path = temp_env_file("malformed");
    std::fs::write(&path, "S3_ENDPOINT=https://example.invalid\nNO_EQUALS\n")
        .expect("temporary config file must be writable");

    let error = match Config::from_env_file(Some(&path)) {
        Ok(_) => panic!("a malformed line must fail"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("line 2"), "got: {message}");
    assert!(
        message.contains(path.to_str().expect("temporary config path must be UTF-8")),
        "the error must name the path: {message}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn optional_values_override_the_defaults() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("S3_REGION", "garage-test");
    env::set_var("DAYFLY_MAX_TEXT_BYTES", "1234");
    env::set_var("DAYFLY_MAX_FILE_BYTES", "5678");
    env::set_var("DAYFLY_DEFAULT_TTL_DAYS", "30");
    env::set_var("DAYFLY_HTTP_BIND", "0.0.0.0:9999");
    env::set_var("DAYFLY_HTTP_BEARER_TOKEN", "token-value");

    let config = Config::from_env().expect("overrides must parse");
    assert_eq!(config.region, "garage-test");
    assert_eq!(config.max_text_bytes, 1234);
    assert_eq!(config.max_file_bytes, 5678);
    assert_eq!(config.default_ttl_days, 30);
    assert_eq!(config.http_bind, "0.0.0.0:9999");
    assert_eq!(config.http_bearer_token.as_deref(), Some("token-value"));
    assert_eq!(config.max_bytes_for_upload(None), 1234);
    assert_eq!(config.max_bytes_for_upload(Some("file.bin")), 5678);
}

#[test]
fn required_values_are_trimmed() {
    let _guard = lock_env();
    clear_env();
    env::set_var("S3_ENDPOINT", "  https://s3.example.invalid  ");
    env::set_var("S3_BUCKET", "  trimmed-bucket  ");
    env::set_var("S3_ACCESS_KEY_ID", "  key  ");
    env::set_var("S3_SECRET_ACCESS_KEY", "  secret  ");

    let config = Config::from_env().expect("padded values must parse");
    assert_eq!(config.endpoint, "https://s3.example.invalid");
    assert_eq!(config.bucket, "trimmed-bucket");
    assert_eq!(config.access_key_id, "key");
    assert_eq!(config.secret_access_key, "secret");
}

#[test]
fn each_missing_required_value_names_the_variable() {
    let _guard = lock_env();

    clear_env();
    let error = Config::from_env().err().expect("no environment must fail");
    assert!(matches!(error, ConfigError::Missing(ref name) if name == "S3_ENDPOINT"));

    clear_env();
    env::set_var("S3_ENDPOINT", "https://s3.example.invalid");
    let error = Config::from_env()
        .err()
        .expect("a missing bucket must fail");
    assert!(matches!(error, ConfigError::Missing(ref name) if name == "S3_BUCKET"));

    clear_env();
    env::set_var("S3_ENDPOINT", "https://s3.example.invalid");
    env::set_var("S3_BUCKET", "dayfly-test");
    let error = Config::from_env()
        .err()
        .expect("a missing access key must fail");
    assert!(matches!(
        error,
        ConfigError::Missing(ref name)
            if name == "S3_ACCESS_KEY_ID or S3_ACCESS_KEY_ID_FILE"
    ));

    clear_env();
    env::set_var("S3_ENDPOINT", "https://s3.example.invalid");
    env::set_var("S3_BUCKET", "dayfly-test");
    env::set_var("S3_ACCESS_KEY_ID", "test-access");
    let error = Config::from_env()
        .err()
        .expect("a missing secret must fail");
    assert!(matches!(
        error,
        ConfigError::Missing(ref name)
            if name == "S3_SECRET_ACCESS_KEY or S3_SECRET_ACCESS_KEY_FILE"
    ));
}

#[test]
fn blank_required_values_are_reported_as_missing() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("S3_BUCKET", "   ");
    let error = Config::from_env().err().expect("a blank bucket must fail");
    assert!(matches!(error, ConfigError::Missing(ref name) if name == "S3_BUCKET"));

    env::set_var("S3_BUCKET", "dayfly-test");
    env::set_var("S3_ACCESS_KEY_ID", "\t");
    let error = Config::from_env()
        .err()
        .expect("a blank access key must fail");
    assert!(matches!(
        error,
        ConfigError::Missing(ref name)
            if name == "S3_ACCESS_KEY_ID or S3_ACCESS_KEY_ID_FILE"
    ));
}

#[test]
fn endpoint_must_use_http_or_https() {
    let _guard = lock_env();
    for endpoint in [
        "ftp://s3.example.invalid",
        "s3.example.invalid:3900",
        "https:/missing-slash",
    ] {
        clear_env();
        set_required();
        env::set_var("S3_ENDPOINT", endpoint);
        let error = Config::from_env()
            .err()
            .expect("a non-HTTP endpoint must fail");
        assert!(
            matches!(error, ConfigError::BadEndpoint),
            "{endpoint} must be rejected, got {error}"
        );
    }
}

#[test]
fn invalid_numeric_values_name_the_variable() {
    let _guard = lock_env();
    for (name, value) in [
        ("DAYFLY_MAX_TEXT_BYTES", "not-a-number"),
        ("DAYFLY_MAX_TEXT_BYTES", "-1"),
        ("DAYFLY_MAX_FILE_BYTES", "1.5"),
        ("DAYFLY_DEFAULT_TTL_DAYS", "seven"),
        ("DAYFLY_DEFAULT_TTL_DAYS", "-2"),
    ] {
        clear_env();
        set_required();
        env::set_var(name, value);
        let error = Config::from_env()
            .err()
            .expect("an invalid number must fail");
        assert!(
            matches!(error, ConfigError::Invalid(ref field, _) if field == name),
            "{name}={value} must be rejected, got {error}"
        );
    }
}

#[test]
fn default_ttl_days_zero_is_rejected() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_DEFAULT_TTL_DAYS", "0");
    let error = Config::from_env()
        .err()
        .expect("a zero default TTL must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_DEFAULT_TTL_DAYS"),
        "the error must name DAYFLY_DEFAULT_TTL_DAYS, got {error}"
    );
}

#[test]
fn optional_numeric_values_accept_zero() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_MAX_TEXT_BYTES", "0");
    env::set_var("DAYFLY_MAX_FILE_BYTES", "0");
    let config = Config::from_env().expect("zero is a non-negative integer");
    assert_eq!(config.max_text_bytes, 0);
    assert_eq!(config.max_file_bytes, 0);
}

#[test]
fn blank_bearer_token_means_no_auth() {
    let _guard = lock_env();
    for value in ["", "   ", "\t\n"] {
        clear_env();
        set_required();
        env::set_var("DAYFLY_HTTP_BEARER_TOKEN", value);
        let config = Config::from_env().expect("a blank token must parse");
        assert_eq!(config.http_bearer_token, None, "value {value:?}");
    }
}

#[test]
fn bearer_token_with_whitespace_is_rejected() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_HTTP_BEARER_TOKEN", "two words");
    let error = Config::from_env()
        .err()
        .expect("a token with whitespace must fail");
    assert!(matches!(
        error,
        ConfigError::Invalid(ref name, _) if name == "DAYFLY_HTTP_BEARER_TOKEN"
    ));
}

#[test]
fn safe_summary_omits_credentials_and_token_presence() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_HTTP_BEARER_TOKEN", "summary-secret");
    let config = Config::from_env().expect("environment must parse");
    let summary = config.safe_summary();
    assert!(summary.contains("bucket=dayfly-test"));
    assert!(summary.contains("endpoint_scheme=https"));
    for secret in ["test-access", "test-secret", "summary-secret"] {
        assert!(
            !summary.contains(secret),
            "summary leaked {secret}: {summary}"
        );
    }
    assert!(!summary.contains("bearer"));
}

#[test]
fn mcp_local_root_is_canonical_and_must_exist() {
    let _guard = lock_env();
    clear_env();
    set_required();

    let dir = env::temp_dir().join(format!("dayfly-root-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("test directory must be creatable");

    env::set_var("DAYFLY_MCP_LOCAL_ROOT", &dir);
    let config = Config::from_env().expect("an existing directory must parse");
    let root = config.mcp_local_root.expect("the root must be set");
    assert!(root.is_absolute(), "the root must be canonical");
    assert_eq!(
        root,
        dir.canonicalize()
            .expect("the test directory must canonicalize"),
        "the stored root must be the canonical path"
    );

    // A path that does not exist is a startup error, not a silent no-op.
    env::set_var(
        "DAYFLY_MCP_LOCAL_ROOT",
        dir.join("missing").to_string_lossy().to_string(),
    );
    match Config::from_env() {
        Err(ConfigError::Invalid(name, _)) => assert_eq!(name, "DAYFLY_MCP_LOCAL_ROOT"),
        Err(other) => panic!("a missing root must fail startup: {other:?}"),
        Ok(_) => panic!("a missing root must fail startup"),
    }

    // A file is not a directory.
    let file = dir.join("not-a-dir.txt");
    std::fs::write(&file, b"x").expect("test file must be writable");
    env::set_var("DAYFLY_MCP_LOCAL_ROOT", &file);
    match Config::from_env() {
        Err(ConfigError::Invalid(name, _)) => assert_eq!(name, "DAYFLY_MCP_LOCAL_ROOT"),
        Err(other) => panic!("a file must not pass as a root: {other:?}"),
        Ok(_) => panic!("a file must not pass as a root"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
