//! Real `Config::from_env` tests.
//!
//! The environment is process-global, so every test in this file takes the
//! same lock and sets the full variable set it needs.

use std::env;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use dayfly::config::{Config, ConfigError};

const VARS: [&str; 35] = [
    "S3_ENDPOINT",
    "S3_BUCKET",
    "S3_REGION",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "S3_ACCESS_KEY_ID_FILE",
    "S3_SECRET_ACCESS_KEY_FILE",
    "DAYFLY_MAX_TEXT_BYTES",
    "DAYFLY_MAX_FILE_BYTES",
    "DAYFLY_MAX_PASTE_BYTES",
    "DAYFLY_SEARCH_MAX_BYTES",
    "DAYFLY_ALIAS_WORDS",
    "DAYFLY_DEFAULT_TTL_DAYS",
    "DAYFLY_LIST_DEFAULT_DAYS",
    "DAYFLY_RETENTION_BY_SIZE",
    "DAYFLY_RETENTION_MIN_DAYS",
    "DAYFLY_RETENTION_MAX_DAYS",
    "DAYFLY_HTTP_BIND",
    "DAYFLY_LOG_FORMAT",
    "DAYFLY_METRICS_BIND",
    "DAYFLY_METRICS_ALLOWED_NETWORKS",
    "DAYFLY_TRUSTED_PROXIES",
    "DAYFLY_LIST_REQUIRES_TOKEN",
    "DAYFLY_WRITE_REQUIRES_TOKEN",
    "DAYFLY_PASTE_TOKENS",
    "DAYFLY_EXPOSE_CREATOR_ADDR",
    "DAYFLY_RATE_LIMIT_PER_MINUTE",
    "DAYFLY_SHOW_WORDS",
    "DAYFLY_SHOW_INTERNAL_PATHS",
    "DAYFLY_INLINE_PDF",
    "DAYFLY_MCP_LOCAL_ROOT",
    "DAYFLY_FAVICON_PATH",
    "DAYFLY_ROBOTS_TXT_PATH",
    "DAYFLY_SECURITY_TXT_PATH",
    "DAYFLY_VIEW_LINE_NUMBERS",
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
    env::remove_var("DAYFLY_HTTP_BEARER_TOKEN_FILE");
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

fn temp_env_dir(name: &str) -> PathBuf {
    let dir = env::temp_dir().join(format!("dayfly-config-dir-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temporary config directory must be creatable");
    dir
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
    assert_eq!(config.max_text_bytes, 100 * 1024 * 1024);
    assert_eq!(config.max_file_bytes, 1024 * 1024 * 1024);
    assert_eq!(config.max_paste_bytes, 1024 * 1024 * 1024);
    assert_eq!(config.search_max_bytes, 64 * 1024 * 1024);
    assert_eq!(config.alias_words, 3);
    assert_eq!(config.default_ttl_days, 7);
    assert!(!config.retention_by_size);
    assert_eq!(config.retention_min_days, 30);
    assert_eq!(config.retention_max_days, 365);
    assert!(!config.paste_tokens);
    assert_eq!(config.http_bind, "127.0.0.1:8080");
    assert!(config.show_words);
    assert!(!config.show_internal_paths);
    assert!(config.favicon.is_none());
    assert!(config.robots_txt.is_none());
    assert!(config.security_txt.is_none());
    assert!(!config.view_line_numbers);
    assert!(config.inline_pdf);
    assert_eq!(config.http_bearer_token, None);
    assert_eq!(config.mcp_local_root, None);
}

#[test]
fn search_max_bytes_parses_and_rejects_zero() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_SEARCH_MAX_BYTES", "1234");
    let config = Config::from_env().expect("the search cap must parse");
    assert_eq!(config.search_max_bytes, 1234);

    env::set_var("DAYFLY_SEARCH_MAX_BYTES", "0");
    let error = match Config::from_env() {
        Ok(_) => panic!("zero must fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("DAYFLY_SEARCH_MAX_BYTES"),
        "got: {error}"
    );
}

#[test]
fn total_paste_cap_lowers_the_creation_caps() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_MAX_TEXT_BYTES", "400");
    env::set_var("DAYFLY_MAX_FILE_BYTES", "2000");
    env::set_var("DAYFLY_MAX_PASTE_BYTES", "500");

    let config = Config::from_env().expect("caps must parse");
    assert_eq!(config.max_bytes_for_upload(None), 400);
    assert_eq!(config.max_bytes_for_upload(Some("file.bin")), 500);
}

#[test]
fn watch_paths_follows_the_env_file_and_its_secret_files() {
    let _guard = lock_env();
    clear_env();
    let env_file = temp_env_file("watch-paths");
    let key_file = env_file.with_extension("key");
    let token_file = env_file.with_extension("token");
    std::fs::write(&key_file, "key\n").expect("the key file must be writable");
    std::fs::write(&token_file, "token\n").expect("the token file must be writable");
    let key_name = key_file
        .file_name()
        .expect("the key path must have a file name")
        .to_string_lossy();
    let token_name = token_file
        .file_name()
        .expect("the token path must have a file name")
        .to_string_lossy();
    std::fs::write(
        &env_file,
        format!(
            "S3_ENDPOINT=https://s3.example.invalid\n\
             S3_BUCKET=bucket\n\
             S3_ACCESS_KEY_ID_FILE={key_name}\n\
             S3_SECRET_ACCESS_KEY=secret\n\
             DAYFLY_HTTP_BEARER_TOKEN_FILE={token_name}\n"
        ),
    )
    .expect("the env file must be writable");

    let paths = Config::watch_paths(&env_file);
    assert_eq!(paths.first(), Some(&env_file));
    assert!(paths.contains(&key_file), "missing the key file: {paths:?}");
    assert!(
        paths.contains(&token_file),
        "missing the token file: {paths:?}"
    );

    let _ = std::fs::remove_file(env_file);
    let _ = std::fs::remove_file(key_file);
    let _ = std::fs::remove_file(token_file);
}

#[test]
fn watch_paths_include_configured_asset_files_relative_to_the_env_file() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("watch-assets");
    for (name, bytes) in [
        ("favicon.png", &b"png-bytes"[..]),
        ("robots.txt", &b"User-agent: *\nDisallow: /\n"[..]),
        ("security.txt", &b"Contact: https://example.invalid/\n"[..]),
    ] {
        std::fs::write(dir.join(name), bytes).expect("the asset must be writable");
    }
    let env_file = dir.join("dayfly.env");
    std::fs::write(
        &env_file,
        "S3_ENDPOINT=https://s3.example.invalid\n\
         S3_BUCKET=bucket\n\
         S3_ACCESS_KEY_ID=key\n\
         S3_SECRET_ACCESS_KEY=secret\n\
         DAYFLY_FAVICON_PATH=favicon.png\n\
         DAYFLY_ROBOTS_TXT_PATH=robots.txt\n\
         DAYFLY_SECURITY_TXT_PATH=security.txt\n",
    )
    .expect("the env file must be writable");

    let paths = Config::watch_paths(&env_file);
    assert_eq!(paths.first(), Some(&env_file));
    for name in ["favicon.png", "robots.txt", "security.txt"] {
        let expected = dir.join(name);
        assert!(paths.contains(&expected), "missing {name} in {paths:?}");
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn watch_paths_keep_missing_asset_files() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("watch-missing-assets");
    let env_file = dir.join("dayfly.env");
    std::fs::write(
        &env_file,
        "DAYFLY_FAVICON_PATH=missing-favicon.ico\n\
         DAYFLY_ROBOTS_TXT_PATH=missing-robots.txt\n\
         DAYFLY_SECURITY_TXT_PATH=missing-security.txt\n",
    )
    .expect("the env file must be writable");

    let names = [
        "missing-favicon.ico",
        "missing-robots.txt",
        "missing-security.txt",
    ];
    let paths = Config::watch_paths(&env_file);
    for name in names {
        let expected = dir.join(name);
        assert!(
            paths.contains(&expected),
            "a missing asset path must stay watched: {paths:?}"
        );
    }

    // A missing path must not stop the watch when the file appears.
    std::fs::write(dir.join(names[0]), b"icon").expect("the asset must be writable");
    let paths = Config::watch_paths(&env_file);
    for name in names {
        let expected = dir.join(name);
        assert!(
            paths.contains(&expected),
            "the asset path must stay watched after creation: {paths:?}"
        );
    }

    let _ = std::fs::remove_dir_all(dir);
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
    env::set_var("DAYFLY_MAX_PASTE_BYTES", "9012");
    env::set_var("DAYFLY_ALIAS_WORDS", "4");
    env::set_var("DAYFLY_DEFAULT_TTL_DAYS", "30");
    env::set_var("DAYFLY_HTTP_BIND", "0.0.0.0:9999");
    env::set_var("DAYFLY_SHOW_WORDS", "false");
    env::set_var("DAYFLY_SHOW_INTERNAL_PATHS", "true");
    env::set_var("DAYFLY_VIEW_LINE_NUMBERS", "true");
    env::set_var("DAYFLY_INLINE_PDF", "false");
    env::set_var("DAYFLY_HTTP_BEARER_TOKEN", "token-value");

    let config = Config::from_env().expect("overrides must parse");
    assert_eq!(config.region, "garage-test");
    assert_eq!(config.max_text_bytes, 1234);
    assert_eq!(config.max_file_bytes, 5678);
    assert_eq!(config.max_paste_bytes, 9012);
    assert_eq!(config.alias_words, 4);
    assert_eq!(config.default_ttl_days, 30);
    assert_eq!(config.http_bind, "0.0.0.0:9999");
    assert!(!config.show_words);
    assert!(config.show_internal_paths);
    assert!(config.view_line_numbers);
    assert!(!config.inline_pdf);
    assert_eq!(config.http_bearer_token.as_deref(), Some("token-value"));
    assert_eq!(config.max_bytes_for_upload(None), 1234);
    assert_eq!(config.max_bytes_for_upload(Some("file.bin")), 5678);
}

#[test]
fn alias_word_count_out_of_range_names_the_range() {
    let _guard = lock_env();
    for value in ["2", "7"] {
        clear_env();
        set_required();
        env::set_var("DAYFLY_ALIAS_WORDS", value);
        let error = Config::from_env()
            .err()
            .expect("an out-of-range alias count must fail");
        let message = error.to_string();
        assert!(message.contains("DAYFLY_ALIAS_WORDS"), "{message}");
        assert!(message.contains("3 to 6"), "{message}");
    }
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
fn list_default_days_defaults_to_two_and_rejects_zero() {
    let _guard = lock_env();
    clear_env();
    set_required();
    let config = Config::from_env().expect("the default config must load");
    assert_eq!(config.list_default_days, 2, "the default window is 2 days");
    env::set_var("DAYFLY_LIST_DEFAULT_DAYS", "5");
    let config = Config::from_env().expect("a custom window must load");
    assert_eq!(config.list_default_days, 5);
    env::set_var("DAYFLY_LIST_DEFAULT_DAYS", "0");
    let error = Config::from_env()
        .err()
        .expect("a zero list window must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_LIST_DEFAULT_DAYS"),
        "the error must name DAYFLY_LIST_DEFAULT_DAYS, got {error}"
    );
}

#[test]
fn favicon_path_loads_the_file_and_rejects_bad_inputs() {
    let _guard = lock_env();
    clear_env();
    set_required();
    let path = env::temp_dir().join(format!("dayfly-favicon-{}.png", std::process::id()));
    std::fs::write(&path, b"png-bytes").expect("the test favicon must write");
    env::set_var("DAYFLY_FAVICON_PATH", &path);
    let config = Config::from_env().expect("a favicon must load");
    let favicon = config.favicon.expect("the favicon must be set");
    assert_eq!(&*favicon.bytes, b"png-bytes");
    assert_eq!(favicon.content_type, "image/png");

    std::fs::write(&path, b"").expect("the empty favicon must write");
    let error = Config::from_env()
        .err()
        .expect("an empty favicon must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_FAVICON_PATH"),
        "the error must name DAYFLY_FAVICON_PATH, got {error}"
    );

    env::set_var("DAYFLY_FAVICON_PATH", path.with_extension("missing"));
    let error = Config::from_env()
        .err()
        .expect("a missing favicon must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_FAVICON_PATH"),
        "the error must name DAYFLY_FAVICON_PATH, got {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn discovery_paths_load_text_files_and_reject_bad_inputs() {
    let _guard = lock_env();
    clear_env();
    set_required();
    let robots = env::temp_dir().join(format!("dayfly-robots-{}.txt", std::process::id()));
    let security = env::temp_dir().join(format!("dayfly-security-{}.txt", std::process::id()));
    std::fs::write(&robots, b"User-agent: *\nDisallow: /\n").expect("robots must write");
    std::fs::write(
        &security,
        b"Contact: https://example.invalid/\nExpires: 2027-01-01T00:00:00Z\n",
    )
    .expect("security must write");
    env::set_var("DAYFLY_ROBOTS_TXT_PATH", &robots);
    env::set_var("DAYFLY_SECURITY_TXT_PATH", &security);

    let config = Config::from_env().expect("discovery files must load");
    assert_eq!(
        &*config.robots_txt.expect("robots must be set").bytes,
        b"User-agent: *\nDisallow: /\n"
    );
    assert_eq!(
        &*config.security_txt.expect("security must be set").bytes,
        b"Contact: https://example.invalid/\nExpires: 2027-01-01T00:00:00Z\n"
    );

    std::fs::write(&robots, b"").expect("the empty robots file must write");
    let error = Config::from_env()
        .err()
        .expect("an empty robots file must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_ROBOTS_TXT_PATH"),
        "the error must name DAYFLY_ROBOTS_TXT_PATH, got {error}"
    );

    env::set_var("DAYFLY_ROBOTS_TXT_PATH", robots.with_extension("missing"));
    let error = Config::from_env()
        .err()
        .expect("a missing robots file must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_ROBOTS_TXT_PATH"),
        "the error must name DAYFLY_ROBOTS_TXT_PATH, got {error}"
    );

    std::fs::write(&robots, [0xff, 0xfe]).expect("the non-UTF-8 robots file must write");
    env::set_var("DAYFLY_ROBOTS_TXT_PATH", &robots);
    let error = Config::from_env()
        .err()
        .expect("a non-UTF-8 robots file must fail config load");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_ROBOTS_TXT_PATH"),
        "the error must name DAYFLY_ROBOTS_TXT_PATH, got {error}"
    );

    let _ = std::fs::remove_file(robots);
    let _ = std::fs::remove_file(security);
}

#[test]
fn file_cap_accepts_32_gib_and_rejects_a_part_overflow() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_MAX_FILE_BYTES", "34359738368");
    let config = Config::from_env().expect("32 GiB must fit one multipart upload");
    assert_eq!(config.max_file_bytes, 34_359_738_368);

    env::set_var("DAYFLY_MAX_FILE_BYTES", "60000000000");
    let error = Config::from_env()
        .err()
        .expect("a cap above the part budget must fail");
    assert!(
        matches!(error, ConfigError::Invalid(ref field, _) if field == "DAYFLY_MAX_FILE_BYTES"),
        "the error must name DAYFLY_MAX_FILE_BYTES, got {error}"
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
fn blank_bearer_token_is_rejected() {
    let _guard = lock_env();
    for value in ["", "   ", "\t\n"] {
        clear_env();
        set_required();
        env::set_var("DAYFLY_HTTP_BEARER_TOKEN", value);
        let error = Config::from_env()
            .err()
            .expect("a set-but-blank token must fail");
        assert!(
            matches!(error, ConfigError::Invalid(ref name, _) if name == "DAYFLY_HTTP_BEARER_TOKEN"),
            "value {value:?} gave {error}"
        );
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
fn write_requires_token_needs_a_bearer_token() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_WRITE_REQUIRES_TOKEN", "true");
    let error = Config::from_env()
        .err()
        .expect("write auth without a bearer token must fail");
    match error {
        ConfigError::Invalid(name, message) => {
            assert_eq!(name, "DAYFLY_WRITE_REQUIRES_TOKEN");
            assert!(
                message.contains("DAYFLY_HTTP_BEARER_TOKEN"),
                "message: {message}"
            );
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn write_requires_token_accepts_a_bearer_token() {
    let _guard = lock_env();
    clear_env();
    set_required();
    env::set_var("DAYFLY_WRITE_REQUIRES_TOKEN", "true");
    env::set_var("DAYFLY_HTTP_BEARER_TOKEN", "secret-token");
    let config = Config::from_env().expect("write auth config must parse");
    assert!(config.write_requires_token);
    assert_eq!(config.http_bearer_token.as_deref(), Some("secret-token"));
}

#[test]
fn write_requires_token_defaults_to_false() {
    let _guard = lock_env();
    clear_env();
    set_required();
    let config = Config::from_env().expect("default config must parse");
    assert!(!config.write_requires_token);
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
    assert!(summary.contains("show_words=true"));
    assert!(summary.contains("show_internal_paths=false"));
    assert!(summary.contains("favicon=unset"));
    assert!(summary.contains("robots_txt=unset"));
    assert!(summary.contains("security_txt=unset"));
    assert!(summary.contains("inline_pdf=true"));
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

#[test]
fn env_file_relative_secret_path_resolves_against_the_file_directory() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("relative-secret");
    std::fs::write(dir.join("access-key"), "relative-access-key\n")
        .expect("the secret file must be writable");
    let path = dir.join("dayfly.env");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://s3.example.invalid\nS3_BUCKET=bucket\nS3_ACCESS_KEY_ID_FILE=access-key\nS3_SECRET_ACCESS_KEY=secret\n",
    )
    .expect("the env file must be writable");

    let config = Config::from_env_file(Some(&path)).expect("the relative secret path must resolve");
    assert_eq!(config.access_key_id, "relative-access-key");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn env_file_relative_asset_paths_resolve_against_the_file_directory() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("relative-assets");
    std::fs::write(dir.join("favicon.png"), b"png-bytes").expect("the favicon must be writable");
    std::fs::write(dir.join("robots.txt"), b"User-agent: *\nDisallow: /\n")
        .expect("the robots file must be writable");
    std::fs::write(
        dir.join("security.txt"),
        b"Contact: https://example.invalid/\n",
    )
    .expect("the security file must be writable");
    let path = dir.join("dayfly.env");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://s3.example.invalid\n\
         S3_BUCKET=bucket\n\
         S3_ACCESS_KEY_ID=key\n\
         S3_SECRET_ACCESS_KEY=secret\n\
         DAYFLY_FAVICON_PATH=favicon.png\n\
         DAYFLY_ROBOTS_TXT_PATH=robots.txt\n\
         DAYFLY_SECURITY_TXT_PATH=security.txt\n",
    )
    .expect("the env file must be writable");

    let config = Config::from_env_file(Some(&path)).expect("relative asset paths must resolve");
    assert_eq!(
        &*config.favicon.expect("the favicon must load").bytes,
        b"png-bytes"
    );
    assert_eq!(
        &*config.robots_txt.expect("the robots file must load").bytes,
        b"User-agent: *\nDisallow: /\n"
    );
    assert_eq!(
        &*config
            .security_txt
            .expect("the security file must load")
            .bytes,
        b"Contact: https://example.invalid/\n"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn bearer_token_file_sets_the_token() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("bearer-token");
    std::fs::write(dir.join("bearer-token"), "file-token\n")
        .expect("the token file must be writable");
    let path = dir.join("dayfly.env");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://s3.example.invalid\nS3_BUCKET=bucket\nS3_ACCESS_KEY_ID=key\nS3_SECRET_ACCESS_KEY=secret\nDAYFLY_HTTP_BEARER_TOKEN_FILE=bearer-token\n",
    )
    .expect("the env file must be writable");

    let config = Config::from_env_file(Some(&path)).expect("the token file must parse");
    assert_eq!(config.http_bearer_token.as_deref(), Some("file-token"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn direct_and_file_bearer_tokens_conflict() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("bearer-conflict");
    std::fs::write(dir.join("bearer-token"), "file-token\n")
        .expect("the token file must be writable");
    let path = dir.join("dayfly.env");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://s3.example.invalid\nS3_BUCKET=bucket\nS3_ACCESS_KEY_ID=key\nS3_SECRET_ACCESS_KEY=secret\nDAYFLY_HTTP_BEARER_TOKEN=direct-token\nDAYFLY_HTTP_BEARER_TOKEN_FILE=bearer-token\n",
    )
    .expect("the env file must be writable");

    let error = match Config::from_env_file(Some(&path)) {
        Ok(_) => panic!("two token sources must fail"),
        Err(error) => error,
    };
    match error {
        ConfigError::ConflictingCredentialSources {
            variable,
            file_variable,
        } => {
            assert_eq!(variable, "DAYFLY_HTTP_BEARER_TOKEN");
            assert_eq!(file_variable, "DAYFLY_HTTP_BEARER_TOKEN_FILE");
        }
        other => panic!("unexpected error: {other}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn empty_bearer_token_file_names_the_path() {
    let _guard = lock_env();
    clear_env();
    let dir = temp_env_dir("bearer-empty");
    let token_path = dir.join("bearer-token");
    std::fs::write(&token_path, "\n").expect("the token file must be writable");
    let path = dir.join("dayfly.env");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://s3.example.invalid\nS3_BUCKET=bucket\nS3_ACCESS_KEY_ID=key\nS3_SECRET_ACCESS_KEY=secret\nDAYFLY_HTTP_BEARER_TOKEN_FILE=bearer-token\n",
    )
    .expect("the env file must be writable");

    let error = match Config::from_env_file(Some(&path)) {
        Ok(_) => panic!("an empty token file must fail"),
        Err(error) => error,
    };
    match error {
        ConfigError::EmptyCredentialFile {
            file_variable,
            path: error_path,
        } => {
            assert_eq!(file_variable, "DAYFLY_HTTP_BEARER_TOKEN_FILE");
            assert_eq!(error_path, token_path.display().to_string());
        }
        other => panic!("unexpected error: {other}"),
    }
    let _ = std::fs::remove_dir_all(dir);
}
