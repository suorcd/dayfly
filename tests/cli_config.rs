//! CLI tests for the optional environment file.

use std::path::PathBuf;
use std::process::{Command, Stdio};

use dayfly::config::Config;

fn temp_env_file(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "dayfly-cli-config-{}-{name}.env",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dayfly"));
    for name in [
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
        "DAYFLY_HTTP_BEARER_TOKEN",
        "DAYFLY_MCP_LOCAL_ROOT",
        "DAYFLY_CONFIG",
        "RUST_LOG",
        "HOME",
        "XDG_CONFIG_HOME",
    ] {
        command.env_remove(name);
    }
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command
}

#[test]
fn config_file_supplies_values_and_verbose_shows_startup() {
    let path = temp_env_file("values");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://file.example:3900\nS3_BUCKET=file-bucket\nS3_ACCESS_KEY_ID=file-key\nS3_SECRET_ACCESS_KEY=file-secret\n",
    )
    .expect("temporary config file must be writable");

    let output = command()
        .arg("--config")
        .arg(&path)
        .arg("--verbose")
        .arg("mcp")
        .output()
        .expect("dayfly must start");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("dayfly starting: bucket=file-bucket"),
        "the config file values must reach startup: {stderr}"
    );
    assert!(
        stderr.contains("INFO"),
        "--verbose must show the INFO startup line: {stderr}"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_named_config_file_must_exist() {
    let path = temp_env_file("missing");
    let output = command()
        .arg("--config")
        .arg(&path)
        .arg("mcp")
        .output()
        .expect("dayfly must start");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot read config file"),
        "a missing named file must fail: {stderr}"
    );
    assert!(
        stderr.contains(path.to_str().expect("path must be UTF-8")),
        "the error must name the path: {stderr}"
    );
}

#[test]
fn process_environment_wins_over_the_config_file() {
    let path = temp_env_file("precedence");
    std::fs::write(
        &path,
        "S3_ENDPOINT=https://file.example:3900\nS3_BUCKET=file-bucket\nS3_ACCESS_KEY_ID=file-key\nS3_SECRET_ACCESS_KEY=file-secret\n",
    )
    .expect("temporary config file must be writable");

    let output = command()
        .env("S3_BUCKET", "environment-bucket")
        .arg("--config")
        .arg(&path)
        .arg("--verbose")
        .arg("mcp")
        .output()
        .expect("dayfly must start");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("dayfly starting: bucket=environment-bucket"),
        "the process environment must win: {stderr}"
    );
    let _ = std::fs::remove_file(path);
}

fn temp_config_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("dayfly-cli-init-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn config_init_writes_a_parseable_private_file() {
    let dir = temp_config_dir("private");
    let path = dir.join("dayfly.env");

    let output = command()
        .arg("config")
        .arg("init")
        .arg("--out")
        .arg(&path)
        .output()
        .expect("config init must run");
    assert!(
        output.status.success(),
        "config init must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Wrote"), "got: {stdout}");
    assert!(stdout.contains("S3_ENDPOINT"), "got: {stdout}");
    assert!(path.exists());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    let config = Config::from_env_file(Some(&path)).expect("the generated file must parse");
    assert_eq!(config.bucket, "pastebin-test");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn config_init_refuses_to_replace_without_force() {
    let dir = temp_config_dir("refuse");
    let path = dir.join("dayfly.env");
    let args = ["config", "init", "--out"];
    let first = command()
        .args(args)
        .arg(&path)
        .output()
        .expect("config init must run");
    assert!(first.status.success());
    let original = std::fs::read_to_string(&path).expect("the file must exist");
    std::fs::write(&path, "S3_BUCKET=changed\n").expect("the file must be writable");

    let second = command()
        .args(args)
        .arg(&path)
        .output()
        .expect("config init must run");
    assert!(!second.status.success(), "a second run must fail");
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"), "got: {stderr}");
    assert_ne!(
        std::fs::read_to_string(&path).expect("the file must still exist"),
        original,
        "the existing file must not be replaced"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn config_init_force_replaces_an_existing_file() {
    let dir = temp_config_dir("force");
    let path = dir.join("dayfly.env");
    std::fs::create_dir_all(&dir).expect("the directory must be creatable");
    std::fs::write(&path, "S3_BUCKET=changed\n").expect("the file must be writable");

    let output = command()
        .arg("config")
        .arg("init")
        .arg("--out")
        .arg(&path)
        .arg("--force")
        .output()
        .expect("config init must run");
    assert!(
        output.status.success(),
        "config init --force must succeed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(std::fs::read_to_string(&path)
        .expect("the file must exist")
        .contains("S3_BUCKET=pastebin-test"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn env_file_name_is_gitignored() {
    let ignore = include_str!("../.gitignore");
    assert!(
        ignore.lines().any(|line| line.trim() == ".env"),
        "the .env name must be ignored"
    );
}
