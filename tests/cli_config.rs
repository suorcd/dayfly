//! CLI tests for the optional environment file.

use std::path::PathBuf;
use std::process::{Command, Stdio};

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
