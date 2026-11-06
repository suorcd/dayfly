//! Integration tests for `scripts/clean.sh`.
//!
//! These tests run the real script in temporary directories. They cover
//! the local build outputs, the `--rust-cache` flag, the dry-run mode,
//! and the flake-root guard.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct TempRoot {
    path: PathBuf,
}

impl TempRoot {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("dayfly-clean-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create the temporary root");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn write(&self, relative: &str, contents: &str) {
        let path = self.path.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create the parent directory");
        }
        fs::write(path, contents).expect("write the test file");
    }

    fn mkdir(&self, relative: &str) {
        fs::create_dir_all(self.path.join(relative)).expect("create the test directory");
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/clean.sh")
}

fn run_clean(root: &Path, args: &[&str]) -> Output {
    Command::new("bash")
        .arg(script())
        .arg("--root")
        .arg(root)
        .args(args)
        .output()
        .expect("run scripts/clean.sh")
}

fn exists(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "clean failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn clean_removes_result_links_target_and_direnv() {
    let root = TempRoot::new("local-build-outputs");
    root.write("flake.nix", "{}");
    root.mkdir("store/dayfly");
    symlink(root.path().join("store/dayfly"), root.path().join("result")).unwrap();
    symlink(
        root.path().join("store/dayfly"),
        root.path().join("result-container"),
    )
    .unwrap();
    root.mkdir("target/debug");
    root.write("target/debug/dayfly", "binary");
    root.mkdir(".direnv");
    root.write(".direnv/flake-profile", "profile");
    root.write("result.txt", "keep");
    root.write("keep.txt", "keep");

    let output = run_clean(root.path(), &[]);
    assert_success(&output);

    assert!(!exists(&root.path().join("result")));
    assert!(!exists(&root.path().join("result-container")));
    assert!(!exists(&root.path().join("target")));
    assert!(!exists(&root.path().join(".direnv")));
    assert!(root.path().join("result.txt").is_file());
    assert!(root.path().join("keep.txt").is_file());
    assert!(
        root.path().join("store/dayfly").is_dir(),
        "clean must not follow the result symlink"
    );
}

#[test]
fn clean_dry_run_reports_without_removing() {
    let root = TempRoot::new("dry-run");
    root.write("flake.nix", "{}");
    root.mkdir("store");
    symlink(root.path().join("store"), root.path().join("result")).unwrap();
    root.mkdir("target");
    root.write("target/artifact", "data");

    let output = run_clean(root.path(), &["--dry-run"]);
    assert_success(&output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("would remove"), "{stdout}");
    assert!(exists(&root.path().join("result")));
    assert!(exists(&root.path().join("target")));
}

#[test]
fn clean_rust_cache_flag_removes_cargo_and_sccache_caches() {
    let root = TempRoot::new("rust-cache");
    root.write("flake.nix", "{}");
    let cargo_home = root.path().join("cargo-home");
    let sccache = root.path().join("sccache");
    for path in [
        "registry/cache/example.crate",
        "registry/src/example/src/lib.rs",
        "registry/index/example",
        "git/db/example",
        "git/checkouts/example/file",
        ".package-cache",
        "credentials.toml",
        "config.toml",
    ] {
        root.write(&format!("cargo-home/{path}"), "data");
    }
    root.write("sccache/object", "data");

    let output = Command::new("bash")
        .arg(script())
        .arg("--root")
        .arg(root.path())
        .arg("--rust-cache")
        .env("CARGO_HOME", &cargo_home)
        .env("SCCACHE_DIR", &sccache)
        .output()
        .expect("run scripts/clean.sh");
    assert_success(&output);

    assert!(!exists(&cargo_home.join("registry/cache")));
    assert!(!exists(&cargo_home.join("registry/src")));
    assert!(!exists(&cargo_home.join("git/db")));
    assert!(!exists(&cargo_home.join("git/checkouts")));
    assert!(!exists(&cargo_home.join(".package-cache")));
    assert!(exists(&cargo_home.join("registry/index")));
    assert!(exists(&cargo_home.join("credentials.toml")));
    assert!(exists(&cargo_home.join("config.toml")));
    assert!(!exists(&sccache));
}

#[test]
fn clean_without_rust_cache_flag_keeps_cargo_caches() {
    let root = TempRoot::new("rust-cache-opt-in");
    root.write("flake.nix", "{}");
    let cargo_home = root.path().join("cargo-home");
    let sccache = root.path().join("sccache");
    root.write("cargo-home/registry/cache/example.crate", "data");
    root.write("sccache/object", "data");

    let output = Command::new("bash")
        .arg(script())
        .arg("--root")
        .arg(root.path())
        .env("CARGO_HOME", &cargo_home)
        .env("SCCACHE_DIR", &sccache)
        .output()
        .expect("run scripts/clean.sh");
    assert_success(&output);

    assert!(exists(&cargo_home.join("registry/cache")));
    assert!(exists(&sccache));
}

#[test]
fn clean_refuses_a_directory_without_a_flake() {
    let root = TempRoot::new("no-flake");
    root.mkdir("target");
    root.write("target/artifact", "data");

    let output = run_clean(root.path(), &[]);
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("flake.nix"), "{stderr}");
    assert!(exists(&root.path().join("target")));
}

#[test]
fn clean_searches_parent_directories_for_the_flake() {
    let root = TempRoot::new("parent-search");
    root.write("flake.nix", "{}");
    root.mkdir("target");
    root.write("target/artifact", "data");
    root.mkdir("sub/dir");

    let output = Command::new("bash")
        .arg(script())
        .current_dir(root.path().join("sub/dir"))
        .output()
        .expect("run scripts/clean.sh");
    assert_success(&output);

    assert!(!exists(&root.path().join("target")));
}
