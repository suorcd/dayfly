//! Reload configuration from the environment file without a restart.
//!
//! The trigger is a file watch. Polling the modification time is simple,
//! works when no signal can reach the process (the container case), and
//! needs no extra dependency.
//!
//! A reload builds a new [`Store`] and swaps it in only after the new
//! configuration passes validation. A malformed file leaves the running
//! configuration in place. Each request takes one store snapshot, so a
//! reload affects the next request, not one in progress.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::config::{bind_is_loopback, ensure_bind_is_safe, Config};
use crate::http::AppState;
use crate::store::Store;

/// The poll interval for the environment-file watch.
const WATCH_INTERVAL: Duration = Duration::from_secs(2);

/// One successful reload.
#[derive(Debug, Default)]
pub struct ReloadReport {
    /// Field names whose values changed. The names are safe to log; a
    /// value is never included.
    pub changed: Vec<&'static str>,
    /// Settings that need a restart. They were not applied.
    pub warnings: Vec<String>,
}

/// Reload from the environment file at `path`.
///
/// The caller passes the address the listener is bound to. A change to
/// `DAYFLY_HTTP_BIND` cannot move the listener, so it is ignored with one
/// warning. A new configuration that removes the bearer token from a
/// non-loopback listener is refused, and the old configuration stays.
pub async fn reload_from_file(
    path: &Path,
    state: &AppState,
    bound_addr: &str,
) -> Result<ReloadReport, String> {
    let config = Config::from_env_file(Some(path)).map_err(|error| error.to_string())?;
    reload_with_config(state, config, bound_addr).await
}

/// Apply one already-parsed configuration.
///
/// Tests call this function directly, so no test depends on watcher
/// timing.
pub async fn reload_with_config(
    state: &AppState,
    new_config: Config,
    bound_addr: &str,
) -> Result<ReloadReport, String> {
    let old = state.store();
    let mut new_config = new_config;
    let mut warnings = Vec::new();

    // The listener is already bound. Keep the old value and report the
    // change, so the stored configuration describes the running service.
    if new_config.http_bind != old.config.http_bind {
        warnings.push(format!(
            "DAYFLY_HTTP_BIND changed from {} to {}. A restart is necessary to rebind. \
             The listener stays on {}.",
            old.config.http_bind, new_config.http_bind, bound_addr
        ));
        new_config.http_bind = old.config.http_bind.clone();
    }
    if new_config.allow_unauthenticated != old.config.allow_unauthenticated {
        warnings.push("DAYFLY_ALLOW_UNAUTHENTICATED changed. A restart is necessary.".to_string());
        new_config.allow_unauthenticated = old.config.allow_unauthenticated;
    }
    if new_config.mcp_local_root != old.config.mcp_local_root {
        warnings.push("DAYFLY_MCP_LOCAL_ROOT changed. A restart is necessary.".to_string());
        new_config.mcp_local_root = old.config.mcp_local_root.clone();
    }

    // A token removal on a reachable listener needs the same guard as
    // startup. The new store is not built when this check fails.
    ensure_bind_is_safe(
        bound_addr,
        new_config.http_bearer_token.is_some(),
        new_config.allow_unauthenticated,
    )
    .map_err(|error| error.to_string())?;
    if !bind_is_loopback(bound_addr) && new_config.http_bearer_token.is_none() {
        warnings.push(format!(
            "serving {bound_addr} without a bearer token: every host that reaches this \
             port can read, append to, and delete every paste"
        ));
    }

    let changed = changed_fields(&old.config, &new_config);
    let new_store = Store::from_config(new_config)
        .await
        .map_err(|error| error.to_string())?;
    state.replace_store(new_store);
    Ok(ReloadReport { changed, warnings })
}

/// Start the environment-file watch in `serve` mode.
///
/// A process that uses only process-environment settings has no file to
/// watch. It logs one line and starts no task.
pub fn spawn_file_watcher(paths: Vec<PathBuf>, state: AppState, bound_addr: String) {
    let Some(env_file) = paths.first().cloned() else {
        tracing::info!("configuration reload: no env file in use; nothing to reload");
        return;
    };
    tokio::spawn(async move {
        let mut watched = paths;
        let mut last = file_stamps(&watched);
        let mut interval = tokio::time::interval(WATCH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let current = file_stamps(&watched);
            if current == last {
                continue;
            }
            match reload_from_file(&env_file, &state, &bound_addr).await {
                Ok(report) => log_reload(&report, &state.store()),
                Err(error) => tracing::error!(
                    "configuration reload failed: {error}; the running configuration stays"
                ),
            }
            // A reload can change the set of secret files. Follow the new
            // set even when the reload failed, so a later fix is seen.
            watched = Config::watch_paths(&env_file);
            last = file_stamps(&watched);
        }
    });
}

fn log_reload(report: &ReloadReport, store: &Store) {
    let changed = if report.changed.is_empty() {
        "no field".to_string()
    } else {
        report.changed.join(", ")
    };
    tracing::info!(
        "configuration reloaded: changed {} ({})",
        changed,
        store.config.safe_summary()
    );
    for warning in &report.warnings {
        tracing::warn!("{warning}");
    }
}

/// The file state that triggers a reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

fn file_stamp(path: &Path) -> Option<FileStamp> {
    let metadata = std::fs::metadata(path).ok()?;
    Some(FileStamp {
        modified: metadata.modified().ok(),
        len: metadata.len(),
    })
}

fn file_stamps(paths: &[PathBuf]) -> Vec<Option<FileStamp>> {
    paths.iter().map(|path| file_stamp(path)).collect()
}

fn changed_fields(old: &Config, new: &Config) -> Vec<&'static str> {
    let mut changed = Vec::new();
    if old.endpoint != new.endpoint {
        changed.push("S3_ENDPOINT");
    }
    if old.bucket != new.bucket {
        changed.push("S3_BUCKET");
    }
    if old.region != new.region {
        changed.push("S3_REGION");
    }
    if old.access_key_id != new.access_key_id || old.secret_access_key != new.secret_access_key {
        changed.push("S3 credentials");
    }
    if old.max_text_bytes != new.max_text_bytes {
        changed.push("DAYFLY_MAX_TEXT_BYTES");
    }
    if old.max_file_bytes != new.max_file_bytes {
        changed.push("DAYFLY_MAX_FILE_BYTES");
    }
    if old.max_paste_bytes != new.max_paste_bytes {
        changed.push("DAYFLY_MAX_PASTE_BYTES");
    }
    if old.default_ttl_days != new.default_ttl_days {
        changed.push("DAYFLY_DEFAULT_TTL_DAYS");
    }
    if old.show_words != new.show_words {
        changed.push("DAYFLY_SHOW_WORDS");
    }
    if old.http_bearer_token != new.http_bearer_token {
        changed.push("DAYFLY_HTTP_BEARER_TOKEN");
    }
    changed
}
