//! Optional periodic cleanup inside `serve`.
//!
//! The loop runs [`crate::store::Store::cleanup`] on a schedule, so one
//! Deployment can clean up without a separate CronJob. The loop is off
//! when `DAYFLY_CLEANUP_INTERVAL_SECONDS` is zero. Enable it on exactly
//! one writer per bucket: two loops on one bucket would scan the same
//! objects twice and could abort a live upload that the other process
//! still drives.
//!
//! A pass reads its interval from the current store snapshot after each
//! run, so a configuration reload can change the pace or stop the loop.
//! A failed pass logs and counts, then the next pass runs on schedule.
//! The loop never stops `serve`.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::metrics::Metrics;
use crate::store::{log_paste_id, CleanupOptions, CleanupReport, StoreHandle};

/// Delay before the first pass when `DAYFLY_CLEANUP_ON_STARTUP` is set.
///
/// The delay is short, so the listener is up before the first scan.
pub const CLEANUP_STARTUP_DELAY: Duration = Duration::from_secs(5);

/// Log one cleanup report the way the CLI prints it.
///
/// The CLI prints one line per failed item and a final count line to
/// stdout. The loop logs the same counts and failures to stderr, so the
/// report stays visible without stdout. Keys are hashes unless the
/// operator opted into raw values with `DAYFLY_LOG_RAW_PASTE_IDS`.
pub fn log_cleanup_report(report: &CleanupReport, log_raw: bool) {
    for item in &report.multipart.failed {
        tracing::warn!(
            upload_id = %item.upload_id,
            key = %log_paste_id(&item.key, log_raw),
            error = %item.message,
            "cleanup multipart abort failed"
        );
    }
    for failure in &report.pastes_failed {
        tracing::warn!(
            key = %log_paste_id(&failure.key, log_raw),
            error = %failure.message,
            "cleanup paste delete failed"
        );
    }
    for failure in &report.aliases_failed {
        tracing::warn!(
            key = %log_paste_id(&failure.key, log_raw),
            error = %failure.message,
            "cleanup alias delete failed"
        );
    }
    tracing::info!(
        "cleanup: dry_run=false aborted={} would_abort={} pastes_deleted={} pastes_would_delete={} aliases_deleted={} aliases_would_delete={} failed={}",
        report.multipart.aborted.len(),
        report.multipart.would_abort.len(),
        report.pastes_deleted.len(),
        report.pastes_would_delete.len(),
        report.aliases_deleted.len(),
        report.aliases_would_delete.len(),
        report.failed()
    );
}

/// Run the cleanup loop until the configured interval becomes zero.
///
/// The first pass runs after `first_pass_delay`. Each later pass runs
/// after the interval from the current configuration. The caller picks
/// `first_pass_delay`: the startup delay when
/// `DAYFLY_CLEANUP_ON_STARTUP` is set, or the interval otherwise.
pub async fn run_cleanup_loop(
    handle: StoreHandle,
    metrics: Option<Arc<Metrics>>,
    first_pass_delay: Duration,
) {
    tokio::time::sleep(first_pass_delay).await;
    loop {
        let store = handle.get();
        let options = CleanupOptions {
            dry_run: false,
            multipart_older_than: Duration::from_secs(store.config.multipart_abort_after_seconds),
        };
        match store.cleanup(options).await {
            Ok(report) => {
                log_cleanup_report(&report, store.config.log_raw_paste_ids);
                if let Some(metrics) = &metrics {
                    metrics.observe_cleanup_pass(
                        report.pastes_deleted.len() as u64,
                        report.aliases_deleted.len() as u64,
                        report.failed() as u64,
                    );
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "cleanup pass failed");
                if let Some(metrics) = &metrics {
                    metrics.observe_cleanup_pass_error();
                }
            }
        }
        let interval_seconds = handle.get().config.cleanup_interval_seconds;
        if interval_seconds == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(interval_seconds)).await;
    }
}

/// Spawn the cleanup loop on a blocking-safe background task.
///
/// The task borrows the store handle, so a reload replaces the store
/// that the next pass uses. The caller keeps the handle to stop the
/// task, or lets it end with the process.
pub fn spawn_cleanup_loop(
    handle: StoreHandle,
    metrics: Option<Arc<Metrics>>,
    first_pass_delay: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_cleanup_loop(handle, metrics, first_pass_delay).await;
    })
}
