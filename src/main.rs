//! dayfly: internal pastebin/file-share backed by a private
//! Garage bucket, with no database in v1.
//!
//! Two modes share one S3 core:
//! * MCP server over stdio (default, no subcommand) — primary interface.
//! * HTTP mode (`serve`) — plain-curl API, a small server-rendered human
//!   frontend (`/`, `/pastes`, `/view/<id>`), and the MCP server over
//!   streamable HTTP at `/mcp`.
//!
//! Logging goes to stderr only in both modes, so the MCP JSON-RPC
//! stream on stdout is never corrupted. Credentials come from
//! environment variables or credential files and are never logged.

use std::env;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rmcp::transport::stdio;
use rmcp::ServiceExt;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use dayfly::config::Config;
use dayfly::http;
use dayfly::http::AppState;
use dayfly::mcp;
use dayfly::store::Store;

#[derive(Parser)]
#[command(
    name = "dayfly",
    version,
    about = "Internal pastebin backed by a private Garage bucket",
    after_help = "Precedence: process environment, then the --config file, then built-in defaults. RUST_LOG overrides --verbose."
)]
struct Cli {
    /// Environment file to read. DAYFLY_CONFIG sets the same path.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Show INFO logs. RUST_LOG overrides this flag.
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the MCP server over stdio (also the default with no subcommand).
    Mcp,
    /// Run HTTP mode: curl API, the human frontend, and MCP at /mcp.
    Serve {
        /// Bind address, overriding DAYFLY_HTTP_BIND.
        #[arg(long)]
        bind: Option<String>,
    },
}

/// Resolve the environment file path and whether the caller named it.
fn resolve_config_path(flag: Option<PathBuf>) -> (Option<PathBuf>, bool) {
    if let Some(path) = flag {
        return (Some(path), true);
    }
    if let Some(path) = env::var_os("DAYFLY_CONFIG").filter(|value| !value.is_empty()) {
        return (Some(PathBuf::from(path)), true);
    }
    (default_config_path(), false)
}

/// The default user config path, when a home directory is known.
fn default_config_path() -> Option<PathBuf> {
    let base = env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".config"))
        })?;
    Some(base.join("dayfly").join("dayfly.env"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Stderr only: stdout belongs to MCP JSON-RPC in MCP mode.
    // The default level is WARN, so an unsafe credential file permission
    // warning is visible without RUST_LOG. RUST_LOG still sets the filter.
    let default_level = if cli.verbose {
        LevelFilter::INFO
    } else {
        LevelFilter::WARN
    };
    let filter = EnvFilter::builder()
        .with_default_directive(default_level.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();

    let (config_path, explicit) = resolve_config_path(cli.config);
    let config = if explicit || config_path.as_ref().is_some_and(|path| path.exists()) {
        Config::from_env_file(config_path.as_deref())?
    } else {
        Config::from_env()?
    };
    tracing::info!("dayfly starting: {}", config.safe_summary());
    let store = Store::from_config(config.clone()).await?;

    match cli.command {
        None | Some(Command::Mcp) => {
            let service = mcp::Pastebin::stdio(store).serve(stdio()).await?;
            service.waiting().await?;
        }
        Some(Command::Serve { bind }) => {
            let bind_addr = bind.unwrap_or(config.http_bind.clone());
            let state = AppState { store };
            let app = http::app_router(state);
            let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
            tracing::info!("HTTP listening on {bind_addr}");
            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}
