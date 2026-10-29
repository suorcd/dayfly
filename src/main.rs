//! dayfly: internal pastebin/file-share backed by a private
//! Garage bucket, with no database in v1.
//!
//! Two transports share one S3 core:
//! * MCP server over stdio (default, no subcommand) — primary interface.
//! * HTTP mode (`serve`) — plain-curl API plus a small server-rendered
//!   human frontend (`/`, `/pastes`, `/view/<id>`).
//!
//! Logging goes to stderr only in both modes, so the MCP JSON-RPC
//! stream on stdout is never corrupted. Credentials come from
//! environment variables or credential files and are never logged.

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
use dayfly::web;

#[derive(Parser)]
#[command(
    name = "dayfly",
    version,
    about = "Internal pastebin backed by a private Garage bucket"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the MCP server over stdio (also the default with no subcommand).
    Mcp,
    /// Run HTTP mode: curl API plus the human frontend.
    Serve {
        /// Bind address, overriding DAYFLY_HTTP_BIND.
        #[arg(long)]
        bind: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Stderr only: stdout belongs to MCP JSON-RPC in MCP mode.
    // The default level is WARN, so an unsafe credential file permission
    // warning is visible without RUST_LOG. RUST_LOG still sets the filter.
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();

    let cli = Cli::parse();
    let config = Config::from_env()?;
    tracing::info!("dayfly starting: {}", config.safe_summary());
    let store = Store::from_config(config.clone()).await?;

    match cli.command {
        None | Some(Command::Mcp) => {
            let service = mcp::Pastebin::new(store).serve(stdio()).await?;
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
