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
use std::fmt::Write as _;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use rmcp::transport::stdio;
use rmcp::ServiceExt;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::EnvFilter;

use dayfly::config::{Config, CONFIG_FIELDS};
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
    /// Manage the environment file.
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Write a new environment file.
    Init {
        /// Write here instead of the --config or default path.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Replace an existing file.
        #[arg(long)]
        force: bool,
    },
}

/// Write a new environment file.
fn init_config_file(path: Option<PathBuf>, force: bool) -> anyhow::Result<()> {
    let path = path.ok_or_else(|| {
        anyhow::anyhow!("no config path: use --out or set HOME or XDG_CONFIG_HOME")
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let parent_existed = parent.exists();
    std::fs::create_dir_all(parent)?;
    if !parent_existed {
        set_mode(parent, 0o700)?;
    }
    if path.exists() && !force {
        anyhow::bail!("config file already exists: {}", path.display());
    }
    std::fs::write(&path, config_template())?;
    set_mode(&path, 0o600)?;
    println!("Wrote {}", path.display());
    let required = CONFIG_FIELDS
        .iter()
        .filter(|field| field.required)
        .map(|field| field.name)
        .collect::<Vec<_>>()
        .join(", ");
    println!("Set real values for: {required}");
    Ok(())
}

/// The environment file template.
fn config_template() -> String {
    let mut out = String::from(
        "# dayfly environment file. Keep this file private.\n# A process environment variable wins over this file.\n",
    );
    for field in CONFIG_FIELDS {
        if field.required {
            writeln!(out, "# {}", field.help).expect("writing to a String cannot fail");
            writeln!(out, "{}={}", field.name, field.value.unwrap_or(""))
                .expect("writing to a String cannot fail");
        } else if let Some(value) = field.value {
            writeln!(out, "# {}\n# {}={}", field.help, field.name, value)
                .expect("writing to a String cannot fail");
        } else {
            writeln!(out, "# {}\n# {}=", field.help, field.name)
                .expect("writing to a String cannot fail");
        }
    }
    out.push_str(
        "#\n# Secret file alternatives. Do not set both forms for one credential.\n# S3_ACCESS_KEY_ID_FILE=/home/you/.config/dayfly/access-key-id\n# S3_SECRET_ACCESS_KEY_FILE=/home/you/.config/dayfly/secret-access-key\n",
    );
    out
}

#[cfg(unix)]
fn set_mode(path: &std::path::Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_path: &std::path::Path, _mode: u32) -> std::io::Result<()> {
    Ok(())
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
    if let Some(Command::Config { action }) = &cli.command {
        return match action {
            ConfigAction::Init { out, force } => {
                init_config_file(out.clone().or(config_path.clone()), *force)
            }
        };
    }
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
        Some(Command::Config { .. }) => {
            unreachable!("config commands return before the store starts")
        }
    }
    Ok(())
}
