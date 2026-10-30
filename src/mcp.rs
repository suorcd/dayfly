//! MCP server handler, shared by the stdio and streamable HTTP transports.
//!
//! All logging goes to stderr, never stdout, so the JSON-RPC stream on
//! stdout is not corrupted. Tool results are JSON strings; binary
//! content is never sent over JSON-RPC — `fetch_file` downloads to a
//! local path and returns its metadata instead.
//!
//! Access model: the stdio transport is spawned locally by the agent
//! host, so spawning is the trust boundary there. The HTTP transport is
//! part of HTTP mode and uses that mode's bearer token when it is set.
//! The binary alone holds the Garage credentials (from environment
//! variables or credential files); tools never return them and no
//! presigned URL is ever issued.
//!
//! The two local-path tools are the exception to "a paste endpoint is
//! not a shell". `store_file` reads a server path and `fetch_file`
//! writes one, so a remote MCP client that could call them would hold
//! the process's file access. The stdio transport keeps them, because
//! the caller already runs on the host. The HTTP transport refuses them
//! unless `DAYFLY_MCP_LOCAL_ROOT` names a directory, and then both tools
//! are confined to that directory.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::store::{parse_list_window_request, ListWindowRequest, Store, StoreError, Ttl};

/// How a transport may use server-local paths.
#[derive(Clone, Debug)]
enum LocalFiles {
    /// Any path the process can reach. This is the stdio behavior when
    /// no root is configured, because the caller already runs on the
    /// host that spawned the binary.
    Open,
    /// Only paths inside this canonical directory.
    Root(Arc<PathBuf>),
    /// No local file access at all.
    Deny,
}

impl LocalFiles {
    /// Resolve the mode for one transport.
    ///
    /// A configured root applies to both transports. Without one, the
    /// caller's `open_when_unset` decides: true for stdio, false for
    /// HTTP, so an HTTP client never inherits the host's filesystem by
    /// default.
    fn resolve(store: &Store, open_when_unset: bool) -> Self {
        match store.config.mcp_local_root.clone() {
            Some(root) => LocalFiles::Root(Arc::new(root)),
            None if open_when_unset => LocalFiles::Open,
            None => LocalFiles::Deny,
        }
    }
}

/// Resolve a path that a tool will read.
///
/// A canonical root compares real paths, so a symlink that points away
/// from the root is refused instead of followed.
async fn local_read_path(local: &LocalFiles, requested: &str) -> Result<PathBuf, StoreError> {
    match local {
        LocalFiles::Open => Ok(PathBuf::from(requested)),
        LocalFiles::Deny => Err(StoreError::LocalFilesDisabled),
        LocalFiles::Root(root) => {
            let resolved = tokio::fs::canonicalize(requested)
                .await
                .map_err(|e| StoreError::InvalidPath(format!("cannot resolve {requested}: {e}")))?;
            if resolved.starts_with(root.as_path()) {
                Ok(resolved)
            } else {
                Err(StoreError::PathOutsideRoot(requested.to_string()))
            }
        }
    }
}

/// Resolve a path that a tool will write.
///
/// A destination that does not exist yet is resolved through its parent,
/// because the file cannot be canonicalized before it exists. An
/// existing destination is canonicalized, so a symlink that points
/// outside the root is refused.
async fn local_write_path(local: &LocalFiles, requested: &str) -> Result<PathBuf, StoreError> {
    match local {
        LocalFiles::Open => Ok(PathBuf::from(requested)),
        LocalFiles::Deny => Err(StoreError::LocalFilesDisabled),
        LocalFiles::Root(root) => {
            let requested = PathBuf::from(requested);
            let parent = requested
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            let real_parent = tokio::fs::canonicalize(&parent).await.map_err(|e| {
                StoreError::InvalidPath(format!("cannot resolve {}: {e}", parent.display()))
            })?;
            if !real_parent.starts_with(root.as_path()) {
                return Err(StoreError::PathOutsideRoot(requested.display().to_string()));
            }
            // `file_name` is None for a path that ends in `..`, which is
            // the one form that would escape the checked parent.
            let Some(name) = requested.file_name() else {
                return Err(StoreError::InvalidPath(requested.display().to_string()));
            };
            let candidate = real_parent.join(name);
            if tokio::fs::symlink_metadata(&candidate).await.is_ok() {
                let real = tokio::fs::canonicalize(&candidate).await.map_err(|e| {
                    StoreError::InvalidPath(format!("cannot resolve {}: {e}", candidate.display()))
                })?;
                if !real.starts_with(root.as_path()) {
                    return Err(StoreError::PathOutsideRoot(requested.display().to_string()));
                }
                return Ok(real);
            }
            Ok(candidate)
        }
    }
}

#[derive(Clone)]
pub struct Pastebin {
    store: Arc<Store>,
    local: LocalFiles,
}

fn result_json(value: serde_json::Value) -> Result<CallToolResult, rmcp::ErrorData> {
    Ok(CallToolResult::success(vec![ContentBlock::text(
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
    )]))
}

/// Map a store error to the value that a tool method returns.
///
/// A routine domain result is a tool result with `isError` set, so a
/// missing or expired paste does not look like a transport fault. A bad
/// argument is a JSON-RPC `-32602` error. Only an internal fault is a
/// `-32603` error.
fn store_error_result(e: StoreError) -> Result<CallToolResult, rmcp::ErrorData> {
    match e {
        StoreError::NotFound(_)
        | StoreError::Expired(_)
        | StoreError::TooLarge { .. }
        | StoreError::RangeNotSatisfiable => Ok(CallToolResult::error(vec![ContentBlock::text(
            e.to_string(),
        )])),
        StoreError::InvalidId(_)
        | StoreError::InvalidDate(_)
        | StoreError::InvalidRange(_)
        | StoreError::InvalidPath(_)
        | StoreError::PathOutsideRoot(_)
        | StoreError::LocalFilesDisabled => {
            Err(rmcp::ErrorData::invalid_params(e.to_string(), None))
        }
        StoreError::Io(_)
        | StoreError::S3(_)
        | StoreError::BucketUnavailable(_)
        | StoreError::AliasUnavailable { .. } => {
            Err(rmcp::ErrorData::internal_error(e.to_string(), None))
        }
    }
}

/// Await a store call. If the call fails, return the mapped value to the
/// client from the tool method.
macro_rules! try_store {
    ($call:expr) => {
        match $call {
            Ok(value) => value,
            Err(e) => return store_error_result(e),
        }
    };
}

fn invalid_ttl(message: impl Into<String>) -> rmcp::ErrorData {
    rmcp::ErrorData::invalid_params(message.into(), None)
}

fn resolve_ttl(ttl: Option<&str>, ttl_days: Option<u32>) -> Result<Option<Ttl>, rmcp::ErrorData> {
    if ttl.is_some() && ttl_days.is_some() {
        return Err(invalid_ttl("use ttl or ttl_days, not both"));
    }
    if let Some(value) = ttl {
        return Ttl::parse(value)
            .map(Some)
            .map_err(|e| invalid_ttl(format!("ttl: {e}")));
    }
    match ttl_days {
        Some(days) => Ttl::checked_days(days)
            .map(Some)
            .map_err(|e| invalid_ttl(format!("ttl_days: {e}"))),
        None => Ok(None),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreTextParams {
    /// The text to store.
    pub text: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    /// TTL as seconds or a duration such as "1h", "1h30m", or "2d".
    /// Do not set both `ttl` and `ttl_days`.
    pub ttl: Option<String>,
    /// TTL in whole days, for example 7. Do not set both `ttl` and
    /// `ttl_days`.
    pub ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreFileParams {
    /// Local path of the file to upload.
    pub path: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    /// TTL as seconds or a duration such as "1h", "1h30m", or "2d".
    /// Do not set both `ttl` and `ttl_days`.
    pub ttl: Option<String>,
    /// TTL in whole days, for example 7. Do not set both `ttl` and
    /// `ttl_days`.
    pub ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AppendTextParams {
    /// Full or bare paste ID, or a three-word alias.
    pub id: String,
    /// Text to append. The text cap applies to this append only.
    pub text: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchTextParams {
    /// Full or bare paste ID, or a three-word alias.
    pub id: String,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchFileParams {
    /// Full or bare paste ID, or a three-word alias.
    pub id: String,
    /// Local destination path for the downloaded file.
    pub dest_path: String,
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct IdParams {
    /// Full or bare paste ID, or a three-word alias.
    pub id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListRecentParams {
    /// Single date, YYYY-MM-DD. Takes precedence over start and end.
    pub date: Option<String>,
    /// Inclusive window start, YYYY-MM-DD. Requires end.
    pub start: Option<String>,
    /// Inclusive window end, YYYY-MM-DD. Requires start.
    pub end: Option<String>,
    pub limit: Option<usize>,
}

/// Parse the MCP listing parameters. `date` takes precedence over the range.
fn list_request(params: &ListRecentParams) -> Result<ListWindowRequest, StoreError> {
    parse_list_window_request(
        params.date.as_deref(),
        params.start.as_deref(),
        params.end.as_deref(),
    )
}

#[tool_router]
impl Pastebin {
    /// Build the handler for the stdio transport.
    ///
    /// Spawning the binary locally is the trust boundary, so the
    /// local-path tools keep any readable path unless
    /// `DAYFLY_MCP_LOCAL_ROOT` narrows them.
    pub fn stdio(store: Store) -> Self {
        let local = LocalFiles::resolve(&store, true);
        Self {
            store: Arc::new(store),
            local,
        }
    }

    /// Build the handler for the HTTP transport.
    ///
    /// The caller is remote, so the local-path tools are refused unless
    /// `DAYFLY_MCP_LOCAL_ROOT` names a directory they may use.
    pub fn http(store: Store) -> Self {
        let local = LocalFiles::resolve(&store, false);
        Self {
            store: Arc::new(store),
            local,
        }
    }

    #[tool(
        description = "Store a text paste in Garage (S3). Returns id, url path, size, sha256, content_type, detection_source, detection_conflict, and expiry. Set ttl to seconds or a duration such as 1h or 1h30m. A bare integer is seconds; use 7d for seven days. Do not set ttl and ttl_days together."
    )]
    async fn store_text(
        &self,
        Parameters(params): Parameters<StoreTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let ttl = resolve_ttl(params.ttl.as_deref(), params.ttl_days)?;
        let stored = try_store!(
            self.store
                .store_text(
                    params.text.as_bytes(),
                    params.filename.as_deref(),
                    params.content_type.as_deref(),
                    ttl,
                )
                .await
        );
        result_json(serde_json::json!({
            "id": stored.id,
            "key": stored.key,
            "words": stored.words,
            "size": stored.size,
            "sha256": stored.sha256,
            "content_type": stored.content_type,
            "filename": stored.filename,
            "detection_source": stored.detection_source,
            "detection_conflict": stored.detection_conflict,
            "expires_at": stored.expires_at.to_rfc3339(),
        }))
    }

    #[tool(
        description = "Store a local file in Garage (S3), using multipart upload for large files. Returns id, size, sha256, content_type, detection_source, and expiry. Set ttl to seconds or a duration such as 1h or 1h30m. A bare integer is seconds; use 7d for seven days. Do not set ttl and ttl_days together."
    )]
    async fn store_file(
        &self,
        Parameters(params): Parameters<StoreFileParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let ttl = resolve_ttl(params.ttl.as_deref(), params.ttl_days)?;
        let path = match local_read_path(&self.local, &params.path).await {
            Ok(path) => path,
            Err(e) => return store_error_result(e),
        };
        let stored = try_store!(
            self.store
                .store_file_path(
                    path.as_path(),
                    params.filename.as_deref(),
                    params.content_type.as_deref(),
                    ttl,
                )
                .await
        );
        result_json(serde_json::json!({
            "id": stored.id,
            "key": stored.key,
            "words": stored.words,
            "size": stored.size,
            "sha256": stored.sha256,
            "content_type": stored.content_type,
            "filename": stored.filename,
            "detection_source": stored.detection_source,
            "detection_conflict": stored.detection_conflict,
            "expires_at": stored.expires_at.to_rfc3339(),
        }))
    }

    #[tool(
        description = "Append text to an existing paste without resending its current contents. Writes one new segment object; reads concatenate the base object and segments. An expired or missing id or alias returns an error and writes nothing. An empty append writes nothing. sha256 is null when segments exist."
    )]
    async fn append_text(
        &self,
        Parameters(params): Parameters<AppendTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let appended = try_store!(
            self.store
                .append_text(&params.id, params.text.as_bytes())
                .await
        );
        result_json(serde_json::json!({
            "id": appended.meta.id,
            "key": appended.meta.key,
            "words": appended.meta.words,
            "appended_bytes": appended.appended_bytes,
            "segment_key": appended.segment_key,
            "size": appended.meta.size,
            "sha256": appended.meta.sha256,
            "content_type": appended.meta.content_type,
            "filename": appended.meta.filename,
            "created_at": appended.meta.created_at.map(|d| d.to_rfc3339()),
            "expires_at": appended.meta.expires_at.map(|d| d.to_rfc3339()),
            "segments": appended.meta.segments.len(),
        }))
    }

    #[tool(
        description = "Fetch a text paste by id or three-word alias, with optional byte offset and limit. Large pastes should be read in slices, not whole."
    )]
    async fn fetch_text(
        &self,
        Parameters(params): Parameters<FetchTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let slice = try_store!(
            self.store
                .fetch_text(&params.id, params.offset.unwrap_or(0), params.limit)
                .await
        );
        result_json(serde_json::json!({
            "text": slice.text,
            "total_bytes": slice.total_bytes,
            "returned_bytes": slice.returned_bytes,
            "truncated": slice.truncated,
        }))
    }

    #[tool(
        description = "Download a paste/file by id or three-word alias to a local path (streamed in chunks, never loaded whole into memory, binary safe). Returns the object's metadata, not its bytes."
    )]
    async fn fetch_file(
        &self,
        Parameters(params): Parameters<FetchFileParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let range = match (params.range_start, params.range_end) {
            (Some(s), Some(e)) => Some((s, e)),
            (Some(s), None) => Some((s, u64::MAX)),
            _ => None,
        };
        let dest = match local_write_path(&self.local, &params.dest_path).await {
            Ok(dest) => dest,
            Err(e) => return store_error_result(e),
        };
        let meta = try_store!(
            self.store
                .download_file(&params.id, dest.as_path(), range)
                .await
        );
        result_json(serde_json::json!({
            "dest_path": params.dest_path,
            "id": meta.id,
            "words": meta.words,
            "size": meta.size,
            "content_type": meta.content_type,
            "filename": meta.filename,
            "detection_source": meta.detection_source,
            "detection_conflict": meta.detection_conflict,
            "sha256": meta.sha256,
            "expires_at": meta.expires_at.map(|d| d.to_rfc3339()),
        }))
    }

    #[tool(
        description = "Fetch metadata for a paste by id or three-word alias (exists, size, content_type, detection_source, filename, sha256, expiry). Equivalent to HeadObject, plus the expiry check."
    )]
    async fn fetch_metadata(
        &self,
        Parameters(params): Parameters<IdParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let meta = try_store!(self.store.head_metadata(&params.id).await);
        result_json(serde_json::json!({
            "id": meta.id,
            "key": meta.key,
            "words": meta.words,
            "filename": meta.filename,
            "content_type": meta.content_type,
            "detection_source": meta.detection_source,
            "detection_conflict": meta.detection_conflict,
            "size": meta.size,
            "sha256": meta.sha256,
            "created_at": meta.created_at.map(|d| d.to_rfc3339()),
            "expires_at": meta.expires_at.map(|d| d.to_rfc3339()),
            "expired": meta.expired,
        }))
    }

    #[tool(description = "Delete a paste by id or three-word alias.")]
    async fn delete_paste(
        &self,
        Parameters(params): Parameters<IdParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        try_store!(self.store.delete(&params.id).await);
        result_json(serde_json::json!({"deleted": params.id}))
    }

    #[tool(
        description = "List current pastes for one date or an inclusive date range. date=YYYY-MM-DD takes precedence over start and end. A range needs both start and end (YYYY-MM-DD), and start must be <= end. With no dates, the window is yesterday and today in UTC. This scans the bucket directly (ListObjectsV2 + HeadObject per key) and filters expired objects; it is prefix listing, not search or sort, and is O(n) in objects scanned."
    )]
    async fn list_recent(
        &self,
        Parameters(params): Parameters<ListRecentParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let request = try_store!(list_request(&params));
        let limit = params.limit.unwrap_or(100);
        let items = match &request {
            ListWindowRequest::Date(date) => {
                try_store!(self.store.list_recent(Some(date), limit).await)
            }
            ListWindowRequest::Range { start, end } => {
                try_store!(self.store.list_window(Some(start), Some(end), limit).await)
            }
            ListWindowRequest::Default => {
                try_store!(self.store.list_window(None, None, limit).await)
            }
        };
        result_json(serde_json::json!({
            "date": params.date,
            "start": params.start,
            "end": params.end,
            "items": items.iter().map(|i| serde_json::json!({
                "id": i.id,
                "key": i.key,
                "words": i.words,
                "size": i.size,
                "last_modified": i.last_modified.map(|d| d.to_rfc3339()),
                "expires_at": i.expires_at.map(|d| d.to_rfc3339()),
                "created_at": i.created_at.map(|d| d.to_rfc3339()),
                "content_type": i.content_type,
                "filename": i.filename,
            })).collect::<Vec<_>>(),
        }))
    }
}

impl Pastebin {
    /// The initialize instructions, including what the local-path tools
    /// allow on this transport.
    fn instructions(&self) -> String {
        let mut text = String::from(
            "Internal pastebin/file-share backed by a private Garage bucket. \
             The binary is the sole S3 reader/writer; agents never get S3 \
             credentials and no presigned URL is issued. The TTL accepts \
             seconds or a duration such as 90m, 1h30m, or 2d. A bare \
             integer is seconds; use 7d for seven days. Use ttl or \
             ttl_days, not both. Expiry is enforced on read. A three-word \
             alias is a second name for a paste; use the ID or the alias.",
        );
        match &self.local {
            LocalFiles::Open => text.push_str(
                " store_file reads and fetch_file writes any path this process can reach, \
                 because this transport is local.",
            ),
            LocalFiles::Root(root) => text.push_str(&format!(
                " store_file and fetch_file are limited to paths under {}.",
                root.display()
            )),
            LocalFiles::Deny => text.push_str(
                " store_file and fetch_file are not available on this transport. \
                 Use store_text, fetch_text, and the HTTP upload route instead.",
            ),
        }
        text
    }
}

#[tool_handler]
impl rmcp::handler::server::ServerHandler for Pastebin {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("dayfly", "0.1.0"))
            .with_instructions(self.instructions())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ErrorCode;

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("dayfly-mcp-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test directory must be creatable");
        dir
    }

    fn rooted(dir: &Path) -> LocalFiles {
        LocalFiles::Root(Arc::new(
            dir.canonicalize().expect("test root must canonicalize"),
        ))
    }

    #[tokio::test]
    async fn a_read_under_the_root_is_allowed_and_an_escape_is_not() {
        let root = test_dir("read");
        let inside = root.join("note.txt");
        std::fs::write(&inside, b"payload").expect("test file must be writable");
        let outside =
            std::env::temp_dir().join(format!("dayfly-mcp-{}-outside.txt", std::process::id()));
        std::fs::write(&outside, b"secret").expect("test file must be writable");

        let local = rooted(&root);
        let ok = local_read_path(&local, inside.to_str().expect("UTF-8")).await;
        assert!(ok.is_ok(), "a file under the root must resolve: {ok:?}");

        // A symlink inside the root that points out of it is resolved and
        // then refused, so the target is not read.
        let link = root.join("escape.txt");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink must be creatable");
        match local_read_path(&local, link.to_str().expect("UTF-8")).await {
            Err(StoreError::PathOutsideRoot(_)) => {}
            other => panic!("a symlink out of the root must be refused, got {other:?}"),
        }

        // `..` out of the root, on a path that exists.
        let traversal = root.join("..").join(outside.file_name().expect("name"));
        match local_read_path(&local, traversal.to_str().expect("UTF-8")).await {
            Err(StoreError::PathOutsideRoot(_)) => {}
            other => panic!("a traversal out of the root must be refused, got {other:?}"),
        }

        match local_read_path(&local, outside.to_str().expect("UTF-8")).await {
            Err(StoreError::PathOutsideRoot(_)) => {}
            other => panic!("an absolute path outside the root must be refused, got {other:?}"),
        }

        let _ = std::fs::remove_file(&outside);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn a_write_under_the_root_is_allowed_for_a_new_file() {
        let root = test_dir("write");
        let local = rooted(&root);
        let dest = root.join("new.txt");
        assert!(!dest.exists());
        let resolved = local_write_path(&local, dest.to_str().expect("UTF-8"))
            .await
            .expect("a new file under the root must resolve");
        assert_eq!(
            resolved,
            root.canonicalize().expect("root").join("new.txt"),
            "the resolved destination keeps the requested file name"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn an_unconfigured_http_transport_denies_both_paths() {
        for path in ["/etc/hostname", "relative.txt"] {
            match local_read_path(&LocalFiles::Deny, path).await {
                Err(StoreError::LocalFilesDisabled) => {}
                other => panic!("a denied transport must refuse reads, got {other:?}"),
            }
            match local_write_path(&LocalFiles::Deny, path).await {
                Err(StoreError::LocalFilesDisabled) => {}
                other => panic!("a denied transport must refuse writes, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn an_open_transport_keeps_the_stdio_path_behavior() {
        let path = std::env::temp_dir().join(format!("dayfly-mcp-{}-open.txt", std::process::id()));
        std::fs::write(&path, b"x").expect("test file must be writable");
        let resolved = local_read_path(&LocalFiles::Open, path.to_str().expect("UTF-8"))
            .await
            .expect("an open transport must resolve any path");
        assert_eq!(resolved, path);
        let _ = std::fs::remove_file(&path);
    }

    fn assert_tool_error(e: StoreError) {
        let expected = e.to_string();
        let result =
            store_error_result(e).expect("a routine result must not become a protocol error");
        assert_eq!(result.is_error, Some(true));
        assert_eq!(result.content.len(), 1);
        assert!(
            matches!(
                &result.content[0],
                ContentBlock::Text(t) if t.text == expected
            ),
            "the store message must survive the mapping: {expected}"
        );
    }

    #[test]
    fn missing_paste_returns_tool_error_result() {
        assert_tool_error(StoreError::NotFound("2026-10-28/missing".into()));
    }

    #[test]
    fn routine_domain_errors_return_tool_error_results() {
        assert_tool_error(StoreError::Expired("2026-10-28/old".into()));
        assert_tool_error(StoreError::TooLarge { limit: 1024 });
        assert_tool_error(StoreError::RangeNotSatisfiable);
    }

    #[test]
    fn bad_arguments_return_invalid_params() {
        for e in [
            StoreError::InvalidId("bad id".into()),
            StoreError::InvalidDate("2026-13-01".into()),
            StoreError::InvalidRange("end before start".into()),
        ] {
            let err = store_error_result(e).expect_err("a bad argument must be a protocol error");
            assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        }
    }

    #[test]
    fn internal_faults_return_internal_errors() {
        for e in [
            StoreError::Io("disk full".into()),
            StoreError::S3("s3 failure".into()),
            StoreError::BucketUnavailable("bucket missing".into()),
            StoreError::AliasUnavailable { attempts: 8 },
        ] {
            let err =
                store_error_result(e).expect_err("an internal fault must be a protocol error");
            assert_eq!(err.code, ErrorCode::INTERNAL_ERROR);
        }
    }

    #[test]
    fn ttl_parameter_accepts_subday_duration() {
        let ttl = resolve_ttl(Some("1h30m"), None)
            .expect("a valid ttl must parse")
            .expect("a ttl must be present");
        assert_eq!(ttl.num_seconds(), 5_400);
    }

    #[test]
    fn ttl_days_still_selects_whole_days() {
        let ttl = resolve_ttl(None, Some(3))
            .expect("ttl_days must be valid")
            .expect("a ttl must be present");
        assert_eq!(ttl.num_seconds(), 3 * 86_400);
    }

    #[test]
    fn ttl_days_zero_is_invalid_params() {
        let err = resolve_ttl(None, Some(0)).expect_err("zero days must fail");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert!(err.message.contains("ttl_days"), "got: {}", err.message);
    }

    #[test]
    fn mixed_ttl_parameters_are_invalid_params() {
        let err = resolve_ttl(Some("1h"), Some(7)).expect_err("both TTL forms must fail");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
        assert!(err.message.contains("ttl_days"), "got: {}", err.message);
    }

    #[test]
    fn malformed_ttl_parameter_is_invalid_params() {
        let err = resolve_ttl(Some("1x"), None).expect_err("a bad ttl must fail");
        assert_eq!(err.code, ErrorCode::INVALID_PARAMS);
    }

    fn params(date: Option<&str>, start: Option<&str>, end: Option<&str>) -> ListRecentParams {
        ListRecentParams {
            date: date.map(str::to_string),
            start: start.map(str::to_string),
            end: end.map(str::to_string),
            limit: None,
        }
    }

    #[test]
    fn list_params_keep_date_as_one_day() {
        assert_eq!(
            list_request(&params(Some("2026-10-28"), None, None)).unwrap(),
            ListWindowRequest::Date("2026-10-28".to_string())
        );
    }

    #[test]
    fn list_params_accept_an_inclusive_range() {
        assert_eq!(
            list_request(&params(None, Some("2026-10-28"), Some("2026-10-29"))).unwrap(),
            ListWindowRequest::Range {
                start: "2026-10-28".to_string(),
                end: "2026-10-29".to_string(),
            }
        );
    }

    #[test]
    fn list_params_reject_a_one_sided_range() {
        assert!(matches!(
            list_request(&params(None, Some("2026-10-28"), None)),
            Err(StoreError::InvalidDate(_))
        ));
        assert!(matches!(
            list_request(&params(None, None, Some("2026-10-28"))),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn list_params_reject_an_inverted_range() {
        assert!(matches!(
            list_request(&params(None, Some("2026-10-29"), Some("2026-10-28"))),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn list_params_reject_a_malformed_date() {
        assert!(matches!(
            list_request(&params(None, Some("2026-13-01"), Some("2026-12-01"))),
            Err(StoreError::InvalidDate(_))
        ));
    }

    #[test]
    fn list_params_use_the_default_window_with_no_dates() {
        assert_eq!(
            list_request(&params(None, None, None)).unwrap(),
            ListWindowRequest::Default
        );
    }
}
