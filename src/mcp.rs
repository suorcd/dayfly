//! MCP server over stdio (the primary interface).
//!
//! All logging goes to stderr, never stdout, so the JSON-RPC stream on
//! stdout is not corrupted. Tool results are JSON strings; binary
//! content is never sent over JSON-RPC — `fetch_file` downloads to a
//! local path and returns its metadata instead.
//!
//! Access model: the MCP process is spawned locally by the agent host,
//! so spawning is the trust boundary here. The binary alone holds the
//! Garage credentials (from the environment); tools never return them
//! and no presigned URL is ever issued.

use std::path::PathBuf;
use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::store::Store;

#[derive(Clone)]
pub struct Pastebin {
    store: Arc<Store>,
    tool_router: ToolRouter<Self>,
}

fn result_json(value: serde_json::Value) -> Result<CallToolResult, rmcp::ErrorData> {
    Ok(CallToolResult::success(vec![ContentBlock::text(
        serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
    )]))
}

fn err_json(e: crate::store::StoreError) -> rmcp::ErrorData {
    rmcp::ErrorData::new(rmcp::model::ErrorCode::INTERNAL_ERROR, e.to_string(), None)
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreTextParams {
    /// The text to store.
    pub text: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct StoreFileParams {
    /// Local path of the file to upload.
    pub path: String,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub ttl_days: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchTextParams {
    pub id: String,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchFileParams {
    pub id: String,
    /// Local destination path for the downloaded file.
    pub dest_path: String,
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct IdParams {
    pub id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListRecentParams {
    /// Date prefix, YYYY-MM-DD. Defaults to today (UTC).
    pub date: Option<String>,
    pub limit: Option<usize>,
}

#[tool_router]
impl Pastebin {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Store a text paste in Garage (S3). Returns id, url path, size, sha256, content_type, detection_source, detection_conflict, and expiry."
    )]
    async fn store_text(
        &self,
        Parameters(params): Parameters<StoreTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let stored = self
            .store
            .store_text(
                params.text.as_bytes(),
                params.filename.as_deref(),
                params.content_type.as_deref(),
                params.ttl_days,
            )
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "id": stored.id,
            "key": stored.key,
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
        description = "Store a local file in Garage (S3), using multipart upload for large files. Returns id, size, sha256, content_type, detection_source, and expiry."
    )]
    async fn store_file(
        &self,
        Parameters(params): Parameters<StoreFileParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let stored = self
            .store
            .store_file_path(
                PathBuf::from(&params.path).as_path(),
                params.filename.as_deref(),
                params.content_type.as_deref(),
                params.ttl_days,
            )
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "id": stored.id,
            "key": stored.key,
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
        description = "Fetch a text paste by id, with optional byte offset and limit. Large pastes should be read in slices, not whole."
    )]
    async fn fetch_text(
        &self,
        Parameters(params): Parameters<FetchTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let slice = self
            .store
            .fetch_text(&params.id, params.offset.unwrap_or(0), params.limit)
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "text": slice.text,
            "total_bytes": slice.total_bytes,
            "returned_bytes": slice.returned_bytes,
            "truncated": slice.truncated,
        }))
    }

    #[tool(
        description = "Download a paste/file by id to a local path (streamed in chunks, never loaded whole into memory, binary safe). Returns the object's metadata, not its bytes."
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
        let meta = self
            .store
            .download_file(
                &params.id,
                PathBuf::from(&params.dest_path).as_path(),
                range,
            )
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "dest_path": params.dest_path,
            "id": meta.id,
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
        description = "Fetch metadata for a paste by id (exists, size, content_type, detection_source, filename, sha256, expiry). Equivalent to HeadObject, plus the expiry check."
    )]
    async fn fetch_metadata(
        &self,
        Parameters(params): Parameters<IdParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let meta = self
            .store
            .head_metadata(&params.id)
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "id": meta.id,
            "key": meta.key,
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

    #[tool(description = "Delete a paste by id.")]
    async fn delete_paste(
        &self,
        Parameters(params): Parameters<IdParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        self.store.delete(&params.id).await.map_err(err_json)?;
        result_json(serde_json::json!({"deleted": params.id}))
    }

    #[tool(
        description = "List recent pastes for one date prefix only (YYYY-MM-DD, default today UTC). This scans the bucket directly (ListObjectsV2 + HeadObject per key) and filters expired objects; it is prefix listing, not search or sort, and is O(n) in objects scanned."
    )]
    async fn list_recent(
        &self,
        Parameters(params): Parameters<ListRecentParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let items = self
            .store
            .list_recent(params.date.as_deref(), params.limit.unwrap_or(100))
            .await
            .map_err(err_json)?;
        result_json(serde_json::json!({
            "date": params.date,
            "items": items.iter().map(|i| serde_json::json!({
                "id": i.id,
                "key": i.key,
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

#[tool_handler]
impl rmcp::handler::server::ServerHandler for Pastebin {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("dayfly", "0.1.0"))
            .with_instructions(
                "Internal pastebin/file-share backed by a private Garage bucket. \
                 The binary is the sole S3 reader/writer; agents never get S3 \
                 credentials and no presigned URL is issued. Expiry is \
                 day-granular and enforced on read.",
            )
    }
}
