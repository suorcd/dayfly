//! MCP server over stdio (the primary interface).
//!
//! All logging goes to stderr, never stdout, so the JSON-RPC stream on
//! stdout is not corrupted. Tool results are JSON strings; binary
//! content is never sent over JSON-RPC — `fetch_file` downloads to a
//! local path and returns its metadata instead.
//!
//! Access model: the MCP process is spawned locally by the agent host,
//! so spawning is the trust boundary here. The binary alone holds the
//! Garage credentials (from environment variables or credential files);
//! tools never return them and no presigned URL is ever issued.

use std::path::PathBuf;
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::store::{Store, StoreError};

#[derive(Clone)]
pub struct Pastebin {
    store: Arc<Store>,
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
        StoreError::InvalidId(_) | StoreError::InvalidDate(_) | StoreError::InvalidRange(_) => {
            Err(rmcp::ErrorData::invalid_params(e.to_string(), None))
        }
        StoreError::Io(_) | StoreError::S3(_) | StoreError::BucketUnavailable(_) => {
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
        }
    }

    #[tool(
        description = "Store a text paste in Garage (S3). Returns id, url path, size, sha256, content_type, detection_source, detection_conflict, and expiry."
    )]
    async fn store_text(
        &self,
        Parameters(params): Parameters<StoreTextParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let stored = try_store!(
            self.store
                .store_text(
                    params.text.as_bytes(),
                    params.filename.as_deref(),
                    params.content_type.as_deref(),
                    params.ttl_days,
                )
                .await
        );
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
        let stored = try_store!(
            self.store
                .store_file_path(
                    PathBuf::from(&params.path).as_path(),
                    params.filename.as_deref(),
                    params.content_type.as_deref(),
                    params.ttl_days,
                )
                .await
        );
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
        let meta = try_store!(
            self.store
                .download_file(
                    &params.id,
                    PathBuf::from(&params.dest_path).as_path(),
                    range,
                )
                .await
        );
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
        let meta = try_store!(self.store.head_metadata(&params.id).await);
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
        try_store!(self.store.delete(&params.id).await);
        result_json(serde_json::json!({"deleted": params.id}))
    }

    #[tool(
        description = "List recent pastes for one date prefix only (YYYY-MM-DD, default today UTC). This scans the bucket directly (ListObjectsV2 + HeadObject per key) and filters expired objects; it is prefix listing, not search or sort, and is O(n) in objects scanned."
    )]
    async fn list_recent(
        &self,
        Parameters(params): Parameters<ListRecentParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let items = try_store!(
            self.store
                .list_recent(params.date.as_deref(), params.limit.unwrap_or(100))
                .await
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::ErrorCode;

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
        ] {
            let err =
                store_error_result(e).expect_err("an internal fault must be a protocol error");
            assert_eq!(err.code, ErrorCode::INTERNAL_ERROR);
        }
    }
}
