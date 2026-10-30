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

use std::path::PathBuf;
use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::store::{parse_list_window_request, ListWindowRequest, Store, StoreError, Ttl};

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
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(store),
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
        let stored = try_store!(
            self.store
                .store_file_path(
                    PathBuf::from(&params.path).as_path(),
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

#[tool_handler]
impl rmcp::handler::server::ServerHandler for Pastebin {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("dayfly", "0.1.0"))
            .with_instructions(
                "Internal pastebin/file-share backed by a private Garage bucket. \
                 The binary is the sole S3 reader/writer; agents never get S3 \
                 credentials and no presigned URL is issued. The TTL accepts \
                 seconds or a duration such as 90m, 1h30m, or 2d. A bare \
                 integer is seconds; use 7d for seven days. Use ttl or \
                 ttl_days, not both. Expiry is enforced on read. A three-word \
                 alias is a second name for a paste; use the ID or the alias.",
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
