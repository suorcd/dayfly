//! MCP stdio integration tests.
//!
//! These tests spawn the real binary, speak newline-delimited JSON-RPC to
//! it, and point it at the in-process fake S3 service. This covers the MCP
//! transport, the tool router, and the store together.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use chrono::Utc;
use common::FakeS3;
use serde_json::{json, Value};

#[derive(Debug)]
enum ToolCallError {
    /// A JSON-RPC protocol error, with its JSON-RPC code.
    JsonRpc { code: i64, message: String },
    /// A tool result with `isError: true`. The request itself succeeded.
    Tool { message: String },
}

struct McpClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Drop for McpClient {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl McpClient {
    fn spawn(fake: &FakeS3, extra_env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dayfly"));
        command
            .arg("mcp")
            .env("S3_ENDPOINT", &fake.endpoint)
            .env("S3_BUCKET", &fake.bucket)
            .env("S3_REGION", "us-east-1")
            .env("S3_ACCESS_KEY_ID", "test-access-key")
            .env("S3_SECRET_ACCESS_KEY", "test-secret-key")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("dayfly MCP process must start");
        let stdin = child.stdin.take().expect("MCP stdin must be piped");
        let stdout = child.stdout.take().expect("MCP stdout must be piped");
        let mut client = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 0,
        };
        client.initialize();
        client
    }

    fn initialize(&mut self) {
        let response = self.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "dayfly-integration", "version": "0.1.0"}
            }),
        );
        let result = response
            .get("result")
            .unwrap_or_else(|| panic!("initialize must succeed: {response}"));
        assert_eq!(result["serverInfo"]["name"], "dayfly");
        assert!(result["capabilities"]["tools"].is_object());
        self.notify("notifications/initialized", json!({}));
    }

    fn notify(&mut self, method: &str, params: Value) {
        let message = json!({"jsonrpc": "2.0", "method": method, "params": params});
        writeln!(self.stdin, "{message}").expect("MCP notification must be writable");
        self.stdin.flush().expect("MCP notification must flush");
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let message = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        writeln!(self.stdin, "{message}").expect("MCP request must be writable");
        self.stdin.flush().expect("MCP request must flush");
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("MCP response must be readable");
            assert!(read > 0, "MCP server closed stdout before responding");
            let value: Value = serde_json::from_str(line.trim())
                .unwrap_or_else(|e| panic!("MCP response must be JSON ({e}): {line}"));
            if value.get("id") == Some(&json!(id)) {
                return value;
            }
            // Ignore server notifications and log messages.
        }
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value, ToolCallError> {
        let response = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        if let Some(error) = response.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown MCP error")
                .to_string();
            return Err(ToolCallError::JsonRpc { code, message });
        }
        let result = &response["result"];
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            let message = result["content"][0]["text"]
                .as_str()
                .unwrap_or("unknown tool error")
                .to_string();
            return Err(ToolCallError::Tool { message });
        }
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("tool result must contain text: {result}"));
        Ok(serde_json::from_str(text).expect("tool result text must be JSON"))
    }
}

fn assert_tool_error(error: ToolCallError, expected: &str) {
    match error {
        ToolCallError::Tool { message } => {
            assert!(message.contains(expected), "got: {message}");
        }
        other => panic!("expected a tool error result, got {other:?}"),
    }
}

fn assert_json_rpc_error(error: ToolCallError, code: i64, expected: &str) {
    match error {
        ToolCallError::JsonRpc {
            code: actual,
            message,
        } => {
            assert_eq!(actual, code, "message: {message}");
            assert!(message.contains(expected), "got: {message}");
        }
        other => panic!("expected JSON-RPC error {code}, got {other:?}"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime must build")
}

#[test]
fn tools_list_exposes_every_dayfly_tool() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let mut client = McpClient::spawn(&fake, &[]);

    let response = client.request("tools/list", json!({}));
    let tools = response["result"]["tools"]
        .as_array()
        .expect("tools/list must return an array");
    let names: Vec<&str> = tools
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    for expected in [
        "store_text",
        "store_file",
        "append_text",
        "fetch_text",
        "fetch_file",
        "fetch_metadata",
        "delete_paste",
        "list_recent",
    ] {
        assert!(
            names.contains(&expected),
            "missing tool {expected}: {names:?}"
        );
    }
}

#[test]
fn store_text_fetch_metadata_list_and_delete_round_trip() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let mut client = McpClient::spawn(&fake, &[]);

    let stored = client
        .call_tool(
            "store_text",
            json!({"text": "hello mcp", "filename": "note.txt", "ttl_days": 1}),
        )
        .expect("store_text must succeed");
    assert_eq!(stored["size"], 9);
    assert_eq!(stored["filename"], "note.txt");
    assert_eq!(stored["content_type"], "text/plain");
    let id = stored["id"].as_str().expect("id must be a string");
    let key = stored["key"].as_str().expect("key must be a string");
    let object = fake.object(key).expect("the fake must hold the object");
    assert_eq!(object.body, b"hello mcp");

    let slice = client
        .call_tool("fetch_text", json!({"id": id, "offset": 0, "limit": 5}))
        .expect("fetch_text must succeed");
    assert_eq!(slice["text"], "hello");
    assert_eq!(slice["total_bytes"], 9);
    assert_eq!(slice["returned_bytes"], 5);
    assert_eq!(slice["truncated"], true);

    let date = id.split_once('/').expect("dated id").0;
    let bare = id.split_once('/').expect("dated id").1;
    let meta = client
        .call_tool("fetch_metadata", json!({"id": bare}))
        .expect("fetch_metadata must resolve a bare id");
    assert_eq!(meta["id"], id);
    assert_eq!(meta["filename"], "note.txt");
    assert_eq!(meta["expired"], false);

    let listed = client
        .call_tool("list_recent", json!({"date": date}))
        .expect("list_recent must succeed");
    let items = listed["items"].as_array().expect("items must be an array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], id);

    let deleted = client
        .call_tool("delete_paste", json!({"id": id}))
        .expect("delete_paste must succeed");
    assert_eq!(deleted["deleted"], id);
    assert!(fake.object(key).is_none());

    let error = client
        .call_tool("fetch_metadata", json!({"id": id}))
        .expect_err("a deleted paste must not be found");
    assert_tool_error(error, "paste not found");
}

#[test]
fn store_file_and_fetch_file_move_bytes_without_json_payloads() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let source_path = common::write_temp_file("mcp-source.bin", b"abcdefghij");
    let dest_path = common::temp_path("mcp-dest.bin");
    let range_path = common::temp_path("mcp-range.bin");
    let mut client = McpClient::spawn(&fake, &[]);

    let stored = client
        .call_tool(
            "store_file",
            json!({
                "path": source_path.to_str().unwrap(),
                "filename": "data.bin",
                "ttl_days": 2
            }),
        )
        .expect("store_file must succeed");
    assert_eq!(stored["size"], 10);
    assert_eq!(stored["filename"], "data.bin");
    let id = stored["id"].as_str().expect("id must be a string");

    let fetched = client
        .call_tool(
            "fetch_file",
            json!({"id": id, "dest_path": dest_path.to_str().unwrap()}),
        )
        .expect("fetch_file must succeed");
    assert_eq!(fetched["id"], id);
    assert_eq!(
        std::fs::read(&dest_path).expect("downloaded file must exist"),
        b"abcdefghij"
    );

    client
        .call_tool(
            "fetch_file",
            json!({
                "id": id,
                "dest_path": range_path.to_str().unwrap(),
                "range_start": 2,
                "range_end": 4
            }),
        )
        .expect("ranged fetch_file must succeed");
    assert_eq!(
        std::fs::read(&range_path).expect("ranged file must exist"),
        b"cde"
    );

    let _ = std::fs::remove_file(source_path);
    let _ = std::fs::remove_file(dest_path);
    let _ = std::fs::remove_file(range_path);
}

#[test]
fn text_cap_and_file_cap_are_enforced_through_mcp() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let source_path = common::write_temp_file("mcp-over-cap.bin", b"123456");
    let mut client = McpClient::spawn(
        &fake,
        &[
            ("DAYFLY_MAX_TEXT_BYTES", "5"),
            ("DAYFLY_MAX_FILE_BYTES", "5"),
        ],
    );

    let error = client
        .call_tool("store_text", json!({"text": "123456"}))
        .expect_err("six text bytes must exceed a five-byte cap");
    assert_tool_error(error, "payload too large");
    assert!(fake.keys().is_empty());

    let error = client
        .call_tool(
            "store_file",
            json!({"path": source_path.to_str().unwrap(), "filename": "over.bin"}),
        )
        .expect_err("six file bytes must exceed a five-byte cap");
    assert_tool_error(error, "payload too large");
    assert!(fake.keys().is_empty());
    let _ = std::fs::remove_file(source_path);
}

#[test]
fn errors_are_reported_for_missing_and_invalid_inputs() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let mut client = McpClient::spawn(&fake, &[]);

    let error = client
        .call_tool("fetch_text", json!({"id": "2026-10-28/missing"}))
        .expect_err("a missing paste must be an error");
    assert_tool_error(error, "paste not found");

    let error = client
        .call_tool("fetch_metadata", json!({"id": "../etc/passwd"}))
        .expect_err("a traversal id must be rejected");
    assert_json_rpc_error(error, -32602, "invalid id");

    let error = client
        .call_tool("store_file", json!({"path": "/no/such/dayfly/file"}))
        .expect_err("a missing local file must be an error");
    assert_json_rpc_error(error, -32603, "io error");

    let error = client
        .call_tool("list_recent", json!({"date": "not-a-date"}))
        .expect_err("an invalid date must be an error");
    assert_json_rpc_error(error, -32602, "invalid date");

    let error = client
        .call_tool("delete_paste", json!({"id": ".."}))
        .expect_err("a traversal id must be rejected on delete");
    assert_json_rpc_error(error, -32602, "invalid id");
}

#[test]
fn ttl_days_zero_is_rejected() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let mut client = McpClient::spawn(&fake, &[]);

    let error = client
        .call_tool(
            "store_text",
            json!({"text": "expired at creation", "ttl_days": 0}),
        )
        .expect_err("ttl_days=0 must be rejected");
    assert_json_rpc_error(error, -32602, "ttl_days");

    let error = client
        .call_tool(
            "store_file",
            json!({"path": "/no/such/file", "ttl_days": 0}),
        )
        .expect_err("ttl_days=0 must be rejected before the file is read");
    assert_json_rpc_error(error, -32602, "ttl_days");
}

#[test]
fn mixed_ttl_forms_are_rejected() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let mut client = McpClient::spawn(&fake, &[]);

    let error = client
        .call_tool(
            "store_text",
            json!({"text": "mixed", "ttl": "1h", "ttl_days": 7}),
        )
        .expect_err("ttl plus ttl_days must be rejected");
    assert_json_rpc_error(error, -32602, "ttl");

    let error = client
        .call_tool(
            "store_file",
            json!({"path": "/no/such/file", "ttl": "1h", "ttl_days": 7}),
        )
        .expect_err("ttl plus ttl_days must be rejected before the file is read");
    assert_json_rpc_error(error, -32602, "ttl");
}

#[test]
fn list_recent_filters_expired_objects_through_mcp() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let valid = common::stored_metadata(
        &(Utc::now() + chrono::Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    let expired = common::stored_metadata(
        &(Utc::now() - chrono::Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    fake.seed(
        &format!("pastes/{today}/valid"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&valid),
    );
    fake.seed(
        &format!("pastes/{today}/expired"),
        b"abc",
        "text/plain",
        &common::metadata_refs(&expired),
    );
    let mut client = McpClient::spawn(&fake, &[]);

    let listed = client
        .call_tool("list_recent", json!({"date": today, "limit": 10}))
        .expect("list_recent must succeed");
    let items = listed["items"].as_array().expect("items must be an array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], format!("{today}/valid"));
}

#[test]
fn fetch_file_missing_object_returns_an_error_without_writing() {
    let runtime = runtime();
    let fake = runtime.block_on(FakeS3::start());
    let dest_path = common::temp_path("mcp-missing.bin");
    let mut client = McpClient::spawn(&fake, &[]);

    let error = client
        .call_tool(
            "fetch_file",
            json!({
                "id": "2026-10-28/missing",
                "dest_path": dest_path.to_str().unwrap()
            }),
        )
        .expect_err("a missing object must be an error");
    assert_tool_error(error, "paste not found");
    assert!(
        !dest_path.exists(),
        "a failed fetch must not create the destination file"
    );
}
