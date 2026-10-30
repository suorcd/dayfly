//! dayfly library.
//!
//! The binary uses this library. Integration tests use the same public
//! modules to drive the S3 core, the HTTP router, and the human web
//! router in process. The MCP server remains a stdio process.

pub mod config;
pub mod content_type;
pub mod http;
pub mod mcp;
pub mod store;
pub mod stream;
#[cfg(test)]
mod test_s3;
pub mod web;
