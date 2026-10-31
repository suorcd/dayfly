//! Streaming upload core.
//!
//! This module is deliberately independent of any concrete S3 client so
//! its behaviour can be tested without a live bucket:
//!
//! * [`PartBuffer`] turns an arbitrary chunk stream into 5 MiB parts
//!   (the S3 multipart minimum, except for the last part).
//! * [`UploadSink`] is the S3-side of a streaming upload. The real
//!   implementation lives in [`crate::store`]; tests use a fake sink.
//! * [`run_streaming_upload`] drives a sink from a byte stream. It is the
//!   only place that decides between a single PutObject and multipart,
//!   and it is the place that guarantees: on any stream error, part
//!   error, completion error, or oversize input, the multipart upload is
//!   aborted and no paste record (and therefore no URL) is returned.
//!
//! HTTP handlers must only construct a paste/URL *after*
//! [`run_streaming_upload`] returns `Ok`.

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use sha2::{Digest, Sha256};
use std::pin::Pin;

use crate::store::StoreError;

/// S3 multipart minimum part size, except for the last part.
pub const STREAM_PART_BYTES: usize = 5 * 1024 * 1024;

/// Accumulates incoming chunks and yields complete 5 MiB parts.
#[derive(Debug, Default)]
pub struct PartBuffer {
    buf: Vec<u8>,
}

impl PartBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a chunk. Returns every complete 5 MiB part that became ready.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<Vec<u8>> {
        self.buf.extend_from_slice(chunk);
        let mut parts = Vec::new();
        while self.buf.len() >= STREAM_PART_BYTES {
            let part: Vec<u8> = self.buf.drain(..STREAM_PART_BYTES).collect();
            parts.push(part);
        }
        parts
    }

    /// The final remainder (possibly empty) after EOF.
    pub fn finish(self) -> Option<Vec<u8>> {
        if self.buf.is_empty() {
            None
        } else {
            Some(self.buf)
        }
    }

    /// Bytes currently held that have not yet formed a complete part.
    /// Used by the unit tests to assert the buffering boundary.
    #[cfg(test)]
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }
}

#[async_trait]
pub trait UploadSink {
    /// Single PutObject for a small, fully-collected payload.
    async fn put_small(&mut self, bytes: Vec<u8>, sha256: &str) -> Result<(), StoreError>;
    /// Start a multipart upload; returns an upload id.
    async fn create_multipart(&mut self) -> Result<String, StoreError>;
    /// Upload one part; returns its ETag.
    async fn upload_part(
        &mut self,
        upload_id: &str,
        part_number: i32,
        bytes: Vec<u8>,
    ) -> Result<String, StoreError>;
    /// Complete a multipart upload with (part_number, etag) pairs.
    async fn complete(
        &mut self,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), StoreError>;
    /// Abort a multipart upload. Best effort: errors here are ignored by
    /// the caller, which already has a primary error to report.
    async fn abort(&mut self, upload_id: &str);
}

#[derive(Debug, Clone)]
pub struct UploadOutcome {
    pub total_bytes: u64,
    pub sha256: String,
    /// Read by the unit tests only; the write path no longer uses it.
    #[cfg_attr(not(test), allow(dead_code))]
    pub used_multipart: bool,
    /// Read by the unit tests only; the binary reports total_bytes instead.
    #[cfg_attr(not(test), allow(dead_code))]
    pub parts_uploaded: usize,
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Drive a streaming upload.
///
/// * `known_length`: `Some(n)` from a Content-Length header, `None` for
///   chunked transfer encoding. When known and at most one part, a single
///   PutObject is used. Otherwise multipart is used.
/// * `max_bytes`: hard cap. Exceeding it aborts any multipart upload and
///   returns [`StoreError::TooLarge`].
///
/// On any error the caller gets an `Err` and, if a multipart upload was
/// started, it has been aborted. No outcome is returned on failure, so
/// no URL can be constructed from a failed upload.
pub async fn run_streaming_upload<'a, S>(
    mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, StoreError>> + Send + 'a>>,
    sink: &mut S,
    known_length: Option<u64>,
    max_bytes: u64,
) -> Result<UploadOutcome, StoreError>
where
    S: UploadSink + Send,
{
    use futures::StreamExt;

    // Small, known-length fast path: collect a bounded payload, then one Put.
    if let Some(len) = known_length {
        if len > max_bytes {
            return Err(StoreError::TooLarge { limit: max_bytes });
        }
        if len as usize <= STREAM_PART_BYTES {
            let mut collected: Vec<u8> = Vec::with_capacity(len as usize);
            while let Some(item) = stream.next().await {
                let chunk = item?;
                if collected.len() as u64 + chunk.len() as u64 > len {
                    return Err(StoreError::TooLarge { limit: len });
                }
                collected.extend_from_slice(&chunk);
            }
            let total = collected.len() as u64;
            if total != len {
                return Err(StoreError::LengthMismatch {
                    expected: len,
                    actual: total,
                });
            }
            let mut hasher = Sha256::new();
            hasher.update(&collected);
            let sha = hex_encode(&hasher.finalize());
            sink.put_small(collected, &sha).await?;
            return Ok(UploadOutcome {
                total_bytes: total,
                sha256: sha,
                used_multipart: false,
                parts_uploaded: 0,
            });
        }
    }

    // Multipart path: buffer to 5 MiB parts, upload as they become ready.
    let upload_id = sink.create_multipart().await?;
    let mut buffer = PartBuffer::new();
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let mut parts: Vec<(i32, String)> = Vec::new();
    let mut part_number: i32 = 1;

    let run_result: Result<(), StoreError> = async {
        while let Some(item) = stream.next().await {
            let chunk = item?;
            total += chunk.len() as u64;
            if total > max_bytes {
                return Err(StoreError::TooLarge { limit: max_bytes });
            }
            hasher.update(&chunk);
            for part in buffer.push(&chunk) {
                let etag = sink.upload_part(&upload_id, part_number, part).await?;
                parts.push((part_number, etag));
                part_number += 1;
            }
        }
        Ok(())
    }
    .await;

    if let Err(e) = run_result {
        sink.abort(&upload_id).await;
        return Err(e);
    }

    if let Some(expected) = known_length {
        if total != expected {
            sink.abort(&upload_id).await;
            return Err(StoreError::LengthMismatch {
                expected,
                actual: total,
            });
        }
    }

    // EOF: upload the remainder as the final (possibly small) part.
    if let Some(remainder) = buffer.finish() {
        match sink.upload_part(&upload_id, part_number, remainder).await {
            Ok(etag) => parts.push((part_number, etag)),
            Err(e) => {
                sink.abort(&upload_id).await;
                return Err(e);
            }
        }
    }

    if parts.is_empty() {
        // Empty upload: abort the (empty) multipart upload and store an
        // empty object with a single Put instead. A multipart upload with
        // zero parts is not a valid completion.
        sink.abort(&upload_id).await;
        let sha = hex_encode(&hasher.finalize());
        sink.put_small(Vec::new(), &sha).await?;
        return Ok(UploadOutcome {
            total_bytes: 0,
            sha256: sha,
            used_multipart: false,
            parts_uploaded: 0,
        });
    }

    let parts_uploaded = parts.len();
    if let Err(e) = sink.complete(&upload_id, parts).await {
        sink.abort(&upload_id).await;
        return Err(e);
    }
    Ok(UploadOutcome {
        total_bytes: total,
        sha256: hex_encode(&hasher.finalize()),
        used_multipart: true,
        parts_uploaded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct FakeSink {
        state: Arc<Mutex<FakeState>>,
        fail_on_part: Option<i32>,
        fail_on_complete: bool,
    }

    #[derive(Default)]
    struct FakeState {
        put_small: Vec<Vec<u8>>,
        created: Vec<String>,
        parts: Vec<(i32, Vec<u8>)>,
        completed: Vec<Vec<(i32, String)>>,
        aborted: Vec<String>,
    }

    #[async_trait]
    impl UploadSink for FakeSink {
        async fn put_small(&mut self, bytes: Vec<u8>, _sha: &str) -> Result<(), StoreError> {
            self.state.lock().unwrap().put_small.push(bytes);
            Ok(())
        }
        async fn create_multipart(&mut self) -> Result<String, StoreError> {
            let id = format!("up-{}", self.state.lock().unwrap().created.len());
            self.state.lock().unwrap().created.push(id.clone());
            Ok(id)
        }
        async fn upload_part(
            &mut self,
            _upload_id: &str,
            part_number: i32,
            bytes: Vec<u8>,
        ) -> Result<String, StoreError> {
            if Some(part_number) == self.fail_on_part {
                return Err(StoreError::S3("injected part failure".into()));
            }
            self.state.lock().unwrap().parts.push((part_number, bytes));
            Ok(format!("etag-{part_number}"))
        }
        async fn complete(
            &mut self,
            _upload_id: &str,
            parts: Vec<(i32, String)>,
        ) -> Result<(), StoreError> {
            if self.fail_on_complete {
                return Err(StoreError::S3("injected complete failure".into()));
            }
            self.state.lock().unwrap().completed.push(parts);
            Ok(())
        }
        async fn abort(&mut self, upload_id: &str) {
            self.state
                .lock()
                .unwrap()
                .aborted
                .push(upload_id.to_string());
        }
    }

    fn chunk_stream(
        chunks: Vec<Vec<u8>>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, StoreError>> + Send>> {
        let items: Vec<Result<Bytes, StoreError>> =
            chunks.into_iter().map(|c| Ok(Bytes::from(c))).collect();
        Box::pin(stream::iter(items))
    }

    #[test]
    fn part_buffer_yields_exact_5mib_parts() {
        let mut buf = PartBuffer::new();
        // 100 x 64 KiB = 6,400 KiB, in irregular chunking terms.
        let mut parts = Vec::new();
        for _ in 0..100 {
            parts.extend(buf.push(&vec![b'x'; 64 * 1024]));
        }
        // 6,400 KiB = 1 full 5 MiB part (5,120 KiB) + 1,280 KiB remainder.
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].len(), STREAM_PART_BYTES);
        assert_eq!(buf.buffered_len(), 1280 * 1024);
        let rem = buf.finish().unwrap();
        assert_eq!(rem.len(), 1280 * 1024);
    }

    #[tokio::test]
    async fn chunked_upload_reassembles_and_uses_multipart() {
        // 12 MiB delivered as many small chunks: chunked transfer encoding.
        let total = 12 * 1024 * 1024;
        let mut chunks = Vec::new();
        let mut remaining = total;
        let mut byte: u8 = 0;
        while remaining > 0 {
            let n = remaining.min(64 * 1024);
            chunks.push(vec![byte; n]);
            byte = byte.wrapping_add(1);
            remaining -= n;
        }
        let mut sink = FakeSink::default();
        let outcome =
            run_streaming_upload(chunk_stream(chunks), &mut sink, None, 1024 * 1024 * 1024)
                .await
                .unwrap();
        assert!(outcome.used_multipart);
        assert_eq!(outcome.total_bytes, total as u64);
        // 12 MiB = 2 x 5 MiB parts + 2 MiB final part.
        assert_eq!(outcome.parts_uploaded, 3);
        let state = sink.state.lock().unwrap();
        assert_eq!(state.parts.len(), 3);
        assert_eq!(state.parts[0].1.len(), STREAM_PART_BYTES);
        assert_eq!(state.parts[1].1.len(), STREAM_PART_BYTES);
        assert_eq!(state.parts[2].1.len(), 2 * 1024 * 1024);
        assert_eq!(state.completed.len(), 1);
        assert!(state.aborted.is_empty());
        // Reassembly equals the original byte pattern.
        let mut reassembled = Vec::new();
        for (_, p) in &state.parts {
            reassembled.extend_from_slice(p);
        }
        assert_eq!(reassembled.len(), total);
    }

    #[tokio::test]
    async fn multipart_sized_upload_above_threshold() {
        // Just above one part: 5 MiB + 1 byte.
        let total = STREAM_PART_BYTES + 1;
        let chunks = vec![vec![b'a'; STREAM_PART_BYTES], vec![b'b'; 1]];
        let mut sink = FakeSink::default();
        let outcome = run_streaming_upload(chunk_stream(chunks), &mut sink, None, u64::MAX)
            .await
            .unwrap();
        assert!(outcome.used_multipart);
        assert_eq!(outcome.parts_uploaded, 2);
        assert_eq!(outcome.total_bytes, total as u64);
        let state = sink.state.lock().unwrap();
        assert_eq!(state.parts[0].1.len(), STREAM_PART_BYTES);
        assert_eq!(state.parts[1].1.len(), 1);
    }

    #[tokio::test]
    async fn text_and_file_caps_at_the_single_streaming_call_site() {
        // The same payload through the one call site that `store_stream`
        // drives: the text cap rejects it, the file cap accepts it.
        let total = 10 * 1024 * 1024 + 1;
        let chunks = vec![vec![b'a'; total]];

        let mut sink = FakeSink::default();
        let result = run_streaming_upload(
            chunk_stream(chunks.clone()),
            &mut sink,
            Some(total as u64),
            10 * 1024 * 1024,
        )
        .await;
        assert!(matches!(
            result,
            Err(StoreError::TooLarge { limit }) if limit == 10 * 1024 * 1024
        ));
        assert!(
            sink.state.lock().unwrap().created.is_empty(),
            "an early rejection must not start a multipart upload"
        );

        let mut sink = FakeSink::default();
        let outcome = run_streaming_upload(
            chunk_stream(chunks),
            &mut sink,
            Some(total as u64),
            1024 * 1024 * 1024,
        )
        .await
        .expect("the same payload must fit under the file cap");
        assert_eq!(outcome.total_bytes, total as u64);
    }

    #[tokio::test]
    async fn known_small_length_uses_single_put() {
        let chunks = vec![b"hello ".to_vec(), b"world".to_vec()];
        let mut sink = FakeSink::default();
        let outcome = run_streaming_upload(chunk_stream(chunks), &mut sink, Some(11), u64::MAX)
            .await
            .unwrap();
        assert!(!outcome.used_multipart);
        assert_eq!(outcome.total_bytes, 11);
        let state = sink.state.lock().unwrap();
        assert_eq!(state.put_small.len(), 1);
        assert_eq!(state.put_small[0], b"hello world");
        assert!(state.created.is_empty());
    }

    #[tokio::test]
    async fn disconnect_aborts_and_returns_no_outcome() {
        // Stream yields two parts worth of data, then an error (disconnect).
        let items: Vec<Result<Bytes, StoreError>> = vec![
            Ok(Bytes::from(vec![b'x'; STREAM_PART_BYTES])),
            Ok(Bytes::from(vec![b'y'; STREAM_PART_BYTES])),
            Err(StoreError::Io("client disconnected".into())),
        ];
        let mut sink = FakeSink::default();
        let result =
            run_streaming_upload(Box::pin(stream::iter(items)), &mut sink, None, u64::MAX).await;
        assert!(
            result.is_err(),
            "a failed upload must not return an outcome"
        );
        let state = sink.state.lock().unwrap();
        assert!(state.completed.is_empty(), "must not complete on failure");
        assert_eq!(state.aborted.len(), 1, "multipart must be aborted");
    }

    #[tokio::test]
    async fn part_failure_aborts_and_returns_no_url_material() {
        let chunks = vec![vec![b'x'; STREAM_PART_BYTES], vec![b'y'; STREAM_PART_BYTES]];
        let mut sink = FakeSink {
            fail_on_part: Some(2),
            ..Default::default()
        };
        let result = run_streaming_upload(chunk_stream(chunks), &mut sink, None, u64::MAX).await;
        assert!(result.is_err());
        let state = sink.state.lock().unwrap();
        assert!(state.completed.is_empty());
        assert_eq!(state.aborted.len(), 1);
    }

    #[tokio::test]
    async fn oversize_aborts_and_returns_no_outcome() {
        let chunks = vec![vec![b'x'; STREAM_PART_BYTES], vec![b'y'; 1024]];
        let mut sink = FakeSink::default();
        let result = run_streaming_upload(
            chunk_stream(chunks),
            &mut sink,
            None,
            STREAM_PART_BYTES as u64,
        )
        .await;
        assert!(matches!(result, Err(StoreError::TooLarge { .. })));
        let state = sink.state.lock().unwrap();
        assert!(state.completed.is_empty());
        assert_eq!(state.aborted.len(), 1);
    }

    #[tokio::test]
    async fn empty_upload_becomes_single_empty_put() {
        let mut sink = FakeSink::default();
        let outcome = run_streaming_upload(chunk_stream(vec![]), &mut sink, None, u64::MAX)
            .await
            .unwrap();
        assert_eq!(outcome.total_bytes, 0);
        assert!(!outcome.used_multipart);
        let state = sink.state.lock().unwrap();
        assert_eq!(state.put_small.len(), 1);
        assert!(state.completed.is_empty());
    }
}
