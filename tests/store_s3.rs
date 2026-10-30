//! Store and S3 path integration tests against the in-process fake S3.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{Duration, Utc};
use common::{FailOp, FakeS3};
use dayfly::store::{
    parse_segment_key, StoreError, Ttl, MULTIPART_CHUNK_BYTES, SEGMENT_SUFFIX_LEN,
};
use futures::stream;
use sha2::{Digest, Sha256};

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let mut out = String::new();
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn expired_metadata(size: usize) -> Vec<(String, String)> {
    common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        size,
        &sha256_hex(b"abcde"),
    )
}

fn future_metadata(size: usize) -> Vec<(String, String)> {
    common::stored_metadata(
        &(Utc::now() + Duration::days(1)).to_rfc3339(),
        "text/plain",
        size,
        &sha256_hex(b"abcde"),
    )
}

#[tokio::test]
async fn store_text_round_trip_through_the_s3_sdk() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;

    let stored = store
        .store_text(
            b"hello, integration",
            Some("note.txt"),
            None,
            Some(Ttl::days(1)),
        )
        .await
        .expect("store_text must succeed against the fake");

    assert!(stored.id.contains('/'), "id must include the date prefix");
    assert_eq!(
        stored.key,
        format!("pastes/{}", stored.id),
        "key layout must be pastes/<date>/<random>"
    );
    assert_eq!(stored.size, 18);
    assert_eq!(stored.sha256, sha256_hex(b"hello, integration"));
    assert_eq!(stored.content_type, "text/plain");
    assert_eq!(stored.detection_source.as_deref(), Some("extension"));

    let object = fake
        .object(&stored.key)
        .expect("the fake must hold the stored object");
    assert_eq!(object.body, b"hello, integration");
    assert_eq!(object.content_type, "text/plain");
    assert_eq!(
        object.metadata.get("sha256").map(String::as_str),
        Some(stored.sha256.as_str())
    );
    assert_eq!(
        object.metadata.get("filename").map(String::as_str),
        Some("note%2Etxt")
    );
    assert!(object.metadata.contains_key("expires-at"));

    let meta = store
        .head_metadata(&stored.id)
        .await
        .expect("head_metadata must find the object");
    assert_eq!(meta.id, stored.id);
    assert_eq!(meta.key, stored.key);
    assert_eq!(meta.size, 18);
    assert_eq!(meta.filename.as_deref(), Some("note.txt"));
    assert_eq!(meta.content_type, "text/plain");
    assert_eq!(meta.detection_source.as_deref(), Some("extension"));
    assert!(meta.sha256.is_some());
    assert!(!meta.expired);
}

#[tokio::test]
async fn store_text_records_ttl_and_detection() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;

    let stored = store
        .store_text(
            b"\x89PNG\r\n\x1a\nfake-body",
            None,
            Some("text/plain"),
            Some(Ttl::days(3)),
        )
        .await
        .expect("store_text must store a sniffed image");
    assert_eq!(stored.content_type, "image/png");
    assert_eq!(stored.detection_source.as_deref(), Some("sniffed"));
    assert_eq!(
        stored.detection_conflict.as_deref(),
        Some("text/plain -> image/png")
    );

    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(
        object.metadata.get("ttl-days").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        object
            .metadata
            .get("detection-conflict")
            .map(String::as_str),
        Some("text/plain -> image/png")
    );
    let expires = object
        .metadata
        .get("expires-at")
        .expect("expiry metadata must be present");
    assert_eq!(expires, &stored.expires_at.to_rfc3339());
}

#[tokio::test]
async fn text_cap_is_enforced_before_any_request() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(5, 1024).await;

    let error = store
        .store_text(b"123456", None, None, None)
        .await
        .expect_err("six bytes must exceed a five-byte text cap");
    assert!(matches!(error, StoreError::TooLarge { limit: 5 }));
    assert!(
        fake.requests().is_empty(),
        "a locally rejected upload must not reach S3"
    );
}

#[tokio::test]
async fn store_file_path_small_file_uses_one_put() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = b"small file body\n";
    let path = common::write_temp_file("small.txt", body);

    let stored = store
        .store_file_path(&path, None, Some("text/csv"), None)
        .await
        .expect("small file upload must succeed");
    assert_eq!(stored.size, body.len() as u64);
    assert_eq!(stored.sha256, sha256_hex(body));
    assert_eq!(stored.content_type, "text/csv");
    assert_eq!(
        stored.filename.as_deref(),
        path.file_name().and_then(|n| n.to_str())
    );
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, body);
    let body_size = body.len().to_string();
    assert_eq!(
        object.metadata.get("size").map(String::as_str),
        Some(body_size.as_str())
    );
    assert!(
        !fake
            .requests()
            .iter()
            .any(|request| request.query.contains("uploads")),
        "a file at or below one chunk must not use multipart"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn store_file_path_large_file_uses_multipart_and_records_metadata() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1234);
    let path = common::write_temp_file("large.bin", &body);

    let stored = store
        .store_file_path(&path, Some("large data.bin"), None, Some(Ttl::days(1)))
        .await
        .expect("large file upload must succeed");
    assert_eq!(stored.size, body.len() as u64);
    assert_eq!(stored.sha256, sha256_hex(&body));
    assert_eq!(stored.content_type, "application/octet-stream");

    let requests = fake.requests();
    assert!(
        requests.iter().any(|request| request.query == "uploads"),
        "multipart must be created: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.query.contains("partNumber=1")),
        "first part must be uploaded: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.query.contains("partNumber=2")),
        "second part must be uploaded: {requests:?}"
    );

    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(
        object.body, body,
        "multipart parts must reassemble in order"
    );
    assert_eq!(
        object.metadata.get("sha256").map(String::as_str),
        Some(stored.sha256.as_str()),
        "copy-in-place must attach the real sha256"
    );
    let body_size = body.len().to_string();
    assert_eq!(
        object.metadata.get("size").map(String::as_str),
        Some(body_size.as_str())
    );
    assert!(object.metadata.contains_key("expires-at"));
    assert_eq!(
        object.metadata.get("filename").map(String::as_str),
        Some("large%20data%2Ebin")
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn store_file_path_enforces_caps_and_rejects_directories() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 5).await;
    let path = common::write_temp_file("over-cap.bin", b"123456");

    let error = store
        .store_file_path(&path, None, None, None)
        .await
        .expect_err("six bytes must exceed a five-byte file cap");
    assert!(matches!(error, StoreError::TooLarge { limit: 5 }));
    assert!(fake.requests().is_empty());

    let dir = common::temp_path("dir");
    std::fs::create_dir_all(&dir).expect("test directory must be created");
    let error = store
        .store_file_path(&dir, None, None, None)
        .await
        .expect_err("a directory is not a regular file");
    assert!(matches!(error, StoreError::Io(_)));
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn store_stream_no_filename_long_text_is_stored_and_fetched_as_text() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = vec![b'a'; 20 * 1024];
    let chunks: Vec<Result<Bytes, StoreError>> = body
        .chunks(4096)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();

    let stored = store
        .store_stream(
            stream::iter(chunks),
            None,
            None,
            None,
            None,
            10 * 1024 * 1024,
        )
        .await
        .expect("a long text paste must store");

    assert_eq!(stored.content_type, "text/plain; charset=utf-8");
    assert_eq!(stored.detection_source.as_deref(), Some("text"));
    assert_eq!(stored.size, body.len() as u64);
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, body);

    let fetched = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("the stored text must be fetchable");
    assert_eq!(
        fetched.text,
        String::from_utf8(body).expect("ASCII is UTF-8")
    );
}

#[tokio::test]
async fn store_stream_no_filename_late_nul_is_not_text() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let mut body = vec![b'a'; 8 * 1024];
    body.push(0);
    let chunks: Vec<Result<Bytes, StoreError>> = body
        .chunks(4096)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();

    let stored = store
        .store_stream(
            stream::iter(chunks),
            None,
            None,
            None,
            None,
            10 * 1024 * 1024,
        )
        .await
        .expect("a binary paste must store");

    assert_eq!(stored.content_type, "application/octet-stream");
    assert_eq!(stored.detection_source.as_deref(), Some("fallback"));
    assert_eq!(stored.size, body.len() as u64);
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, body);
}

#[tokio::test]
async fn store_stream_small_known_length_uses_one_put() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let chunks: Vec<Result<Bytes, StoreError>> = vec![
        Ok(Bytes::from_static(b"hello ")),
        Ok(Bytes::from_static(b"stream")),
    ];
    let stored = store
        .store_stream(
            stream::iter(chunks),
            Some("hello.txt"),
            None,
            None,
            Some(12),
            1024,
        )
        .await
        .expect("small stream must store");
    assert_eq!(stored.size, 12);
    assert_eq!(stored.sha256, sha256_hex(b"hello stream"));
    assert_eq!(stored.filename.as_deref(), Some("hello.txt"));
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, b"hello stream");
    assert_eq!(
        object.metadata.get("sha256").map(String::as_str),
        Some(stored.sha256.as_str())
    );
    assert!(
        !fake
            .requests()
            .iter()
            .any(|request| request.query.contains("uploads")),
        "a known small length must not use multipart"
    );
}

#[tokio::test]
async fn store_stream_chunked_upload_uses_multipart_without_a_copy() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(6 * 1024 * 1024);
    let chunks: Vec<Result<Bytes, StoreError>> = body
        .chunks(64 * 1024)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();

    let stored = store
        .store_stream(
            stream::iter(chunks),
            Some("chunked.bin"),
            None,
            Some(Ttl::days(1)),
            None,
            u64::MAX,
        )
        .await
        .expect("chunked stream must store");
    assert_eq!(stored.size, body.len() as u64);
    assert_eq!(stored.sha256, sha256_hex(&body));
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, body);
    // S3 takes metadata only at CreateMultipartUpload. The hash and size
    // are not known then, so the object omits both. The stored record
    // still returns them.
    assert!(!object.metadata.contains_key("sha256"));
    assert!(!object.metadata.contains_key("size"));
    assert!(object.metadata.contains_key("expires-at"));
    assert!(
        !fake.requests().iter().any(|request| {
            request
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("x-amz-copy-source"))
        }),
        "multipart must set metadata at create time, not with a copy"
    );
}

#[tokio::test]
async fn store_stream_rejects_known_length_over_cap_before_read() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let polled = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&polled);
    let stream = stream::poll_fn(move |_| {
        flag.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(None)
    });

    let error = store
        .store_stream(stream, None, None, None, Some(11), 10)
        .await
        .expect_err("a known length over the cap must be rejected");
    assert!(matches!(error, StoreError::TooLarge { limit: 10 }));
    assert!(
        !polled.load(Ordering::SeqCst),
        "an over-cap known length must be rejected before the body is read"
    );
    assert!(fake.requests().is_empty());
}

#[tokio::test]
async fn store_stream_disconnect_aborts_and_returns_no_url_material() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let items: Vec<Result<Bytes, StoreError>> = vec![
        Ok(Bytes::from(common::pattern_bytes(6 * 1024 * 1024))),
        Err(StoreError::Io("client disconnected".into())),
    ];
    let error = store
        .store_stream(
            stream::iter(items),
            Some("large.bin"),
            None,
            None,
            None,
            u64::MAX,
        )
        .await
        .expect_err("a disconnected stream must fail");
    assert!(matches!(error, StoreError::Io(_)));
    assert_eq!(
        fake.aborted_uploads().len(),
        1,
        "multipart must be aborted on disconnect"
    );
    assert!(
        fake.keys().is_empty(),
        "a failed upload must not create an object"
    );
    assert!(!error.to_string().contains("http"));
    assert!(!error.to_string().contains("/v1/pastes"));
}

#[tokio::test]
async fn store_stream_part_failure_aborts() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_part(1);
    let items: Vec<Result<Bytes, StoreError>> =
        vec![Ok(Bytes::from(common::pattern_bytes(6 * 1024 * 1024)))];

    let error = store
        .store_stream(
            stream::iter(items),
            Some("large.bin"),
            None,
            None,
            None,
            u64::MAX,
        )
        .await
        .expect_err("an injected part failure must fail the upload");
    assert!(matches!(error, StoreError::S3(_)));
    assert_eq!(fake.aborted_uploads().len(), 1);
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn store_stream_completion_failure_aborts() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail(FailOp::CompleteMultipartUpload);
    let items: Vec<Result<Bytes, StoreError>> =
        vec![Ok(Bytes::from(common::pattern_bytes(6 * 1024 * 1024)))];

    let error = store
        .store_stream(
            stream::iter(items),
            Some("large.bin"),
            None,
            None,
            None,
            u64::MAX,
        )
        .await
        .expect_err("an injected completion failure must fail the upload");
    assert!(matches!(error, StoreError::S3(_)));
    assert_eq!(fake.aborted_uploads().len(), 1);
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn store_stream_oversize_aborts() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let items: Vec<Result<Bytes, StoreError>> =
        vec![Ok(Bytes::from(common::pattern_bytes(6 * 1024 * 1024)))];

    let error = store
        .store_stream(
            stream::iter(items),
            Some("large.bin"),
            None,
            None,
            None,
            5 * 1024 * 1024,
        )
        .await
        .expect_err("an oversize chunked file must fail");
    assert!(matches!(error, StoreError::TooLarge { .. }));
    assert_eq!(fake.aborted_uploads().len(), 1);
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn store_stream_no_filename_oversize_body_is_rejected_before_s3() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(10, 1024).await;
    let items: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"12345678901"))];

    let error = store
        .store_stream(stream::iter(items), None, None, None, None, 10)
        .await
        .expect_err("a chunked text body over the cap must fail");
    assert!(matches!(error, StoreError::TooLarge { limit: 10 }));
    assert!(
        fake.requests().is_empty(),
        "an oversize text paste must be rejected before any S3 write"
    );
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn store_stream_put_failure_returns_no_url_material() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail(FailOp::PutObject);
    let items: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"hello"))];

    let error = store
        .store_stream(stream::iter(items), None, None, None, Some(5), 1024)
        .await
        .expect_err("an injected PutObject failure must fail the upload");
    assert!(
        !error.to_string().contains("/v1/pastes"),
        "a failed store call must not return paste URL material: {error}"
    );
    assert!(fake.keys().is_empty());
}

#[tokio::test]
async fn head_metadata_reports_missing_and_expired_objects() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;

    let error = store
        .head_metadata("2026-10-28/missing")
        .await
        .expect_err("a missing object must be NotFound");
    assert!(matches!(error, StoreError::NotFound(_)));

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let key = format!("pastes/{today}/expired1");
    let metadata = expired_metadata(5);
    fake.seed(
        &key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    let meta = store
        .head_metadata(&format!("{today}/expired1"))
        .await
        .expect("head_metadata must find the seeded object");
    assert!(meta.expired, "past expires-at must mark the object expired");
    assert_eq!(meta.size, 5);
}

#[tokio::test]
async fn head_metadata_falls_back_to_metadata_content_type() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let key = "pastes/2026-10-28/fallback";
    let mut metadata = future_metadata(3);
    metadata.push(("content-type".to_string(), "text/csv".to_string()));
    fake.seed(key, b"a,b", "", &common::metadata_refs(&metadata));

    let meta = store
        .head_metadata("2026-10-28/fallback")
        .await
        .expect("head_metadata must use stored metadata");
    assert_eq!(meta.content_type, "text/csv");
}

#[tokio::test]
async fn fetch_text_slices_offsets_limits_and_utf8_boundaries() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text("aé".as_bytes(), None, None, None)
        .await
        .expect("text must store");

    let full = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("full fetch must succeed");
    assert_eq!(full.text, "aé");
    assert_eq!(full.total_bytes, 3);
    assert_eq!(full.returned_bytes, 3);
    assert!(!full.truncated);

    let split = store
        .fetch_text(&stored.id, 0, Some(2))
        .await
        .expect("bounded fetch must succeed");
    assert_eq!(split.text, "a", "a split UTF-8 character is dropped");
    assert_eq!(split.returned_bytes, 1);
    assert!(split.truncated);

    let past_end = store
        .fetch_text(&stored.id, 3, Some(10))
        .await
        .expect("offset at the end must return an empty slice");
    assert!(past_end.text.is_empty());
    assert!(!past_end.truncated);
}

#[tokio::test]
async fn fetch_text_rejects_expired_objects() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let key = "pastes/2026-10-28/oldtext";
    let metadata = expired_metadata(5);
    fake.seed(
        key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );

    let error = store
        .fetch_text("2026-10-28/oldtext", 0, None)
        .await
        .expect_err("an expired object must not be fetched");
    assert!(matches!(error, StoreError::Expired(_)));
}

#[tokio::test]
async fn fetch_range_by_key_reads_an_exact_window() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"abcdefghij", None, None, None)
        .await
        .expect("text must store");

    let whole = store
        .fetch_range_by_key(&stored.key, None)
        .await
        .expect("whole object fetch must succeed");
    assert_eq!(whole, b"abcdefghij");

    let window = store
        .fetch_range_by_key(&stored.key, Some((2, 4)))
        .await
        .expect("range fetch must succeed");
    assert_eq!(window, b"cde");

    let error = store
        .fetch_range_by_key("pastes/2026-10-28/missing", None)
        .await
        .expect_err("a missing key must be NotFound");
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[tokio::test]
async fn download_file_writes_whole_and_ranged_content() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"abcdefghij", None, None, None)
        .await
        .expect("text must store");

    let whole_path = common::temp_path("whole.txt");
    let meta = store
        .download_file(&stored.id, &whole_path, None)
        .await
        .expect("whole download must succeed");
    assert_eq!(meta.size, 10);
    assert_eq!(
        std::fs::read(&whole_path).expect("downloaded file must be readable"),
        b"abcdefghij"
    );

    let range_path = common::temp_path("range.txt");
    store
        .download_file(&stored.id, &range_path, Some((2, 4)))
        .await
        .expect("range download must succeed");
    assert_eq!(
        std::fs::read(&range_path).expect("ranged file must be readable"),
        b"cde"
    );

    let _ = std::fs::remove_file(whole_path);
    let _ = std::fs::remove_file(range_path);
}

#[tokio::test]
async fn download_file_streams_multiple_s3_chunks() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1024);
    let source = common::write_temp_file("multi-chunk-source.bin", &body);
    let stored = store
        .store_file_path(&source, Some("multi-chunk.bin"), None, None)
        .await
        .expect("large file must store");

    let whole_path = common::temp_path("multi-chunk-whole.bin");
    let meta = store
        .download_file(&stored.id, &whole_path, None)
        .await
        .expect("multi-chunk download must succeed");
    assert_eq!(meta.size, body.len() as u64);
    assert_eq!(
        std::fs::read(&whole_path).expect("downloaded file must be readable"),
        body
    );

    let start = MULTIPART_CHUNK_BYTES - 10;
    let end = MULTIPART_CHUNK_BYTES + 10;
    let range_path = common::temp_path("multi-chunk-range.bin");
    store
        .download_file(&stored.id, &range_path, Some((start as u64, end as u64)))
        .await
        .expect("range across a chunk boundary must succeed");
    assert_eq!(
        std::fs::read(&range_path).expect("ranged file must be readable"),
        body[start..=end].to_vec()
    );

    let _ = std::fs::remove_file(source);
    let _ = std::fs::remove_file(whole_path);
    let _ = std::fs::remove_file(range_path);
}

#[tokio::test]
async fn download_file_supports_empty_objects() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let empty = stream::iter(Vec::<Result<Bytes, StoreError>>::new());
    let stored = store
        .store_stream(empty, None, None, None, Some(0), 1024)
        .await
        .expect("an empty upload must store");
    assert_eq!(stored.size, 0);

    let path = common::temp_path("empty.bin");
    let meta = store
        .download_file(&stored.id, &path, None)
        .await
        .expect("empty download must succeed");
    assert_eq!(meta.size, 0);
    assert_eq!(std::fs::metadata(&path).expect("file must exist").len(), 0);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn delete_removes_objects_and_missing_keys_are_idempotent() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"delete me", None, None, None)
        .await
        .expect("text must store");

    store.delete(&stored.id).await.expect("delete must succeed");
    assert!(
        fake.object(&stored.key).is_none(),
        "delete must remove the object"
    );
    store
        .delete(&stored.id)
        .await
        .expect("S3 delete of a missing key is successful");
}

#[tokio::test]
async fn delete_reports_a_missing_bucket_as_unavailable() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.set_missing_bucket(true);

    let error = store
        .delete("2026-10-28/anything")
        .await
        .expect_err("a missing bucket must be an error");
    assert!(
        matches!(error, StoreError::BucketUnavailable(_)),
        "NoSuchBucket must stay distinct from NoSuchKey: {error}"
    );
}

#[tokio::test]
async fn list_recent_filters_expired_objects_and_applies_the_limit() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    let expired = expired_metadata(5);
    fake.seed(
        &format!("pastes/{today}/aaa"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&valid),
    );
    fake.seed(
        &format!("pastes/{today}/bbb"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&expired),
    );
    fake.seed(
        &format!("pastes/{today}/ccc"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&valid),
    );

    let items = store
        .list_recent(None, 100)
        .await
        .expect("listing must succeed");
    assert_eq!(items.len(), 2, "expired objects must be filtered out");
    assert_eq!(items[0].id, format!("{today}/aaa"));
    assert_eq!(items[0].size, 5);
    assert_eq!(items[0].content_type.as_deref(), Some("text/plain"));
    assert!(items[0].expires_at.is_some());

    let limited = store
        .list_recent(Some(&today), 1)
        .await
        .expect("limited listing must succeed");
    assert_eq!(limited.len(), 1);
}

#[tokio::test]
async fn list_window_default_limit_keeps_the_newest_day() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let now = Utc::now();
    let today = now.format("%Y-%m-%d").to_string();
    let yesterday = (now - Duration::days(1)).format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    for key in ["aaa", "bbb"] {
        fake.seed(
            &format!("pastes/{yesterday}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
        fake.seed(
            &format!("pastes/{today}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }

    let items = store
        .list_recent(None, 2)
        .await
        .expect("listing must succeed");
    let ids: Vec<&str> = items.iter().map(|item| item.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![format!("{today}/aaa"), format!("{today}/bbb"),],
        "the limit must keep the newest day, not the oldest"
    );
}

#[tokio::test]
async fn list_window_range_returns_both_days_in_ascending_order() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let now = Utc::now();
    let today = now.format("%Y-%m-%d").to_string();
    let yesterday = (now - Duration::days(1)).format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    for key in ["aaa", "bbb"] {
        fake.seed(
            &format!("pastes/{yesterday}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
        fake.seed(
            &format!("pastes/{today}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }

    let items = store
        .list_window(Some(&yesterday), Some(&today), 10)
        .await
        .expect("listing must succeed");
    let ids: Vec<String> = items.iter().map(|item| item.id.clone()).collect();
    assert_eq!(
        ids,
        vec![
            format!("{yesterday}/aaa"),
            format!("{yesterday}/bbb"),
            format!("{today}/aaa"),
            format!("{today}/bbb"),
        ],
        "a full range must return both days in ascending date order"
    );
}

#[tokio::test]
async fn list_window_range_limit_omits_older_days() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let now = Utc::now();
    let today = now.format("%Y-%m-%d").to_string();
    let yesterday = (now - Duration::days(1)).format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    for key in ["aaa", "bbb"] {
        fake.seed(
            &format!("pastes/{yesterday}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
        fake.seed(
            &format!("pastes/{today}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }

    let items = store
        .list_window(Some(&yesterday), Some(&today), 2)
        .await
        .expect("listing must succeed");
    let ids: Vec<&str> = items.iter().map(|item| item.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![format!("{today}/aaa"), format!("{today}/bbb")],
        "the limit must omit older dates before newer objects"
    );
}

#[tokio::test]
async fn list_window_follows_continuation_tokens() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let expired = expired_metadata(5);
    let valid = future_metadata(5);
    for n in 0..1000 {
        fake.seed(
            &format!("pastes/{today}/expired-{n:04}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&expired),
        );
    }
    fake.seed(
        &format!("pastes/{today}/zzz-live"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&valid),
    );

    let items = store
        .list_recent(Some(&today), 1000)
        .await
        .expect("listing must succeed");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, format!("{today}/zzz-live"));
    let list_calls = fake
        .requests()
        .into_iter()
        .filter(|request| request.query.contains("list-type=2"))
        .count();
    assert!(
        list_calls >= 2,
        "the continuation token must fetch a second page, saw {list_calls} list calls"
    );
}

#[tokio::test]
async fn list_recent_rejects_an_invalid_date() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let error = store
        .list_recent(Some("28-10-2026"), 10)
        .await
        .expect_err("a non-ISO date must be rejected");
    assert!(matches!(error, StoreError::InvalidDate(_)));
    assert!(fake.requests().is_empty());
}

#[tokio::test]
async fn list_recent_drops_objects_whose_head_fails() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = future_metadata(5);
    let key = format!("pastes/{today}/gone");
    fake.seed(
        &key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    fake.seed(
        &format!("pastes/{today}/here"),
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    fake.fail_head(&key);

    let items = store
        .list_recent(Some(&today), 10)
        .await
        .expect("listing must not fail when one head fails");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, format!("{today}/here"));
}

#[tokio::test]
async fn list_recent_reports_a_missing_bucket_as_unavailable() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.set_missing_bucket(true);
    let error = store
        .list_recent(Some("2026-10-28"), 10)
        .await
        .expect_err("a missing bucket must be an error");
    assert!(matches!(error, StoreError::BucketUnavailable(_)));
}

#[tokio::test]
async fn resolve_key_finds_a_bare_id_by_scanning_the_prefix() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"bare id", None, None, None)
        .await
        .expect("text must store");
    let bare = stored
        .id
        .split_once('/')
        .expect("stored id must be dated")
        .1;

    let meta = store
        .head_metadata(bare)
        .await
        .expect("a bare id must resolve through ListObjectsV2");
    assert_eq!(meta.id, stored.id);
    assert_eq!(meta.key, stored.key);
}

#[tokio::test]
async fn multipart_metadata_is_set_at_create_time_without_a_copy() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1);
    let path = common::write_temp_file("metadata-at-create.bin", &body);
    // A copy failure must not matter, because the multipart path does not
    // use a copy to attach metadata.
    fake.fail(FailOp::CopyObject);

    let stored = store
        .store_file_path(&path, None, None, None)
        .await
        .expect("the upload itself must succeed");
    let object = fake.object(&stored.key).expect("object must exist");
    assert_eq!(object.body, body);
    assert_eq!(
        object.metadata.get("sha256").map(String::as_str),
        Some(stored.sha256.as_str())
    );
    let size = stored.size.to_string();
    assert_eq!(
        object.metadata.get("size").map(String::as_str),
        Some(size.as_str())
    );
    assert!(object.metadata.contains_key("expires-at"));
    assert!(
        !fake.requests().iter().any(|request| {
            request
                .headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("x-amz-copy-source"))
        }),
        "multipart must set metadata at create time, not with a copy"
    );
    let _ = std::fs::remove_file(path);
}

// --- Append tests that need a fake S3 service -----------------------------

#[tokio::test]
async fn store_and_fetch_roundtrip_against_fake_s3() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let slice = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("fetch must succeed against the fake");
    assert_eq!(slice.text, "hello");
    assert!(fake.object(&stored.key).is_some());
}

#[tokio::test]
async fn append_writes_only_new_bytes_and_fetch_concatenates() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let expires_before = store
        .head_metadata(&stored.id)
        .await
        .expect("head must succeed")
        .expires_at;
    let puts_before = fake.puts().len();

    let appended = store
        .append_text(&stored.id, b" world")
        .await
        .expect("append must succeed");

    assert_eq!(appended.appended_bytes, 6);
    assert!(appended.segment_key.is_some());
    assert_eq!(appended.meta.size, 11);
    assert_eq!(appended.meta.expires_at, expires_before);
    let puts = fake.puts();
    assert_eq!(puts.len(), puts_before + 1, "append must write one object");
    assert_eq!(
        puts.last().expect("append must record a put").1,
        b" world".to_vec(),
        "append must write only the new bytes"
    );
    assert!(
        fake.gets().is_empty(),
        "append must not read the base object or any segment body"
    );
    assert_eq!(
        fake.object(&stored.key)
            .expect("the base object must still exist")
            .body,
        b"hello".to_vec(),
        "append must not rewrite the base object"
    );
    let segment = appended
        .segment_key
        .as_ref()
        .expect("append must name a segment");
    let (sequence, suffix) =
        parse_segment_key(&stored.key, segment).expect("an append must write a suffixed key");
    assert_eq!(sequence, 0);
    assert_eq!(suffix.len(), SEGMENT_SUFFIX_LEN);
    assert!(
        segment.starts_with(&format!("{}/{:020}-", stored.key, 0)),
        "the segment key must start with the sequence: {segment:?}"
    );
    assert_eq!(
        fake.object(segment)
            .expect("the segment must exist")
            .metadata
            .get("expires-at"),
        Some(
            &expires_before
                .expect("the stored paste must have an expiry")
                .to_rfc3339()
        ),
        "the segment must inherit the base expiry"
    );

    let slice = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("fetch must succeed");
    assert_eq!(slice.text, "hello world");
    assert_eq!(slice.total_bytes, 11);

    let middle = store
        .fetch_text(&stored.id, 3, Some(5))
        .await
        .expect("range fetch must succeed");
    assert_eq!(middle.text, "lo wo");

    let by_key = store
        .fetch_range_by_key(&stored.key, Some((3, 7)))
        .await
        .expect("range fetch by key must succeed");
    assert_eq!(by_key, b"lo wo".to_vec());
}

#[tokio::test]
async fn append_writes_a_suffixed_segment_key() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let appended = store
        .append_text(&stored.id, b"more")
        .await
        .expect("append must succeed");
    let key = appended.segment_key.expect("an append must name a segment");
    let (sequence, suffix) =
        parse_segment_key(&stored.key, &key).expect("the segment key must have a suffix");
    assert_eq!(sequence, 0);
    assert_eq!(suffix.len(), SEGMENT_SUFFIX_LEN);
    assert!(
        key.starts_with(&format!("{}/{:020}-", stored.key, 0)),
        "the segment key must start with the sequence: {key:?}"
    );
    assert!(suffix.bytes().all(|b| b.is_ascii_alphanumeric()));
}

#[tokio::test]
async fn head_metadata_reports_total_size_and_omits_the_base_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let before = store
        .head_metadata(&stored.id)
        .await
        .expect("head must succeed");
    assert_eq!(before.size, 5);
    assert!(before.sha256.is_some());
    assert!(before.segments.is_empty());

    store
        .append_text(&stored.id, b" world")
        .await
        .expect("append must succeed");
    let after = store
        .head_metadata(&stored.id)
        .await
        .expect("head must succeed");
    assert_eq!(after.base_size, 5);
    assert_eq!(after.size, 11);
    assert_eq!(after.segments.len(), 1);
    assert_eq!(
        after.sha256, None,
        "a base-only hash must not describe the concatenated paste"
    );
}

#[tokio::test]
async fn range_fetch_reads_only_the_objects_it_touches() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let appended = store
        .append_text(&stored.id, b" world")
        .await
        .expect("append must succeed");
    let segment = appended.segment_key.expect("append must name a segment");

    let before = fake.gets().len();
    let inside = store
        .fetch_text(&stored.id, 6, Some(3))
        .await
        .expect("segment range must succeed");
    assert_eq!(inside.text, "wor");
    assert_eq!(
        fake.gets()[before..].to_vec(),
        vec![(segment.clone(), Some("bytes=1-3".to_string()))],
        "a range inside a segment must not read the base object"
    );

    let before = fake.gets().len();
    let spanned = store
        .fetch_text(&stored.id, 3, Some(5))
        .await
        .expect("spanning range must succeed");
    assert_eq!(spanned.text, "lo wo");
    assert_eq!(
        fake.gets()[before..].to_vec(),
        vec![
            (stored.key.clone(), Some("bytes=3-4".to_string())),
            (segment, Some("bytes=0-2".to_string())),
        ],
        "each object must get a local range request"
    );
}

#[tokio::test]
async fn download_file_concatenates_base_and_segments() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    store
        .append_text(&stored.id, b" world")
        .await
        .expect("append must succeed");
    let path = common::temp_path("append-download");
    store
        .download_file(&stored.id, &path, None)
        .await
        .expect("full download must succeed");
    assert_eq!(
        tokio::fs::read(&path).await.expect("download must exist"),
        b"hello world".to_vec()
    );
    store
        .download_file(&stored.id, &path, Some((3, 7)))
        .await
        .expect("range download must succeed");
    assert_eq!(
        tokio::fs::read(&path).await.expect("download must exist"),
        b"lo wo".to_vec()
    );
    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn list_recent_skips_segment_keys() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    store
        .append_text(&stored.id, b"+more")
        .await
        .expect("append must succeed");
    let date = stored
        .id
        .split_once('/')
        .expect("full id must contain a date")
        .0;
    let items = store
        .list_recent(Some(date), 100)
        .await
        .expect("list must succeed");
    assert_eq!(items.len(), 1, "a segment must not appear as a paste");
    assert_eq!(items[0].id, stored.id);
    assert_eq!(items[0].size, 9, "the listing must report the total size");
}

#[tokio::test]
async fn bare_id_resolution_skips_segment_keys() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let appended = store
        .append_text(&stored.id, b"more")
        .await
        .expect("append must succeed");
    let segment_name = appended
        .segment_key
        .as_ref()
        .expect("append must name a segment")
        .rsplit('/')
        .next()
        .expect("a segment key must have a name")
        .to_string();
    let result = store.resolve_key(&segment_name).await;
    assert!(
        matches!(result, Err(StoreError::NotFound(_))),
        "a segment key must not resolve as a bare paste id: {result:?}"
    );
}

#[tokio::test]
async fn delete_removes_the_base_object_and_every_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let first = store
        .append_text(&stored.id, b"one")
        .await
        .expect("first append must succeed");
    let second = store
        .append_text(&stored.id, b"two")
        .await
        .expect("second append must succeed");
    store.delete(&stored.id).await.expect("delete must succeed");
    assert!(fake.object(&stored.key).is_none());
    assert!(fake
        .object(first.segment_key.as_ref().expect("first segment"))
        .is_none());
    assert!(fake
        .object(second.segment_key.as_ref().expect("second segment"))
        .is_none());
}

#[tokio::test]
async fn append_errors_before_write_for_missing_expired_or_oversize() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(10, 1024 * 1024).await;
    let missing = store.append_text("2026-10-28/missing", b"x").await;
    assert!(missing.is_err(), "a missing id must be an error");
    assert!(fake.puts().is_empty(), "a missing id must write nothing");

    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let past = (Utc::now() - Duration::days(1)).to_rfc3339();
    fake.set_metadata(&stored.key, "expires-at", &past);
    let expired = store.append_text(&stored.id, b"x").await;
    assert!(
        matches!(expired, Err(StoreError::Expired(_))),
        "an expired id must be an error: {expired:?}"
    );
    assert_eq!(fake.puts().len(), 1, "the expired append must not write");

    let oversize = store.append_text(&stored.id, b"01234567890").await;
    assert!(
        matches!(oversize, Err(StoreError::TooLarge { limit: 10 })),
        "the append cap must apply to the appended bytes: {oversize:?}"
    );
    assert_eq!(fake.puts().len(), 1, "the oversize append must not write");
}

#[tokio::test]
async fn empty_append_writes_nothing_and_keeps_the_base_hash() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let puts_before = fake.puts().len();
    let result = store
        .append_text(&stored.id, b"")
        .await
        .expect("an empty append is a no-op");
    assert_eq!(result.appended_bytes, 0);
    assert!(result.segment_key.is_none());
    assert_eq!(fake.puts().len(), puts_before);
    assert!(
        result.meta.sha256.is_some(),
        "a no-op must not drop the base hash"
    );
}

#[tokio::test]
async fn same_sequence_segments_with_distinct_suffixes_both_survive_a_read() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let seeded = [
        ("AAAAAAAAAAAA", &b" first"[..]),
        ("BBBBBBBBBBBB", &b" second"[..]),
    ];
    for (suffix, body) in seeded {
        let key = format!("{}/{:020}-{suffix}", stored.key, 0);
        fake.seed(&key, body, "", &[]);
        assert!(fake.object(&key).is_some(), "the seeded segment must exist");
    }

    let slice = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("fetch must succeed");
    assert_eq!(
        slice.text, "base first second",
        "a read must return both same-sequence chunks in suffix order"
    );
    assert_eq!(slice.total_bytes, 17);
}

#[tokio::test]
async fn concurrent_appends_write_distinct_segments_and_both_read_back() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 1024 * 1024).await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("store must succeed against the fake");
    let (first, second) = tokio::join!(
        store.append_text(&stored.id, b" first"),
        store.append_text(&stored.id, b" second"),
    );
    let first = first.expect("the first concurrent append must succeed");
    let second = second.expect("the second concurrent append must succeed");
    let first_key = first
        .segment_key
        .expect("the first append must name a segment");
    let second_key = second
        .segment_key
        .expect("the second append must name a segment");
    assert_ne!(
        first_key, second_key,
        "concurrent appends must write distinct objects"
    );
    assert!(fake.object(&first_key).is_some());
    assert!(fake.object(&second_key).is_some());

    let mut chunks = [
        (first_key, b" first".to_vec()),
        (second_key, b" second".to_vec()),
    ];
    chunks.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = String::from("base");
    for (_, body) in chunks {
        expected.push_str(std::str::from_utf8(&body).expect("test text is UTF-8"));
    }
    let slice = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("fetch must succeed");
    assert_eq!(slice.text, expected);
    assert_eq!(slice.total_bytes, 17);
}
