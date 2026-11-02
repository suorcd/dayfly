//! Store and S3 path integration tests against the in-process fake S3.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use bytes::Bytes;
use chrono::{Duration, Utc};
use common::{FailOp, FakeS3, Gate};
use dayfly::store::{
    normalize_words, parse_segment_key, segment_key, StoreError, Ttl, MAX_SEGMENTS_PER_PASTE,
    MULTIPART_CHUNK_BYTES, SEGMENT_SUFFIX_LEN,
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
async fn store_text_returns_words_and_fetch_text_accepts_them() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;

    let stored = store
        .store_text(
            b"three word body",
            None,
            Some("text/plain"),
            Some(Ttl::days(1)),
        )
        .await
        .expect("store_text must succeed against the fake");

    let words = stored
        .words
        .as_deref()
        .expect("a new paste must have a three-word alias");
    assert!(
        words.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
        "the alias must be lowercase hyphen form: {words}"
    );

    let alias = fake
        .object(&format!("words/{words}"))
        .expect("the alias object must exist");
    assert_eq!(
        alias.body,
        stored.key.as_bytes(),
        "the alias body must hold the canonical key"
    );
    assert_eq!(alias.content_type, "text/plain");
    assert_eq!(
        alias.metadata.get("target").map(String::as_str),
        Some(stored.key.as_str())
    );
    assert_eq!(
        alias.metadata.get("kind").map(String::as_str),
        Some("alias")
    );
    assert_eq!(
        alias.metadata.get("expires-at").map(String::as_str),
        Some(stored.expires_at.to_rfc3339().as_str())
    );

    let fetched = store
        .fetch_text(words, 0, None)
        .await
        .expect("fetch_text must resolve the word alias");
    assert_eq!(fetched.text, "three word body");
    assert_eq!(
        normalize_words(words).as_deref(),
        Some(words),
        "the stored alias must be the canonical hyphen form"
    );
}

/// A phrase that is valid and different from `taken`.
fn other_phrase(taken: &str) -> String {
    for candidate in [
        "abacus-abdomen-zoology",
        "abacus-abdomen-abdominal",
        "abacus-abdomen-abide",
    ] {
        if candidate != taken {
            return candidate.to_string();
        }
    }
    panic!("no unused test phrase");
}

#[tokio::test]
async fn word_alias_resolves_fetch_metadata_append_and_delete() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"hello", None, Some("text/plain"), Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have an alias");

    let meta = store
        .head_metadata(&words)
        .await
        .expect("metadata by words must resolve");
    assert_eq!(meta.id, stored.id);
    assert_eq!(meta.words.as_deref(), Some(words.as_str()));

    let appended = store
        .append_text(&words, b" world")
        .await
        .expect("append by words must resolve");
    assert_eq!(appended.meta.id, stored.id);
    assert_eq!(appended.meta.words.as_deref(), Some(words.as_str()));

    let fetched = store
        .fetch_text(&words, 0, None)
        .await
        .expect("fetch by words must resolve after an append");
    assert_eq!(fetched.text, "hello world");

    let spaced = words.replace('-', " ").to_uppercase();
    let meta = store
        .head_metadata(&spaced)
        .await
        .expect("a phrase with spaces and capitals must resolve");
    assert_eq!(meta.id, stored.id);

    store
        .delete(&words)
        .await
        .expect("delete by words must remove the paste");
    assert!(fake.object(&stored.key).is_none(), "the base must be gone");
    assert!(
        fake.object(&format!("words/{words}")).is_none(),
        "the alias must be gone"
    );
    assert!(
        fake.keys().is_empty(),
        "segments must be gone: {:?}",
        fake.keys()
    );

    let error = store
        .fetch_text(&words, 0, None)
        .await
        .expect_err("a deleted alias must not resolve");
    assert!(matches!(error, StoreError::NotFound(_)), "got {error:?}");
}

#[tokio::test]
async fn delete_by_canonical_id_removes_the_alias() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"delete me", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have an alias");

    store
        .delete(&stored.id)
        .await
        .expect("delete by canonical id must succeed");
    assert!(fake.object(&stored.key).is_none(), "the base must be gone");
    assert!(
        fake.object(&format!("words/{words}")).is_none(),
        "delete by canonical id must remove the alias"
    );
}

#[tokio::test]
async fn missing_expired_and_stale_aliases_do_not_resolve() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"live body", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have an alias");
    let unused = other_phrase(&words);

    let error = store
        .fetch_text(&unused, 0, None)
        .await
        .expect_err("a phrase with no alias object must not resolve");
    assert!(matches!(error, StoreError::NotFound(_)), "got {error:?}");

    let past = (Utc::now() - Duration::days(1)).to_rfc3339();
    fake.set_metadata(&stored.key, "expires-at", &past);
    let error = store
        .fetch_text(&words, 0, None)
        .await
        .expect_err("an expired paste must not resolve through its alias");
    assert!(matches!(error, StoreError::Expired(_)), "got {error:?}");

    let stale_target = "pastes/2026-10-28/stalemissing";
    fake.seed(
        &format!("words/{unused}"),
        stale_target.as_bytes(),
        "text/plain",
        &[
            ("kind", "alias"),
            ("target", stale_target),
            ("expires-at", "2999-01-01T00:00:00+00:00"),
        ],
    );
    let error = store
        .fetch_text(&unused, 0, None)
        .await
        .expect_err("an alias with a missing target must not resolve");
    assert!(matches!(error, StoreError::NotFound(_)), "got {error:?}");
}

#[tokio::test]
async fn alias_collision_retries_until_a_new_phrase_is_chosen() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let taken = "abacus-abdomen-zoology";
    let target = "pastes/2026-10-28/taken";
    fake.seed(
        &format!("words/{taken}"),
        target.as_bytes(),
        "text/plain",
        &[
            ("kind", "alias"),
            ("target", target),
            ("expires-at", "2999-01-01T00:00:00+00:00"),
        ],
    );

    let chosen = store
        .choose_alias_from(vec![
            taken.to_string(),
            taken.to_string(),
            "abacus-abdomen-abdominal".to_string(),
        ])
        .await
        .expect("the allocator must skip a taken phrase");
    assert_eq!(chosen, "abacus-abdomen-abdominal");
    assert_eq!(
        fake.object(&format!("words/{taken}"))
            .expect("the taken alias must remain")
            .body,
        target.as_bytes(),
        "the allocator must not overwrite an existing alias"
    );
}

#[tokio::test]
async fn alias_allocation_fails_when_every_candidate_collides() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let taken = "abacus-abdomen-zoology";
    let target = "pastes/2026-10-28/taken";
    fake.seed(
        &format!("words/{taken}"),
        target.as_bytes(),
        "text/plain",
        &[
            ("kind", "alias"),
            ("target", target),
            ("expires-at", "2999-01-01T00:00:00+00:00"),
        ],
    );

    let error = store
        .choose_alias_from(std::iter::repeat_n(taken.to_string(), 8))
        .await
        .expect_err("eight collisions must stop the allocator");
    assert!(
        matches!(error, StoreError::AliasUnavailable { attempts: 8 }),
        "got {error:?}"
    );
}

#[tokio::test]
async fn concurrent_stores_cannot_claim_the_same_alias() {
    let fake = FakeS3::start().await;
    let candidate = "abacus-abdomen-zoology".to_string();
    let store = fake
        .store()
        .await
        .with_alias_candidates(vec![candidate.clone()]);
    fake.hold(Gate::HeadObject);

    let first = tokio::spawn({
        let store = store.clone();
        async move { store.store_text(b"first", None, None, None).await }
    });
    fake.wait_until_blocked(Gate::HeadObject).await;

    let second = tokio::spawn({
        let store = store.clone();
        async move { store.store_text(b"second", None, None, None).await }
    });

    // On the old code both stores issue an alias HEAD and block. With
    // the claim lock, the second store waits before its HEAD. Give it
    // time to reach that point, then release.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
    while tokio::time::Instant::now() < deadline {
        let alias_heads = fake
            .requests()
            .iter()
            .filter(|request| request.method == "HEAD" && request.path.contains("/words/"))
            .count();
        if alias_heads >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    fake.release();

    let first = first.await.expect("the first store must not panic");
    let second = second.await.expect("the second store must not panic");
    let (winner, loser) = match (first, second) {
        (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
        (Ok(a), Ok(b)) => panic!("two stores returned aliases: {a:?} {b:?}"),
        (Err(a), Err(b)) => panic!("both stores failed: {a:?} {b:?}"),
    };
    assert_eq!(winner.words.as_deref(), Some(candidate.as_str()));
    assert!(
        matches!(loser, StoreError::AliasUnavailable { .. }),
        "the losing store must fail alias allocation, got {loser:?}"
    );
    let bases: Vec<String> = fake
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("pastes/"))
        .collect();
    assert_eq!(bases, vec![winner.key.clone()]);
    let alias = fake
        .object(&format!("words/{candidate}"))
        .expect("the winner alias must remain");
    assert_eq!(alias.body, winner.key.as_bytes());
}

#[tokio::test]
async fn a_store_must_not_delete_an_alias_rebound_to_another_paste() {
    let fake = FakeS3::start().await;
    let candidate = "abacus-abdomen-zoology".to_string();
    let store = fake
        .store()
        .await
        .with_alias_candidates(vec![candidate.clone()]);
    let other_key = "pastes/2026-10-28/other".to_string();
    let other_meta = future_metadata(5);
    fake.seed(
        &other_key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&other_meta),
    );
    let path = common::write_temp_file(
        "alias-rebound.bin",
        &common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1),
    );

    fake.hold(Gate::CreateMultipartUpload);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("rebound.bin"), None, None)
                .await
        }
    });
    fake.wait_until_blocked(Gate::CreateMultipartUpload).await;

    // The store has claimed the alias before the upload. Rebind it to
    // another paste, as a second process could.
    let alias_meta = [
        ("kind".to_string(), "alias".to_string()),
        ("target".to_string(), other_key.clone()),
        (
            "expires-at".to_string(),
            "2999-01-01T00:00:00+00:00".to_string(),
        ),
    ];
    fake.seed(
        &format!("words/{candidate}"),
        other_key.as_bytes(),
        "text/plain",
        &common::metadata_refs(&alias_meta),
    );
    fake.release();

    let result = task.await.expect("the store task must not panic");
    assert!(
        matches!(result, Err(StoreError::AliasUnavailable { .. })),
        "a rebound alias must fail the store, got {result:?}"
    );
    let alias = fake
        .object(&format!("words/{candidate}"))
        .expect("the other paste's alias must remain");
    assert_eq!(alias.body, other_key.as_bytes());
    let bases: Vec<String> = fake
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("pastes/"))
        .collect();
    assert_eq!(
        bases,
        vec![other_key.clone()],
        "the failed paste must be removed"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn a_transient_alias_read_failure_keeps_the_stored_paste() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_get_prefix("words/");

    let error = store
        .store_text(b"keep me", None, None, Some(Ttl::days(1)))
        .await
        .expect_err("the alias read must fail");
    assert!(matches!(error, StoreError::S3(_)), "got {error:?}");

    let bases: Vec<String> = fake
        .keys()
        .into_iter()
        .filter(|key| key.starts_with("pastes/"))
        .collect();
    assert_eq!(
        bases.len(),
        1,
        "a transient alias read must keep the new object: {bases:?}"
    );
}

#[tokio::test]
async fn a_paste_over_the_segment_limit_fails_a_metadata_read() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let base_key = "pastes/2026-10-28/manysegments";
    let meta = future_metadata(1);
    fake.seed(base_key, b"x", "text/plain", &common::metadata_refs(&meta));
    for index in 0..=MAX_SEGMENTS_PER_PASTE {
        let key = segment_key(base_key, index as u64, &format!("seg{index:09}"));
        fake.seed(&key, b"a", "text/plain", &[]);
    }

    let error = store
        .head_metadata("2026-10-28/manysegments")
        .await
        .expect_err("a paste over the segment limit must not be read");
    assert!(
        matches!(error, StoreError::TooManySegments { .. }),
        "got {error:?}"
    );
}

#[tokio::test]
async fn an_append_at_the_segment_limit_writes_no_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let base_key = "pastes/2026-10-28/atlimit";
    let meta = future_metadata(1);
    fake.seed(base_key, b"x", "text/plain", &common::metadata_refs(&meta));
    for index in 0..MAX_SEGMENTS_PER_PASTE {
        let key = segment_key(base_key, index as u64, &format!("seg{index:09}"));
        fake.seed(&key, b"a", "text/plain", &[]);
    }

    let error = store
        .append_text("2026-10-28/atlimit", b"more")
        .await
        .expect_err("an append at the segment limit must fail");
    assert!(
        matches!(error, StoreError::TooManySegments { .. }),
        "got {error:?}"
    );
    assert!(
        fake.puts().is_empty(),
        "a refused append must write no segment"
    );
}

#[tokio::test]
async fn delete_by_id_keeps_an_alias_that_points_to_another_paste() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let date = "2026-10-28";
    let words = "abacus-abdomen-zoology";
    let a_key = format!("pastes/{date}/aaa");
    let b_key = format!("pastes/{date}/bbb");
    let mut a_meta = future_metadata(3);
    a_meta.push(("words".to_string(), words.to_string()));
    let b_meta = future_metadata(3);
    fake.seed(
        &a_key,
        b"aaa",
        "text/plain",
        &common::metadata_refs(&a_meta),
    );
    fake.seed(
        &b_key,
        b"bbb",
        "text/plain",
        &common::metadata_refs(&b_meta),
    );
    let alias_meta = [
        ("kind".to_string(), "alias".to_string()),
        ("target".to_string(), b_key.clone()),
        (
            "expires-at".to_string(),
            "2999-01-01T00:00:00+00:00".to_string(),
        ),
    ];
    fake.seed(
        &format!("words/{words}"),
        b_key.as_bytes(),
        "text/plain",
        &common::metadata_refs(&alias_meta),
    );

    store
        .delete(&format!("{date}/aaa"))
        .await
        .expect("delete must succeed");
    assert!(
        fake.object(&a_key).is_none(),
        "the requested paste must be gone"
    );
    assert_eq!(
        fake.object(&b_key)
            .expect("the other paste must remain")
            .body,
        b"bbb"
    );
    let alias = fake
        .object(&format!("words/{words}"))
        .expect("the alias for the other paste must remain");
    assert_eq!(alias.body, b_key.as_bytes());
}

#[tokio::test]
async fn alias_write_failure_writes_no_text_base_object() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_put_prefix("words/");

    let error = store
        .store_text(b"no alias", None, None, Some(Ttl::days(1)))
        .await
        .expect_err("a failed alias write must fail the store");
    assert!(matches!(error, StoreError::S3(_)), "got {error:?}");
    assert!(
        fake.keys().is_empty(),
        "the failed store must write no base object: {:?}",
        fake.keys()
    );
}

#[tokio::test]
async fn alias_write_failure_writes_no_stream_base_object() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    fake.fail_put_prefix("words/");
    let body = Bytes::from_static(b"stream body");
    let length = body.len() as u64;
    let upload = stream::iter(vec![Ok::<Bytes, StoreError>(body)]);

    let error = store
        .store_stream(
            upload,
            Some("stream.bin"),
            None,
            Some(Ttl::days(1)),
            Some(length),
            1024,
        )
        .await
        .expect_err("a failed alias write must fail the stream store");
    assert!(matches!(error, StoreError::S3(_)), "got {error:?}");
    assert!(
        fake.keys().is_empty(),
        "the failed store must write no streamed base object: {:?}",
        fake.keys()
    );
}

#[tokio::test]
async fn alias_write_failure_writes_no_multipart_base_object() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let path = common::write_temp_file(
        "alias-multipart.bin",
        &common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1),
    );
    fake.fail_put_prefix("words/");

    let error = store
        .store_file_path(&path, Some("large.bin"), None, Some(Ttl::days(1)))
        .await
        .expect_err("a failed alias write must fail the multipart store");
    assert!(matches!(error, StoreError::S3(_)), "got {error:?}");
    assert!(
        fake.keys().is_empty(),
        "the failed store must write no multipart base object: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn store_file_path_returns_words_and_writes_the_alias() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let path = common::write_temp_file("alias-file.txt", b"file body");

    let stored = store
        .store_file_path(&path, None, Some("text/plain"), Some(Ttl::days(1)))
        .await
        .expect("store_file_path must succeed");
    let words = stored.words.clone().expect("a file must have an alias");
    let alias = fake
        .object(&format!("words/{words}"))
        .expect("the alias object must exist");
    assert_eq!(alias.body, stored.key.as_bytes());

    let fetched = store
        .fetch_text(&words, 0, None)
        .await
        .expect("fetch by words must resolve the file");
    assert_eq!(fetched.text, "file body");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn legacy_object_without_an_alias_still_works() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let key = "pastes/2026-10-28/legacy123";
    fake.seed(
        key,
        b"legacy body",
        "text/plain",
        &common::metadata_refs(&future_metadata(11)),
    );

    let meta = store
        .head_metadata("2026-10-28/legacy123")
        .await
        .expect("a legacy object must resolve by id");
    assert_eq!(meta.words, None);
    let fetched = store
        .fetch_text("2026-10-28/legacy123", 0, None)
        .await
        .expect("a legacy object must be readable");
    assert_eq!(fetched.text, "legacy body");

    let items = store
        .list_recent(Some("2026-10-28"), 10)
        .await
        .expect("a legacy object must list");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].words, None);
}

#[tokio::test]
async fn list_recent_includes_the_word_alias() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"listed", None, None, Some(Ttl::days(1)))
        .await
        .expect("store_text must succeed");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have an alias");
    let date = stored.id.split_once('/').expect("dated id").0;

    let items = store
        .list_recent(Some(date), 10)
        .await
        .expect("list_recent must succeed");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].words.as_deref(), Some(words.as_str()));
    assert!(
        items.iter().all(|item| !item.key.starts_with("words/")),
        "an alias object must not appear as a paste"
    );
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
async fn store_stream_rejects_a_short_known_length_text_body() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let chunks: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"short"))];

    let error = store
        .store_stream(stream::iter(chunks), None, None, None, Some(10), 1024)
        .await
        .expect_err("a short text body must fail");

    let message = error.to_string();
    assert!(message.contains("expected 10 bytes"), "got: {message}");
    assert!(message.contains("received 5 bytes"), "got: {message}");
    assert!(
        fake.keys().is_empty(),
        "a short body must store nothing: {:?}",
        fake.keys()
    );
}

#[tokio::test]
async fn store_stream_rejects_a_short_known_length_multipart_body() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let expected = 6 * 1024 * 1024;
    let chunks: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"short"))];

    let error = store
        .store_stream(
            stream::iter(chunks),
            Some("short.bin"),
            None,
            None,
            Some(expected),
            1024 * 1024 * 1024,
        )
        .await
        .expect_err("a short multipart body must fail");

    let message = error.to_string();
    assert!(
        message.contains(&format!("expected {expected} bytes")),
        "got: {message}"
    );
    assert!(message.contains("received 5 bytes"), "got: {message}");
    assert!(
        fake.keys().is_empty(),
        "a short multipart body must store nothing: {:?}",
        fake.keys()
    );
}

#[tokio::test]
async fn store_stream_accepts_a_matching_known_length_multipart_body() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(6 * 1024 * 1024);
    let len = body.len() as u64;
    let chunks: Vec<Result<Bytes, StoreError>> = body
        .chunks(64 * 1024)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();

    let stored = store
        .store_stream(
            stream::iter(chunks),
            Some("full.bin"),
            None,
            None,
            Some(len),
            1024 * 1024 * 1024,
        )
        .await
        .expect("a matching multipart body must store");

    assert_eq!(stored.size, len);
    assert_eq!(
        fake.object(&stored.key)
            .expect("the object must exist")
            .body
            .len() as u64,
        len
    );
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
    fake.fail_put_prefix("pastes/");
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
async fn download_file_rejects_a_reversed_range_without_touching_the_destination() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"abcdefghij", None, None, None)
        .await
        .expect("text must store");
    let dest = common::temp_path("reversed-range.bin");
    std::fs::write(&dest, b"keep me").expect("destination sentinel must be writable");

    let error = store
        .download_file(&stored.id, &dest, Some((5, 2)))
        .await
        .expect_err("a reversed range must be rejected");
    assert!(
        matches!(error, StoreError::InvalidRange(_)),
        "a reversed range must be an invalid range, got {error:?}"
    );
    assert_eq!(
        std::fs::read(&dest).expect("destination must still exist"),
        b"keep me",
        "a rejected range must not truncate the destination"
    );

    let _ = std::fs::remove_file(dest);
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
async fn list_recent_skips_an_object_deleted_between_list_and_head() {
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
        .expect("listing must not fail when one head is a 404");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, format!("{today}/here"));
}

#[tokio::test]
async fn list_recent_propagates_a_head_access_denied_error() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = future_metadata(5);
    let key = format!("pastes/{today}/denied");
    fake.seed(
        &key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    fake.fail_head_with(&key, StatusCode::FORBIDDEN, "AccessDenied");

    let error = store
        .list_recent(Some(&today), 10)
        .await
        .expect_err("a non-404 head error must fail the listing");
    assert!(
        matches!(error, StoreError::S3(_)),
        "a head error must reach the caller, got {error:?}"
    );
}

#[tokio::test]
async fn list_recent_propagates_a_segment_list_error() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let metadata = future_metadata(5);
    let key = format!("pastes/{today}/segments");
    fake.seed(
        &key,
        b"abcde",
        "text/plain",
        &common::metadata_refs(&metadata),
    );
    fake.fail_list_prefix(
        &format!("{key}/"),
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
    );

    let error = store
        .list_recent(Some(&today), 10)
        .await
        .expect_err("a segment-list error must fail the listing");
    assert!(
        matches!(error, StoreError::S3(_)),
        "a segment-list error must reach the caller, got {error:?}"
    );
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
    let gets_before = fake.gets().len();

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
    assert_eq!(
        fake.gets().len(),
        gets_before,
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
    let puts_after_store = fake.puts().len();
    assert_eq!(
        puts_after_store, 2,
        "the store writes the base object and its alias"
    );
    let past = (Utc::now() - Duration::days(1)).to_rfc3339();
    fake.set_metadata(&stored.key, "expires-at", &past);
    let expired = store.append_text(&stored.id, b"x").await;
    assert!(
        matches!(expired, Err(StoreError::Expired(_))),
        "an expired id must be an error: {expired:?}"
    );
    assert_eq!(
        fake.puts().len(),
        puts_after_store,
        "the expired append must not write"
    );

    let oversize = store.append_text(&stored.id, b"01234567890").await;
    assert!(
        matches!(oversize, Err(StoreError::TooLarge { limit: 10 })),
        "the append cap must apply to the appended bytes: {oversize:?}"
    );
    assert_eq!(
        fake.puts().len(),
        puts_after_store,
        "the oversize append must not write"
    );
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

// --- Issue #72: bound local-file reads and detect changes ---

/// A small file that grows past one chunk must switch to multipart instead
/// of being buffered whole.
#[tokio::test]
async fn store_file_path_switches_to_multipart_when_a_file_grows_past_one_chunk() {
    let fake = FakeS3::start().await;
    let max_file = (MULTIPART_CHUNK_BYTES + 1024 * 1024) as u64;
    let store = fake.store_with_caps(1024, max_file).await;
    let path = common::write_temp_file("grow-past-chunk.bin", &common::pattern_bytes(32));

    fake.hold(Gate::HeadObject);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("grow.bin"), None, None)
                .await
        }
    });

    fake.wait_until_blocked(Gate::HeadObject).await;
    append_to_file(&path, &common::pattern_bytes(MULTIPART_CHUNK_BYTES + 1));
    fake.release();

    let stored = task
        .await
        .expect("the store task must not panic")
        .expect("a file that is still under the cap must store");
    let expected = (32 + MULTIPART_CHUNK_BYTES + 1) as u64;
    assert_eq!(stored.size, expected);
    assert!(
        fake.requests()
            .iter()
            .any(|request| request.query.contains("uploads")),
        "a file that grew past one chunk must use multipart"
    );
    assert_eq!(
        fake.object(&stored.key)
            .expect("the object must exist")
            .body
            .len() as u64,
        expected
    );
    let _ = std::fs::remove_file(path);
}

/// A same-length overwrite at CreateMultipartUpload must leave no paste.
#[tokio::test]
async fn store_file_path_rejects_a_same_length_change_during_a_multipart_upload() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let len = MULTIPART_CHUNK_BYTES + 4096;
    let path = common::write_temp_file("same-length-change.bin", &common::pattern_bytes(len));

    fake.hold(Gate::CreateMultipartUpload);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("changing.bin"), None, None)
                .await
        }
    });

    fake.wait_until_blocked(Gate::CreateMultipartUpload).await;
    std::fs::write(&path, vec![b'z'; len]).expect("the same-length overwrite must succeed");
    fake.release();

    let result = task.await.expect("the store task must not panic");
    assert!(
        result.is_err(),
        "a same-length change must fail, got {result:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a same-length change must leave no object and no alias: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);
}

/// A multipart file that grows past the cap at HeadObject must be rejected.
#[tokio::test]
async fn store_file_path_rejects_a_multipart_file_that_grows_past_the_cap() {
    let fake = FakeS3::start().await;
    let len = MULTIPART_CHUNK_BYTES + 1;
    let max_file = (len + 1024) as u64;
    let store = fake.store_with_caps(1024, max_file).await;
    let path = common::write_temp_file("multipart-grow-past-cap.bin", &common::pattern_bytes(len));

    fake.hold(Gate::HeadObject);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("grow.bin"), None, None)
                .await
        }
    });

    fake.wait_until_blocked(Gate::HeadObject).await;
    append_to_file(&path, &common::pattern_bytes(4096));
    fake.release();

    let result = task.await.expect("the store task must not panic");
    assert!(
        matches!(result, Err(StoreError::TooLarge { limit }) if limit == max_file),
        "a multipart file that grew past the cap must be rejected, got {result:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a rejected file must store nothing: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);
}

// --- Issue #58: one stable snapshot for size, hash, and bytes ---

/// Append to a file while a store call is parked inside a request.
fn append_to_file(path: &std::path::Path, bytes: &[u8]) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("the test file must reopen for append");
    file.write_all(bytes).expect("the append must succeed");
    file.sync_all().expect("the append must reach the file");
}

/// A file that grows past the cap after the size check must be rejected.
///
/// The alias probe is a HeadObject that happens before the read, so holding
/// it puts the growth between the metadata call and the read.
#[tokio::test]
async fn store_file_path_rejects_a_file_that_grows_past_the_cap() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_caps(1024, 64).await;
    let path = common::write_temp_file("grow-past-cap.bin", &common::pattern_bytes(32));

    fake.hold(Gate::HeadObject);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("grow.bin"), None, None)
                .await
        }
    });

    fake.wait_until_blocked(Gate::HeadObject).await;
    // 32 bytes at the size check, 96 bytes at the read, cap is 64.
    append_to_file(&path, &common::pattern_bytes(64));
    fake.release();

    let result = task.await.expect("the store task must not panic");
    assert!(
        matches!(result, Err(StoreError::TooLarge { limit: 64 })),
        "a file that grew past the cap must be rejected, got {result:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a rejected file must store nothing: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);
}

/// A file that changes during a multipart upload must leave no paste.
///
/// Metadata for a multipart object is fixed at CreateMultipartUpload, so a
/// change between the hash pass and the upload pass stores a hash that
/// describes bytes nobody uploaded. Holding CreateMultipartUpload makes that
/// window deterministic.
#[tokio::test]
async fn store_file_path_rejects_a_file_that_changes_during_a_multipart_upload() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let len = MULTIPART_CHUNK_BYTES + 4096;
    let path = common::write_temp_file("changes-mid-upload.bin", &common::pattern_bytes(len));

    fake.hold(Gate::CreateMultipartUpload);
    let task = tokio::spawn({
        let store = store.clone();
        let path = path.clone();
        async move {
            store
                .store_file_path(&path, Some("changing.bin"), None, None)
                .await
        }
    });

    fake.wait_until_blocked(Gate::CreateMultipartUpload).await;
    append_to_file(&path, &common::pattern_bytes(4096));
    fake.release();

    let result = task.await.expect("the store task must not panic");
    assert!(
        result.is_err(),
        "a file that changed mid-upload must fail, got {result:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a changed file must leave no object and no alias: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);
}

/// The stored size and hash must describe the bytes in the object.
#[tokio::test]
async fn store_file_path_metadata_describes_the_stored_bytes() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let body = common::pattern_bytes(4096);
    let path = common::write_temp_file("describes-bytes.bin", &body);

    let stored = store
        .store_file_path(&path, Some("bytes.bin"), None, None)
        .await
        .expect("the store must succeed");

    let object = fake.object(&stored.key).expect("the object must exist");
    assert_eq!(stored.size, object.body.len() as u64, "size must match");
    assert_eq!(stored.sha256, sha256_hex(&object.body), "hash must match");
    assert_eq!(
        object.metadata.get("sha256").map(String::as_str),
        Some(stored.sha256.as_str()),
        "stored metadata must carry the same hash"
    );
    assert_eq!(
        object.metadata.get("size").map(String::as_str),
        Some(stored.size.to_string().as_str()),
        "stored metadata must carry the same size"
    );
    let _ = std::fs::remove_file(path);
}
