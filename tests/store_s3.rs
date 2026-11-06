//! Store and S3 path integration tests against the in-process fake S3.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use bytes::Bytes;
use chrono::{Duration, Utc};
use common::{FailOp, FakeS3, Gate};
use dayfly::store::{
    normalize_words, normalize_words_up_to, parse_segment_key, segment_key, PasteFields, Store,
    StoreError, Ttl, MAX_FETCH_TEXT_BYTES, MAX_SEGMENTS_PER_PASTE, MULTIPART_CHUNK_BYTES,
    SEGMENT_SUFFIX_LEN,
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

fn object_expiry(fake: &FakeS3, key: &str) -> String {
    fake.object(key)
        .unwrap_or_else(|| panic!("object {key} must exist"))
        .metadata
        .get("expires-at")
        .unwrap_or_else(|| panic!("object {key} must have an expiry"))
        .clone()
}

fn segment_keys(fake: &FakeS3, base_key: &str) -> Vec<String> {
    let prefix = format!("{base_key}/");
    let mut keys: Vec<String> = fake
        .keys()
        .into_iter()
        .filter(|key| key.starts_with(&prefix))
        .collect();
    keys.sort();
    keys
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
async fn alias_word_count_applies_to_new_aliases_and_old_aliases_resolve() {
    let fake = FakeS3::start().await;
    let mut store = fake.store().await;

    let three = store
        .store_text(b"three", None, None, None)
        .await
        .expect("a three-word paste must store");
    let three_words = three.words.expect("a default alias must have words");
    assert!(normalize_words_up_to(&three_words, 3).is_some());

    store.config.alias_words = 4;
    let four = store
        .store_text(b"four", None, None, None)
        .await
        .expect("a four-word paste must store");
    let four_words = four.words.expect("a four-word alias must have words");
    assert!(normalize_words_up_to(&four_words, 4).is_some());

    store.config.alias_words = 6;
    let six = store
        .store_text(b"six", None, None, None)
        .await
        .expect("a six-word paste must store");
    let six_words = six.words.expect("a six-word alias must have words");
    assert!(normalize_words_up_to(&six_words, 6).is_some());

    for words in [&three_words, &four_words, &six_words] {
        let fetched = store
            .fetch_text(words, 0, None)
            .await
            .expect("an older alias must keep resolving");
        assert!(!fetched.text.is_empty());
    }
    store
        .delete(&three_words)
        .await
        .expect("an older alias must delete");
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
async fn a_paste_over_the_segment_limit_stays_readable_and_deletable() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let base_key = "pastes/2026-10-28/manysegments";
    let meta = future_metadata(1);
    fake.seed(base_key, b"x", "text/plain", &common::metadata_refs(&meta));
    for index in 0..=MAX_SEGMENTS_PER_PASTE {
        let key = segment_key(base_key, index as u64, &format!("seg{index:09}"));
        fake.seed(&key, b"a", "text/plain", &[]);
    }

    let meta = store
        .head_metadata("2026-10-28/manysegments")
        .await
        .expect("a paste one segment over the cap must stay readable");
    assert_eq!(meta.segments.len(), MAX_SEGMENTS_PER_PASTE + 1);

    store
        .delete("2026-10-28/manysegments")
        .await
        .expect("a paste over the cap must still delete");
    assert!(fake.object(base_key).is_none());
    assert!(
        fake.keys().iter().all(|key| !key.starts_with(base_key)),
        "the delete must remove the segments"
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
async fn append_respects_the_total_paste_cap() {
    let fake = FakeS3::start().await;
    let mut store = fake.store_with_caps(1024, 1024 * 1024).await;
    store.config.max_paste_bytes = 10;
    let stored = store
        .store_text(b"12345678", None, None, None)
        .await
        .expect("base paste must store");

    let under = store
        .append_text(&stored.id, b"1")
        .await
        .expect("an append under the cap must succeed");
    assert_eq!(under.meta.size, 9);

    let at = store
        .append_text(&stored.id, b"2")
        .await
        .expect("an append at the cap must succeed");
    assert_eq!(at.meta.size, 10);

    let puts_at_cap = fake.puts().len();
    let error = store
        .append_text(&stored.id, b"3")
        .await
        .expect_err("an append past the cap must fail");
    assert!(
        matches!(error, StoreError::TooLarge { limit } if limit == 10),
        "got {error:?}"
    );
    assert_eq!(
        fake.puts().len(),
        puts_at_cap,
        "a refused append must write no segment"
    );

    let zero = store
        .append_text(&stored.id, b"")
        .await
        .expect("a zero-byte append at the cap must succeed");
    assert_eq!(zero.appended_bytes, 0);
    assert!(zero.segment_key.is_none());
    assert_eq!(fake.puts().len(), puts_at_cap);

    let read = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("a paste at the cap must read");
    assert_eq!(read.text, "1234567812");

    store
        .delete(&stored.id)
        .await
        .expect("a paste at the cap must delete");
    assert!(fake.object(&stored.key).is_none());
}

#[tokio::test]
async fn create_text_above_the_total_paste_cap_stores_nothing() {
    let fake = FakeS3::start().await;
    let mut store = fake.store_with_caps(1024, 1024).await;
    store.config.max_paste_bytes = 10;

    let error = store
        .store_text(b"12345678901", None, None, None)
        .await
        .expect_err("an 11-byte text paste must fail under a 10-byte total cap");
    assert!(
        matches!(error, StoreError::TooLarge { limit: 10 }),
        "the error must name the total cap, got {error:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a refused paste must store nothing: {:?}",
        fake.keys()
    );

    let stored = store
        .store_text(b"1234567890", None, None, None)
        .await
        .expect("a text paste at the total cap must store");
    assert_eq!(stored.size, 10);
}

#[tokio::test]
async fn create_file_above_the_total_paste_cap_stores_nothing() {
    let fake = FakeS3::start().await;
    let mut store = fake.store_with_caps(1024, 1024).await;
    store.config.max_paste_bytes = 10;
    let path = common::write_temp_file("total-cap.bin", &common::pattern_bytes(11));

    let error = store
        .store_file_path(&path, Some("total-cap.bin"), None, None)
        .await
        .expect_err("an 11-byte file paste must fail under a 10-byte total cap");
    assert!(
        matches!(error, StoreError::TooLarge { limit: 10 }),
        "the error must name the total cap, got {error:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a refused paste must store nothing: {:?}",
        fake.keys()
    );
    let _ = std::fs::remove_file(path);

    let at_cap = common::write_temp_file("total-cap-exact.bin", &common::pattern_bytes(10));
    let stored = store
        .store_file_path(&at_cap, Some("total-cap-exact.bin"), None, None)
        .await
        .expect("a file paste at the total cap must store");
    assert_eq!(stored.size, 10);
    let _ = std::fs::remove_file(at_cap);
}

#[tokio::test]
async fn create_stream_above_the_total_paste_cap_stores_nothing() {
    let fake = FakeS3::start().await;
    let mut store = fake.store_with_caps(1024, 1024).await;
    store.config.max_paste_bytes = 10;

    let items: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"12345678901"))];
    let error = store
        .store_stream(
            stream::iter(items),
            Some("total-cap.bin"),
            None,
            None,
            Some(11),
            1024,
        )
        .await
        .expect_err("a known length above the total cap must fail");
    assert!(
        matches!(error, StoreError::TooLarge { limit: 10 }),
        "the store must clamp the caller cap, got {error:?}"
    );
    assert!(
        fake.requests().is_empty(),
        "a known length above the cap must fail before any request"
    );

    let items: Vec<Result<Bytes, StoreError>> = vec![Ok(Bytes::from_static(b"12345678901"))];
    let error = store
        .store_stream(
            stream::iter(items),
            Some("total-cap.bin"),
            None,
            None,
            None,
            1024,
        )
        .await
        .expect_err("an unknown-length stream above the total cap must fail");
    assert!(
        matches!(error, StoreError::TooLarge { limit: 10 }),
        "the stream must stop at the total cap, got {error:?}"
    );
    assert!(
        fake.keys().is_empty(),
        "a refused stream must store nothing: {:?}",
        fake.keys()
    );
    assert_eq!(
        fake.aborted_uploads().len(),
        1,
        "the multipart upload must be aborted"
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
async fn store_text_round_trips_title_and_normalized_tags() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let fields = PasteFields {
        title: Some("  Build log  ".to_string()),
        tags: vec!["Ops".to_string(), "ops".to_string(), " build ".to_string()],
        ..PasteFields::default()
    };
    let stored = store
        .store_text_with_fields(b"body", &fields, Some(Ttl::days(1)))
        .await
        .expect("a titled paste must store");
    assert_eq!(stored.title.as_deref(), Some("Build log"));
    assert_eq!(stored.tags, vec!["ops".to_string(), "build".to_string()]);

    let meta = store
        .head_metadata(&stored.id)
        .await
        .expect("metadata must be readable");
    assert_eq!(meta.title.as_deref(), Some("Build log"));
    assert_eq!(meta.tags, vec!["ops".to_string(), "build".to_string()]);
}

#[tokio::test]
async fn store_text_rejects_an_oversized_title_or_tags() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let title_fields = PasteFields {
        title: Some("x".repeat(201)),
        ..PasteFields::default()
    };
    let error = store
        .store_text_with_fields(b"body", &title_fields, None)
        .await
        .expect_err("an oversized title must be rejected");
    assert!(matches!(error, StoreError::InvalidTitle(_)));

    let tag_fields = PasteFields {
        tags: (0..9).map(|n| format!("tag-{n}")).collect(),
        ..PasteFields::default()
    };
    let error = store
        .store_text_with_fields(b"body", &tag_fields, None)
        .await
        .expect_err("too many tags must be rejected");
    assert!(matches!(error, StoreError::InvalidTags(_)));
}

#[tokio::test]
async fn list_window_pages_with_a_stateless_cursor() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    for key in ["aaa", "bbb", "ccc", "ddd", "eee"] {
        fake.seed(
            &format!("pastes/{today}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }

    let first = store
        .list_window_page(Some(&today), Some(&today), 2, None)
        .await
        .expect("the first page must succeed");
    assert_eq!(first.items.len(), 2);
    assert!(first.truncated);
    let first_next = first.next.clone().expect("the first page needs a cursor");
    assert_eq!(first_next.date, today);
    assert_eq!(
        first_next.start_after,
        first.items.last().expect("two items").key
    );

    let second = store
        .list_window_page(Some(&today), Some(&today), 2, Some(&first_next))
        .await
        .expect("the second page must succeed");
    let second_ids: Vec<&str> = second.items.iter().map(|item| item.id.as_str()).collect();
    assert_eq!(
        second_ids,
        vec![format!("{today}/ccc"), format!("{today}/ddd")]
    );
    assert!(second.truncated);
    let second_next = second.next.expect("the second page needs a cursor");

    let third = store
        .list_window_page(Some(&today), Some(&today), 2, Some(&second_next))
        .await
        .expect("the third page must succeed");
    assert_eq!(third.items.len(), 1);
    assert_eq!(third.items[0].id, format!("{today}/eee"));
    assert!(!third.truncated);
    assert!(third.next.is_none());
}

#[tokio::test]
async fn list_window_cursor_moves_to_an_older_date() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let now = Utc::now();
    let today = now.format("%Y-%m-%d").to_string();
    let yesterday = (now - Duration::days(1)).format("%Y-%m-%d").to_string();
    let valid = future_metadata(5);
    for key in ["aaa", "bbb"] {
        fake.seed(
            &format!("pastes/{today}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }
    for key in ["aaa", "bbb", "ccc"] {
        fake.seed(
            &format!("pastes/{yesterday}/{key}"),
            b"abcde",
            "text/plain",
            &common::metadata_refs(&valid),
        );
    }

    let first = store
        .list_window_page(Some(&yesterday), Some(&today), 2, None)
        .await
        .expect("the first page must succeed");
    assert_eq!(first.items.len(), 2);
    assert!(first.truncated);
    let next = first.next.expect("the first page needs a cursor");

    let second = store
        .list_window_page(Some(&yesterday), Some(&today), 2, Some(&next))
        .await
        .expect("the second page must succeed");
    let ids: Vec<&str> = second.items.iter().map(|item| item.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![format!("{yesterday}/aaa"), format!("{yesterday}/bbb")]
    );
    assert!(second.truncated);
    let next = second.next.expect("the second page needs a cursor");

    let third = store
        .list_window_page(Some(&yesterday), Some(&today), 2, Some(&next))
        .await
        .expect("the third page must succeed");
    assert_eq!(third.items.len(), 1);
    assert_eq!(third.items[0].id, format!("{yesterday}/ccc"));
    assert!(!third.truncated);
    assert!(third.next.is_none());
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
async fn fetch_text_caps_the_returned_bytes() {
    let fake = FakeS3::start().await;
    let payload = vec![b'a'; MAX_FETCH_TEXT_BYTES as usize + 10];
    let store = fake
        .store_with_caps(MAX_FETCH_TEXT_BYTES + 1024, 1024 * 1024)
        .await;
    let stored = store
        .store_text(&payload, None, Some("text/plain"), None)
        .await
        .expect("the large text paste must store");

    let no_limit = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("a fetch with no limit must succeed");
    assert_eq!(no_limit.returned_bytes, MAX_FETCH_TEXT_BYTES);
    assert!(no_limit.truncated, "bytes must remain after the cap");

    let above_cap = store
        .fetch_text(&stored.id, 0, Some(MAX_FETCH_TEXT_BYTES + 100))
        .await
        .expect("a limit above the cap must succeed");
    assert_eq!(above_cap.returned_bytes, MAX_FETCH_TEXT_BYTES);
    assert!(above_cap.truncated);

    let last = store
        .fetch_text(&stored.id, MAX_FETCH_TEXT_BYTES, None)
        .await
        .expect("the last slice must succeed");
    assert_eq!(last.returned_bytes, 10);
    assert!(!last.truncated, "the last slice must reach the end");
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

#[tokio::test]
async fn set_ttl_moves_base_segments_alias_and_rejects_an_expired_paste() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, Some("text/plain"), Some(Ttl::days(7)))
        .await
        .expect("the base paste must store");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have an alias");
    let appended = store
        .append_text(&stored.id, b"more")
        .await
        .expect("the append must store");
    let segment_key = appended
        .segment_key
        .clone()
        .expect("a non-empty append must write a segment");
    let base_sha = fake
        .object(&stored.key)
        .expect("the base object must exist")
        .metadata
        .get("sha256")
        .cloned();
    let before = store
        .head_metadata(&stored.id)
        .await
        .expect("metadata must load");
    assert!(
        before.sha256.is_none(),
        "a segmented paste has no total hash"
    );

    let updated = store
        .set_ttl(&stored.id, Ttl::parse("1h").expect("one hour must parse"))
        .await
        .expect("set_ttl must succeed");
    let seconds = (updated.expires_at.expect("expiry must be set") - Utc::now()).num_seconds();
    assert!(
        (seconds - 3_600).abs() < 60,
        "the new expiry must be about one hour away, got {seconds}"
    );
    assert_eq!(updated.id, before.id);
    assert_eq!(updated.words, before.words);
    assert_eq!(updated.size, before.size);
    assert!(updated.sha256.is_none());

    let base = fake
        .object(&stored.key)
        .expect("the base object must exist");
    assert_eq!(base.body, b"base");
    assert_eq!(
        base.metadata.get("sha256").cloned(),
        base_sha,
        "set_ttl must not change the stored hash"
    );
    assert_eq!(
        fake.object(&segment_key)
            .expect("the segment must exist")
            .body,
        b"more"
    );

    for key in [
        stored.key.clone(),
        segment_key.clone(),
        format!("words/{words}"),
    ] {
        let object = fake.object(&key).expect("every object must exist");
        let expiry = object
            .metadata
            .get("expires-at")
            .expect("every object must carry expiry");
        let expiry = chrono::DateTime::parse_from_rfc3339(expiry)
            .expect("expiry must be RFC 3339")
            .with_timezone(&Utc);
        let seconds = (expiry - Utc::now()).num_seconds();
        assert!(
            (seconds - 3_600).abs() < 60,
            "{key} must expire in about one hour, got {seconds}"
        );
    }

    let extended = store
        .set_ttl(&words, Ttl::days(30))
        .await
        .expect("set_ttl by alias must succeed");
    let seconds = (extended.expires_at.expect("expiry must be set") - Utc::now()).num_seconds();
    assert!(
        (seconds - 30 * 86_400).abs() < 120,
        "the extension must be about 30 days, got {seconds}"
    );
    let by_alias = store
        .head_metadata(&words)
        .await
        .expect("the alias must still resolve");
    assert_eq!(by_alias.expires_at, extended.expires_at);

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let expired_key = format!("pastes/{today}/expired");
    let expired = common::stored_metadata(
        &(Utc::now() - Duration::days(1)).to_rfc3339(),
        "text/plain",
        3,
        "abc",
    );
    fake.seed(
        &expired_key,
        b"abc",
        "text/plain",
        &common::metadata_refs(&expired),
    );
    let mut keys_before = fake.keys();
    keys_before.sort();
    let error = store
        .set_ttl(&format!("{today}/expired"), Ttl::days(1))
        .await
        .expect_err("an expired paste must fail");
    assert!(matches!(error, StoreError::Expired(_)), "got: {error}");
    let mut keys_after = fake.keys();
    keys_after.sort();
    assert_eq!(
        keys_after, keys_before,
        "an expired paste must write nothing"
    );
}

#[tokio::test]
async fn store_text_clamps_a_long_ttl_to_the_size_curve() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;

    let before = Utc::now();
    let stored = store
        .store_text(b"x", None, None, Some(Ttl::days(3650)))
        .await
        .expect("store_text must succeed against the fake");
    let after = Utc::now();

    assert!(
        stored.expires_at > before + Duration::days(354),
        "a one-byte paste under a 100-byte cap must get about 355 days, not 3650: {}",
        stored.expires_at
    );
    assert!(
        stored.expires_at < after + Duration::days(356),
        "the size curve must clamp the requested 3650 days: {}",
        stored.expires_at
    );
}

#[tokio::test]
async fn default_retention_keeps_the_requested_day_and_subday_ttls() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;

    let before = Utc::now();
    let day = store
        .store_text(b"x", None, None, Some(Ttl::days(3650)))
        .await
        .expect("day TTL must store");
    let after = Utc::now();
    assert!(
        day.expires_at >= before + Duration::days(3650)
            && day.expires_at <= after + Duration::days(3650),
        "the default path must keep the requested day TTL: {}",
        day.expires_at
    );

    let before = Utc::now();
    let subday = store
        .store_text(b"x", None, None, Some(Ttl::parse("1h").unwrap()))
        .await
        .expect("sub-day TTL must store");
    let after = Utc::now();
    assert!(
        subday.expires_at >= before + Duration::hours(1)
            && subday.expires_at <= after + Duration::hours(1),
        "the default path must keep the requested sub-day TTL: {}",
        subday.expires_at
    );
}

#[tokio::test]
async fn retention_keeps_a_client_ttl_shorter_than_the_curve() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;

    let before = Utc::now();
    let stored = store
        .store_text(b"x", None, None, Some(Ttl::parse("1h").unwrap()))
        .await
        .expect("a short TTL must store");
    let after = Utc::now();

    assert!(
        stored.expires_at >= before + Duration::hours(1)
            && stored.expires_at <= after + Duration::hours(1),
        "a client TTL below the curve must be kept: {}",
        stored.expires_at
    );
}

#[tokio::test]
async fn append_past_a_boundary_shortens_the_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;

    let first = store
        .store_text(b"0123456789", None, None, Some(Ttl::days(3650)))
        .await
        .expect("the first paste must store");
    let first_expiry = first.expires_at;
    let created = store
        .head_metadata(&first.id)
        .await
        .expect("metadata must exist")
        .created_at
        .expect("created-at must be stored");

    let second = store
        .append_text(&first.id, b"0123456789012345678901234567890123456789")
        .await
        .expect("the first append must succeed");
    let third = store
        .append_text(&first.id, &[b'x'; 50])
        .await
        .expect("the second append must succeed");

    assert!(
        second.meta.expires_at.unwrap() < first_expiry,
        "the first append must shorten the expiry"
    );
    assert_eq!(
        third.meta.expires_at,
        Some(created + Duration::days(30)),
        "a paste at the cap must get the minimum retention"
    );
    assert_eq!(
        first.words.as_deref().and_then(|words| {
            fake.object(&dayfly::store::alias_key(words))
                .and_then(|object| object.metadata.get("expires-at").cloned())
        }),
        Some(first_expiry.to_rfc3339()),
        "the alias must keep its later expiry"
    );

    let expected = third.meta.expires_at.unwrap().to_rfc3339();
    let base = fake.object(&third.meta.key).expect("the base must exist");
    assert_eq!(
        base.metadata.get("expires-at").map(String::as_str),
        Some(expected.as_str()),
        "the base metadata must carry the new expiry"
    );
    for segment_key in [second.segment_key, third.segment_key]
        .into_iter()
        .flatten()
    {
        let segment = fake.object(&segment_key).expect("the segment must exist");
        assert_eq!(
            segment.metadata.get("expires-at").map(String::as_str),
            Some(expected.as_str()),
            "every segment must carry the new expiry"
        );
    }
}

#[tokio::test]
async fn set_ttl_cannot_extend_past_the_size_curve() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;

    let stored = store
        .store_text(b"0123456789", None, None, Some(Ttl::days(3650)))
        .await
        .expect("the paste must store");
    let created = store
        .head_metadata(&stored.id)
        .await
        .expect("metadata must exist")
        .created_at
        .expect("created-at must be stored");
    store
        .append_text(&stored.id, &[b'x'; 90])
        .await
        .expect("the append must reach the cap");

    let extended = store
        .set_ttl(&stored.id, Ttl::days(3650))
        .await
        .expect("set_ttl must succeed");
    assert_eq!(
        extended.expires_at,
        Some(created + Duration::days(30)),
        "set_ttl must stop at the curve value for the current size"
    );
}

#[tokio::test]
async fn a_paste_above_the_cap_gets_the_minimum_retention() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let key = format!("pastes/{today}/above-cap");
    let metadata = common::stored_metadata(
        &(Utc::now() + Duration::days(1)).to_rfc3339(),
        "application/octet-stream",
        200,
        "abc",
    );
    fake.seed(
        &key,
        &[b'x'; 200],
        "application/octet-stream",
        &common::metadata_refs(&metadata),
    );

    let extended = store
        .set_ttl(&format!("{today}/above-cap"), Ttl::days(3650))
        .await
        .expect("set_ttl must not panic above the cap");
    let created = extended.created_at.expect("created-at must be stored");
    assert_eq!(
        extended.expires_at,
        Some(created + Duration::days(30)),
        "a paste above the cap must get the minimum retention"
    );
}

#[tokio::test]
async fn an_unknown_length_stream_uses_the_request_cap() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;
    let body = stream::iter(vec![Ok(Bytes::from_static(b"x"))]);

    let before = Utc::now();
    let stored = store
        .store_stream(body, Some("x.bin"), None, Some(Ttl::days(3650)), None, 100)
        .await
        .expect("the stream must store");
    let after = Utc::now();

    assert!(
        stored.expires_at >= before + Duration::days(30)
            && stored.expires_at <= after + Duration::days(30),
        "an unknown length must use the request cap, not the final size: {}",
        stored.expires_at
    );
}

#[tokio::test]
async fn store_text_returns_a_paste_token_only_when_enabled() {
    let fake = FakeS3::start().await;

    let off = fake.store().await;
    let stored = off
        .store_text(b"no token", None, None, None)
        .await
        .expect("the default store must succeed");
    assert!(
        stored.paste_token.is_none(),
        "the default must not return a token"
    );

    let on = fake.store_with_paste_tokens().await;
    let stored = on
        .store_text(b"token body", None, None, None)
        .await
        .expect("the token store must succeed");
    let token = stored
        .paste_token
        .expect("the token setting must return a token");
    assert_eq!(token.len(), 32, "a 128-bit token is 32 hex characters");
    assert!(
        token.chars().all(|c| c.is_ascii_hexdigit()),
        "the token must be hex: {token}"
    );

    let object = fake.object(&stored.key).expect("the base must exist");
    assert_eq!(
        object
            .metadata
            .get("paste-token-sha256")
            .map(String::as_str),
        Some(sha256_hex(token.as_bytes()).as_str()),
        "the base must store the SHA-256 of the token"
    );
    assert!(
        on.verify_paste_token(&stored.id, &token).await,
        "the stored hash must accept the token"
    );
    assert!(
        !on.verify_paste_token(&stored.id, &"0".repeat(32)).await,
        "the stored hash must reject a wrong token"
    );
    assert!(
        !on.verify_paste_token(&stored.id, "").await,
        "the stored hash must reject an empty token"
    );
}

#[tokio::test]
async fn a_paste_token_stays_out_of_the_alias_and_segments() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_paste_tokens().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the paste must store");
    let token = stored.paste_token.expect("the token must be present");
    let words = stored.words.as_deref().expect("the alias must exist");
    assert!(
        store.verify_paste_token(words, &token).await,
        "the alias must resolve to the same token check"
    );

    let alias = fake
        .object(&dayfly::store::alias_key(words))
        .expect("the alias must exist");
    assert!(
        !alias.metadata.contains_key("paste-token-sha256"),
        "the alias must not hold the token hash"
    );

    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the append must succeed");
    let segment = fake
        .object(
            appended
                .segment_key
                .as_deref()
                .expect("the segment key must exist"),
        )
        .expect("the segment must exist");
    assert!(
        !segment.metadata.contains_key("paste-token-sha256"),
        "a segment must not hold the token hash"
    );

    let base = fake.object(&stored.key).expect("the base must exist");
    assert_eq!(
        base.metadata.get("paste-token-sha256").map(String::as_str),
        Some(sha256_hex(token.as_bytes()).as_str()),
        "the base keeps the hash after an append"
    );
}

#[tokio::test]
async fn set_ttl_shortening_failure_at_the_base_keeps_the_old_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(7)))
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");
    let alias_key = dayfly::store::alias_key(stored.words.as_deref().expect("the alias"));
    let old_base = object_expiry(&fake, &stored.key);
    let old_segment = object_expiry(&fake, &segment_key);
    let old_alias = object_expiry(&fake, &alias_key);

    fake.fail_copy_key(&stored.key);
    let error = store
        .set_ttl(&stored.id, Ttl::parse("1h").expect("one hour"))
        .await
        .expect_err("the base rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "base");
            assert_eq!(key, stored.key);
        }
        other => panic!("expected a base step error, got {other:?}"),
    }
    assert_eq!(object_expiry(&fake, &stored.key), old_base);
    assert_eq!(object_expiry(&fake, &segment_key), old_segment);
    assert_eq!(object_expiry(&fake, &alias_key), old_alias);
}

#[tokio::test]
async fn set_ttl_shortening_failure_at_a_segment_keeps_the_new_base_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(7)))
        .await
        .expect("the base must store");
    let first = store
        .append_text(&stored.id, b"first")
        .await
        .expect("the first segment must append");
    let second = store
        .append_text(&stored.id, b"second")
        .await
        .expect("the second segment must append");
    let first_key = first.segment_key.expect("the first segment key");
    let second_key = second.segment_key.expect("the second segment key");
    let alias_key = dayfly::store::alias_key(stored.words.as_deref().expect("the alias"));
    let old_base = object_expiry(&fake, &stored.key);
    let old_first = object_expiry(&fake, &first_key);
    let old_second = object_expiry(&fake, &second_key);
    let old_alias = object_expiry(&fake, &alias_key);

    fake.fail_copy_key(&first_key);
    let error = store
        .set_ttl(&stored.id, Ttl::parse("1h").expect("one hour"))
        .await
        .expect_err("the first segment rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "segment");
            assert_eq!(key, first_key);
        }
        other => panic!("expected a segment step error, got {other:?}"),
    }
    assert_ne!(object_expiry(&fake, &stored.key), old_base);
    assert_eq!(object_expiry(&fake, &first_key), old_first);
    assert_ne!(object_expiry(&fake, &second_key), old_second);
    assert_ne!(object_expiry(&fake, &alias_key), old_alias);
    let meta = store
        .head_metadata(&stored.id)
        .await
        .expect("the base metadata must be readable");
    let seconds = (meta.expires_at.expect("the expiry") - Utc::now()).num_seconds();
    assert!(
        (3_500..=3_700).contains(&seconds),
        "the read path must use the new one-hour expiry, got {seconds}"
    );
}

#[tokio::test]
async fn set_ttl_shortening_failure_at_the_alias_keeps_the_new_base_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(7)))
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");
    let alias_key = dayfly::store::alias_key(stored.words.as_deref().expect("the alias"));
    let old_base = object_expiry(&fake, &stored.key);
    let old_segment = object_expiry(&fake, &segment_key);
    let old_alias = object_expiry(&fake, &alias_key);

    fake.fail_copy_key(&alias_key);
    let error = store
        .set_ttl(&stored.id, Ttl::parse("1h").expect("one hour"))
        .await
        .expect_err("the alias rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "alias");
            assert_eq!(key, alias_key);
        }
        other => panic!("expected an alias step error, got {other:?}"),
    }
    assert_ne!(object_expiry(&fake, &stored.key), old_base);
    assert_ne!(object_expiry(&fake, &segment_key), old_segment);
    assert_eq!(object_expiry(&fake, &alias_key), old_alias);
}

#[tokio::test]
async fn set_ttl_extension_failure_at_a_segment_keeps_the_old_base_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(1)))
        .await
        .expect("the base must store");
    let first = store
        .append_text(&stored.id, b"first")
        .await
        .expect("the first segment must append");
    let first_key = first.segment_key.expect("the first segment key");
    let old_base = object_expiry(&fake, &stored.key);

    fake.fail_copy_key(&first_key);
    let error = store
        .set_ttl(&stored.id, Ttl::parse("30d").expect("thirty days"))
        .await
        .expect_err("the segment rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "segment");
            assert_eq!(key, first_key);
        }
        other => panic!("expected a segment step error, got {other:?}"),
    }
    assert_eq!(
        object_expiry(&fake, &stored.key),
        old_base,
        "an extension must keep the old base expiry when a step fails"
    );
    let meta = store
        .head_metadata(&stored.id)
        .await
        .expect("the base metadata must be readable");
    let seconds = (meta.expires_at.expect("the expiry") - Utc::now()).num_seconds();
    assert!(
        seconds < 2 * 86_400,
        "the read path must keep the old one-day expiry, got {seconds}"
    );
}

#[tokio::test]
async fn set_ttl_extension_failure_at_the_base_keeps_the_old_base_expiry() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(1)))
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");
    let old_base = object_expiry(&fake, &stored.key);
    let old_segment = object_expiry(&fake, &segment_key);

    fake.fail_copy_key(&stored.key);
    let error = store
        .set_ttl(&stored.id, Ttl::parse("30d").expect("thirty days"))
        .await
        .expect_err("the base rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "base");
            assert_eq!(key, stored.key);
        }
        other => panic!("expected a base step error, got {other:?}"),
    }
    assert_eq!(object_expiry(&fake, &stored.key), old_base);
    assert_ne!(
        object_expiry(&fake, &segment_key),
        old_segment,
        "the earlier step must have reached the segment"
    );
}

#[tokio::test]
async fn set_ttl_retries_a_transient_segment_failure() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, Some(Ttl::days(1)))
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");
    let alias_key = dayfly::store::alias_key(stored.words.as_deref().expect("the alias"));
    let old_base = object_expiry(&fake, &stored.key);

    fake.fail_copy_once(&segment_key);
    let meta = store
        .set_ttl(&stored.id, Ttl::parse("30d").expect("thirty days"))
        .await
        .expect("the retry must finish the update");
    let new_base = object_expiry(&fake, &stored.key);
    assert_ne!(new_base, old_base);
    assert_eq!(object_expiry(&fake, &segment_key), new_base);
    assert_eq!(object_expiry(&fake, &alias_key), new_base);
    assert_eq!(meta.expires_at.expect("the expiry").to_rfc3339(), new_base);
}

#[tokio::test]
async fn append_retention_failure_at_the_base_writes_no_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;
    let stored = store
        .store_text(b"0123456789", None, None, Some(Ttl::days(3650)))
        .await
        .expect("the base must store");
    let old_base = object_expiry(&fake, &stored.key);

    fake.fail_copy_key(&stored.key);
    let error = store
        .append_text(&stored.id, &[b'a'; 40])
        .await
        .expect_err("the base rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "base");
            assert_eq!(key, stored.key);
        }
        other => panic!("expected a base step error, got {other:?}"),
    }
    assert!(
        segment_keys(&fake, &stored.key).is_empty(),
        "a failed base rewrite must write no new segment"
    );
    assert_eq!(object_expiry(&fake, &stored.key), old_base);

    fake.clear_failures();
    let appended = store
        .append_text(&stored.id, &[b'a'; 40])
        .await
        .expect("the retry must append once");
    assert!(appended.segment_key.is_some());
    assert_eq!(segment_keys(&fake, &stored.key).len(), 1);
    assert_ne!(object_expiry(&fake, &stored.key), old_base);
}

#[tokio::test]
async fn append_retention_failure_at_an_earlier_segment_writes_no_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;
    let stored = store
        .store_text(b"x", None, None, Some(Ttl::days(3650)))
        .await
        .expect("the base must store");
    let first = store
        .append_text(&stored.id, &[b'a'; 40])
        .await
        .expect("the first segment must append");
    let first_key = first.segment_key.expect("the first segment key");
    let old_base = object_expiry(&fake, &stored.key);
    let old_first = object_expiry(&fake, &first_key);

    fake.fail_copy_key(&first_key);
    let error = store
        .append_text(&stored.id, &[b'b'; 50])
        .await
        .expect_err("the earlier segment rewrite must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "segment");
            assert_eq!(key, first_key);
        }
        other => panic!("expected a segment step error, got {other:?}"),
    }
    assert_eq!(
        segment_keys(&fake, &stored.key).len(),
        1,
        "a failed earlier-segment rewrite must write no new segment"
    );
    assert_ne!(object_expiry(&fake, &stored.key), old_base);
    assert_eq!(object_expiry(&fake, &first_key), old_first);

    fake.clear_failures();
    let second = store
        .append_text(&stored.id, &[b'b'; 50])
        .await
        .expect("the retry must append once");
    assert!(second.segment_key.is_some());
    assert_eq!(segment_keys(&fake, &stored.key).len(), 2);
}

#[tokio::test]
async fn append_retention_failure_at_the_new_segment_writes_no_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store_with_retention(100, 30, 365).await;
    let stored = store
        .store_text(b"0123456789", None, None, Some(Ttl::days(3650)))
        .await
        .expect("the base must store");
    let first = store
        .append_text(&stored.id, &[b'a'; 40])
        .await
        .expect("the first segment must append");
    let first_key = first.segment_key.expect("the first segment key");
    let old_base = object_expiry(&fake, &stored.key);
    let old_first = object_expiry(&fake, &first_key);

    fake.fail_put_prefix(&format!("{}/", stored.key));
    let error = store
        .append_text(&stored.id, &[b'b'; 50])
        .await
        .expect_err("the new segment write must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "new segment");
            assert!(key.starts_with(&format!("{}/", stored.key)));
        }
        other => panic!("expected a new-segment step error, got {other:?}"),
    }
    assert_eq!(
        segment_keys(&fake, &stored.key).len(),
        1,
        "a failed new-segment write must not leave a segment"
    );
    assert_ne!(object_expiry(&fake, &stored.key), old_base);
    assert_ne!(object_expiry(&fake, &first_key), old_first);

    fake.clear_failures();
    let second = store
        .append_text(&stored.id, &[b'b'; 50])
        .await
        .expect("the retry must append once");
    assert!(second.segment_key.is_some());
    assert_eq!(segment_keys(&fake, &stored.key).len(), 2);
}

#[tokio::test]
async fn delete_racing_an_append_removes_the_late_segment() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the base must store");
    let base_key = stored.key.clone();
    let id = stored.id.clone();

    fake.hold(Gate::PutObject);
    let append = tokio::spawn({
        let store = store.clone();
        let id = id.clone();
        async move { store.append_text(&id, b"late segment").await }
    });
    fake.wait_until_blocked(Gate::PutObject).await;
    store.delete(&id).await.expect("the delete must succeed");
    fake.release();

    let result = append.await.expect("the append task must not panic");
    assert!(
        matches!(result, Err(StoreError::NotFound(_))),
        "an append that loses the base must return NotFound, got {result:?}"
    );
    assert!(fake.object(&base_key).is_none());
    assert!(
        segment_keys(&fake, &base_key).is_empty(),
        "a late segment must not survive the delete"
    );
}

#[tokio::test]
async fn delete_relists_segments_that_arrive_before_the_base_delete() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the base must store");
    let base_key = stored.key.clone();
    let id = stored.id.clone();

    fake.hold(Gate::DeleteObject);
    let delete = tokio::spawn({
        let store = store.clone();
        let id = id.clone();
        async move { store.delete(&id).await }
    });
    fake.wait_until_blocked(Gate::DeleteObject).await;

    let appended = store
        .append_text(&id, b"late segment")
        .await
        .expect("the append must succeed while the base is still there");
    let segment_key = appended.segment_key.expect("the segment key");
    assert!(fake.object(&segment_key).is_some());

    fake.release();
    delete
        .await
        .expect("the delete task must not panic")
        .expect("the delete must succeed");
    assert!(fake.object(&base_key).is_none());
    assert!(
        fake.object(&segment_key).is_none(),
        "the second segment listing must remove the late segment"
    );
}

#[tokio::test]
async fn a_failed_segment_delete_returns_an_error_and_a_later_full_id_delete_cleans_it() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");

    fake.fail_delete_key(&segment_key);
    let error = store
        .delete(&stored.id)
        .await
        .expect_err("the segment delete must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "segment delete");
            assert_eq!(key, segment_key);
        }
        other => panic!("expected a segment-delete step error, got {other:?}"),
    }
    assert!(
        fake.object(&stored.key).is_none(),
        "the base is deleted before the segment failure"
    );
    assert!(fake.object(&segment_key).is_some());

    fake.clear_failures();
    store
        .delete(&stored.id)
        .await
        .expect("a later full-id delete must clean the segment");
    assert!(fake.object(&segment_key).is_none());
}

#[tokio::test]
async fn a_failed_base_delete_reports_the_step_and_keeps_the_paste() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");

    fake.fail_delete_key(&stored.key);
    let error = store
        .delete(&stored.id)
        .await
        .expect_err("the base delete must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "base delete");
            assert_eq!(key, stored.key);
        }
        other => panic!("expected a base-delete step error, got {other:?}"),
    }
    assert!(fake.object(&stored.key).is_some());
    assert!(fake.object(&segment_key).is_some());

    fake.clear_failures();
    store
        .delete(&stored.id)
        .await
        .expect("the retry must delete the paste");
    assert!(fake.object(&stored.key).is_none());
    assert!(fake.object(&segment_key).is_none());
}

#[tokio::test]
async fn a_failed_segment_list_before_delete_keeps_the_paste() {
    let fake = FakeS3::start().await;
    let store = fake.store().await;
    let stored = store
        .store_text(b"base", None, None, None)
        .await
        .expect("the base must store");
    let appended = store
        .append_text(&stored.id, b"segment")
        .await
        .expect("the segment must append");
    let segment_key = appended.segment_key.expect("the segment key");

    fake.fail_list_prefix(
        &format!("{}/", stored.key),
        StatusCode::INTERNAL_SERVER_ERROR,
        "InternalError",
    );
    let error = store
        .delete(&stored.id)
        .await
        .expect_err("the segment list must fail");
    match error {
        StoreError::PartialUpdate { step, key } => {
            assert_eq!(step, "segment list");
            assert_eq!(key, stored.key);
        }
        other => panic!("expected a segment-list step error, got {other:?}"),
    }
    assert!(fake.object(&stored.key).is_some());
    assert!(fake.object(&segment_key).is_some());

    fake.clear_failures();
    store
        .delete(&stored.id)
        .await
        .expect("the retry must delete the paste");
    assert!(fake.object(&stored.key).is_none());
    assert!(fake.object(&segment_key).is_none());
}

#[tokio::test]
async fn configured_ttl_range_rejects_out_of_range_values() {
    let fake = FakeS3::start().await;
    let mut config = fake.config();
    config.min_ttl_days = 1;
    config.max_ttl_days = 3;
    config.ttl_choices = vec![1, 2, 3];
    let store = Store::from_config(config)
        .await
        .expect("fake S3 config must build without network access");

    let short = store
        .store_text(
            b"short",
            None,
            None,
            Some(Ttl::parse("6h").expect("6h must parse")),
        )
        .await;
    match short {
        Err(StoreError::TtlOutOfRange(message)) => {
            assert!(message.contains("minimum TTL is 1 day"), "got: {message}");
        }
        other => panic!("expected a minimum TTL error, got {other:?}"),
    }

    let long = store
        .store_text(
            b"long",
            None,
            None,
            Some(Ttl::parse("4d").expect("4d must parse")),
        )
        .await;
    match long {
        Err(StoreError::TtlOutOfRange(message)) => {
            assert!(message.contains("maximum TTL is 3 days"), "got: {message}");
        }
        other => panic!("expected a maximum TTL error, got {other:?}"),
    }

    store
        .store_text(
            b"ok",
            None,
            None,
            Some(Ttl::parse("3d").expect("3d must parse")),
        )
        .await
        .expect("a TTL at the maximum must be accepted");
}
