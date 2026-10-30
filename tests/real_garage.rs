//! Optional tests against a real Garage or S3-compatible endpoint.
//!
//! These tests are ignored by default. Run them only with a disposable test
//! bucket and explicit opt-in:
//!
//! ```sh
//! DAYFLY_TEST_S3_ENDPOINT=https://garage.example.invalid:3900 \
//! DAYFLY_TEST_S3_BUCKET=dayfly-test-disposable \
//! DAYFLY_TEST_S3_ACCESS_KEY_ID=TEST-KEY \
//! DAYFLY_TEST_S3_SECRET_ACCESS_KEY=TEST-SECRET \
//! cargo test --test real_garage -- --ignored --test-threads=1
//! ```
//!
//! The default `cargo test` run never reaches a network service.

use std::path::Path;

use chrono::Utc;
use dayfly::config::Config;
use dayfly::store::{Store, StoreError};

fn test_config() -> Config {
    let endpoint = std::env::var("DAYFLY_TEST_S3_ENDPOINT")
        .unwrap_or_else(|_| panic!("set DAYFLY_TEST_S3_ENDPOINT to the disposable test endpoint"));
    let bucket = std::env::var("DAYFLY_TEST_S3_BUCKET")
        .unwrap_or_else(|_| panic!("set DAYFLY_TEST_S3_BUCKET to a disposable test bucket"));
    let access_key_id = std::env::var("DAYFLY_TEST_S3_ACCESS_KEY_ID")
        .unwrap_or_else(|_| panic!("set DAYFLY_TEST_S3_ACCESS_KEY_ID"));
    let secret_access_key = std::env::var("DAYFLY_TEST_S3_SECRET_ACCESS_KEY")
        .unwrap_or_else(|_| panic!("set DAYFLY_TEST_S3_SECRET_ACCESS_KEY"));
    Config {
        endpoint,
        bucket,
        region: std::env::var("DAYFLY_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_id,
        secret_access_key,
        max_text_bytes: 10 * 1024 * 1024,
        max_file_bytes: 1024 * 1024 * 1024,
        default_ttl_days: 1,
        http_bind: "127.0.0.1:0".to_string(),
        http_bearer_token: None,
    }
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dayfly-real-garage-{}-{}-{name}",
        std::process::id(),
        rand::random::<u64>()
    ))
}

#[tokio::test]
#[ignore = "requires a disposable real Garage/S3 bucket and explicit opt-in"]
async fn real_store_fetch_list_and_delete_text() {
    let store = Store::from_config(test_config())
        .await
        .expect("real S3 configuration must build");
    let marker = format!("dayfly real test {}", Utc::now().to_rfc3339());
    let stored = store
        .store_text(marker.as_bytes(), Some("real-test.txt"), None, Some(1))
        .await
        .expect("real store_text must succeed");
    assert_eq!(stored.size, marker.len() as u64);

    let meta = store
        .head_metadata(&stored.id)
        .await
        .expect("real head_metadata must succeed");
    assert_eq!(meta.size, marker.len() as u64);
    assert!(!meta.expired);

    let text = store
        .fetch_text(&stored.id, 0, None)
        .await
        .expect("real fetch_text must succeed");
    assert_eq!(text.text, marker);

    let range = store
        .fetch_range_by_key(&stored.key, Some((0, 6)))
        .await
        .expect("real range fetch must succeed");
    assert_eq!(range, marker.as_bytes()[..7].to_vec());

    let dest = temp_path("real-download.txt");
    store
        .download_file(&stored.id, &dest, None)
        .await
        .expect("real download must succeed");
    assert_eq!(
        std::fs::read(&dest).expect("downloaded file must be readable"),
        marker.as_bytes()
    );
    let _ = std::fs::remove_file(&dest);

    let date = Utc::now().format("%Y-%m-%d").to_string();
    let listed = store
        .list_recent(Some(&date), 1000)
        .await
        .expect("real listing must succeed");
    assert!(
        listed.iter().any(|item| item.id == stored.id),
        "the stored paste must appear in the listing"
    );

    store
        .delete(&stored.id)
        .await
        .expect("real delete must succeed");
    let error = store
        .head_metadata(&stored.id)
        .await
        .expect_err("a deleted paste must be gone");
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[tokio::test]
#[ignore = "requires a disposable real Garage/S3 bucket and explicit opt-in"]
async fn real_multipart_file_round_trip() {
    let store = Store::from_config(test_config())
        .await
        .expect("real S3 configuration must build");
    let marker: Vec<u8> = (0..9 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let source = temp_path("real-source.bin");
    std::fs::write(&source, &marker).expect("temporary source file must be writable");

    let stored = store
        .store_file_path(
            Path::new(&source),
            Some("real-multipart.bin"),
            None,
            Some(1),
        )
        .await
        .expect("real multipart store_file_path must succeed");
    assert_eq!(stored.size, marker.len() as u64);

    let dest = temp_path("real-download.bin");
    let meta = store
        .download_file(&stored.id, &dest, None)
        .await
        .expect("real multipart download must succeed");
    assert_eq!(meta.size, marker.len() as u64);
    assert_eq!(
        std::fs::read(&dest).expect("downloaded file must be readable"),
        marker
    );

    store
        .delete(&stored.id)
        .await
        .expect("real delete must succeed");
    let _ = std::fs::remove_file(source);
    let _ = std::fs::remove_file(dest);
}
