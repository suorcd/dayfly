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

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Region};
use aws_sdk_s3::Client;
use chrono::Utc;
use dayfly::config::Config;
use dayfly::store::{Store, StoreError, Ttl};

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
        max_paste_bytes: 1024 * 1024 * 1024,
        multipart_abort_after_seconds: 3_600,
        search_max_bytes: 64 * 1024 * 1024,
        alias_words: 3,
        default_ttl_days: 1,
        min_ttl_days: 0,
        max_ttl_days: 3650,
        ttl_choices: vec![1, 3, 7, 14, 30, 90],
        list_default_days: 2,
        retention_by_size: false,
        retention_min_days: 30,
        retention_max_days: 365,
        http_bind: "127.0.0.1:0".to_string(),
        log_format: dayfly::config::LogFormat::Text,
        metrics_bind: None,
        metrics_allowed_networks: Vec::new(),
        trusted_proxies: Vec::new(),
        show_words: true,
        show_internal_paths: false,
        inline_pdf: true,
        view_line_numbers: false,
        allow_unauthenticated: false,
        write_requires_token: false,
        list_requires_token: false,
        list_enabled: true,
        paste_tokens: false,
        expose_creator_addr: false,
        rate_limit_per_minute: 0,
        http_bearer_token: None,
        mcp_local_root: None,
        favicon: None,
        robots_txt: None,
        security_txt: None,
        site_name: None,
        contact: None,
        aup_url: None,
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
        .store_text(
            marker.as_bytes(),
            Some("real-test.txt"),
            None,
            Some(Ttl::days(1)),
        )
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
            Some(Ttl::days(1)),
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

#[tokio::test]
#[ignore = "requires a disposable real Garage/S3 bucket and explicit opt-in"]
async fn real_list_and_abort_multipart_upload() {
    let config = test_config();
    let store = Store::from_config(config.clone())
        .await
        .expect("real S3 configuration must build");
    let credentials = Credentials::new(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        None,
        None,
        "dayfly-real-test",
    );
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(config.endpoint.clone())
        .region(Region::new(config.region.clone()))
        .credentials_provider(credentials)
        .load()
        .await;
    let client = Client::from_conf(
        S3ConfigBuilder::from(&sdk_config)
            .force_path_style(true)
            .build(),
    );
    let key = format!(
        "pastes/2020-01-01/dayfly-real-abort-{}",
        rand::random::<u64>()
    );
    let upload = client
        .create_multipart_upload()
        .bucket(&config.bucket)
        .key(&key)
        .send()
        .await
        .expect("the incomplete upload must start");
    let upload_id = upload
        .upload_id()
        .expect("the upload must have an id")
        .to_string();

    let aborted = store
        .abort_stale_multipart_uploads(std::time::Duration::from_secs(0))
        .await
        .expect("the sweep must reach the real endpoint");
    assert!(aborted >= 1, "the incomplete upload must be aborted");

    let listed = client
        .list_multipart_uploads()
        .bucket(&config.bucket)
        .prefix(&key)
        .send()
        .await
        .expect("the list must reach the real endpoint");
    assert!(
        !listed
            .uploads()
            .iter()
            .any(|upload| upload.upload_id() == Some(upload_id.as_str())),
        "the aborted upload must not be listed"
    );
}

#[tokio::test]
#[ignore = "requires a disposable real Garage/S3 bucket and explicit opt-in"]
async fn real_cleanup_deletes_expired_paste_and_alias() {
    let store = Store::from_config(test_config())
        .await
        .expect("real S3 configuration must build");
    let stored = store
        .store_text(
            b"real cleanup",
            None,
            Some("text/plain"),
            Some(Ttl::parse("5s").expect("5s must parse")),
        )
        .await
        .expect("real store_text must succeed");
    let words = stored
        .words
        .clone()
        .expect("a new paste must have a word alias");
    store
        .append_text(&stored.id, b" tail")
        .await
        .expect("real append must succeed");
    tokio::time::sleep(std::time::Duration::from_millis(5_500)).await;

    let report = store
        .cleanup(dayfly::store::CleanupOptions {
            dry_run: false,
            multipart_older_than: std::time::Duration::from_secs(3_600),
        })
        .await
        .expect("real cleanup must succeed");

    assert!(
        report.pastes_deleted.contains(&stored.key),
        "the expired paste must be deleted: {report:?}"
    );
    assert_eq!(report.failed(), 0, "the real cleanup must not fail");
    let error = store
        .head_metadata(&stored.id)
        .await
        .expect_err("the expired paste must be gone");
    assert!(matches!(error, StoreError::NotFound(_)));
    let alias_error = store
        .head_metadata(&words)
        .await
        .expect_err("the alias must be gone");
    assert!(matches!(alias_error, StoreError::NotFound(_)));
}
