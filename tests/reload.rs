//! Reload tests for issue #135.
//!
//! The reload path is a library function, so these tests call it directly.
//! No test depends on watcher timing.

mod common;

use axum::body::Body;
use axum::http::header::AUTHORIZATION;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use dayfly::http::{app_router, AppState};
use dayfly::reload::{reload_from_file, reload_with_config};
use dayfly::store::Store;
use tower::ServiceExt;

use common::FakeS3;

const OLD_TOKEN: &str = "old-token";
const NEW_TOKEN: &str = "new-token";

async fn state_with_token(fake: &FakeS3, token: Option<&str>) -> AppState {
    let mut config = fake.config();
    config.http_bearer_token = token.map(str::to_string);
    AppState::new(
        Store::from_config(config)
            .await
            .expect("test S3 config must build without network access"),
    )
}

fn signed_with(fake: &FakeS3, access_key: &str) -> bool {
    fake.requests().iter().any(|request| {
        request
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .is_some_and(|(_, value)| value.contains(access_key))
    })
}

fn request(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .expect("request must build")
}

#[cfg(unix)]
#[tokio::test]
async fn reload_from_file_warns_on_a_world_readable_env_file() {
    use std::os::unix::fs::PermissionsExt;

    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some(OLD_TOKEN)).await;
    let path = common::temp_path("reload-env-mode");
    std::fs::write(
        &path,
        "S3_ENDPOINT=http://127.0.0.1:1\n\
         S3_BUCKET=reload-test\n\
         S3_REGION=us-east-1\n\
         S3_ACCESS_KEY_ID=test-access\n\
         S3_SECRET_ACCESS_KEY=test-secret\n",
    )
    .expect("the env file must write");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
        .expect("the env file mode must set");

    let report = reload_from_file(&path, &state, "127.0.0.1:8080")
        .await
        .expect("the env file must reload");
    let _ = std::fs::remove_file(&path);
    assert!(
        report
            .warnings
            .iter()
            .any(|warning| warning.contains("mode 0644") && warning.contains("Set mode 0600")),
        "the reload must warn about the file mode: {:?}",
        report.warnings
    );
}

#[tokio::test]
async fn reload_swaps_credentials_token_caps_ttl_and_show_words() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some(OLD_TOKEN)).await;
    let app = app_router(state.clone());

    let mut new_config = fake.config();
    new_config.access_key_id = "rotated-access-key".to_string();
    new_config.secret_access_key = "rotated-secret-key".to_string();
    new_config.http_bearer_token = Some(NEW_TOKEN.to_string());
    new_config.max_text_bytes = 8;
    new_config.default_ttl_days = 2;
    new_config.retention_by_size = true;
    new_config.retention_min_days = 10;
    new_config.retention_max_days = 90;
    new_config.paste_tokens = true;
    new_config.show_words = true;
    new_config.list_requires_token = true;
    new_config.rate_limit_per_minute = 7;
    new_config.show_internal_paths = true;
    new_config.view_line_numbers = true;
    new_config.list_default_days = 5;
    new_config.favicon = Some(dayfly::config::Favicon {
        bytes: std::sync::Arc::from(&b"icon"[..]),
        content_type: "image/x-icon",
    });
    new_config.robots_txt = Some(dayfly::config::TextAsset {
        bytes: std::sync::Arc::from(&b"User-agent: *\nDisallow: /\n"[..]),
    });
    new_config.security_txt = Some(dayfly::config::TextAsset {
        bytes: std::sync::Arc::from(&b"Contact: https://example.invalid/\n"[..]),
    });

    let report = reload_with_config(&state, new_config, "127.0.0.1:8080")
        .await
        .expect("a valid config must reload");
    for field in [
        "S3 credentials",
        "DAYFLY_HTTP_BEARER_TOKEN",
        "DAYFLY_MAX_TEXT_BYTES",
        "DAYFLY_DEFAULT_TTL_DAYS",
        "DAYFLY_RETENTION_BY_SIZE",
        "DAYFLY_RETENTION_MIN_DAYS",
        "DAYFLY_RETENTION_MAX_DAYS",
        "DAYFLY_PASTE_TOKENS",
        "DAYFLY_SHOW_WORDS",
        "DAYFLY_LIST_REQUIRES_TOKEN",
        "DAYFLY_RATE_LIMIT_PER_MINUTE",
        "DAYFLY_SHOW_INTERNAL_PATHS",
        "DAYFLY_VIEW_LINE_NUMBERS",
        "DAYFLY_LIST_DEFAULT_DAYS",
        "DAYFLY_FAVICON_PATH",
        "DAYFLY_ROBOTS_TXT_PATH",
        "DAYFLY_SECURITY_TXT_PATH",
    ] {
        assert!(
            report.changed.contains(&field),
            "the report must name {field}: {:?}",
            report.changed
        );
    }

    let old = app
        .clone()
        .oneshot(request("/v1/pastes", OLD_TOKEN))
        .await
        .expect("router call must complete");
    assert_eq!(old.status(), StatusCode::UNAUTHORIZED);

    let new = app
        .clone()
        .oneshot(request("/v1/pastes", NEW_TOKEN))
        .await
        .expect("router call must complete");
    assert_eq!(
        new.status(),
        StatusCode::OK,
        "the new token must pass the old router"
    );
    assert!(
        signed_with(&fake, "rotated-access-key"),
        "the next S3 request must use the rotated access key"
    );

    let body = vec![b'x'; 9];
    let over_cap = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/upload")
                .header(AUTHORIZATION, format!("Bearer {NEW_TOKEN}"))
                .header("content-length", body.len())
                .body(Body::from(body))
                .expect("request must build"),
        )
        .await
        .expect("router call must complete");
    assert_eq!(
        over_cap.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "the reloaded text cap must apply to the old router"
    );

    let stored = state
        .store()
        .store_text(b"ttl", None, None, None)
        .await
        .expect("the store must read the reloaded TTL");
    let remaining = stored.expires_at - Utc::now();
    assert!(
        remaining > chrono::Duration::days(2) - chrono::Duration::minutes(1)
            && remaining < chrono::Duration::days(2) + chrono::Duration::minutes(1),
        "the reloaded default TTL must be about two days, got {remaining}"
    );
    assert!(state.store().config.show_words);
}

#[tokio::test]
async fn reload_from_file_applies_asset_changes_and_names_them() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some(OLD_TOKEN)).await;
    let app = app_router(state.clone());

    let dir = common::temp_path("reload-assets");
    std::fs::create_dir_all(&dir).expect("the asset directory must be creatable");
    std::fs::write(dir.join("favicon.ico"), b"old-icon").expect("the favicon must write");
    std::fs::write(dir.join("robots.txt"), b"User-agent: *\nDisallow: /\n")
        .expect("the robots file must write");
    std::fs::write(dir.join("security.txt"), b"Contact: old@example.invalid\n")
        .expect("the security file must write");
    let env_file = dir.join("dayfly.env");
    std::fs::write(
        &env_file,
        format!(
            "S3_ENDPOINT={}\n\
             S3_BUCKET={}\n\
             S3_REGION=us-east-1\n\
             S3_ACCESS_KEY_ID=test-access-key\n\
             S3_SECRET_ACCESS_KEY=test-secret-key\n\
             DAYFLY_HTTP_BIND=127.0.0.1:0\n\
             DAYFLY_HTTP_BEARER_TOKEN={OLD_TOKEN}\n\
             DAYFLY_FAVICON_PATH=favicon.ico\n\
             DAYFLY_ROBOTS_TXT_PATH=robots.txt\n\
             DAYFLY_SECURITY_TXT_PATH=security.txt\n",
            fake.endpoint, fake.bucket
        ),
    )
    .expect("the env file must write");

    // The first reload proves that the relative asset paths resolve against
    // the env file directory, as they do at startup.
    reload_from_file(&env_file, &state, "127.0.0.1:8080")
        .await
        .expect("the relative asset paths must reload");

    async fn get(app: &axum::Router, uri: &str) -> Vec<u8> {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .expect("router call must complete");
        assert_eq!(response.status(), StatusCode::OK, "GET {uri}");
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body must read")
            .to_vec()
    }

    assert_eq!(get(&app, "/favicon.ico").await.as_slice(), b"old-icon");
    assert_eq!(
        get(&app, "/robots.txt").await.as_slice(),
        b"User-agent: *\nDisallow: /\n"
    );
    assert_eq!(
        get(&app, "/.well-known/security.txt").await.as_slice(),
        b"Contact: old@example.invalid\n"
    );

    std::fs::write(dir.join("favicon.ico"), b"new-icon").expect("the favicon must write");
    std::fs::write(dir.join("robots.txt"), b"User-agent: *\nDisallow: /new\n")
        .expect("the robots file must write");
    std::fs::write(dir.join("security.txt"), b"Contact: new@example.invalid\n")
        .expect("the security file must write");

    let report = reload_from_file(&env_file, &state, "127.0.0.1:8080")
        .await
        .expect("a changed asset must reload");
    for field in [
        "DAYFLY_FAVICON_PATH",
        "DAYFLY_ROBOTS_TXT_PATH",
        "DAYFLY_SECURITY_TXT_PATH",
    ] {
        assert!(
            report.changed.contains(&field),
            "the report must name {field}: {:?}",
            report.changed
        );
    }

    assert_eq!(get(&app, "/favicon.ico").await.as_slice(), b"new-icon");
    assert_eq!(
        get(&app, "/robots.txt").await.as_slice(),
        b"User-agent: *\nDisallow: /new\n"
    );
    assert_eq!(
        get(&app, "/.well-known/security.txt").await.as_slice(),
        b"Contact: new@example.invalid\n"
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn reload_applies_write_auth_and_reports_the_setting() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some("secret")).await;

    let mut new_config = fake.config();
    new_config.http_bearer_token = Some("secret".to_string());
    new_config.write_requires_token = true;
    let report = reload_with_config(&state, new_config, "127.0.0.1:8080")
        .await
        .expect("a write auth config must reload");
    assert!(
        report.changed.contains(&"DAYFLY_WRITE_REQUIRES_TOKEN"),
        "the report must name the setting: {:?}",
        report.changed
    );
    assert!(state.store().config.write_requires_token);

    let app = app_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/pastes")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await
        .expect("router call must complete");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the reloaded write mode must leave the list anonymous"
    );
}

#[tokio::test]
async fn reload_rejects_a_malformed_file_and_keeps_the_running_config() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some(OLD_TOKEN)).await;
    let app = app_router(state.clone());

    // `Config::from_env_file` reads the process environment first. Clear
    // the names this test supplies so the file is authoritative.
    for name in [
        "S3_ENDPOINT",
        "S3_BUCKET",
        "S3_REGION",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_ACCESS_KEY_ID_FILE",
        "S3_SECRET_ACCESS_KEY_FILE",
        "DAYFLY_MAX_TEXT_BYTES",
        "DAYFLY_MAX_FILE_BYTES",
        "DAYFLY_MAX_PASTE_BYTES",
        "DAYFLY_DEFAULT_TTL_DAYS",
        "DAYFLY_HTTP_BIND",
        "DAYFLY_SHOW_WORDS",
        "DAYFLY_ALLOW_UNAUTHENTICATED",
        "DAYFLY_HTTP_BEARER_TOKEN",
        "DAYFLY_HTTP_BEARER_TOKEN_FILE",
        "DAYFLY_MCP_LOCAL_ROOT",
    ] {
        std::env::remove_var(name);
    }
    let path = std::env::temp_dir().join(format!(
        "dayfly-reload-malformed-{}.env",
        std::process::id()
    ));
    std::fs::write(
        &path,
        format!(
            "S3_ENDPOINT={}\nS3_BUCKET={}\nS3_ACCESS_KEY_ID=key\nS3_SECRET_ACCESS_KEY=secret\nDAYFLY_MAX_PASTE_BYTES=not-a-number\n",
            fake.endpoint, fake.bucket
        ),
    )
    .expect("the env file must be writable");

    let error = dayfly::reload::reload_from_file(&path, &state, "127.0.0.1:8080")
        .await
        .expect_err("a malformed file must fail the reload");
    assert!(
        error.contains("DAYFLY_MAX_PASTE_BYTES"),
        "the error must name the bad field: {error}"
    );
    assert_eq!(state.store().config.endpoint, fake.endpoint);
    assert_eq!(
        state.store().config.http_bearer_token.as_deref(),
        Some(OLD_TOKEN)
    );

    let old = app
        .oneshot(request("/v1/pastes", OLD_TOKEN))
        .await
        .expect("router call must complete");
    assert_eq!(
        old.status(),
        StatusCode::OK,
        "the old token must keep serving after a failed reload"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn reload_ignores_the_bind_and_other_startup_only_settings() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, None).await;
    let old = state.store().config.clone();

    let mut new_config = fake.config();
    new_config.http_bind = "0.0.0.0:9999".to_string();
    new_config.allow_unauthenticated = !old.allow_unauthenticated;
    new_config.mcp_local_root = Some(std::env::temp_dir());

    let report = reload_with_config(&state, new_config, "127.0.0.1:8080")
        .await
        .expect("a bind change must not fail the reload");
    for field in [
        "DAYFLY_HTTP_BIND",
        "DAYFLY_ALLOW_UNAUTHENTICATED",
        "DAYFLY_MCP_LOCAL_ROOT",
    ] {
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.contains(field) && warning.contains("restart")),
            "the warning must name {field} and a restart: {:?}",
            report.warnings
        );
    }
    assert_eq!(state.store().config.http_bind, old.http_bind);
    assert_eq!(
        state.store().config.allow_unauthenticated,
        old.allow_unauthenticated
    );
    assert_eq!(state.store().config.mcp_local_root, old.mcp_local_root);
}

#[tokio::test]
async fn reload_refuses_to_remove_the_token_on_a_reachable_bind() {
    let fake = FakeS3::start().await;
    let state = state_with_token(&fake, Some(OLD_TOKEN)).await;

    let mut new_config = fake.config();
    new_config.http_bearer_token = None;

    let error = reload_with_config(&state, new_config, "0.0.0.0:8080")
        .await
        .expect_err("a reachable bind must keep a token");
    assert!(
        error.contains("DAYFLY_ALLOW_UNAUTHENTICATED"),
        "the error must name the opt-in: {error}"
    );
    assert_eq!(
        state.store().config.http_bearer_token.as_deref(),
        Some(OLD_TOKEN)
    );
}
