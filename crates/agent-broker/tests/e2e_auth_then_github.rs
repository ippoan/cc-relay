//! End-to-end: refresh against an auth-worker stand-in, then call
//! `api.github.com` with the refreshed `github_token`.
//!
//! Two `wiremock` servers run side by side. The first impersonates
//! auth.ippoan.org (`/mcp/token`, `/mcp/introspect`). The second
//! impersonates api.github.com (`/repos/.../issues/...`). The
//! `GitHubBroker` is built with a `TokenManager::from_cache` pointing
//! at the auth-worker mock, then a single `list_agents()` call drives
//! both round-trips.
//!
//! The assertion that ties it together: the `Authorization` header on
//! the GitHub request is `Bearer gho_refreshed`, not the initial
//! `gho_initial` from the on-disk cache — proving `ensure_fresh`
//! refreshed before the call.

use std::sync::Arc;

use agent_broker::auth::AuthConfig;
use agent_broker::token_cache::{self, TokenSet};
use agent_broker::{Broker, GitHubBroker, TokenManager};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn tmp_token_path() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cc-relay-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("token")
}

#[tokio::test]
async fn refresh_then_github_request_uses_new_token() {
    let auth_server = MockServer::start().await;
    let gh_server = MockServer::start().await;

    // ----- auth-worker stand-in -----
    Mock::given(method("POST"))
        .and(path("/mcp/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "jwt2.body.sig",
            "refresh_token": "rt-2",
            "scope": "mcp.read mcp.write",
            "expires_in": 3600,
        })))
        .expect(1)
        .mount(&auth_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/mcp/introspect"))
        .and(header("authorization", "shh-internal-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": true,
            "github_login": "octocat",
            "github_token": "gho_refreshed",
            "exp": now() + 3600,
        })))
        .expect(1)
        .mount(&auth_server)
        .await;

    // ----- api.github.com stand-in -----
    // Assert the Authorization header is the *refreshed* token.
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/issues/42"))
        .and(header("authorization", "Bearer gho_refreshed"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "body": "{\"v\":1,\"agents\":[{\"agent_id\":\"octocat\",\"joined_at\":1}],\"plan\":[]}",
        })))
        .expect(1)
        .mount(&gh_server)
        .await;

    // ----- cached TokenSet: expired-soon so ensure_fresh fires -----
    let cache_path = tmp_token_path();
    token_cache::save(
        &cache_path,
        &TokenSet {
            access_token: "jwt.body.sig".into(),
            refresh_token: "rt-1".into(),
            scope: "mcp.read mcp.write".into(),
            github_token: Some("gho_initial".into()),
            // 60 seconds left -- inside the 5 min skew window.
            expires_at: now() + 60,
            acquired_at: now(),
        },
    )
    .unwrap();

    let mgr = TokenManager::from_cache(
        cache_path,
        AuthConfig {
            base_url: auth_server.uri(),
            client_id: "cc-relay".into(),
            scopes: vec!["mcp.read".into(), "mcp.write".into()],
        },
        "shh-internal-secret".into(),
        reqwest::Client::new(),
    )
    .unwrap();
    let broker = GitHubBroker::with_token_manager("owner", "repo", 42, "octocat", Arc::clone(&mgr))
        .unwrap()
        .with_base_url(gh_server.uri());

    // One GitHub call drives: ensure_fresh -> auth-worker token+introspect
    // -> bearer -> api.github.com GET issue -> snapshot parse.
    let agents = broker.list_agents().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_id, "octocat");
}

#[tokio::test]
async fn no_refresh_when_token_is_fresh() {
    let auth_server = MockServer::start().await;
    let gh_server = MockServer::start().await;

    // Auth-worker should be UNTOUCHED. Don't mount any mocks --
    // wiremock returns 404 for unmatched routes, which would surface
    // through TokenManager as an error if a refresh actually fired.

    // api.github.com responds normally with the cached (still-fresh)
    // token.
    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/issues/42"))
        .and(header("authorization", "Bearer gho_initial"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "body": "{\"v\":1,\"agents\":[],\"plan\":[]}",
        })))
        .expect(1)
        .mount(&gh_server)
        .await;

    let cache_path = tmp_token_path();
    token_cache::save(
        &cache_path,
        &TokenSet {
            access_token: "jwt.body.sig".into(),
            refresh_token: "rt-1".into(),
            scope: "mcp.read mcp.write".into(),
            github_token: Some("gho_initial".into()),
            // 1 hour left -- well outside the skew window.
            expires_at: now() + 3600,
            acquired_at: now(),
        },
    )
    .unwrap();

    let mgr = TokenManager::from_cache(
        cache_path,
        AuthConfig {
            base_url: auth_server.uri(),
            client_id: "cc-relay".into(),
            scopes: vec!["mcp.read".into(), "mcp.write".into()],
        },
        "shh-internal-secret".into(),
        reqwest::Client::new(),
    )
    .unwrap();
    let broker = GitHubBroker::with_token_manager("owner", "repo", 42, "octocat", mgr)
        .unwrap()
        .with_base_url(gh_server.uri());

    let agents = broker.list_agents().await.unwrap();
    assert!(agents.is_empty());
}
