//! End-to-end: drive a single `api.github.com` call through the
//! pair-flow-provisioned [`TokenManager`].
//!
//! Since auth-worker issue #145 the broker no longer refreshes
//! automatically. We therefore exercise two paths:
//!
//! - cached JWT well within lifetime → GitHub call uses the bundled
//!   `github_token`.
//! - cached JWT inside the 5-minute skew window → `ensure_fresh` returns
//!   `BrokerError::Auth`, surfacing "re-pair" guidance to the operator.

use std::sync::Arc;

use agent_broker::auth::AuthConfig;
use agent_broker::token_cache::{self, TokenSet};
use agent_broker::types::BrokerError;
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

fn cfg(base: String) -> AuthConfig {
    AuthConfig {
        base_url: base,
        client_id: "cc-relay".into(),
        scopes: vec!["mcp.read".into(), "mcp.write".into()],
    }
}

#[tokio::test]
async fn cached_jwt_within_lifetime_drives_github_request() {
    let auth_server = MockServer::start().await;
    let gh_server = MockServer::start().await;

    // Auth-worker should be UNTOUCHED. Don't mount any mocks; wiremock
    // returns 404 for unmatched routes and the test would fail.

    Mock::given(method("GET"))
        .and(path("/repos/owner/repo/issues/42"))
        .and(header("authorization", "Bearer gho_initial"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "body": "{\"v\":1,\"agents\":[{\"agent_id\":\"octocat\",\"joined_at\":1}],\"plan\":[]}",
        })))
        .expect(1)
        .mount(&gh_server)
        .await;

    let cache_path = tmp_token_path();
    token_cache::save(
        &cache_path,
        &TokenSet {
            access_token: "jwt.body.sig".into(),
            refresh_token: None,
            scope: "mcp.read mcp.write".into(),
            github_token: Some("gho_initial".into()),
            expires_at: now() + 3600,
            acquired_at: now(),
        },
    )
    .unwrap();

    let mgr = TokenManager::from_cache(cache_path, cfg(auth_server.uri())).unwrap();
    let broker = GitHubBroker::with_token_manager("owner", "repo", 42, "octocat", Arc::clone(&mgr))
        .unwrap()
        .with_base_url(gh_server.uri());

    let agents = broker.list_agents().await.unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].agent_id, "octocat");
}

#[tokio::test]
async fn cached_jwt_near_expiry_surfaces_auth_error_for_repair() {
    let cache_path = tmp_token_path();
    token_cache::save(
        &cache_path,
        &TokenSet {
            access_token: "jwt.body.sig".into(),
            refresh_token: None,
            scope: "mcp.read mcp.write".into(),
            github_token: Some("gho_initial".into()),
            // 60s left → inside the 5 min skew window.
            expires_at: now() + 60,
            acquired_at: now(),
        },
    )
    .unwrap();

    let mgr = TokenManager::from_cache(cache_path, cfg("http://unused".into())).unwrap();
    let err = mgr.ensure_fresh().await.unwrap_err();
    match err {
        BrokerError::Auth(s) => assert!(s.contains("re-pair"), "got {s}"),
        other => panic!("expected Auth(re-pair), got {other:?}"),
    }
}
