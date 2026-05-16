//! [`TokenManager`] — owns the live OAuth state for a [`GitHubBroker`].
//!
//! Two modes:
//!
//! - [`TokenManager::static_token`] — a fixed, never-refreshed bearer
//!   token. Used by tests and by callers that already have a long-lived
//!   credential (e.g. a GitHub PAT in env). `ensure_fresh` is a no-op.
//! - [`TokenManager::from_cache`] — refreshable bundle backed by
//!   `~/.cc-relay/token`. `ensure_fresh` checks the JWT `expires_at`
//!   against a 5-minute skew window and, if close, drives
//!   [`auth::refresh`] → [`introspect`] → [`token_cache::save`] and
//!   updates the in-memory copy.
//!
//! Concurrency: `ensure_fresh` may be called from multiple async tasks.
//! Two simultaneous refreshes are wasteful but not incorrect — the
//! second one overwrites the first's cache file with an equivalent
//! `TokenSet` and the in-memory copy converges. Adding a dedupe
//! `Mutex<()>` is a future optimization.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::auth::{self, AuthConfig};
use crate::introspect;
use crate::token_cache::{self, TokenSet};
use crate::types::{BrokerError, Result};

/// How close to expiry the JWT must be before [`ensure_fresh`] refreshes
/// it. 5 minutes matches the upstream `github-mcp-server-rs` default.
const REFRESH_SKEW_SECS: i64 = 300;

/// Holder of the credential a [`GitHubBroker`](crate::GitHubBroker) uses
/// on every `api.github.com` call.
pub struct TokenManager {
    mode: Mode,
}

impl std::fmt::Debug for TokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never log token material. Just say which variant this is.
        match &self.mode {
            Mode::Static { .. } => f.debug_struct("TokenManager::Static").finish(),
            Mode::Refreshable(_) => f.debug_struct("TokenManager::Refreshable").finish(),
        }
    }
}

enum Mode {
    Static { bearer: String },
    // Boxed: RefreshableInner is significantly larger than the Static
    // variant (RwLock + reqwest::Client + several Strings/PathBufs), so
    // pay one allocation to keep the discriminant small.
    Refreshable(Box<RefreshableInner>),
}

// `secret` is `Option<String>`: `Some` → legacy shared-secret mode on
// `/mcp/introspect`; `None` → Bearer-JWT mode (CLI / end-user default).
struct RefreshableInner {
    state: RwLock<TokenSet>,
    cache_path: PathBuf,
    cfg: AuthConfig,
    secret: Option<String>,
    http: reqwest::Client,
}

impl TokenManager {
    /// Fixed bearer token. `ensure_fresh` is a no-op. The token is used
    /// verbatim as `Authorization: Bearer <token>`.
    pub fn static_token(token: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            mode: Mode::Static {
                bearer: token.into(),
            },
        })
    }

    /// Load a [`TokenSet`] from `cache_path` and wrap it in a
    /// refreshable manager.
    ///
    /// Returns an error if the cache file is missing (the user has not
    /// run `rust-mcp-agent auth` yet) or if the cached set has no
    /// `github_token` (a prior auth run was interrupted between
    /// `token` and `introspect`).
    pub fn from_cache(
        cache_path: PathBuf,
        cfg: AuthConfig,
        secret: Option<String>,
        http: reqwest::Client,
    ) -> Result<Arc<Self>> {
        let set = token_cache::load(&cache_path)?.ok_or_else(|| {
            BrokerError::Auth(format!(
                "no cached token at {} -- run `rust-mcp-agent auth`",
                cache_path.display()
            ))
        })?;
        if set.github_token.is_none() {
            return Err(BrokerError::Auth(format!(
                "cached token at {} is missing github_token (incomplete auth) -- re-run `rust-mcp-agent auth`",
                cache_path.display()
            )));
        }
        Ok(Arc::new(Self {
            mode: Mode::Refreshable(Box::new(RefreshableInner {
                state: RwLock::new(set),
                cache_path,
                cfg,
                secret,
                http,
            })),
        }))
    }

    /// Refresh the access token if it falls within the skew window of
    /// expiry; otherwise no-op. After this returns `Ok(())`, callers
    /// may read [`bearer`](Self::bearer) and rely on it being valid for
    /// at least the next several minutes.
    pub async fn ensure_fresh(&self) -> Result<()> {
        let r = match &self.mode {
            Mode::Static { .. } => return Ok(()),
            Mode::Refreshable(r) => r,
        };

        let needs_refresh = r.state.read().await.is_expired(REFRESH_SKEW_SECS);
        if !needs_refresh {
            return Ok(());
        }

        let refresh_token = r.state.read().await.refresh_token.clone();
        let new_set = auth::refresh(&r.http, &r.cfg, &refresh_token).await?;
        let active =
            introspect::introspect(&r.http, &r.cfg, r.secret.as_deref(), &new_set.access_token)
                .await?
                .ok_or_else(|| {
                    BrokerError::Auth(
                        "refresh succeeded but introspect returned inactive token".into(),
                    )
                })?;
        let new_set = new_set.with_github_token(active.github_token);
        token_cache::save(&r.cache_path, &new_set)?;
        *r.state.write().await = new_set;
        Ok(())
    }

    /// Return the `Authorization` header value (`Bearer <token>`) to
    /// attach to a single `api.github.com` request. Callers should
    /// invoke [`ensure_fresh`](Self::ensure_fresh) immediately before.
    pub async fn bearer(&self) -> Result<String> {
        match &self.mode {
            Mode::Static { bearer } => Ok(format!("Bearer {bearer}")),
            Mode::Refreshable(r) => {
                let t = r.state.read().await;
                let gh = t.github_token.as_deref().ok_or_else(|| {
                    BrokerError::Other(anyhow::anyhow!(
                        "TokenManager state has no github_token (introspect never ran)"
                    ))
                })?;
                Ok(format!("Bearer {gh}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cfg(base: String) -> AuthConfig {
        AuthConfig {
            base_url: base,
            client_id: "cc-relay".into(),
            scopes: vec!["mcp.read".into(), "mcp.write".into()],
        }
    }

    fn sample(expires_at: i64) -> TokenSet {
        TokenSet {
            access_token: "jwt.payload.sig".into(),
            refresh_token: "rt-1".into(),
            scope: "mcp.read mcp.write".into(),
            github_token: Some("gho_initial".into()),
            expires_at,
            acquired_at: 0,
        }
    }

    fn write_cache(set: &TokenSet) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cc-relay-tm-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("token");
        token_cache::save(&path, set).unwrap();
        path
    }

    #[tokio::test]
    async fn static_token_bearer_is_verbatim() {
        let m = TokenManager::static_token("gho_static");
        assert_eq!(m.bearer().await.unwrap(), "Bearer gho_static");
        // ensure_fresh is a no-op for static tokens.
        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer gho_static");
    }

    #[tokio::test]
    async fn refreshable_no_refresh_when_fresh() {
        let server = MockServer::start().await;
        // The mock would only be hit if a refresh fires. Don't mount any
        // token / introspect mocks — a request would 404 the test.
        let now = token_cache::now_secs();
        let path = write_cache(&sample(now + 3600));
        let m = TokenManager::from_cache(
            path,
            cfg(server.uri()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap();

        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer gho_initial");
    }

    #[tokio::test]
    async fn refreshable_refreshes_when_within_skew() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "jwt2.body.sig",
                "refresh_token": "rt-2",
                "scope": "mcp.read mcp.write",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .and(header("authorization", "shh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true,
                "github_login": "octocat",
                "github_token": "gho_refreshed",
                "exp": token_cache::now_secs() + 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Cached set is "expired-soon" (within the 300s skew).
        let now = token_cache::now_secs();
        let cache_path = write_cache(&sample(now + 60));
        let m = TokenManager::from_cache(
            cache_path.clone(),
            cfg(server.uri()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap();

        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer gho_refreshed");

        // Cache file should now reflect the new state.
        let on_disk = token_cache::load(&cache_path).unwrap().unwrap();
        assert_eq!(on_disk.access_token, "jwt2.body.sig");
        assert_eq!(on_disk.refresh_token, "rt-2");
        assert_eq!(on_disk.github_token.as_deref(), Some("gho_refreshed"));
    }

    #[tokio::test]
    async fn refreshable_refresh_denied_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "access_denied",
                "error_description": "refresh token revoked",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let now = token_cache::now_secs();
        let cache_path = write_cache(&sample(now + 60));
        let m = TokenManager::from_cache(
            cache_path,
            cfg(server.uri()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap();

        let err = m.ensure_fresh().await.unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[tokio::test]
    async fn from_cache_missing_file_returns_auth_error() {
        let bogus = std::env::temp_dir().join(format!(
            "cc-relay-tm-missing-{}-{}.token",
            std::process::id(),
            token_cache::now_secs()
        ));
        let err = TokenManager::from_cache(
            bogus,
            cfg("http://unused".into()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[tokio::test]
    async fn debug_format_does_not_leak_token_material() {
        // Static variant.
        let m = TokenManager::static_token("gho_secret_value");
        let s = format!("{m:?}");
        assert!(s.contains("Static"), "expected Static in debug, got {s}");
        assert!(
            !s.contains("gho_secret_value"),
            "token leaked in debug: {s}"
        );

        // Refreshable variant.
        let now = token_cache::now_secs();
        let path = write_cache(&sample(now + 3600));
        let m = TokenManager::from_cache(
            path,
            cfg("http://unused".into()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap();
        let s = format!("{m:?}");
        assert!(
            s.contains("Refreshable"),
            "expected Refreshable in debug, got {s}"
        );
        assert!(!s.contains("gho_initial"), "token leaked in debug: {s}");
    }

    #[tokio::test]
    async fn refreshable_inactive_introspect_after_refresh_surfaces_auth_error() {
        let server = MockServer::start().await;
        // Refresh succeeds.
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "jwt2.body.sig",
                "refresh_token": "rt-2",
                "scope": "mcp.read mcp.write",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;
        // Introspect reports the freshly-refreshed JWT as inactive.
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .and(header("authorization", "shh"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": false,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let now = token_cache::now_secs();
        let cache_path = write_cache(&sample(now + 60));
        let m = TokenManager::from_cache(
            cache_path,
            cfg(server.uri()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap();

        let err = m.ensure_fresh().await.unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("introspect returned inactive")),
            other => panic!("expected Auth(inactive), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn from_cache_incomplete_token_returns_auth_error() {
        // Cached set is missing github_token (auth was interrupted
        // before introspect).
        let mut incomplete = sample(token_cache::now_secs() + 3600);
        incomplete.github_token = None;
        let cache_path = write_cache(&incomplete);
        let err = TokenManager::from_cache(
            cache_path,
            cfg("http://unused".into()),
            Some("shh".into()),
            reqwest::Client::new(),
        )
        .unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("missing github_token")),
            other => panic!("expected Auth(_), got {other:?}"),
        }
    }

    /// P10 #11: secret material must never appear in `Debug` output.
    /// Both `TokenManager::Static` (PAT / installation token) and the
    /// `Refreshable` mode (auth-worker JWT) must redact.
    #[test]
    fn debug_impl_does_not_leak_static_token() {
        let secret = "ghp_supersecretvalue123456789abc";
        let tm = TokenManager::static_token(secret);
        let dbg = format!("{tm:?}");
        assert!(
            !dbg.contains(secret),
            "TokenManager Debug leaked the static token: {dbg}"
        );
        // Sanity: variant tag is still present so logs remain useful.
        assert!(dbg.contains("Static"), "Debug lost the variant tag: {dbg}");
    }
}
