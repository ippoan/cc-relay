//! [`TokenManager`] — owns the live OAuth state for a [`GitHubBroker`].
//!
//! Two modes:
//!
//! - [`TokenManager::static_token`] — a fixed, never-refreshed bearer
//!   token. Used by tests and by callers that already have a long-lived
//!   credential (e.g. a GitHub PAT in env). `ensure_fresh` is a no-op.
//! - [`TokenManager::from_cache`] — non-refreshable bundle backed by
//!   `~/.cc-relay/token`. Since auth-worker issue #145 the pair flow
//!   does not mint refresh tokens, so `ensure_fresh` only validates
//!   that the JWT is still within its lifetime; if it falls inside the
//!   5-minute skew window the call returns a `BrokerError::Auth`
//!   prompting the operator to re-run `rust-mcp-agent auth`.
//!
//! Concurrency: `ensure_fresh` only reads from the cached `TokenSet`,
//! so concurrent callers are safe.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::auth::AuthConfig;
use crate::token_cache::{self, TokenSet};
use crate::types::{BrokerError, Result};

/// How close to expiry the JWT must be before [`ensure_fresh`] starts
/// reporting expiry. 5 minutes matches the upstream
/// `github-mcp-server-rs` default and the previous device-flow refresh
/// threshold.
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
            Mode::Cached(_) => f.debug_struct("TokenManager::Cached").finish(),
        }
    }
}

enum Mode {
    Static { bearer: String },
    // Boxed: `CachedInner` is significantly larger than the Static
    // variant (RwLock + Strings/PathBufs), so pay one allocation to
    // keep the discriminant small.
    Cached(Box<CachedInner>),
}

struct CachedInner {
    state: RwLock<TokenSet>,
    /// Retained for future re-auth flows (operator-visible error
    /// messages reference the path). Not used to write back since pair
    /// flow doesn't mint refresh tokens.
    cache_path: PathBuf,
    /// Retained for symmetry with the previous device-flow design;
    /// callers may need it when they reissue the JWT via pair flow.
    #[allow(dead_code)]
    cfg: AuthConfig,
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

    /// Load a [`TokenSet`] from `cache_path` and wrap it. The cached set
    /// must already contain a `github_token` (populated by
    /// `rust-mcp-agent auth` immediately after the JWT was provisioned).
    ///
    /// Returns an error if the cache file is missing or the cached set
    /// is incomplete.
    pub fn from_cache(cache_path: PathBuf, cfg: AuthConfig) -> Result<Arc<Self>> {
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
            mode: Mode::Cached(Box::new(CachedInner {
                state: RwLock::new(set),
                cache_path,
                cfg,
            })),
        }))
    }

    /// Verify the cached JWT still has lifetime left. Returns a
    /// `BrokerError::Auth` (rather than triggering an auto-refresh)
    /// when the JWT falls within the skew window — pair flow does not
    /// issue refresh tokens, so the operator must re-run
    /// `rust-mcp-agent auth`.
    pub async fn ensure_fresh(&self) -> Result<()> {
        let r = match &self.mode {
            Mode::Static { .. } => return Ok(()),
            Mode::Cached(r) => r,
        };
        let set = r.state.read().await;
        if set.is_expired(REFRESH_SKEW_SECS) {
            return Err(BrokerError::Auth(format!(
                "cached JWT at {} is within {}s of expiry; re-run `rust-mcp-agent auth` to re-pair",
                r.cache_path.display(),
                REFRESH_SKEW_SECS
            )));
        }
        Ok(())
    }

    /// Return the `Authorization` header value (`Bearer <token>`) to
    /// attach to a single `api.github.com` request. Callers should
    /// invoke [`ensure_fresh`](Self::ensure_fresh) immediately before.
    pub async fn bearer(&self) -> Result<String> {
        match &self.mode {
            Mode::Static { bearer } => Ok(format!("Bearer {bearer}")),
            Mode::Cached(r) => {
                let t = r.state.read().await;
                // Invariant: `from_cache` rejects a TokenSet with no
                // `github_token`; once we hand out a `Mode::Cached`
                // the field is always `Some`.
                let gh = t
                    .github_token
                    .as_deref()
                    .expect("Cached TokenSet always has github_token (invariant)");
                Ok(format!("Bearer {gh}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AuthConfig {
        AuthConfig {
            base_url: "http://unused".into(),
            client_id: "cc-relay".into(),
            scopes: vec!["mcp.read".into(), "mcp.write".into()],
        }
    }

    fn sample(expires_at: i64) -> TokenSet {
        TokenSet {
            access_token: "jwt.payload.sig".into(),
            refresh_token: None,
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
    async fn cached_passes_when_fresh() {
        let now = token_cache::now_secs();
        let path = write_cache(&sample(now + 3600));
        let m = TokenManager::from_cache(path, cfg()).unwrap();
        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer gho_initial");
    }

    #[tokio::test]
    async fn cached_returns_auth_error_when_within_skew() {
        let now = token_cache::now_secs();
        let path = write_cache(&sample(now + 60));
        let m = TokenManager::from_cache(path, cfg()).unwrap();
        let err = m.ensure_fresh().await.unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("re-pair"), "got {s}"),
            other => panic!("expected Auth(near-expiry), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn from_cache_missing_file_returns_auth_error() {
        let bogus = std::env::temp_dir().join(format!(
            "cc-relay-tm-missing-{}-{}.token",
            std::process::id(),
            token_cache::now_secs()
        ));
        let err = TokenManager::from_cache(bogus, cfg()).unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[tokio::test]
    async fn from_cache_incomplete_token_returns_auth_error() {
        let mut incomplete = sample(token_cache::now_secs() + 3600);
        incomplete.github_token = None;
        let cache_path = write_cache(&incomplete);
        let err = TokenManager::from_cache(cache_path, cfg()).unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("missing github_token")),
            other => panic!("expected Auth(_), got {other:?}"),
        }
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

        // Cached variant.
        let now = token_cache::now_secs();
        let path = write_cache(&sample(now + 3600));
        let m = TokenManager::from_cache(path, cfg()).unwrap();
        let s = format!("{m:?}");
        assert!(s.contains("Cached"), "expected Cached in debug, got {s}");
        assert!(!s.contains("gho_initial"), "token leaked in debug: {s}");
    }

    /// P10 #11: secret material must never appear in `Debug` output.
    /// Both `TokenManager::Static` (PAT / installation token) and the
    /// `Cached` mode (auth-worker JWT) must redact.
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
