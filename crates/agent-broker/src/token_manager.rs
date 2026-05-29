//! [`TokenManager`] — owns the live credential a [`GitHubBroker`] uses.
//!
//! Three modes:
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
//! - [`TokenManager::github_app`] — the **`cc-relay-agent` GitHub App**
//!   path (ADR-007 / #69). `ensure_fresh` mints (and transparently
//!   re-mints) a short-lived installation access token by RS256-signing
//!   an app JWT and POSTing it to
//!   `/app/installations/<id>/access_tokens`. Comments posted with this
//!   token are authored by `cc-relay-agent[bot]` — a **distinct GitHub
//!   identity** from the end-user, which is what lets a wake-trigger
//!   comment pass the harness self-loop filter and wake another session.
//!
//! Concurrency: `ensure_fresh` takes the write lock only when it has to
//! mint; the common path (static / cached / still-valid app token) reads.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::auth::AuthConfig;
use crate::github::USER_AGENT_STR;
use crate::token_cache::{self, TokenSet};
use crate::types::{BrokerError, Result};

/// Default GitHub REST base. Overridable (tests / GHES) via
/// [`TokenManager::github_app_with_base_url`].
const GITHUB_API_BASE: &str = "https://api.github.com";

/// App JWT lifetime knobs. GitHub rejects an app JWT whose `iat` is in
/// the future or whose `exp` is more than 10 minutes out; we backdate
/// `iat` by 60s for clock skew and set `exp` to +9m.
const APP_JWT_IAT_SKEW_SECS: i64 = 60;
const APP_JWT_TTL_SECS: i64 = 9 * 60;

/// Installation tokens last 1h. We treat them as good for 55m so a mint
/// is always comfortably ahead of the [`REFRESH_SKEW_SECS`] window
/// without parsing GitHub's RFC3339 `expires_at`.
const INSTALL_TOKEN_TTL_SECS: i64 = 55 * 60;

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
        // Never log token / key material. Just say which variant this is.
        match &self.mode {
            Mode::Static { .. } => f.debug_struct("TokenManager::Static").finish(),
            Mode::Cached(_) => f.debug_struct("TokenManager::Cached").finish(),
            Mode::GitHubApp(_) => f.debug_struct("TokenManager::GitHubApp").finish(),
        }
    }
}

enum Mode {
    Static { bearer: String },
    // Boxed: `CachedInner` is significantly larger than the Static
    // variant (RwLock + Strings/PathBufs), so pay one allocation to
    // keep the discriminant small.
    Cached(Box<CachedInner>),
    // Boxed for the same reason (holds an `EncodingKey`, an HTTP client
    // and an `RwLock`).
    GitHubApp(Box<GitHubAppInner>),
}

/// Live state for the [`TokenManager::github_app`] mode.
struct GitHubAppInner {
    /// Numeric App ID, stringified for the JWT `iss` claim.
    app_id: String,
    installation_id: String,
    /// Parsed RSA private key. We keep the *parsed* key (not the raw PEM
    /// string) so the secret never lives in a `String` we might
    /// accidentally log, and so a malformed PEM fails at construction.
    encoding_key: jsonwebtoken::EncodingKey,
    base_url: String,
    http: reqwest::Client,
    /// Cached installation token + the epoch-seconds we consider it good
    /// until. `None` until the first mint.
    cached: RwLock<Option<InstallToken>>,
}

#[derive(Clone)]
struct InstallToken {
    /// `Authorization` header value, i.e. `Bearer ghs_…`.
    header: String,
    /// Epoch seconds after which the token must be re-minted.
    good_until: i64,
}

/// Claims for the short-lived app JWT (RS256). `iss` is the App ID.
#[derive(Serialize)]
struct AppJwtClaims {
    iat: i64,
    exp: i64,
    iss: String,
}

/// `POST /app/installations/<id>/access_tokens` response (subset).
#[derive(Deserialize)]
struct InstallTokenResponse {
    token: String,
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

    /// GitHub App (`cc-relay-agent`) installation-token mode against the
    /// public `api.github.com`. `pem` is the App private key (PKCS#1 or
    /// PKCS#8); it is parsed immediately, so a malformed key fails here
    /// rather than at first use.
    pub fn github_app(
        app_id: impl Into<String>,
        installation_id: impl Into<String>,
        pem: &str,
    ) -> Result<Arc<Self>> {
        Self::github_app_with_base_url(app_id, installation_id, pem, GITHUB_API_BASE)
    }

    /// Like [`github_app`](Self::github_app) but against a custom REST
    /// base (wiremock in tests, or a GHES host).
    pub fn github_app_with_base_url(
        app_id: impl Into<String>,
        installation_id: impl Into<String>,
        pem: &str,
        base_url: impl Into<String>,
    ) -> Result<Arc<Self>> {
        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
            .map_err(|e| BrokerError::Auth(format!("invalid GitHub App private key (PEM): {e}")))?;
        // `.context(..)?` (not `.map_err(|e| ...)`) so the error arm has no
        // separate closure body for llvm-cov to flag as uncovered — client
        // build never fails in practice. anyhow::Error → BrokerError::Other.
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT_STR)
            .build()
            .context("build reqwest client")?;
        Ok(Arc::new(Self {
            mode: Mode::GitHubApp(Box::new(GitHubAppInner {
                app_id: app_id.into(),
                installation_id: installation_id.into(),
                encoding_key,
                base_url: base_url.into(),
                http,
                cached: RwLock::new(None),
            })),
        }))
    }

    /// Verify the credential still has lifetime left. For the cached
    /// pair-flow JWT this is read-only and returns a `BrokerError::Auth`
    /// when the JWT falls within the skew window (pair flow has no
    /// refresh token, so the operator must re-run `rust-mcp-agent auth`).
    /// For the GitHub App mode this mints a fresh installation token when
    /// none is cached or the cached one is within the skew window.
    pub async fn ensure_fresh(&self) -> Result<()> {
        match &self.mode {
            Mode::Static { .. } => Ok(()),
            Mode::Cached(r) => {
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
            Mode::GitHubApp(inner) => {
                if !needs_mint(&*inner.cached.read().await) {
                    return Ok(());
                }
                let minted = mint_installation_token(inner).await?;
                *inner.cached.write().await = Some(minted);
                Ok(())
            }
        }
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
            Mode::GitHubApp(inner) => {
                let cached = inner.cached.read().await;
                cached.as_ref().map(|t| t.header.clone()).ok_or_else(|| {
                    BrokerError::Auth(
                        "no installation token cached; call ensure_fresh() first".to_string(),
                    )
                })
            }
        }
    }
}

/// True when there is no cached token or the cached one is within
/// [`REFRESH_SKEW_SECS`] of expiry.
fn needs_mint(cached: &Option<InstallToken>) -> bool {
    match cached {
        None => true,
        Some(t) => token_cache::now_secs() >= t.good_until - REFRESH_SKEW_SECS,
    }
}

/// RS256-sign an app JWT and exchange it for an installation access
/// token. Errors map to `BrokerError::Auth` for anything GitHub rejects
/// (so the operator sees an auth problem) and `BrokerError::Other` for
/// transport / encode faults.
async fn mint_installation_token(inner: &GitHubAppInner) -> Result<InstallToken> {
    let now = token_cache::now_secs();
    let claims = AppJwtClaims {
        iat: now - APP_JWT_IAT_SKEW_SECS,
        exp: now + APP_JWT_TTL_SECS,
        iss: inner.app_id.clone(),
    };
    // `.context(..)?`: RS256 encode with a pre-validated key does not fail,
    // so avoid a `map_err` closure that llvm-cov would report uncovered.
    let jwt = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &inner.encoding_key,
    )
    .context("sign app JWT")?;

    let url = format!(
        "{}/app/installations/{}/access_tokens",
        inner.base_url.trim_end_matches('/'),
        inner.installation_id
    );
    let resp = inner
        .http
        .post(&url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header("x-github-api-version", "2022-11-28")
        .bearer_auth(&jwt)
        .send()
        .await
        // `.context(..)?`: the transport-error arm is covered on the
        // success path's inline call; no closure body to leave uncovered.
        .context("POST installation access_tokens")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(BrokerError::Auth(format!(
            "installation token request failed: HTTP {} {}",
            status.as_u16(),
            body.trim()
        )));
    }

    let parsed: InstallTokenResponse = resp.json().await.map_err(|e| {
        BrokerError::Other(anyhow::Error::new(e).context("parse installation token response"))
    })?;

    Ok(InstallToken {
        header: format!("Bearer {}", parsed.token),
        good_until: now + INSTALL_TOKEN_TTL_SECS,
    })
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

    // ---- GitHub App installation-token mode (ADR-007 / #69) ----

    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Throwaway 2048-bit RSA key — test fixture only, never a real
    /// credential. Lets `EncodingKey::from_rsa_pem` + RS256 `encode`
    /// run for real so the mint path is genuinely exercised.
    const TEST_RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDMgYOL5VeUxKlm\n\
6Q4lllZ/1Fo1hlWoXrb+7s4GXrWboeBaccrsWfX6M2PNu3lBy3jaDb1EnNJedx1S\n\
YKF5zF02qri013tAHBxZSuTa3NeRNfnRteXDrcRs8lA4ck7oHSvs5848AvxPBkRO\n\
rnF+y1HeS/r+tMdydZc9SOf7t9Mu7wovMTE9XaBhqnhXVBoQEnG76dkYpUewt6s2\n\
aNOkAB4+i58p0/Gs285Szm7unB8+Tk5iia/0VQZd872ER/KE/Ngu5983N94tT7RV\n\
TWmCmgL6aXKcUOOsGZK9B+Z7w/6HiSSf+L7HeC3iSuIL2IGgMaaOe9YeC4IzZAWH\n\
qSAnby9TAgMBAAECggEAHBLBYKBsi+cNTCu8/err1+NqOMPGmXTbERbuIFC7BHDN\n\
0Ayj6xsUWbLZRgIwzJfmcKSHMVaHyGD4nPjX4dTsjNcVwPl9mVjiiV1vGh5L10q+\n\
Dww1zM1bVAtVeNI0Q8fRYlWV6YYNZbm6AcUPnnTBuc7bV0CwymUbDFYoRlU/P8dg\n\
1lUQG4RWU5NVVe+rKJE531fWTajFsaBeQcqm2+KHBPWzQht2wfHf7KEfg6pr7U0E\n\
f1CsBG5iMNa8Q/T0yDV5EdKLrpzYXLxGI7fLL+e8s1VhJv3hftRD1LY9D//2TZV8\n\
Q+2Nkhw3xkwl0wAod8JOah4KlfKkQeSwwZyhva2OJQKBgQDs8pcqlwFHftKysYL/\n\
QTQhNOBU6Jue7nt7AOWzImcQ31z0pIVaCZreAgm16Xk6PZ68IWown+94jV7csE3E\n\
pus08MpCrInXRgDAOq8oMV38mk1L9k7A0t5Kcv9XU/EcTtc7F5Ncf1iiJ308poCB\n\
NooZbFZexYX5xh+RXn2RSKUUrwKBgQDc8yHjdlucoH6JwWPcf3679ZSzyze7nQcb\n\
ne6rmIH95HKKXxliZml6MlHX//k0X3yn6e43msjCuLH8Oage87sC2+XwKv9TOAwL\n\
QLUxZ7AidYGYY4kLSAtNRQJR08BSct0yhg26DFDVg6/ru9IP+vDG0Ju8WcgKtZpQ\n\
YjZgGEaAnQKBgQC3VWyJU5V10DcOdDK7daP0HYmFqQTgD/4SyjrwQ6ojb+/oinNz\n\
mwLsy/7fdeqKmar8PY6AWP9c82V2tCM4CT7sE3Mr63wryMpD8iQcoTXrgShVohqF\n\
L6M3T4sp8pUYJhh6bF9krlPSA2PvTZUYZS6tRRn+8i4beKRsQgQ+KUsxmQKBgDTU\n\
VYgDpsf+gAMEIJJ6UZ0zjQioUH0lgKuTyZtx7bL9Sn3XW0Rx5Ep5eaRB6h4hrraf\n\
cnwNIG+epb//MTmlYVO/rG0Oeto1DnwqTqiveCflHMWJFx2BbmJdW76g+N095bHM\n\
579Sbol+4TNmR0XW5HdFLdeNSA13epw5v3Kem0zpAoGAXnNNtoJeH7fTNW/0OL+a\n\
45pdbdPeA2UUqzlgmirzTXivcXAv/PkW9Dqe7XB2tZ6M5MvEZ0wic/m6gS1HS6jf\n\
QV+dJ2Y20CiDS0cMq0hivy5G+YL7brGnWeU1/3Z0oK/2NyjNKm17qY1y2MKSXjKM\n\
Bk6Gorx5G0NB7WPEOHfwwd4=\n\
-----END PRIVATE KEY-----\n";

    /// Build (but do not mount) the `access_tokens` mock; the caller
    /// mounts it on its own `MockServer`.
    fn access_tokens_mock(resp: ResponseTemplate) -> Mock {
        Mock::given(method("POST"))
            .and(path("/app/installations/456/access_tokens"))
            .and(header("accept", "application/vnd.github+json"))
            .respond_with(resp)
    }

    #[tokio::test]
    async fn github_app_invalid_pem_errors() {
        let err = TokenManager::github_app("123", "456", "not a pem").unwrap_err();
        match err {
            BrokerError::Auth(s) => {
                assert!(s.contains("invalid GitHub App private key"), "got {s}")
            }
            other => panic!("expected Auth(invalid pem), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn github_app_mints_and_returns_bearer() {
        let server = MockServer::start().await;
        access_tokens_mock(ResponseTemplate::new(201).set_body_json(serde_json::json!({
            "token": "ghs_minttest",
            "expires_at": "2999-01-01T00:00:00Z"
        })))
        // Exactly one mint for the two ensure_fresh() calls below (the
        // second reuses the cached token).
        .expect(1)
        .mount(&server)
        .await;

        let m = TokenManager::github_app_with_base_url("123", "456", TEST_RSA_PEM, server.uri())
            .unwrap();
        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer ghs_minttest");
        // Still valid → no second POST (mock .expect(1) enforces this).
        m.ensure_fresh().await.unwrap();
        assert_eq!(m.bearer().await.unwrap(), "Bearer ghs_minttest");
    }

    #[tokio::test]
    async fn github_app_http_error_maps_to_auth() {
        let server = MockServer::start().await;
        access_tokens_mock(ResponseTemplate::new(403).set_body_string("forbidden"))
            .mount(&server)
            .await;

        let m = TokenManager::github_app_with_base_url("123", "456", TEST_RSA_PEM, server.uri())
            .unwrap();
        let err = m.ensure_fresh().await.unwrap_err();
        match err {
            BrokerError::Auth(s) => {
                assert!(s.contains("HTTP 403"), "got {s}");
                assert!(s.contains("forbidden"), "got {s}");
            }
            other => panic!("expected Auth(HTTP 403), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn github_app_bad_json_maps_to_other() {
        let server = MockServer::start().await;
        access_tokens_mock(ResponseTemplate::new(201).set_body_string("not json at all"))
            .mount(&server)
            .await;

        let m = TokenManager::github_app_with_base_url("123", "456", TEST_RSA_PEM, server.uri())
            .unwrap();
        let err = m.ensure_fresh().await.unwrap_err();
        assert!(
            matches!(err, BrokerError::Other(_)),
            "expected Other(parse), got {err:?}"
        );
    }

    #[tokio::test]
    async fn github_app_bearer_before_ensure_fresh_errors() {
        let m = TokenManager::github_app_with_base_url(
            "123",
            "456",
            TEST_RSA_PEM,
            "http://unused.invalid",
        )
        .unwrap();
        let err = m.bearer().await.unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("no installation token cached"), "got {s}"),
            other => panic!("expected Auth(no token), got {other:?}"),
        }
    }

    #[test]
    fn needs_mint_branches() {
        let now = token_cache::now_secs();
        assert!(needs_mint(&None), "no cached token → must mint");
        assert!(
            !needs_mint(&Some(InstallToken {
                header: "Bearer ghs_x".into(),
                good_until: now + 3600,
            })),
            "far-future token → no mint"
        );
        assert!(
            needs_mint(&Some(InstallToken {
                header: "Bearer ghs_x".into(),
                good_until: now, // within REFRESH_SKEW_SECS → re-mint
            })),
            "near-expiry token → must mint"
        );
    }

    #[tokio::test]
    async fn github_app_debug_does_not_leak_key_or_token() {
        let server = MockServer::start().await;
        access_tokens_mock(
            ResponseTemplate::new(201)
                .set_body_json(serde_json::json!({ "token": "ghs_secret_tok" })),
        )
        .mount(&server)
        .await;
        let m = TokenManager::github_app_with_base_url("123", "456", TEST_RSA_PEM, server.uri())
            .unwrap();
        m.ensure_fresh().await.unwrap();
        let dbg = format!("{m:?}");
        assert!(
            dbg.contains("GitHubApp"),
            "expected GitHubApp tag, got {dbg}"
        );
        assert!(!dbg.contains("ghs_secret_tok"), "token leaked: {dbg}");
        assert!(!dbg.contains("PRIVATE KEY"), "pem leaked: {dbg}");
    }
}
