//! `POST /mcp/introspect` client.
//!
//! RFC 7662-style token introspection with the auth-worker extension:
//! the response includes the raw GitHub OAuth token (`github_token`)
//! bound to the JWT's session. The broker reads this on every refresh
//! and uses it as the `Authorization: Bearer …` on `api.github.com`
//! calls.
//!
//! Auth modes (see auth-worker `mcp-introspect.ts` for the server side):
//!
//! - **`Some(secret)`** — legacy shared-secret mode. `Authorization: <secret>`
//!   (raw, no `Bearer` prefix) + body `{ "token": "<jwt>" }`. Originally
//!   shared with `github-mcp-server-rs`; phased out (auth-worker #91).
//! - **`None`** — Bearer JWT mode. `Authorization: Bearer <jwt>`. The JWT
//!   itself proves the user is allowed to read their own `github_token`
//!   (OAuth already authenticated them via DCR/PKCE or device flow). No
//!   shared secret needed for end-user CLI. body still includes
//!   `{ "token": "<jwt>" }` for legacy server compat — auth-worker
//!   ignores it in mode 1.

use serde::Deserialize;

use crate::auth::AuthConfig;
use crate::types::{BrokerError, Result};

/// Successful (`active: true`) introspection response. Inactive tokens
/// surface as `Ok(None)` from [`introspect`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct IntrospectionActive {
    /// Space-separated scope string the access token was granted.
    #[serde(default)]
    pub scope: String,
    /// JWT `sub`. Auth-worker emits `"github:{login}"`.
    #[serde(default)]
    pub sub: String,
    /// GitHub login the token is bound to (derived from `sub`).
    #[serde(default)]
    pub github_login: String,
    /// Raw GitHub OAuth token to pass to `api.github.com`. This is the
    /// auth-worker extension to RFC 7662.
    pub github_token: String,
    /// Unix seconds at which the JWT expires.
    pub exp: i64,
}

#[derive(Debug, Deserialize)]
struct IntrospectionResponse {
    active: bool,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    github_login: Option<String>,
    #[serde(default)]
    github_token: Option<String>,
    #[serde(default)]
    exp: Option<i64>,
}

/// Call `POST /mcp/introspect` to resolve a JWT into its bound
/// `github_token` (plus metadata).
///
/// Returns:
///
/// - `Ok(Some(_))` — token is active; caller stores `github_token`
///   into the [`TokenSet`](crate::token_cache::TokenSet).
/// - `Ok(None)` — auth-worker reports `{ active: false }`; the cached
///   JWT is dead and the caller must drive a refresh.
/// - `Err(BrokerError::Auth(_))` — 401, i.e. wrong shared secret /
///   invalid or expired JWT.
/// - `Err(BrokerError::Other(_))` — 503 (auth-worker misconfigured) or
///   any other unexpected failure mode.
pub async fn introspect(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    secret: Option<&str>,
    token: &str,
) -> Result<Option<IntrospectionActive>> {
    let url = format!("{}/mcp/introspect", cfg.base_url);
    // `secret = Some` → legacy mode (raw secret in header).
    // `secret = None` → Bearer JWT mode (the token itself is the auth).
    let auth_value = match secret {
        Some(s) => s.to_string(),
        None => format!("Bearer {token}"),
    };
    let resp = http
        .post(&url)
        .header(reqwest::header::AUTHORIZATION, auth_value)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await
        .map_err(transport_err)?;

    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(BrokerError::Auth(
            "introspect: 401 (invalid shared secret or invalid/expired JWT)".into(),
        ));
    }
    if status.as_u16() == 503 {
        return Err(BrokerError::Other(anyhow::anyhow!(
            "introspect: auth-worker misconfigured (503)",
        )));
    }
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(BrokerError::Other(anyhow::anyhow!(
            "introspect returned {status}: {body}"
        )));
    }

    let r: IntrospectionResponse = resp.json().await.map_err(transport_err)?;
    if !r.active {
        return Ok(None);
    }
    let github_token = r.github_token.ok_or_else(|| {
        BrokerError::Other(anyhow::anyhow!(
            "introspect response active=true but github_token missing",
        ))
    })?;
    Ok(Some(IntrospectionActive {
        scope: r.scope.unwrap_or_default(),
        sub: r.sub.unwrap_or_default(),
        github_login: r.github_login.unwrap_or_default(),
        github_token,
        exp: r.exp.unwrap_or(0),
    }))
}

fn transport_err(e: reqwest::Error) -> BrokerError {
    BrokerError::Other(anyhow::Error::new(e).context("HTTP transport (introspect)"))
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

    #[tokio::test]
    async fn active_response_yields_some() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .and(header("authorization", "shh-secret"))
            .and(header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true,
                "scope": "mcp.read mcp.write",
                "sub": "github:octocat",
                "github_login": "octocat",
                "github_token": "gho_xxx",
                "exp": 1_700_003_600_i64,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let out = introspect(
            &http,
            &cfg(server.uri()),
            Some("shh-secret"),
            "jwt.body.sig",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(out.github_token, "gho_xxx");
        assert_eq!(out.github_login, "octocat");
        assert_eq!(out.scope, "mcp.read mcp.write");
        assert_eq!(out.exp, 1_700_003_600);
    }

    #[tokio::test]
    async fn inactive_response_yields_none() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": false,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let out = introspect(&http, &cfg(server.uri()), Some("shh"), "jwt.body.sig")
            .await
            .unwrap();
        assert!(out.is_none());
    }

    #[tokio::test]
    async fn unauthorized_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = introspect(
            &http,
            &cfg(server.uri()),
            Some("bad-secret"),
            "jwt.body.sig",
        )
        .await
        .unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[tokio::test]
    async fn service_unavailable_surfaces_other_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = introspect(&http, &cfg(server.uri()), Some("shh"), "jwt.body.sig")
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => assert!(e.to_string().contains("misconfigured")),
            other => panic!("expected Other(misconfigured), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn bearer_jwt_mode_uses_token_as_authorization() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            // Bearer mode: Authorization header is `Bearer <jwt>`, not a
            // raw shared secret.
            .and(header("authorization", "Bearer jwt.body.sig"))
            .and(header("content-type", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true,
                "scope": "read:user",
                "sub": "github:octocat",
                "github_login": "octocat",
                "github_token": "gho_via_bearer",
                "exp": 1_700_003_600_i64,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let out = introspect(&http, &cfg(server.uri()), None, "jwt.body.sig")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.github_token, "gho_via_bearer");
        assert_eq!(out.github_login, "octocat");
    }

    #[tokio::test]
    async fn other_http_error_surfaces_other_error() {
        // 500 is neither 401 nor 503, so we should hit the generic
        // `!status.is_success()` branch.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = introspect(&http, &cfg(server.uri()), Some("shh"), "jwt.body.sig")
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("500"), "expected status in message, got {s}");
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn transport_error_surfaces_other_error() {
        // Point at an unreachable host so reqwest fails at the transport
        // layer (covers the `transport_err` mapper).
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        // Reserved TEST-NET-1 address — guaranteed unroutable.
        let err = introspect(
            &http,
            &cfg("http://192.0.2.1:1".to_string()),
            Some("shh"),
            "jwt.body.sig",
        )
        .await
        .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                assert!(e.to_string().contains("HTTP transport"));
            }
            other => panic!("expected Other(transport), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn active_without_github_token_surfaces_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/introspect"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true,
                // github_token omitted
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = introspect(&http, &cfg(server.uri()), Some("shh"), "jwt.body.sig")
            .await
            .unwrap_err();
        assert!(matches!(err, BrokerError::Other(_)));
    }
}
