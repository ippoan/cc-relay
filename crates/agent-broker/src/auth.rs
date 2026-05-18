//! Auth-worker MCP OAuth Provider client (issue #145 — pair flow only).
//!
//! Until #145, cc-relay's CLI ran RFC 8628 Device Authorization Grant
//! against `auth.ippoan.org/mcp/device_authorization`. That flow had a
//! poor UX (paste a code in a browser) and required cc-relay to hold a
//! refresh token. #145 retires it in favor of auth-worker's **1-click
//! pair flow** (auth-worker issue #144): the user opens a single URL,
//! GitHub OAuth runs once, and a 24h binding JWT is bound to that
//! browser session.
//!
//! What lives here now:
//!
//! - [`AuthConfig`] — shared with [`crate::introspect`].
//! - [`pair_new`] — `POST /mcp/pair/new` (anonymous start of the pair).
//!
//! The binding JWT itself does NOT come back from `/mcp/pair/new` — the
//! happy-path target is `github-mcp-server-rs` (or another pair-WS
//! consumer) that holds an outbound WS to auth-worker and receives the
//! JWT pushed over that channel after the user clicks "Paired ✓". For
//! cc-relay (which is server-side automation, not a CLI sitting on a
//! WS) the JWT is provisioned manually: `rust-mcp-agent auth` prints
//! the pair URL and then reads the JWT from stdin / `--jwt` flag. See
//! `crates/agent-cli/src/runners.rs::run_auth` for the user-facing
//! flow.

use serde::{Deserialize, Serialize};

use crate::types::{BrokerError, Result};

/// Default base URL for the auth-worker MCP OAuth Provider. Tests point
/// this at a wiremock server; production keeps the default.
pub const DEFAULT_BASE_URL: &str = "https://auth.ippoan.org";

/// Default static client id for cc-relay. The pair flow does not
/// validate `client_id` (`/mcp/pair/new` is anonymous); this is kept
/// solely so logs / future telemetry can tell consumers apart.
pub const DEFAULT_CLIENT_ID: &str = "cc-relay";

/// Default MCP scope set. `mcp.write` maps to GitHub `read:user repo` on
/// the auth-worker side — the minimum to drive the cc-relay broker
/// (Issues r/w + private repo access).
pub fn default_scopes() -> Vec<String> {
    vec!["mcp.read".to_string(), "mcp.write".to_string()]
}

/// Configuration shared between auth-worker endpoints.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Auth-worker base URL — paths like `/mcp/pair/new` are appended.
    /// No trailing slash.
    pub base_url: String,
    /// Static identifier this binary uses with the auth-worker.
    pub client_id: String,
    /// Scope tokens to request. Currently informational — auth-worker
    /// pair flow always issues `mcp.read mcp.write`. Kept on the struct
    /// so callers do not have to special-case the device-flow vs pair
    /// flow API surface.
    pub scopes: Vec<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            client_id: DEFAULT_CLIENT_ID.to_string(),
            scopes: default_scopes(),
        }
    }
}

/// `POST /mcp/pair/new` request body (auth-worker `mcp-pair-new.ts`).
#[derive(Debug, Clone, Serialize)]
struct PairNewRequest<'a> {
    claim_login: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    binary_version: Option<&'a str>,
}

/// Successful response of `POST /mcp/pair/new`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PairNewResponse {
    /// 40-char base64url pair code; the WS auth handshake takes this as
    /// `Authorization: Bearer <pair_code>` and auth-worker swaps it for
    /// the bound JWT after the user clicks.
    pub pair_code: String,
    /// The single URL the user opens in a browser
    /// (`https://auth.ippoan.org/mcp/pair/<pair_code>`).
    pub pair_url: String,
    /// Seconds until the pair code expires (auth-worker default = 300).
    pub expires_in: u64,
}

/// Start a 1-click pair flow. `claim_login` is the GitHub username the
/// pair URL will be bound to — the browser session must sign into the
/// same login or the claim page rejects the link.
pub async fn pair_new(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    claim_login: &str,
    binary_version: Option<&str>,
) -> Result<PairNewResponse> {
    let url = format!("{}/mcp/pair/new", cfg.base_url);
    let resp = http
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&PairNewRequest {
            claim_login,
            binary_version,
        })
        .send()
        .await
        .map_err(transport_err)?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(BrokerError::Other(anyhow::anyhow!(
            "pair_new returned {status}: {body}"
        )));
    }
    resp.json::<PairNewResponse>().await.map_err(transport_err)
}

fn transport_err(e: reqwest::Error) -> BrokerError {
    BrokerError::Other(anyhow::Error::new(e).context("HTTP transport (auth-worker pair_new)"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cfg(base: String) -> AuthConfig {
        AuthConfig {
            base_url: base,
            client_id: "cc-relay".into(),
            scopes: default_scopes(),
        }
    }

    #[tokio::test]
    async fn pair_new_happy_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/pair/new"))
            .and(body_string_contains("\"claim_login\":\"yhonda-ohishi\""))
            .and(body_string_contains("\"binary_version\":\"v0.1.0\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pair_code": "abc123",
                "pair_url": "https://auth.example/mcp/pair/abc123",
                "expires_in": 300,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let r = pair_new(&http, &cfg(server.uri()), "yhonda-ohishi", Some("v0.1.0"))
            .await
            .unwrap();
        assert_eq!(r.pair_code, "abc123");
        assert_eq!(r.pair_url, "https://auth.example/mcp/pair/abc123");
        assert_eq!(r.expires_in, 300);
    }

    #[tokio::test]
    async fn pair_new_without_binary_version_omits_field() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/pair/new"))
            // `binary_version` is skipped → field absent in body.
            // We sanity-check by asserting claim_login is present and no
            // `"binary_version"` substring leaks.
            .and(body_string_contains("\"claim_login\":\"alice\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "pair_code": "c",
                "pair_url": "u",
                "expires_in": 60,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let r = pair_new(&http, &cfg(server.uri()), "alice", None)
            .await
            .unwrap();
        assert_eq!(r.pair_code, "c");
    }

    #[tokio::test]
    async fn pair_new_non_success_surfaces_other_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/pair/new"))
            .respond_with(ResponseTemplate::new(429).set_body_string("rate-limited"))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = pair_new(&http, &cfg(server.uri()), "alice", None)
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("pair_new returned"), "got {s}");
                assert!(s.contains("rate-limited"), "got {s}");
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pair_new_transport_error_surfaces_other_error() {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        let err = pair_new(&http, &cfg("http://192.0.2.1:1".to_string()), "alice", None)
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                assert!(e.to_string().contains("HTTP transport"));
            }
            other => panic!("expected Other(transport), got {other:?}"),
        }
    }

    #[test]
    fn auth_config_default_uses_module_constants() {
        let c = AuthConfig::default();
        assert_eq!(c.base_url, DEFAULT_BASE_URL);
        assert_eq!(c.client_id, DEFAULT_CLIENT_ID);
        assert_eq!(c.scopes, default_scopes());
    }
}
