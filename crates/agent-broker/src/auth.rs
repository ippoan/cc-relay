//! RFC 8628 device authorization grant client for `auth.ippoan.org`.
//!
//! Three free functions cover the full lifecycle:
//!
//! - [`start_device_authorization`] — kicks off a flow, returns the
//!   `user_code` + `verification_uri_complete` for the human to approve.
//! - [`poll_token`] — polls `POST /mcp/token` with `device_code` grant,
//!   honoring `authorization_pending` / `slow_down` per RFC 8628 §3.5.
//! - [`refresh`] — exchanges a refresh token for a new access token
//!   without re-prompting the user (refresh tokens live 30d per the
//!   auth-worker consumer-integration guide).
//!
//! The resulting [`TokenSet`](crate::token_cache::TokenSet) carries the
//! JWT access token plus refresh token. The `github_token` field stays
//! empty here — populating it requires
//! [`introspect`](crate::introspect::introspect), which is a separate
//! step.
//!
//! See `docs/credentials.md` for the end-to-end flow.

use std::time::Duration;

use serde::Deserialize;

use crate::token_cache::TokenSet;
use crate::types::{BrokerError, Result};

/// Default base URL for the auth-worker MCP OAuth Provider. Tests point
/// this at a wiremock server; production keeps the default.
pub const DEFAULT_BASE_URL: &str = "https://auth.ippoan.org";

/// Default static client id for cc-relay. Device flow on auth-worker
/// does not validate `client_id` — the real gate is
/// `GITHUB_MCP_USER_ALLOWLIST` at the GitHub callback (see
/// auth-worker `docs/consumer-integration.md` §3.1).
pub const DEFAULT_CLIENT_ID: &str = "cc-relay";

/// Default MCP scope set. `mcp.write` maps to GitHub `read:user repo` on
/// the auth-worker side, which is the minimum to drive the cc-relay
/// broker (Issues r/w + private repo access).
pub fn default_scopes() -> Vec<String> {
    vec!["mcp.read".to_string(), "mcp.write".to_string()]
}

/// RFC 8628 grant type URI for the device authorization grant.
const GRANT_DEVICE_CODE: &str = "urn:ietf:params:oauth:grant-type:device_code";

/// Configuration shared across all three functions.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Auth-worker base URL — paths like `/mcp/device_authorization` are
    /// appended. No trailing slash.
    pub base_url: String,
    /// Static identifier this binary uses with the auth-worker.
    pub client_id: String,
    /// Scope tokens to request. Joined with `+` (RFC 6749 space-form
    /// encoded as `+` in the form body).
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

/// RFC 8628 §3.2 device authorization response. The `verification_uri_complete`
/// field is optional in the RFC; auth-worker emits it so the CLI can
/// print one click-through link instead of "go here and type this code".
#[derive(Debug, Clone, Deserialize)]
pub struct DeviceAuthorizationResponse {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
    pub expires_in: i64,
    /// Minimum seconds between `poll_token` calls. RFC default 5 if the
    /// server omits it.
    #[serde(default = "default_interval")]
    pub interval: i64,
}

fn default_interval() -> i64 {
    5
}

/// Shape of `POST /mcp/token` success response. Fields not used here
/// (`token_type`, `scope` when null, etc.) are tolerated by `#[serde]`'s
/// default deny-extras=false.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    #[serde(default)]
    scope: Option<String>,
    /// Server-reported lifetime in seconds. RFC 6749 §5.1 allows omission
    /// (we then fall back to JWT `exp`).
    #[serde(default)]
    expires_in: Option<i64>,
}

/// RFC 8628 §3.5 polling error.
#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

/// Kick off a device authorization flow. The returned struct contains
/// the `user_code` + URI the human must visit; the CLI prints these and
/// then calls [`poll_token`].
pub async fn start_device_authorization(
    http: &reqwest::Client,
    cfg: &AuthConfig,
) -> Result<DeviceAuthorizationResponse> {
    let url = format!("{}/mcp/device_authorization", cfg.base_url);
    let scope = cfg.scopes.join(" ");
    let form = [
        ("client_id", cfg.client_id.as_str()),
        ("scope", scope.as_str()),
    ];

    let resp = http
        .post(&url)
        .form(&form)
        .send()
        .await
        .map_err(transport_err)?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(BrokerError::Other(anyhow::anyhow!(
            "device_authorization returned {status}: {body}"
        )));
    }

    resp.json::<DeviceAuthorizationResponse>()
        .await
        .map_err(transport_err)
}

/// Poll `/mcp/token` until the user approves, denies, or the device
/// code expires. Sleeps `device.interval` seconds between polls; widens
/// by +5 on `slow_down` per RFC 8628 §3.5.
///
/// Returns a [`TokenSet`] with the JWT + refresh token populated;
/// `github_token` stays `None` (call
/// [`introspect`](crate::introspect::introspect) to fill it in).
pub async fn poll_token(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    device: &DeviceAuthorizationResponse,
) -> Result<TokenSet> {
    let mut interval_secs = device.interval.max(1);
    loop {
        tokio::time::sleep(Duration::from_secs(interval_secs as u64)).await;

        let resp = post_token_request(
            http,
            cfg,
            &[
                ("grant_type", GRANT_DEVICE_CODE),
                ("device_code", device.device_code.as_str()),
                ("client_id", cfg.client_id.as_str()),
            ],
        )
        .await?;

        match resp {
            TokenPoll::Success(t) => return Ok(t),
            TokenPoll::Pending => continue,
            TokenPoll::SlowDown => {
                interval_secs = interval_secs.saturating_add(5);
                continue;
            }
            TokenPoll::Denied(reason) => {
                return Err(BrokerError::Auth(format!("device flow denied: {reason}")));
            }
            TokenPoll::Expired => {
                return Err(BrokerError::Other(anyhow::anyhow!(
                    "device code expired before user approval"
                )));
            }
            TokenPoll::OtherError(reason) => {
                return Err(BrokerError::Other(anyhow::anyhow!(
                    "token endpoint error: {reason}"
                )));
            }
        }
    }
}

/// Exchange a refresh token for a fresh access token. Used by
/// `TokenManager::ensure_fresh` when the cached access token's `exp`
/// is within the 5-minute skew window.
pub async fn refresh(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    refresh_token: &str,
) -> Result<TokenSet> {
    let resp = post_token_request(
        http,
        cfg,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", cfg.client_id.as_str()),
        ],
    )
    .await?;
    match resp {
        TokenPoll::Success(t) => Ok(t),
        TokenPoll::Denied(reason) => Err(BrokerError::Auth(format!("refresh denied: {reason}"))),
        TokenPoll::Expired => Err(BrokerError::Auth("refresh_token expired".into())),
        TokenPoll::Pending | TokenPoll::SlowDown => Err(BrokerError::Other(anyhow::anyhow!(
            "unexpected pending/slow_down on refresh_token grant"
        ))),
        TokenPoll::OtherError(reason) => Err(BrokerError::Other(anyhow::anyhow!(
            "refresh endpoint error: {reason}"
        ))),
    }
}

/// Internal: which RFC 8628 branch the token endpoint took.
enum TokenPoll {
    Success(TokenSet),
    Pending,
    SlowDown,
    Denied(String),
    Expired,
    OtherError(String),
}

async fn post_token_request(
    http: &reqwest::Client,
    cfg: &AuthConfig,
    form: &[(&str, &str)],
) -> Result<TokenPoll> {
    let url = format!("{}/mcp/token", cfg.base_url);
    let resp = http
        .post(&url)
        .form(form)
        .send()
        .await
        .map_err(transport_err)?;

    let status = resp.status();
    if status.is_success() {
        let t: TokenResponse = resp.json().await.map_err(transport_err)?;
        let acquired_at = crate::token_cache::now_secs();
        // Prefer the server's `expires_in` if present; fall back to the
        // JWT's `exp` claim; last resort, +3600s (matches upstream).
        let expires_at = match t.expires_in {
            Some(n) => acquired_at.saturating_add(n),
            None => jwt_exp(&t.access_token).unwrap_or(acquired_at + 3600),
        };
        return Ok(TokenPoll::Success(TokenSet {
            access_token: t.access_token,
            refresh_token: t.refresh_token,
            scope: t.scope.unwrap_or_default(),
            github_token: None,
            expires_at,
            acquired_at,
        }));
    }

    // RFC 8628 §3.5: token endpoint reports pending/slow_down/etc. with
    // HTTP 400 + JSON body { "error": "...", ... }.
    let err_text = resp.text().await.unwrap_or_default();
    let parsed: std::result::Result<TokenErrorResponse, _> = serde_json::from_str(&err_text);
    let (code, desc) = match parsed {
        Ok(e) => (e.error, e.error_description.unwrap_or_default()),
        Err(_) => (String::new(), err_text),
    };
    Ok(match code.as_str() {
        "authorization_pending" => TokenPoll::Pending,
        "slow_down" => TokenPoll::SlowDown,
        "access_denied" => TokenPoll::Denied(desc),
        "expired_token" => TokenPoll::Expired,
        // Empty `code` means the body wasn't a parseable RFC 8628 error;
        // bubble the raw text up for diagnostics.
        "" => TokenPoll::OtherError(format!("{status}: {desc}")),
        other => TokenPoll::OtherError(format!("{other}: {desc}")),
    })
}

/// Extract the `exp` claim (Unix seconds) from a JWT without verifying
/// the signature. Used only to seed `TokenSet.expires_at` when the
/// token endpoint doesn't return `expires_in`. Verification is the
/// auth-worker's responsibility (we just consume the value).
fn jwt_exp(jwt: &str) -> Option<i64> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let bytes = base64url_decode(payload_b64)?;
    let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    v.get("exp").and_then(|x| x.as_i64())
}

/// Minimal base64url decoder (no padding, `-_` alphabet). Avoids adding
/// the `base64` crate just for one optional code path.
fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(s.len() * 3 / 4 + 2);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for c in s.bytes() {
        let v: u32 = match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'-' => 62,
            b'_' => 63,
            b'=' => break, // padding terminator (rare for JWT)
            _ => return None,
        };
        buf = (buf << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xFF) as u8);
        }
    }
    Some(out)
}

fn transport_err(e: reqwest::Error) -> BrokerError {
    BrokerError::Other(anyhow::Error::new(e).context("HTTP transport (auth-worker)"))
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
    async fn start_device_authorization_happy_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/device_authorization"))
            .and(body_string_contains("client_id=cc-relay"))
            .and(body_string_contains("scope=mcp.read+mcp.write"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "dc-1",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://auth.example/device",
                "verification_uri_complete": "https://auth.example/device?user_code=ABCD-EFGH",
                "expires_in": 600,
                "interval": 1,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let resp = start_device_authorization(&http, &cfg(server.uri()))
            .await
            .unwrap();
        assert_eq!(resp.device_code, "dc-1");
        assert_eq!(resp.user_code, "ABCD-EFGH");
        assert_eq!(resp.interval, 1);
    }

    #[tokio::test]
    async fn poll_token_pending_then_success() {
        let server = MockServer::start().await;
        // First poll: authorization_pending (400 + JSON error).
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "authorization_pending",
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // Second poll: success with explicit expires_in.
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "jwt.payload.sig",
                "refresh_token": "rt-1",
                "scope": "mcp.read mcp.write",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc-1".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0, // zero sleep so the test doesn't drag
        };
        let t = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap();
        assert_eq!(t.access_token, "jwt.payload.sig");
        assert_eq!(t.refresh_token, "rt-1");
        assert_eq!(t.scope, "mcp.read mcp.write");
        assert!(t.github_token.is_none());
        assert!(t.expires_at >= t.acquired_at + 3500);
    }

    #[tokio::test]
    async fn poll_token_denied_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "access_denied",
                "error_description": "user rejected",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc-1".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let err = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[tokio::test]
    async fn poll_token_expired_surfaces_other_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "expired_token",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc-1".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let err = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap_err();
        assert!(matches!(err, BrokerError::Other(_)));
    }

    #[tokio::test]
    async fn refresh_happy_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=rt-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "jwt2.payload.sig",
                "refresh_token": "rt-2",
                "scope": "mcp.read mcp.write",
                "expires_in": 3600,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let t = refresh(&http, &cfg(server.uri()), "rt-1").await.unwrap();
        assert_eq!(t.access_token, "jwt2.payload.sig");
        assert_eq!(t.refresh_token, "rt-2");
    }

    #[tokio::test]
    async fn refresh_denied_surfaces_auth_error() {
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

        let http = reqwest::Client::new();
        let err = refresh(&http, &cfg(server.uri()), "rt-1")
            .await
            .unwrap_err();
        assert!(matches!(err, BrokerError::Auth(_)));
    }

    #[test]
    fn jwt_exp_extracts_from_payload() {
        // Build a JWT with a known exp by base64url-encoding a JSON
        // payload `{"exp": 1700000000}`. Header / sig are stubbed.
        let payload = serde_json::json!({ "exp": 1_700_000_000_i64 });
        let payload_b64 = base64url_encode(serde_json::to_vec(&payload).unwrap().as_slice());
        let jwt = format!("eyJhbGciOiJIUzI1NiJ9.{payload_b64}.sig");
        assert_eq!(jwt_exp(&jwt), Some(1_700_000_000));
    }

    fn base64url_encode(bytes: &[u8]) -> String {
        const ALPH: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut out = String::new();
        let mut buf: u32 = 0;
        let mut bits: u32 = 0;
        for &b in bytes {
            buf = (buf << 8) | b as u32;
            bits += 8;
            while bits >= 6 {
                bits -= 6;
                out.push(ALPH[((buf >> bits) & 0x3F) as usize] as char);
            }
        }
        if bits > 0 {
            out.push(ALPH[((buf << (6 - bits)) & 0x3F) as usize] as char);
        }
        out
    }

    #[test]
    fn base64url_roundtrip() {
        let v = b"hello world!";
        let s = base64url_encode(v);
        let back = base64url_decode(&s).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn base64url_roundtrip_with_trailing_bits() {
        // 1 byte → 2 base64 chars + 4 trailing bits → exercises the
        // `if bits > 0` tail emission in the test-only encoder.
        let v = b"x";
        let s = base64url_encode(v);
        let back = base64url_decode(&s).unwrap();
        assert_eq!(back, v);
        // 2 bytes → 3 base64 chars + 2 trailing bits → also hits the
        // tail emission path.
        let v = b"xy";
        let s = base64url_encode(v);
        let back = base64url_decode(&s).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn base64url_decode_handles_special_alphabet_and_padding() {
        // Exercise the `b'-'`, `b'_'`, and `b'='` (padding terminator)
        // arms of the decoder. We don't pin the exact byte values — just
        // that decoding succeeds and the `=` terminator stops decoding.
        let out = base64url_decode("A-_=").expect("should decode");
        assert!(!out.is_empty());
        // Sanity: `=` truly terminates — any trailing garbage after `=`
        // is ignored.
        let with_garbage = base64url_decode("A-_=zzzz").expect("should decode");
        assert_eq!(out, with_garbage);
    }

    #[test]
    fn base64url_decode_rejects_invalid_chars() {
        // `!` is not in the base64url alphabet → decoder returns None.
        assert!(base64url_decode("abc!").is_none());
    }

    #[test]
    fn default_interval_returns_five() {
        assert_eq!(default_interval(), 5);
    }

    #[test]
    fn auth_config_default_uses_module_constants() {
        let c = AuthConfig::default();
        assert_eq!(c.base_url, DEFAULT_BASE_URL);
        assert_eq!(c.client_id, DEFAULT_CLIENT_ID);
        assert_eq!(c.scopes, default_scopes());
    }

    #[tokio::test]
    async fn start_device_authorization_non_success_surfaces_other_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/device_authorization"))
            .respond_with(ResponseTemplate::new(500).set_body_string("kaboom"))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let err = start_device_authorization(&http, &cfg(server.uri()))
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("device_authorization returned"));
                assert!(s.contains("kaboom"));
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn start_device_authorization_transport_error() {
        // Unreachable address → reqwest transport-layer failure.
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(50))
            .build()
            .unwrap();
        let err = start_device_authorization(&http, &cfg("http://192.0.2.1:1".to_string()))
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
    async fn poll_token_slow_down_widens_interval_then_succeeds() {
        let server = MockServer::start().await;
        // First poll: slow_down (covers the SlowDown branch).
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "slow_down",
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // Second poll: success.
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "a.b.c",
                "refresh_token": "rt",
                "expires_in": 60,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0, // 0 sleep, +5 still ok-ish — but saturating_add(5)=5s.
                         // Set to negative so the .max(1) is exercised AND saturating_add(5) yields 6s
                         // ... Actually keep 0; the first sleep is 1s (max(1)), then +5 = 6s.
                         // We accept ~7s test wall time.
        };
        let t = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap();
        assert_eq!(t.access_token, "a.b.c");
    }

    #[tokio::test]
    async fn poll_token_unparseable_error_body_surfaces_other_error() {
        // 400 with a non-JSON body → parser falls into the `Err(_)` arm
        // of TokenErrorResponse parsing → `code` is empty → OtherError
        // path. Exercises both line 280 and line 289.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_string("not-json"))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let err = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("token endpoint error"), "got {s}");
                assert!(s.contains("not-json"), "expected raw body in err, got {s}");
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_token_unknown_error_code_surfaces_other_error() {
        // 400 + JSON with an unknown `error` code → hits the `other =>`
        // arm of the match (line 290).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "device_code unknown",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let err = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("invalid_grant"), "got {s}");
                assert!(s.contains("device_code unknown"), "got {s}");
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_token_success_without_expires_in_falls_back_to_jwt_exp() {
        // Build a JWT whose `exp` claim is far in the future. The token
        // endpoint returns no `expires_in`, so the code path on line 262
        // (`jwt_exp(...).unwrap_or(...)`) is exercised.
        let payload = serde_json::json!({ "exp": 9_999_999_999_i64 });
        let payload_b64 = base64url_encode(serde_json::to_vec(&payload).unwrap().as_slice());
        let jwt = format!("eyJhbGciOiJIUzI1NiJ9.{payload_b64}.sig");

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": jwt,
                "refresh_token": "rt",
                // expires_in deliberately omitted
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let t = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap();
        assert_eq!(t.expires_at, 9_999_999_999);
    }

    #[tokio::test]
    async fn poll_token_success_without_expires_in_and_bad_jwt_falls_back_to_plus_3600() {
        // No `expires_in` AND an unparseable JWT → unwrap_or branch hits
        // the `acquired_at + 3600` fallback (covers the `unwrap_or` arm).
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "not-a-jwt",
                "refresh_token": "rt",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let http = reqwest::Client::new();
        let device = DeviceAuthorizationResponse {
            device_code: "dc".into(),
            user_code: "X".into(),
            verification_uri: "u".into(),
            verification_uri_complete: None,
            expires_in: 600,
            interval: 0,
        };
        let t = poll_token(&http, &cfg(server.uri()), &device)
            .await
            .unwrap();
        assert!(t.expires_at >= t.acquired_at + 3600);
    }

    #[tokio::test]
    async fn refresh_expired_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "expired_token",
            })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let err = refresh(&http, &cfg(server.uri()), "rt-1")
            .await
            .unwrap_err();
        match err {
            BrokerError::Auth(s) => assert!(s.contains("refresh_token expired")),
            other => panic!("expected Auth, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn refresh_pending_is_unexpected_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "authorization_pending",
            })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let err = refresh(&http, &cfg(server.uri()), "rt-1")
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => assert!(e.to_string().contains("unexpected")),
            other => panic!("expected Other(unexpected), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn refresh_other_error_surfaces_other_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/mcp/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "boom",
            })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let err = refresh(&http, &cfg(server.uri()), "rt-1")
            .await
            .unwrap_err();
        match err {
            BrokerError::Other(e) => {
                let s = e.to_string();
                assert!(s.contains("refresh endpoint error"), "got {s}");
                assert!(s.contains("invalid_grant"), "got {s}");
            }
            other => panic!("expected Other(_), got {other:?}"),
        }
    }
}
