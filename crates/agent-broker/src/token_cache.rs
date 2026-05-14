//! Persistent token cache at `~/.cc-relay/token`.
//!
//! Stores a [`TokenSet`] as plain JSON with file mode `0o600` on Unix.
//! No encryption — the security model is "trusted host, restricted
//! permissions" (same as `github-mcp-server-rs`). Hardening with a
//! keyring / age / chacha20 is tracked separately.
//!
//! Layout:
//!
//! ```text
//! ~/.cc-relay/
//!   token        # JSON, mode 0600
//! ```
//!
//! End-users run `rust-mcp-agent auth` on the host (Claude Code on Web
//! sandbox cannot reach `auth.ippoan.org` — see
//! `docs/relay-validation.md`). The resulting file is read-only-mounted
//! into the sandbox; the broker process only reads it.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::types::{BrokerError, Result};

/// Cached OAuth token bundle.
///
/// `access_token` is the JWT issued by auth-worker. `github_token` is
/// the raw GitHub OAuth token returned by `/mcp/introspect`; the broker
/// uses *that* for `api.github.com` calls. The JWT is held only to
/// re-introspect after a refresh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenSet {
    /// JWT from `POST /mcp/token`.
    pub access_token: String,
    /// Refresh token from `POST /mcp/token` (30d TTL on auth-worker).
    pub refresh_token: String,
    /// Scope string as returned by the token endpoint (e.g.
    /// `"mcp.read mcp.write"`).
    #[serde(default)]
    pub scope: String,
    /// Raw GitHub OAuth token, populated by
    /// [`introspect`](crate::introspect::introspect). `None` until the
    /// first introspect succeeds.
    #[serde(default)]
    pub github_token: Option<String>,
    /// Unix seconds at which `access_token` expires.
    pub expires_at: i64,
    /// Unix seconds at which this `TokenSet` was acquired (i.e. when
    /// `POST /mcp/token` returned). Useful for refresh telemetry.
    pub acquired_at: i64,
}

impl TokenSet {
    /// Return `true` if the access token's `expires_at` falls within
    /// `skew_secs` of now. The broker uses `skew_secs = 300` so it
    /// refreshes before tokens actually expire.
    pub fn is_expired(&self, skew_secs: i64) -> bool {
        self.expires_at <= now_secs().saturating_add(skew_secs)
    }

    /// Return a copy of this set with `github_token` populated. Used
    /// after an introspect response.
    pub fn with_github_token(mut self, t: impl Into<String>) -> Self {
        self.github_token = Some(t.into());
        self
    }
}

/// Default path: `$HOME/.cc-relay/token`. Returns an error if `$HOME`
/// cannot be resolved (which would be deeply unusual on Linux).
pub fn default_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| {
        BrokerError::Other(anyhow::anyhow!(
            "cannot resolve $HOME; set --token-path explicitly",
        ))
    })?;
    Ok(home.join(".cc-relay").join("token"))
}

/// Read a [`TokenSet`] from `path`. Returns `Ok(None)` if the file does
/// not exist — the broker treats that as "user has not run
/// `rust-mcp-agent auth` yet". Any other I/O / parse error surfaces as
/// `BrokerError::Other`.
pub fn load(path: &Path) -> Result<Option<TokenSet>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(BrokerError::Other(
                anyhow::Error::new(e).context(format!("read token cache {}", path.display())),
            ));
        }
    };
    let t: TokenSet = serde_json::from_slice(&bytes).map_err(|e| {
        BrokerError::Other(
            anyhow::Error::new(e).context(format!("parse token cache {}", path.display())),
        )
    })?;
    Ok(Some(t))
}

/// Write `t` to `path` atomically (write to `path.tmp`, fsync, rename)
/// with mode `0o600` on Unix. Creates parent directories with mode
/// `0o700`. Replaces any prior file.
pub fn save(path: &Path, t: &TokenSet) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            BrokerError::Other(
                anyhow::Error::new(e).context(format!("create dir {}", parent.display())),
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let bytes = serde_json::to_vec_pretty(t)
        .map_err(|e| BrokerError::Other(anyhow::Error::new(e).context("serialize TokenSet")))?;

    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &bytes).map_err(|e| {
        BrokerError::Other(anyhow::Error::new(e).context(format!("write tmp {}", tmp.display())))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
            BrokerError::Other(
                anyhow::Error::new(e).context(format!("chmod 0600 {}", tmp.display())),
            )
        })?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        BrokerError::Other(anyhow::Error::new(e).context(format!(
            "rename {} -> {}",
            tmp.display(),
            path.display()
        )))
    })?;
    Ok(())
}

/// Remove the token file. `Ok(())` even if the file did not exist.
pub fn delete(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(BrokerError::Other(
            anyhow::Error::new(e).context(format!("remove {}", path.display())),
        )),
    }
}

/// Current wall-clock as Unix seconds, clamped at 0. Exposed for
/// other modules in the crate (auth.rs uses it to stamp
/// `acquired_at` consistently).
pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(expires_at: i64) -> TokenSet {
        TokenSet {
            access_token: "jwt.payload.sig".into(),
            refresh_token: "rt-1".into(),
            scope: "mcp.read mcp.write".into(),
            github_token: None,
            expires_at,
            acquired_at: 1_700_000_000,
        }
    }

    #[test]
    fn roundtrip_json() {
        let t = sample(1_700_003_600).with_github_token("gho_xxx");
        let s = serde_json::to_string(&t).unwrap();
        let back: TokenSet = serde_json::from_str(&s).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn json_tolerates_missing_optional_fields() {
        // Minimum fields a legacy / hand-crafted cache might contain.
        let s = r#"{
            "access_token": "a",
            "refresh_token": "r",
            "expires_at": 1700003600,
            "acquired_at": 1700000000
        }"#;
        let t: TokenSet = serde_json::from_str(s).unwrap();
        assert_eq!(t.scope, "");
        assert!(t.github_token.is_none());
    }

    #[test]
    fn is_expired_respects_skew() {
        let now = now_secs();
        // expires in 10 minutes — not expired under a 5 min skew.
        let fresh = sample(now + 600);
        assert!(!fresh.is_expired(300));
        // expires in 2 minutes — under a 5 min skew, treated as expired.
        let stale = sample(now + 120);
        assert!(stale.is_expired(300));
        // expired in the past — always expired.
        let dead = sample(now - 10);
        assert!(dead.is_expired(0));
    }

    #[test]
    fn with_github_token_sets_field() {
        let t = sample(1_700_003_600).with_github_token("gho_test");
        assert_eq!(t.github_token.as_deref(), Some("gho_test"));
    }

    #[test]
    fn load_missing_file_returns_none() {
        let dir = tempdir();
        let path = dir.path.join("nope");
        assert!(load(&path).unwrap().is_none());
    }

    #[test]
    fn save_then_load_roundtrips() {
        let dir = tempdir();
        let path = dir.path.join("inner").join("token");
        let t = sample(1_700_003_600).with_github_token("gho_xxx");
        save(&path, &t).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded, t);
    }

    #[cfg(unix)]
    #[test]
    fn save_sets_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir();
        let path = dir.path.join("token");
        save(&path, &sample(1_700_003_600)).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        // mode bits include the file-type prefix; mask down to perms.
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn save_overwrites_existing() {
        let dir = tempdir();
        let path = dir.path.join("token");
        save(&path, &sample(1)).unwrap();
        save(&path, &sample(2)).unwrap();
        let loaded = load(&path).unwrap().unwrap();
        assert_eq!(loaded.expires_at, 2);
    }

    #[test]
    fn delete_missing_is_ok() {
        let dir = tempdir();
        delete(&dir.path.join("nope")).unwrap();
    }

    #[test]
    fn delete_removes_file() {
        let dir = tempdir();
        let path = dir.path.join("token");
        save(&path, &sample(1)).unwrap();
        delete(&path).unwrap();
        assert!(load(&path).unwrap().is_none());
    }

    /// Minimal scoped temp dir — pulling in `tempfile` for two tests
    /// isn't worth the dev-dep bump. Cleans up on Drop.
    struct TmpDir {
        path: PathBuf,
    }
    fn tempdir() -> TmpDir {
        let base = std::env::temp_dir();
        let unique = format!(
            "cc-relay-token-cache-{}-{}",
            std::process::id(),
            now_secs_nanos(),
        );
        let p = base.join(unique);
        std::fs::create_dir_all(&p).unwrap();
        TmpDir { path: p }
    }
    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
    fn now_secs_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    }
}
