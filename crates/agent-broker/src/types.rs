//! Value types shared across every [`Broker`](crate::Broker) impl.
//!
//! These are intentionally backend-agnostic: a `GitHubBroker` and a
//! future `PubSubBroker` see the same [`AgentMeta`], [`Cursor`], and
//! [`BrokerError`]. Only the bytes-on-the-wire layout (Issue body JSON
//! vs. Pub/Sub message attributes vs. …) is backend-specific.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// One participant in a cc-relay session.
///
/// Brokers maintain a list of currently-live agents. In the GitHub
/// backend the list lives in the broker Issue body's `agents` array;
/// stale entries are pruned by [`Broker::leave`](crate::Broker::leave)
/// or by a TTL the concrete backend decides on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMeta {
    /// Identity used in
    /// [`NotifyMessage::from`](agent_core::NotifyMessage::from) and
    /// [`NotifyTarget::Agent`](agent_core::NotifyTarget::Agent).
    pub agent_id: String,

    /// Milliseconds since the UNIX epoch at this agent's most recent
    /// [`Broker::join`](crate::Broker::join). Used by brokers to TTL-out
    /// inactive entries.
    pub joined_at: i64,
}

impl AgentMeta {
    /// Build an [`AgentMeta`] for `agent_id` with `joined_at` stamped at
    /// the wall clock now.
    pub fn now(agent_id: impl Into<String>) -> Self {
        let joined_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            agent_id: agent_id.into(),
            joined_at,
        }
    }
}

/// Resume point for
/// [`Broker::fetch_since`](crate::Broker::fetch_since).
///
/// Pairs a monotonic comment id (the highest id we have already drained)
/// with the last `ETag` observed on the snapshot document. The ETag lets
/// brokers short-circuit unchanged-body reads with a conditional GET so
/// idle agents do not burn rate limit on otherwise-empty polls.
///
/// Backends without numeric ids may stash any monotonic token they like
/// in `last_comment_id`; this type stays `u64` because the GitHub case
/// is the only consumer for the MVP and it pays no encoding cost.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Cursor {
    /// Highest comment id consumed by
    /// [`Broker::fetch_since`](crate::Broker::fetch_since). `0` means
    /// "nothing consumed yet".
    #[serde(default)]
    pub last_comment_id: u64,

    /// Last `ETag` header observed on the snapshot document. Sent as
    /// `If-None-Match` on the next snapshot GET; `None` forces a full
    /// read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_etag: Option<String>,
}

impl Cursor {
    /// A fresh cursor that has consumed nothing.
    pub fn beginning() -> Self {
        Self::default()
    }
}

/// Errors a [`Broker`](crate::Broker) method can produce.
///
/// The enum is exhaustive for the cases callers need to dispatch on
/// (auth failures, CAS conflicts, rate limiting, missing resources).
/// Anything else collapses to [`BrokerError::Other`] carrying an
/// [`anyhow::Error`] for context.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    /// Authentication failed — for example, an installation token has
    /// expired or been revoked. Callers should refresh credentials and
    /// retry.
    #[error("authentication failed: {0}")]
    Auth(String),

    /// CAS write lost too many races. The broker has exhausted its own
    /// retry budget; the caller decides whether the op is still
    /// relevant.
    #[error("CAS conflict after {retries} retries")]
    Conflict { retries: u32 },

    /// Backend signalled rate-limit exhaustion. `reset_epoch_ms` is when
    /// the limit is expected to lift. Brokers sleep internally when
    /// they can; this variant surfaces only when the wait would exceed
    /// the broker's budget.
    #[error("rate limited until epoch ms {reset_epoch_ms}")]
    RateLimited {
        /// Unix epoch milliseconds at which the limit is expected to
        /// lift.
        reset_epoch_ms: i64,
    },

    /// Requested resource (issue, comment, task id) does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// Catch-all wrapping an [`anyhow::Error`] for unexpected failures.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Shorthand result alias for broker operations.
pub type Result<T> = std::result::Result<T, BrokerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_default_is_beginning() {
        let c = Cursor::default();
        assert_eq!(c, Cursor::beginning());
        assert_eq!(c.last_comment_id, 0);
        assert_eq!(c.last_etag, None);
    }

    #[test]
    fn cursor_roundtrip_preserves_etag() {
        let c = Cursor {
            last_comment_id: 12345,
            last_etag: Some("\"deadbeef\"".into()),
        };
        let s = serde_json::to_string(&c).unwrap();
        let back: Cursor = serde_json::from_str(&s).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn cursor_omits_etag_when_none() {
        let c = Cursor::beginning();
        let s = serde_json::to_string(&c).unwrap();
        // ETag is `skip_serializing_if = "Option::is_none"`, so an empty
        // cursor is just the comment id.
        assert_eq!(s, r#"{"last_comment_id":0}"#);
    }

    #[test]
    fn agent_meta_now_produces_recent_timestamp() {
        let m = AgentMeta::now("alice");
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(m.joined_at <= now_ms);
        assert!(now_ms - m.joined_at < 5_000);
        assert_eq!(m.agent_id, "alice");
    }

    #[test]
    fn agent_meta_roundtrip() {
        let m = AgentMeta {
            agent_id: "alice".into(),
            joined_at: 1_700_000_000_000,
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: AgentMeta = serde_json::from_str(&s).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn broker_error_display_strings_are_stable() {
        // These show up in tracing logs and user-facing tool errors;
        // pin the format so a refactor cannot accidentally garble them.
        assert_eq!(
            BrokerError::Auth("bad token".into()).to_string(),
            "authentication failed: bad token"
        );
        assert_eq!(
            BrokerError::Conflict { retries: 3 }.to_string(),
            "CAS conflict after 3 retries"
        );
        assert_eq!(
            BrokerError::RateLimited {
                reset_epoch_ms: 1_700_000_000_000
            }
            .to_string(),
            "rate limited until epoch ms 1700000000000"
        );
        assert_eq!(
            BrokerError::NotFound("task T-1".into()).to_string(),
            "not found: task T-1"
        );
    }
}
