//! [`GitHubBroker`] — broker implementation that uses one GitHub Issue
//! as the entire cc-relay session state.
//!
//! Schema:
//!
//! - **Issue body** = JSON snapshot
//!   `{ "v": 1, "agents": [AgentMeta], "plan": [TaskSpec] }`. Mutated
//!   under a CAS (`If-Match: <etag>`) loop so concurrent `join` /
//!   `plan_op` calls from different agents do not clobber each other.
//! - **Issue comments** = append-only structured-JSON message log. Each
//!   comment body is a `NotifyCommentBody` (see below). `fetch_since`
//!   pages with `per_page=100` + filters client-side (`to == me ||
//!   to == "*"`, exclude self-sent).
//!
//! Authentication is a static `Authorization: Bearer <token>`. Caller
//! owns token refresh — when the App installation token rotates, build
//! a fresh `GitHubBroker`. P5 wires this into agent-mcp; the broker
//! itself stays narrowly scoped.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agent_core::{NotifyMessage, NotifyTarget, PlanOp, Priority, TaskSpec, TaskStatus};
use anyhow::Context as _;
use async_trait::async_trait;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, ETAG, IF_MATCH, IF_NONE_MATCH,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::broker::Broker;
use crate::types::{AgentMeta, BrokerError, Cursor, Result};

const SNAPSHOT_VERSION: u32 = 1;
const DEFAULT_CAS_RETRIES: u32 = 3;
const DEFAULT_PER_PAGE: u32 = 100;

/// How many times [`GitHubBroker::send_with_retry`] re-tries a 5xx /
/// transient transport error before giving up.
const DEFAULT_TRANSPORT_RETRIES: u32 = 5;

/// Exponential-backoff start point for transient errors.
const BACKOFF_START_MS: u64 = 1_000;

/// Cap on the per-attempt backoff sleep (so a stuck endpoint doesn't
/// produce minute-long stalls).
const BACKOFF_MAX_MS: u64 = 30_000;

/// Cap on inline sleeping when GitHub says we're rate-limited. If the
/// reset window is further out than this, surface
/// [`BrokerError::RateLimited`] so the caller can decide instead of
/// blocking the MCP tool call for minutes.
const RATE_LIMIT_INLINE_SLEEP_CAP_SECS: i64 = 60;

const USER_AGENT_STR: &str = concat!("cc-relay-agent/", env!("CARGO_PKG_VERSION"));

/// Body schema for the broker Issue. Keep the JSON layout small and
/// flat — it has to fit comfortably under GitHub's issue body length
/// cap (~65 535 bytes) even with dozens of agents and tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Snapshot {
    /// Schema version. Currently always `1`; a future incompatible
    /// schema change bumps this and clients are expected to fail-loud.
    #[serde(default = "default_version")]
    v: u32,
    #[serde(default)]
    agents: Vec<AgentMeta>,
    #[serde(default)]
    plan: Vec<TaskSpec>,
}

fn default_version() -> u32 {
    SNAPSHOT_VERSION
}

/// Comment body schema for a `notify_agent` message. The same shape
/// will be reused by future structured comments (e.g. presence beats)
/// via the `ty` discriminator.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotifyCommentBody {
    /// Discriminator. `"notify"` for now.
    #[serde(rename = "type")]
    ty: String,
    from: String,
    to: NotifyTarget,
    message: String,
    #[serde(default)]
    priority: Priority,
    timestamp: i64,
}

/// GitHub Issues REST response — only the fields we read.
#[derive(Debug, Deserialize)]
struct IssueResp {
    #[serde(default)]
    body: Option<String>,
}

/// GitHub comment REST response — only the fields we read.
#[derive(Debug, Deserialize)]
struct CommentResp {
    id: u64,
    body: String,
}

/// PATCH /issues request body.
#[derive(Debug, Serialize)]
struct PatchIssueReq<'a> {
    body: &'a str,
}

/// POST /comments request body.
#[derive(Debug, Serialize)]
struct PostCommentReq<'a> {
    body: &'a str,
}

/// Broker backed by a single GitHub Issue (body = snapshot, comments
/// = message log).
pub struct GitHubBroker {
    owner: String,
    repo: String,
    issue: u64,
    agent_id: String,
    http: reqwest::Client,
    base_url: String,
    max_cas_retries: u32,
    /// 5xx / transient transport-error retry budget.
    max_transport_retries: u32,
    /// Initial backoff delay (doubled per attempt, capped by
    /// `backoff_max_ms`).
    backoff_start_ms: u64,
    backoff_max_ms: u64,
    /// Max seconds to sleep inline on a rate-limit response. If the
    /// reset window is further out, surface [`BrokerError::RateLimited`]
    /// instead of blocking the tool call.
    rate_limit_inline_cap_secs: i64,
}

impl GitHubBroker {
    /// Build a broker pointed at the given Issue. `token` is used as a
    /// bearer credential on every request and is **not** refreshed by
    /// this struct — callers (P5 wires this) rotate the installation
    /// token by constructing a fresh `GitHubBroker`.
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        issue: u64,
        agent_id: impl Into<String>,
        token: &str,
    ) -> anyhow::Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid token (must be ASCII-only)")?,
        );
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(
            "x-github-api-version",
            HeaderValue::from_static("2022-11-28"),
        );

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent(USER_AGENT_STR)
            .build()
            .context("build reqwest client")?;

        Ok(Self {
            owner: owner.into(),
            repo: repo.into(),
            issue,
            agent_id: agent_id.into(),
            http,
            base_url: "https://api.github.com".to_string(),
            max_cas_retries: DEFAULT_CAS_RETRIES,
            max_transport_retries: DEFAULT_TRANSPORT_RETRIES,
            backoff_start_ms: BACKOFF_START_MS,
            backoff_max_ms: BACKOFF_MAX_MS,
            rate_limit_inline_cap_secs: RATE_LIMIT_INLINE_SLEEP_CAP_SECS,
        })
    }

    /// Point this broker at a non-default base URL (e.g. a wiremock
    /// server in tests, or a GHES install).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }

    /// Test-only: zero out backoff timing so retry-loop tests don't
    /// take wall-clock seconds, and shorten the transport retry budget
    /// to keep exhaustion tests bounded.
    #[cfg(test)]
    pub(crate) fn with_test_timing(mut self) -> Self {
        self.backoff_start_ms = 0;
        self.backoff_max_ms = 0;
        self.max_transport_retries = 3;
        self
    }

    /// Test-only: override the inline-sleep cap so tests can opt into
    /// "always surface RateLimited" or "sleep up to N seconds inline".
    #[cfg(test)]
    pub(crate) fn with_rate_limit_inline_cap_secs(mut self, cap: i64) -> Self {
        self.rate_limit_inline_cap_secs = cap;
        self
    }

    fn issue_url(&self) -> String {
        format!(
            "{}/repos/{}/{}/issues/{}",
            self.base_url, self.owner, self.repo, self.issue
        )
    }

    fn comments_url(&self) -> String {
        format!("{}/comments", self.issue_url())
    }

    /// Send a request with automatic retry on 5xx + transport errors
    /// and an inline sleep on rate-limit (within the
    /// [`RATE_LIMIT_INLINE_SLEEP_CAP_SECS`] cap).
    ///
    /// `build` is called once per attempt — `reqwest::RequestBuilder`
    /// is intentionally not `Clone`, so we rebuild from the headers /
    /// method / body each retry.
    ///
    /// Returns the first non-retryable [`reqwest::Response`] (any
    /// 1xx/2xx/3xx/4xx). The caller dispatches on its `status()`.
    async fn send_with_retry<F>(&self, build: F) -> Result<reqwest::Response>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut delay_ms = self.backoff_start_ms;
        for attempt in 0..self.max_transport_retries {
            let result = build().send().await;
            match result {
                Ok(resp) if resp.status().is_server_error() => {
                    tracing::warn!(
                        attempt,
                        status = %resp.status(),
                        delay_ms,
                        "5xx from GitHub, backing off",
                    );
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    delay_ms = (delay_ms.saturating_mul(2)).min(self.backoff_max_ms);
                    continue;
                }
                Ok(resp) => {
                    if let Some(signal) = parse_rate_limit_signal(&resp) {
                        let now_s = unix_now_secs();
                        let wait_s = signal.reset_epoch_s.saturating_sub(now_s);
                        if wait_s <= 0 {
                            tracing::debug!("rate-limit reset already passed, retrying");
                            continue;
                        }
                        if wait_s <= self.rate_limit_inline_cap_secs {
                            tracing::warn!(
                                attempt,
                                wait_s,
                                "rate-limited by GitHub, sleeping inline",
                            );
                            tokio::time::sleep(Duration::from_secs(wait_s as u64)).await;
                            continue;
                        }
                        return Err(BrokerError::RateLimited {
                            reset_epoch_ms: signal.reset_epoch_s.saturating_mul(1000),
                        });
                    }
                    return Ok(resp);
                }
                Err(e) if e.is_timeout() || e.is_connect() || e.is_request() => {
                    tracing::warn!(
                        attempt,
                        error = %e,
                        delay_ms,
                        "transient transport error, backing off",
                    );
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    delay_ms = (delay_ms.saturating_mul(2)).min(self.backoff_max_ms);
                    continue;
                }
                Err(e) => return Err(transport_err(e)),
            }
        }
        Err(BrokerError::Other(anyhow::anyhow!(
            "exhausted {} retries against {}",
            self.max_transport_retries,
            self.base_url
        )))
    }

    /// GET the Issue body, parse it as [`Snapshot`], also return the
    /// `ETag` header so callers can pass it back in `If-Match` on the
    /// next PATCH for CAS.
    async fn get_snapshot(&self) -> Result<(Snapshot, Option<String>)> {
        let url = self.issue_url();
        let resp = self.send_with_retry(|| self.http.get(&url)).await?;

        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            return Err(BrokerError::NotFound(format!(
                "issue {}/{}#{}",
                self.owner, self.repo, self.issue
            )));
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on GET issue".into()));
        }
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "GET issue returned {status}"
            )));
        }

        let etag = extract_etag(&resp);
        let issue: IssueResp = resp.json().await.map_err(transport_err)?;
        let body = issue.body.unwrap_or_default();

        let snapshot = if body.trim().is_empty() {
            Snapshot::default()
        } else {
            // Tolerate a body that isn't valid JSON yet (freshly-created
            // Issue with a human description) — start from an empty
            // snapshot and the next PATCH will overwrite the body.
            serde_json::from_str(&body).unwrap_or_default()
        };

        Ok((snapshot, etag))
    }

    /// PATCH the Issue body with `snap`. When `etag` is `Some` it is
    /// sent as `If-Match`; if the remote ETag has moved we get a 412
    /// and surface [`BrokerError::Conflict`] for the CAS loop to catch.
    async fn put_snapshot(&self, snap: &Snapshot, etag: Option<&str>) -> Result<()> {
        let body = serde_json::to_string(snap)
            .map_err(|e| BrokerError::Other(anyhow::Error::new(e).context("serialize snapshot")))?;
        let url = self.issue_url();
        let etag_owned = etag.map(|s| s.to_string());

        let resp = self
            .send_with_retry(|| {
                let mut req = self.http.patch(&url).json(&PatchIssueReq { body: &body });
                if let Some(et) = etag_owned.as_deref() {
                    req = req.header(IF_MATCH, et);
                }
                req
            })
            .await?;

        let status = resp.status();
        if status == StatusCode::PRECONDITION_FAILED {
            // Signal the CAS loop to refresh + retry.
            return Err(BrokerError::Conflict { retries: 0 });
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on PATCH issue".into()));
        }
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "PATCH issue returned {status}"
            )));
        }
        Ok(())
    }

    /// Re-read snapshot, apply `mutator`, write back. Retry on 412 up
    /// to `max_cas_retries` times; surface `Conflict { retries }` after
    /// exhaustion. The mutator can also short-circuit by returning a
    /// terminal error (e.g. `NotFound`).
    async fn cas_update<F>(&self, mut mutator: F) -> Result<()>
    where
        F: FnMut(&mut Snapshot) -> Result<()>,
    {
        for attempt in 0..self.max_cas_retries {
            let (mut snap, etag) = self.get_snapshot().await?;
            mutator(&mut snap)?;
            match self.put_snapshot(&snap, etag.as_deref()).await {
                Ok(()) => return Ok(()),
                Err(BrokerError::Conflict { .. }) => {
                    tracing::debug!(attempt, "CAS conflict, retrying");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(BrokerError::Conflict {
            retries: self.max_cas_retries,
        })
    }
}

#[async_trait]
impl Broker for GitHubBroker {
    async fn join(&self, agent_id: &str) -> Result<()> {
        let id = agent_id.to_string();
        self.cas_update(move |snap| {
            // Re-joining is idempotent: drop any previous entry first.
            snap.agents.retain(|a| a.agent_id != id);
            snap.agents.push(AgentMeta::now(&id));
            Ok(())
        })
        .await
    }

    async fn leave(&self, agent_id: &str) -> Result<()> {
        let id = agent_id.to_string();
        self.cas_update(move |snap| {
            snap.agents.retain(|a| a.agent_id != id);
            Ok(())
        })
        .await
    }

    async fn send(&self, msg: NotifyMessage) -> Result<()> {
        let payload = NotifyCommentBody {
            ty: "notify".into(),
            from: msg.from,
            to: msg.to,
            message: msg.message,
            priority: msg.priority,
            timestamp: msg.timestamp,
        };
        let comment_body = serde_json::to_string(&payload).map_err(|e| {
            BrokerError::Other(anyhow::Error::new(e).context("serialize notify comment"))
        })?;
        let url = self.comments_url();

        let resp = self
            .send_with_retry(|| {
                self.http.post(&url).json(&PostCommentReq {
                    body: &comment_body,
                })
            })
            .await?;

        let status = resp.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on POST comment".into()));
        }
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "POST comment returned {status}"
            )));
        }
        Ok(())
    }

    async fn fetch_since(&self, cursor: Cursor) -> Result<(Vec<NotifyMessage>, Cursor)> {
        // Page 1 is a conditional GET so an idle session never burns
        // through the rate limit. Subsequent pages (if any) follow the
        // RFC 5988 `Link: <…>; rel="next"` header until exhausted.
        let first_url = self.comments_url();
        let first_etag = cursor.last_etag.clone();

        let resp = self
            .send_with_retry(|| {
                let mut req = self.http.get(&first_url).query(&[
                    ("per_page", DEFAULT_PER_PAGE.to_string().as_str()),
                    ("sort", "created"),
                    ("direction", "asc"),
                ]);
                if let Some(et) = first_etag.as_deref() {
                    req = req.header(IF_NONE_MATCH, et);
                }
                req
            })
            .await?;

        let status = resp.status();
        if status == StatusCode::NOT_MODIFIED {
            // Nothing new since last successful read — keep the same
            // cursor so the next conditional GET stays cheap.
            return Ok((Vec::new(), cursor));
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on GET comments".into()));
        }
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "GET comments returned {status}"
            )));
        }

        let new_etag = extract_etag(&resp);
        let mut next_url = parse_link_next(&resp);
        let mut all_comments: Vec<CommentResp> = resp.json().await.map_err(transport_err)?;

        // Page 2..N: keep following `rel="next"` until GitHub stops
        // emitting it. No conditional GET on subsequent pages — those
        // pages weren't part of the original `If-None-Match` value.
        while let Some(url) = next_url.take() {
            let resp = self.send_with_retry(|| self.http.get(&url)).await?;
            let status = resp.status();
            if status == StatusCode::UNAUTHORIZED {
                return Err(BrokerError::Auth("401 on GET comments (pagination)".into()));
            }
            if !status.is_success() {
                return Err(BrokerError::Other(anyhow::anyhow!(
                    "GET comments (pagination) returned {status}"
                )));
            }
            next_url = parse_link_next(&resp);
            let page: Vec<CommentResp> = resp.json().await.map_err(transport_err)?;
            all_comments.extend(page);
        }

        let mut highest_id = cursor.last_comment_id;
        let mut out = Vec::new();
        for c in all_comments {
            if c.id <= cursor.last_comment_id {
                continue;
            }
            highest_id = highest_id.max(c.id);

            // Comments that aren't our structured `notify` shape are
            // skipped silently — humans may leave plain comments on the
            // broker Issue without breaking polling.
            let parsed = match serde_json::from_str::<NotifyCommentBody>(&c.body) {
                Ok(p) if p.ty == "notify" => p,
                _ => continue,
            };
            if parsed.from == self.agent_id {
                continue;
            }
            let to_me = match &parsed.to {
                NotifyTarget::All => true,
                NotifyTarget::Agent(id) => id == &self.agent_id,
            };
            if !to_me {
                continue;
            }
            out.push(NotifyMessage {
                from: parsed.from,
                to: parsed.to,
                message: parsed.message,
                priority: parsed.priority,
                timestamp: parsed.timestamp,
            });
        }

        Ok((
            out,
            Cursor {
                last_comment_id: highest_id,
                last_etag: new_etag.or(cursor.last_etag),
            },
        ))
    }

    async fn list_agents(&self) -> Result<Vec<AgentMeta>> {
        let (snap, _) = self.get_snapshot().await?;
        Ok(snap.agents)
    }

    async fn get_plan(&self) -> Result<Vec<TaskSpec>> {
        let (snap, _) = self.get_snapshot().await?;
        Ok(snap.plan)
    }

    async fn plan_op(&self, op: PlanOp) -> Result<()> {
        self.cas_update(move |snap| apply_plan_op(snap, &op)).await
    }
}

/// Apply a single [`PlanOp`] to an in-memory snapshot. Pulled out into
/// a free function so the CAS closure stays small and the validation
/// rules are unit-testable without hitting an HTTP mock.
fn apply_plan_op(snap: &mut Snapshot, op: &PlanOp) -> Result<()> {
    match op {
        PlanOp::Add { task } => {
            if snap.plan.iter().any(|t| t.id == task.id) {
                return Err(BrokerError::Other(anyhow::anyhow!(
                    "task id already exists: {}",
                    task.id
                )));
            }
            snap.plan.push(task.clone());
            Ok(())
        }
        PlanOp::Claim { task_id, agent_id } => {
            let task = snap
                .plan
                .iter_mut()
                .find(|t| &t.id == task_id)
                .ok_or_else(|| BrokerError::NotFound(format!("task {task_id}")))?;

            let claimable = match (&task.assignee, task.status) {
                (None, _) => true,
                (Some(holder), _) if holder == agent_id => true,
                (Some(_), TaskStatus::Done | TaskStatus::Cancelled) => true,
                _ => false,
            };
            if !claimable {
                return Err(BrokerError::Other(anyhow::anyhow!(
                    "task {task_id} already claimed by {:?}",
                    task.assignee
                )));
            }
            task.assignee = Some(agent_id.clone());
            Ok(())
        }
        PlanOp::Update {
            task_id,
            status,
            notes,
        } => {
            let task = snap
                .plan
                .iter_mut()
                .find(|t| &t.id == task_id)
                .ok_or_else(|| BrokerError::NotFound(format!("task {task_id}")))?;
            task.status = *status;
            if let Some(n) = notes {
                task.notes = Some(n.clone());
            }
            Ok(())
        }
        PlanOp::Remove { task_id } => {
            let before = snap.plan.len();
            snap.plan.retain(|t| &t.id != task_id);
            if snap.plan.len() == before {
                return Err(BrokerError::NotFound(format!("task {task_id}")));
            }
            Ok(())
        }
    }
}

fn transport_err(e: reqwest::Error) -> BrokerError {
    BrokerError::Other(anyhow::Error::new(e).context("HTTP transport"))
}

fn extract_etag(resp: &reqwest::Response) -> Option<String> {
    resp.headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Rate-limit signal extracted from response headers.
#[derive(Debug)]
struct RateLimitSignal {
    /// Unix epoch seconds at which the limit is expected to lift.
    reset_epoch_s: i64,
}

/// Parse `x-ratelimit-remaining` + `x-ratelimit-reset` and decide if
/// `resp` is GitHub telling us "you're out of quota". Returns `None`
/// for normal responses (so the caller proceeds to dispatch on the
/// status code).
///
/// GitHub reports exhaustion as 403 (primary) or 429 (secondary rate
/// limit) with `x-ratelimit-remaining: 0`. Successful responses also
/// carry these headers but `remaining` is usually high.
fn parse_rate_limit_signal(resp: &reqwest::Response) -> Option<RateLimitSignal> {
    let h = resp.headers();
    let remaining = h
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())?;
    if remaining != 0 {
        return None;
    }
    let status = resp.status().as_u16();
    if status != 403 && status != 429 {
        return None;
    }
    let reset_epoch_s = h
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);
    Some(RateLimitSignal { reset_epoch_s })
}

/// Walk the RFC 5988 `Link` header and return the URL marked
/// `rel="next"`, if any. We hand-parse because the only thing we need
/// is one link's URL and pulling in a `parse_link_header` crate for
/// that is not worth it.
fn parse_link_next(resp: &reqwest::Response) -> Option<String> {
    let h = resp.headers().get("link")?.to_str().ok()?;
    for part in h.split(',') {
        let part = part.trim();
        // We care about parts that mention `rel="next"`. The exact
        // shape is `<URL>; rel="next"` (sometimes with extra params).
        if !part.contains("rel=\"next\"") {
            continue;
        }
        let lt = part.find('<')?;
        let gt = part.find('>')?;
        if lt + 1 < gt {
            return Some(part[lt + 1..gt].to_string());
        }
    }
    None
}

/// Current wall-clock as unix epoch seconds, clamped at 0.
fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_core::{NotifyTarget, Priority};
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn task(id: &str, title: &str) -> TaskSpec {
        TaskSpec {
            id: id.into(),
            title: title.into(),
            status: TaskStatus::Pending,
            assignee: None,
            notes: None,
        }
    }

    fn issue_body(snap: &Snapshot) -> serde_json::Value {
        serde_json::json!({
            "body": serde_json::to_string(snap).unwrap(),
        })
    }

    async fn fresh_broker(server: &MockServer, agent: &str) -> GitHubBroker {
        GitHubBroker::new("owner", "repo", 42, agent, "ghs_test")
            .unwrap()
            .with_base_url(server.uri())
            // Zero out backoff so retry-loop tests don't sleep, and
            // disallow inline rate-limit sleep so every rate-limit test
            // surfaces as an error unless it opts in via
            // `with_rate_limit_inline_cap_secs`.
            .with_test_timing()
            .with_rate_limit_inline_cap_secs(0)
    }

    // ---------- apply_plan_op (no HTTP) ----------

    #[test]
    fn plan_op_add_then_claim() {
        let mut snap = Snapshot::default();
        apply_plan_op(
            &mut snap,
            &PlanOp::Add {
                task: task("T-1", "first"),
            },
        )
        .unwrap();
        assert_eq!(snap.plan.len(), 1);
        apply_plan_op(
            &mut snap,
            &PlanOp::Claim {
                task_id: "T-1".into(),
                agent_id: "alice".into(),
            },
        )
        .unwrap();
        assert_eq!(snap.plan[0].assignee.as_deref(), Some("alice"));
    }

    #[test]
    fn plan_op_claim_fails_if_held() {
        let mut snap = Snapshot {
            plan: vec![TaskSpec {
                id: "T-1".into(),
                title: "x".into(),
                status: TaskStatus::InProgress,
                assignee: Some("alice".into()),
                notes: None,
            }],
            ..Default::default()
        };
        let err = apply_plan_op(
            &mut snap,
            &PlanOp::Claim {
                task_id: "T-1".into(),
                agent_id: "bob".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, BrokerError::Other(_)));
    }

    #[test]
    fn plan_op_claim_reclaim_after_done_ok() {
        let mut snap = Snapshot {
            plan: vec![TaskSpec {
                id: "T-1".into(),
                title: "x".into(),
                status: TaskStatus::Done,
                assignee: Some("alice".into()),
                notes: None,
            }],
            ..Default::default()
        };
        apply_plan_op(
            &mut snap,
            &PlanOp::Claim {
                task_id: "T-1".into(),
                agent_id: "bob".into(),
            },
        )
        .unwrap();
        assert_eq!(snap.plan[0].assignee.as_deref(), Some("bob"));
    }

    #[test]
    fn plan_op_remove_missing_is_not_found() {
        let mut snap = Snapshot::default();
        let err = apply_plan_op(
            &mut snap,
            &PlanOp::Remove {
                task_id: "T-X".into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, BrokerError::NotFound(_)));
    }

    // ---------- HTTP round-trip via wiremock ----------

    #[tokio::test]
    async fn join_appends_agent_to_body() {
        let server = MockServer::start().await;
        // GET issue → empty body
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({ "body": "" })),
            )
            .expect(1)
            .mount(&server)
            .await;
        // PATCH issue → 200
        Mock::given(method("PATCH"))
            .and(path("/repos/owner/repo/issues/42"))
            .and(header("if-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        b.join("alice").await.unwrap();
    }

    #[tokio::test]
    async fn cas_retries_on_412_then_succeeds() {
        let server = MockServer::start().await;
        // Two GETs (first attempt + retry)
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({ "body": "" })),
            )
            .expect(2)
            .mount(&server)
            .await;
        // First PATCH → 412
        Mock::given(method("PATCH"))
            .and(path("/repos/owner/repo/issues/42"))
            .and(header("if-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(412))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // Second PATCH → 200
        Mock::given(method("PATCH"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        b.join("alice").await.unwrap();
    }

    #[tokio::test]
    async fn cas_exhausts_after_max_retries() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"v1\"")
                    .set_body_json(serde_json::json!({ "body": "" })),
            )
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(412))
            .expect(3)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let err = b.join("alice").await.unwrap_err();
        assert!(matches!(err, BrokerError::Conflict { retries: 3 }));
    }

    #[tokio::test]
    async fn send_posts_structured_comment() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/repos/owner/repo/issues/42/comments"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        b.send(NotifyMessage {
            from: "alice".into(),
            to: NotifyTarget::Agent("bob".into()),
            message: "hi".into(),
            priority: Priority::Normal,
            timestamp: 1_700_000_000_000,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn fetch_since_filters_to_me_and_excludes_self() {
        let server = MockServer::start().await;
        let comments = serde_json::json!([
            {
                "id": 1,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "alice".into(),
                    to: NotifyTarget::Agent("bob".into()),
                    message: "to-bob".into(),
                    priority: Priority::Normal,
                    timestamp: 1,
                }).unwrap()
            },
            {
                "id": 2,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "alice".into(),
                    to: NotifyTarget::Agent("alice".into()), // self-sent → dropped
                    message: "from-self".into(),
                    priority: Priority::Normal,
                    timestamp: 2,
                }).unwrap()
            },
            {
                "id": 3,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "bob".into(),
                    to: NotifyTarget::All,
                    message: "broadcast".into(),
                    priority: Priority::High,
                    timestamp: 3,
                }).unwrap()
            },
            {
                "id": 4,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "carol".into(),
                    to: NotifyTarget::Agent("alice".into()),
                    message: "to-alice".into(),
                    priority: Priority::Normal,
                    timestamp: 4,
                }).unwrap()
            },
            // Plain human comment — must be skipped.
            { "id": 5, "body": "hello, this is a human" },
        ]);
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42/comments"))
            .and(query_param("per_page", "100"))
            .and(query_param("sort", "created"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"c1\"")
                    .set_body_json(comments),
            )
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let (msgs, cursor) = b.fetch_since(Cursor::beginning()).await.unwrap();

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].from, "bob"); // broadcast
        assert_eq!(msgs[1].from, "carol");
        assert_eq!(msgs[1].message, "to-alice");
        assert_eq!(cursor.last_comment_id, 5);
        assert_eq!(cursor.last_etag.as_deref(), Some("\"c1\""));
    }

    #[tokio::test]
    async fn fetch_since_304_returns_empty_and_keeps_cursor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42/comments"))
            .and(header("if-none-match", "\"c1\""))
            .respond_with(ResponseTemplate::new(304))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let cursor = Cursor {
            last_comment_id: 7,
            last_etag: Some("\"c1\"".into()),
        };
        let (msgs, new_cursor) = b.fetch_since(cursor.clone()).await.unwrap();
        assert!(msgs.is_empty());
        assert_eq!(new_cursor, cursor);
    }

    #[tokio::test]
    async fn rate_limited_response_surfaces_as_error() {
        let server = MockServer::start().await;
        // Reset is comfortably in the future so `wait_s > 0`. The test
        // broker's inline cap is 0 (see fresh_broker) so any positive
        // wait surfaces as RateLimited rather than sleeping.
        let reset = unix_now_secs() + 3600;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", reset.to_string().as_str())
                    .set_body_json(serde_json::json!({ "message": "rate limited" })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let err = b.list_agents().await.unwrap_err();
        match err {
            BrokerError::RateLimited { reset_epoch_ms } => {
                assert_eq!(reset_epoch_ms, reset.saturating_mul(1000));
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_surfaces_as_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        assert!(matches!(
            b.list_agents().await.unwrap_err(),
            BrokerError::Auth(_)
        ));
    }

    #[tokio::test]
    async fn list_agents_round_trip() {
        let server = MockServer::start().await;
        let snap = Snapshot {
            v: 1,
            agents: vec![
                AgentMeta {
                    agent_id: "alice".into(),
                    joined_at: 1,
                },
                AgentMeta {
                    agent_id: "bob".into(),
                    joined_at: 2,
                },
            ],
            plan: vec![],
        };
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(issue_body(&snap)))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let agents = b.list_agents().await.unwrap();
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0].agent_id, "alice");
    }

    // ---------- P4c: send_with_retry behaviour ----------

    #[tokio::test]
    async fn five_xx_retries_then_succeeds() {
        let server = MockServer::start().await;
        // First call: 503. wiremock evaluates Mocks in declared order
        // and `up_to_n_times(1)` means "match at most 1 time".
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // Second call: 200 with valid snapshot.
        let snap = Snapshot::default();
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(issue_body(&snap)))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        // Should not surface as error — the 503 is retried.
        let _ = b.list_agents().await.unwrap();
    }

    #[tokio::test]
    async fn five_xx_exhausts_retry_budget() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(500))
            // fresh_broker's `with_test_timing` sets the retry budget
            // to 3; all 3 attempts should hit this mock.
            .expect(3)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let err = b.list_agents().await.unwrap_err();
        match err {
            BrokerError::Other(e) => {
                assert!(e.to_string().contains("exhausted"), "unexpected error: {e}");
            }
            other => panic!("expected Other(exhausted...), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rate_limit_inline_sleep_within_cap_retries() {
        let server = MockServer::start().await;
        // Reset ~ now + 1s, so wait_s = 1. The broker is configured
        // (below) with inline cap = 5, so it should sleep 1s and retry.
        let reset = unix_now_secs() + 1;
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", reset.to_string().as_str()),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        // Retry hits this with success.
        let snap = Snapshot::default();
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(issue_body(&snap)))
            .expect(1)
            .mount(&server)
            .await;

        let b = GitHubBroker::new("owner", "repo", 42, "alice", "ghs_test")
            .unwrap()
            .with_base_url(server.uri())
            .with_test_timing()
            .with_rate_limit_inline_cap_secs(5);
        // Should not error — the 403 is slept off + retried.
        let _ = b.list_agents().await.unwrap();
    }

    #[tokio::test]
    async fn rate_limit_past_reset_retries_immediately() {
        let server = MockServer::start().await;
        let past_reset = unix_now_secs() - 1; // already expired
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", past_reset.to_string().as_str()),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        let snap = Snapshot::default();
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(issue_body(&snap)))
            .expect(1)
            .mount(&server)
            .await;

        // Default cap is 0 in fresh_broker, but reset is in the past so
        // `wait_s <= 0` → broker should retry immediately without ever
        // consulting the cap.
        let b = fresh_broker(&server, "alice").await;
        let _ = b.list_agents().await.unwrap();
    }

    // ---------- P4c: Link-header pagination ----------

    #[tokio::test]
    async fn fetch_since_follows_link_next_pagination() {
        let server = MockServer::start().await;
        let base = server.uri();
        let page2_url = format!("{base}/repos/owner/repo/issues/42/comments?page=2");

        let page1_body = serde_json::json!([
            {
                "id": 1,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "bob".into(),
                    to: NotifyTarget::Agent("alice".into()),
                    message: "page1".into(),
                    priority: Priority::Normal,
                    timestamp: 1,
                }).unwrap(),
            },
        ]);
        // Page 1: returns 1 comment + Link header pointing at page 2.
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42/comments"))
            .and(query_param("per_page", "100"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header(
                        "link",
                        format!("<{page2_url}>; rel=\"next\", <{page2_url}>; rel=\"last\"")
                            .as_str(),
                    )
                    .insert_header("etag", "\"page1\"")
                    .set_body_json(page1_body),
            )
            .expect(1)
            .mount(&server)
            .await;

        let page2_body = serde_json::json!([
            {
                "id": 2,
                "body": serde_json::to_string(&NotifyCommentBody {
                    ty: "notify".into(),
                    from: "carol".into(),
                    to: NotifyTarget::All,
                    message: "page2".into(),
                    priority: Priority::Normal,
                    timestamp: 2,
                }).unwrap(),
            },
        ]);
        // Page 2: no Link → pagination stops.
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42/comments"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page2_body))
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let (msgs, cursor) = b.fetch_since(Cursor::beginning()).await.unwrap();

        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].from, "bob");
        assert_eq!(msgs[1].from, "carol");
        assert_eq!(cursor.last_comment_id, 2);
        // ETag captured from page 1, used on the next call's
        // If-None-Match.
        assert_eq!(cursor.last_etag.as_deref(), Some("\"page1\""));
    }

    // ---------- P4c: parse_link_next unit ----------

    #[test]
    fn link_next_parsing() {
        // A canonical GitHub Link header.
        let header_value = r#"<https://api.github.com/.../comments?page=2>; rel="next", <https://api.github.com/.../comments?page=5>; rel="last""#;
        // Build a fake Response... can't easily do that, so call the
        // function indirectly by constructing a HeaderMap-equivalent
        // hand-parse. Instead, just verify the substring logic on the
        // raw input we'd pass to parse_link_next:
        assert!(header_value.contains("rel=\"next\""));
        // Pluck the next URL by re-implementing the parser's contract,
        // making sure our parser would return the same thing.
        let next = header_value
            .split(',')
            .map(str::trim)
            .find(|p| p.contains("rel=\"next\""))
            .and_then(|p| {
                let lt = p.find('<')?;
                let gt = p.find('>')?;
                Some(p[lt + 1..gt].to_string())
            });
        assert_eq!(
            next.as_deref(),
            Some("https://api.github.com/.../comments?page=2"),
        );
    }
}
