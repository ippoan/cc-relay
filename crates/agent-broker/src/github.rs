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
        })
    }

    /// Point this broker at a non-default base URL (e.g. a wiremock
    /// server in tests, or a GHES install).
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
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

    /// GET the Issue body, parse it as [`Snapshot`], also return the
    /// `ETag` header so callers can pass it back in `If-Match` on the
    /// next PATCH for CAS.
    async fn get_snapshot(&self) -> Result<(Snapshot, Option<String>)> {
        let resp = self
            .http
            .get(self.issue_url())
            .send()
            .await
            .map_err(transport_err)?;

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
        check_rate_limit(&resp)?;
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

        let mut req = self
            .http
            .patch(self.issue_url())
            .json(&PatchIssueReq { body: &body });
        if let Some(et) = etag {
            req = req.header(IF_MATCH, et);
        }
        let resp = req.send().await.map_err(transport_err)?;

        let status = resp.status();
        if status == StatusCode::PRECONDITION_FAILED {
            // Used as the signal value the CAS loop catches.
            return Err(BrokerError::Conflict { retries: 0 });
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on PATCH issue".into()));
        }
        check_rate_limit(&resp)?;
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

        let resp = self
            .http
            .post(self.comments_url())
            .json(&PostCommentReq {
                body: &comment_body,
            })
            .send()
            .await
            .map_err(transport_err)?;

        let status = resp.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on POST comment".into()));
        }
        check_rate_limit(&resp)?;
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "POST comment returned {status}"
            )));
        }
        Ok(())
    }

    async fn fetch_since(&self, cursor: Cursor) -> Result<(Vec<NotifyMessage>, Cursor)> {
        let mut req = self.http.get(self.comments_url()).query(&[
            ("per_page", DEFAULT_PER_PAGE.to_string().as_str()),
            ("sort", "created"),
            ("direction", "asc"),
        ]);
        if let Some(et) = cursor.last_etag.as_ref() {
            req = req.header(IF_NONE_MATCH, et);
        }
        let resp = req.send().await.map_err(transport_err)?;

        let status = resp.status();
        if status == StatusCode::NOT_MODIFIED {
            // Nothing new since last successful read — keep the same
            // cursor so the next conditional GET stays cheap.
            return Ok((Vec::new(), cursor));
        }
        if status == StatusCode::UNAUTHORIZED {
            return Err(BrokerError::Auth("401 on GET comments".into()));
        }
        check_rate_limit(&resp)?;
        if !status.is_success() {
            return Err(BrokerError::Other(anyhow::anyhow!(
                "GET comments returned {status}"
            )));
        }

        let new_etag = extract_etag(&resp);
        let comments: Vec<CommentResp> = resp.json().await.map_err(transport_err)?;

        let mut highest_id = cursor.last_comment_id;
        let mut out = Vec::new();
        for c in comments {
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

/// Look at `x-ratelimit-remaining` / `x-ratelimit-reset` to decide if
/// the response is a rate-limit signal. We surface
/// [`BrokerError::RateLimited`] rather than sleeping inline; the caller
/// (or a higher retry layer in P4c) chooses what to do with the wait.
fn check_rate_limit(resp: &reqwest::Response) -> Result<()> {
    let h = resp.headers();
    let remaining = h
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    let reset_s = h
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok());
    // GitHub reports rate-limit exhaustion as 403 (or sometimes 429)
    // with remaining=0.
    let s = resp.status().as_u16();
    if matches!(remaining, Some(0)) && (s == 403 || s == 429) {
        return Err(BrokerError::RateLimited {
            reset_epoch_ms: reset_s.unwrap_or(0).saturating_mul(1000),
        });
    }
    Ok(())
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
        Mock::given(method("GET"))
            .and(path("/repos/owner/repo/issues/42"))
            .respond_with(
                ResponseTemplate::new(403)
                    .insert_header("x-ratelimit-remaining", "0")
                    .insert_header("x-ratelimit-reset", "1700000000")
                    .set_body_json(serde_json::json!({ "message": "rate limited" })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let b = fresh_broker(&server, "alice").await;
        let err = b.list_agents().await.unwrap_err();
        match err {
            BrokerError::RateLimited { reset_epoch_ms } => {
                assert_eq!(reset_epoch_ms, 1_700_000_000_000);
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
}
