---
name: cc-relay-map
generated-from: cc-relay:769455e6db4f0feca3ebffae3a4340a10c08a22e
paths: [crates/, hooks/, docs/]
description: cc-relay の構造ナビゲーション。まず読むもの・ブランチ/worktree 運用・Sub-agent 並列開発・ビルドテスト lint・Wire protocol 変更・Hooks・GitHub 自動化 (broker 認証・auto-merge ワークフロー) の詳細手順を収録。
---
# cc-relay-map — 構造ナビゲーション

## CLAUDE.md から移設 (2026-07-07)

## まず読むもの

- [`README.md`](./README.md) — cc-relay とは何か、ざっくり。
- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — 設計の根拠。一番下の **ADR-001**
  が現行設計、その上のセクションは履歴 (廃案済み)。
- [`docs/github-app.md`](./docs/github-app.md) — broker が使う GitHub App
  (App ID / Installation ID / 環境変数 / token フロー)。`agent-broker` を
  触る前に読む。
- [Project #7](https://github.com/orgs/ippoan/projects/7) — フェーズ別ロード
  マップ。各 issue (#2–#11) の「完了条件」セクションが done の定義。

## ブランチと worktree

- 作業は `main` から切った短命ブランチ上で行う。命名規則は
  `claude/issue-<n>` または `claude/<topic>-<n>`。
- 並走するセッションが別 crate を触る時は `git worktree` で分離する:
  ```
  git worktree add ../cc-relay-issue-4 claude/issue-4
  ```
- **`main` に直接 push しない。** PR を開く → CI が green になれば
  auto-merge ワークフローが自動で merge する (下の「GitHub 自動化」)。

## Sub-agent で並列開発する

cc-relay 本体 (Broker impl) が動くまでの間、`Agent` tool で疑似的に
multi-agent を組んで開発を進める。よく使う 3 パターン:

- **並列 crate 実装** (`isolation: "worktree"`) — 独立 crate を別 worktree
  で同時に書く。共有型 (`agent-core`) を触る変更には使わない。
- **背景 issue 更新** (`run_in_background: true`) — tracking issue に進捗
  コメントを淡々と投げる。本体は実装に集中する。
- **PR 監視 / autofix** — 本体が `mcp__github__subscribe_pr_activity` で
  webhook を待ち、CI failure 時に必要なら sub-agent に fix を委譲する。
  `sleep` / polling は禁止。

詳しい手順とプロンプト本文は [`docs/sub-agent-workflow.md`](./docs/sub-agent-workflow.md)
と [`examples/sub-agent-recipes/`](./examples/sub-agent-recipes/) を参照。
P4b/P5 が merge されたら、ここでの sub-agent 起動は別 Claude Code on Web
セッション起動 + Broker 経由通信に置き換える前提で設計している。

## ビルド / テスト / lint

```
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

CI (`.github/workflows/ci.yml`) は `main` への PR ごとに上の 3 つを
matrix で回す。

## Wire protocol の変更

`crates/agent-core/src/protocol.rs` が wire protocol の唯一の真実。ADR-001
で TypeScript export (ts-rs) と coordinator は削除されたので、Rust 側だけ
編集すればよい。後方互換を壊す変更を入れる時は `PROTOCOL_VERSION`
(現在 `1`) を上げる。

## Hooks

`hooks/` は `.claude/settings.json` から呼び出すシェルスクリプト
(`SessionStart` / `UserPromptSubmit` / `PostToolUse` / `SessionEnd`)。
versioning と sha256 ピン留めは issue #9 で扱う。

## GitHub 自動化

### broker 認証

GitHub App `cc-relay-agent` の installation token を使う:

- App ID `3710243` / Installation ID `132248860` (公開、`docs/github-app.md`)
- 秘密鍵 (`.pem`) はリポジトリ外 (1Password 等) で管理
- `.gitignore` が `*.pem` `*.key` を弾く

実装は P4 (`crates/agent-broker/`) で。

### auto-merge ワークフロー

`.github/workflows/auto-merge.yml` が PR 作成時 (非 draft) に
`gh pr merge --auto --squash` を叩く。実際の merge gate は `main` の
ブランチ保護ルールが握る。リポジトリ側で:

- Settings → General → **Allow auto-merge** を ON
- Settings → Branches → `main` の保護で `rust (fmt)` / `rust (clippy)` /
  `rust (test)` を **Required status checks** に追加

の 2 つが設定済みである前提。これらが無いと auto-merge は単に「PR を
即時 squash merge」する挙動になる (ガードレール無し)。
