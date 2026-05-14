# CLAUDE.md

このリポジトリで Claude Code セッションを動かす時の作業ガイド。

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

## やってはいけないこと

- daemon を panic させない。runtime 経路の `Result` は全て log して
  捨てる、`main` まで bubble させない。
- MVP が Linux x86_64 で green になる前に macOS / Windows 向けの
  分岐コードを足さない。
- 秘密鍵 (`.pem`) をリポジトリにコミットしない。`.gitignore` が
  守っているが、`git add -f` で強制追加しないこと。
- `main` に直 push しない (proxy 側でも block されるが、習慣として)。
