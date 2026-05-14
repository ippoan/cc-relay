# pr-autofix レシピ

PR の CI failure を webhook で受け取り、必要なら sub-agent に fix を委譲する
ためのコピペ用テンプレ。

## 構成図

```
parent session
  ├─ subscribe_pr_activity(pr#)   ← イベント待機 (sleep しない)
  └─ webhook 受信時
      ├─ 軽い fix → parent 自身が push
      └─ 重い fix → sub-agent に委譲 (worktree 隔離)
```

`subscribe_pr_activity` を一度呼んで turn を終わらせると、GitHub の webhook
(CI completion / review comment / push 等) でセッションが起こされる。これが
このレシピの中核で、polling 不要の唯一の方法。

## 大事な前提

- **`sleep` / polling を絶対に書かない。** harness 側で `sleep` は弾かれる
  し、書いた時点でこのレシピを誤読している。
- 監視そのもの (= 待つ役) には sub-agent は要らない。`subscribe_pr_activity`
  は parent session 自身で呼ぶ。
- sub-agent を起動するのは「CI が落ちて、parent が fix を別文脈に切り出した
  い」と判断した瞬間だけ。
- CLAUDE.md の 3 点 (`cargo fmt --check` / `clippy -D warnings` /
  `cargo test --workspace`) を sub-agent 側でも必ず local で通してから push。
- `main` への直 push 禁止。fix は PR ブランチに対する追加 commit。

## parent session 側の手順

1. PR 番号を確認する (例: `#42`)。
2. `mcp__github__subscribe_pr_activity` を `pull_number=42` で呼ぶ。
3. **その turn で他に何もせず終了する。** 待つために自分でループしない。
4. webhook で起こされたら `mcp__github__pull_request_read` などで状態を
   再取得し、failure の job log を `mcp__72d6cc20-...__get_job_logs` または
   `grep_job_logs` で読む。
5. 失敗の性質で分岐:
   - **tractable** (fmt 崩れ / 単純な clippy lint / 1 ファイルの test 修正)
     → parent が直接 edit して push。push 後は subscribe が継続している
     ので、次の CI 結果でまた起きる。
   - **重い** (複数 crate の API 変更 / refactor / test 戦略の再設計)
     → 下記テンプレで sub-agent を起動する。
6. PR が merge されたか、ユーザが止めるまで subscribe は維持。

## sub-agent 起動の prompt テンプレ

そのままコピペして埋める想定:

```
PR #<n> (branch: <claude/issue-x>) の CI が <job 名> で fail した。
ログ抜粋:

  <log excerpt 10-30 行>

原因はおそらく <仮説>。次を頼む:

- `git worktree add` で `<branch>` を別ディレクトリに checkout
  (isolation: "worktree")
- ローカルで以下を順に通す:
    cargo fmt --all -- --check
    cargo clippy --workspace -- -D warnings
    cargo test --workspace
- 通ったら同 branch に commit して push
- diff のサマリを返す

スコープ外の refactor はしない。判断に迷ったら AskUserQuestion で
parent に聞き返す。
```

ポイント:

- ログ抜粋は 1MB 投げない。落ちた assertion / compiler error の周辺だけ。
- worktree を指定するのは、parent session の作業ディレクトリと衝突
  させないため。
- 「スコープ外の refactor はしない」を明示する。明示しないと sub-agent は
  ついでに直したくなる。

## やってはいけない

- `sleep 60` や `while true; do ...` で polling する。**禁止。**
- 本体に subscribe させずに sub-agent 側に「ずっと監視して」と投げる。
  sub-agent は wake-on-webhook の対象にならない設計。
- 同じ PR に対して複数の sub-agent を同時に fix push させる。push 競合と
  force-push の事故が必ず起きる。1 PR 1 fixer。
- 「auto-fix」の名前で大規模 refactor を投げる。曖昧な失敗は
  AskUserQuestion で人間に確認する。
- branch 保護を緩めて無理に通す。CI が落ちているのには理由がある。

## いつ unsubscribe するか

`mcp__github__unsubscribe_pr_activity` を呼ぶタイミング:

- ユーザが「もう監視やめて」と明示した時。
- PR が merge された (auto-merge が走った) のを webhook で確認した時。
- PR が close された時。
- 同一 PR に対して別 session が監視を引き継ぐと判断した時 (二重 wake 防止)。

---

要約: parent が `subscribe_pr_activity` で待ち、webhook で起きて failure を
分類する。軽い fix は parent 本体、重い fix は worktree 隔離した sub-agent
に投げる。polling と並列 push は禁止、merge 確定で unsubscribe する。
