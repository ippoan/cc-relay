# parallel-crates レシピ

2 つの独立した crate を別々の sub-agent に同時に書かせるためのコピペ用
プロンプト集。

## いつ使う

- 触る crate が完全に分かれている (例: `agent-broker` 実装 +
  `agent-mcp` のリファクタ)。
- どちらも単体で `cargo test --workspace` を通せる粒度に閉じている。
- ファイル衝突が起きる可能性が無い (`Cargo.lock` を除く。下の「注意」
  参照)。
- 各 sub-agent の成果がそれぞれ独立した PR として `main` に乗せられる。

## いつ使わない

- 共有型 (`crates/agent-core/src/protocol.rs` など) を両方が編集する。
  → 直列にやる。
- 同じ crate の別ファイルを触る。
  → ファイルが衝突しなくても、整合性確認のため 1 セッションで進める。
- 結合テストが両方の変更を前提にする。
  → 片方が green でも CI が落ちる。先に基盤側を merge してから次を出す。
- どちらか一方がまだ設計議論中。
  → 並列化しても片方が空転する。

## 手順

1. `main` を最新化した上で、worktree を 2 本切る。crate ごとに専用
   ディレクトリを作っておくと、エディタとシェルの履歴も分離される。

   ```
   git fetch origin
   git worktree add ../cc-relay-broker  -b claude/issue-<A> origin/main
   git worktree add ../cc-relay-mcp     -b claude/issue-<B> origin/main
   ```

2. 1 本目の sub-agent を Agent tool で起動する。下の「sub-agent prompt
   テンプレ (crate A)」をそのまま貼る。
   - `subagent_type: "general-purpose"`
   - `isolation: "worktree"` を指定し、cwd を `../cc-relay-broker` に
     向ける。
   - 「PR を開くところまで」を完了条件にする。merge は auto-merge
     ワークフローに任せる。

3. 続けて 2 本目の sub-agent を起動する。「sub-agent prompt テンプレ
   (crate B)」を貼り、cwd を `../cc-relay-mcp` に向ける。

4. 両方の sub-agent が PR を開いたら、人間側で:
   - 各 PR の CI (`rust (fmt)` / `rust (clippy)` / `rust (test)`) が
     green になるのを待つ。
   - 先に green になった方を auto-merge に任せる。
   - 2 本目は `main` を取り込んでから再度 CI を回す。`Cargo.lock` の
     衝突が出るのはここ。

5. 両方が merge されたら worktree を片付ける。

   ```
   git worktree remove ../cc-relay-broker
   git worktree remove ../cc-relay-mcp
   ```

## sub-agent prompt テンプレ

### crate A 用

```
あなたは cc-relay リポジトリの `<crate A 名>` を実装するサブエージェント
です。

- 作業ブランチ: `claude/issue-<A>` (既に worktree が切られている)
- 触ってよい範囲: `crates/<crate A 名>/` 配下のみ。
- 触ってはいけない範囲: 他の crate、`Cargo.lock` 以外のワークスペース
  ルートのファイル。
- 完了条件:
  - <issue #A の「完了条件」セクションをそのまま貼る>
  - `cargo fmt --all -- --check` / `cargo clippy --workspace -- -D warnings`
    / `cargo test --workspace` が全て通る。
  - `main` 向けに PR を開き、本文に対応 issue 番号を書く (`Closes #<A>`)。
- PR は draft で開かない (auto-merge が走らないため)。
- 不明点が出たら推測で進めず、コメントで人間に投げて止まる。
```

### crate B 用

```
あなたは cc-relay リポジトリの `<crate B 名>` を実装するサブエージェント
です。

- 作業ブランチ: `claude/issue-<B>` (既に worktree が切られている)
- 触ってよい範囲: `crates/<crate B 名>/` 配下のみ。
- 触ってはいけない範囲: 他の crate、`Cargo.lock` 以外のワークスペース
  ルートのファイル。
- 完了条件:
  - <issue #B の「完了条件」セクションをそのまま貼る>
  - `cargo fmt --all -- --check` / `cargo clippy --workspace -- -D warnings`
    / `cargo test --workspace` が全て通る。
  - `main` 向けに PR を開き、本文に対応 issue 番号を書く (`Closes #<B>`).
- PR は draft で開かない。
- 不明点が出たら推測で進めず、コメントで人間に投げて止まる。
```

## 注意

- **`Cargo.lock` は両方が触る**。新しい依存を入れた crate が 2 つあると
  確実に衝突する。先に merge された方を取り込んで `cargo build` で
  `Cargo.lock` を再生成し、後発の PR は `main` を rebase してから
  push し直す。
- `crates/agent-core/` を変更してはいけない。共有型の変更は並列化に
  向かない。どうしても必要になったら sub-agent を止めて、人間が
  `agent-core` を先に 1 本の PR で merge する。
- sub-agent には `main` への直 push を禁止する。CLAUDE.md の規約と
  同じ。
- ワークスペース全体で `cargo test --workspace` を回すので、片方の
  crate が壊れていればもう片方の CI も落ちる。先に小さい方を merge
  しておくと、後発の rebase で問題に気付ける。
- sub-agent を 3 本以上に増やさない。worktree とレビュー負荷が線形で
  増える割に、3 本目以降は依存関係が絡んで結局直列待ちになりがち。
