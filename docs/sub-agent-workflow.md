# Sub-agent で並列開発する

cc-relay 本体 (P4b の `GitHubBroker` impl, P5 の agent-mcp refactor) が
動くまで、複数 Claude Code on Web セッションを GitHub Issue 経由で連携
させることはできない。それまでの間、1 つの Claude Code セッション内の
`Agent` tool で **疑似 multi-agent** を組み、開発体験を先取りする。

このドキュメントは「どう sub-agent を使うか」を決めるためのガイドで、
具体的なプロンプト本文は `examples/sub-agent-recipes/` 配下を参照する。

## 役割分担パターン

| 役 | 何 | 起動の仕方 |
| --- | --- | --- |
| **本体セッション** | 設計判断、コア実装、最終 review | (このセッション自身) |
| **背景 issue 更新** | tracking issue に進捗コメントを投げる | `Agent` (`run_in_background: true`) |
| **並列 crate 実装** | 独立 crate を別 worktree で書く | `Agent` (`isolation: "worktree"`) |
| **広域探索** | 「どこで X を参照してる?」の read-only 検索 | `Agent` (`subagent_type: "Explore"` または `"general-purpose"`) |
| **PR 監視 / autofix** | CI failure を受けて fix を出す | parent が `subscribe_pr_activity`、必要なら sub-agent に委譲 |

PR 監視には sub-agent を貼り付けない。`mcp__github__subscribe_pr_activity`
を呼んで turn を終えれば、webhook でセッションが起きる。`sleep` での
polling は CLAUDE.md と上位プロンプトの両方で禁止されている。

## レシピへのリンク

| やりたいこと | レシピ |
| --- | --- |
| 2 つの crate を並列で実装したい | [`examples/sub-agent-recipes/parallel-crates.md`](../examples/sub-agent-recipes/parallel-crates.md) |
| 本体実装中に issue へ進捗を流したい | [`examples/sub-agent-recipes/background-issue-updater.md`](../examples/sub-agent-recipes/background-issue-updater.md) |
| PR の CI failure に自動で対応したい | [`examples/sub-agent-recipes/pr-autofix.md`](../examples/sub-agent-recipes/pr-autofix.md) |

## 使ってはいけないパターン

- **`sleep` / polling**。`mcp__github__subscribe_pr_activity` を使う。
- **同じファイルを 2 つの sub-agent に同時編集させる**。worktree で
  ディレクトリを分けても、最後の merge 時に高確率で衝突する。crate 単位で
  切る。
- **本体と sub-agent の両方に同じ issue コメントを書かせる**。重複コメント
  になる。背景 sub-agent を起動したら、その issue へのコメントは本体側で
  止める (`background-issue-updater.md` 参照)。
- **共有型 (`crates/agent-core/src/protocol.rs`) を並列に触る**。型変更は
  必ず 1 本の PR に閉じ込めて先に merge、その後で並列を再開する。
- **sub-agent を 3 本以上に増やす**。レビュー負荷と worktree 管理が線形で
  膨らむ。3 本目以降は依存待ちで実質直列になりがち。
- **`AskUserQuestion` を sub-agent から多発させる**。sub-agent は決められた
  範囲を仕上げて返すのが役目。判断が分かれたら 1 回だけ親に聞き返す。

## cc-relay 完成後の移行マッピング

P4b → P4c → P5 が merge されて Broker が動き出した時点で、上の表の
sub-agent 起動は別 Claude Code on Web セッションの起動に置き換える。
役割の対応は以下:

| 今の sub-agent | cc-relay 完成後 |
| --- | --- |
| 背景 issue 更新 (`run_in_background: true`) | 別 web セッションを起動、`notify_agent` で本体に進捗を送る (または本体が `get_inbox` で受ける) |
| 並列 crate 実装 (`isolation: "worktree"`) | 別 web セッションが共有 plan の別タスクを `claim_task` で取り、独立 PR を出す |
| 広域探索 (Explore) | そのまま sub-agent。context 隔離は sub-agent の方が向く |
| PR 監視 / autofix | 本体が `subscribe_pr_activity`、重い fix は **どの web セッションでもよい**。共有 plan の `claim_task` で取り合う |

つまり「今 sub-agent でやっていること」は、Broker が動いた瞬間に
**そのまま web セッション間の連携プロトコルに翻訳できる** ように
設計してある。recipe のプロンプト本文も、`<owner/repo>` `<issue#>`
`<branch>` といったプレースホルダ構成を Broker 経由に乗せ換えやすく
してある (web セッション同士の起動 prompt のテンプレに流用できる)。

## 実例

P4b (`GitHubBroker` impl) と P4c (`cursor.rs` 永続化) を並列で進めるなら:

1. 本体セッションで `parallel-crates.md` の手順に従い worktree を 2 本切る。
2. sub-agent A を P4b 用に起動 (`isolation: "worktree"`)。
3. sub-agent B を P4c 用に起動 (同上、別 worktree)。
4. 本体は別途、`background-issue-updater.md` の prompt で issue #16 へ
   「P4b 着手」「P4c 着手」「両方の PR を確認待ち」の checkpoint を投げる
   `run_in_background: true` agent を 1 本起動する。
5. PR が開いたら本体は `subscribe_pr_activity` でそれぞれを購読し、CI
   結果待ちに入る。
6. CI 失敗が来たら `pr-autofix.md` の手順で対応する。

この一連の流れが、cc-relay 完成後の「web セッション 3 つで P4b / P4c /
進捗ボード役を分担する」シナリオの予行演習になる。
