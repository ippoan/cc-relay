# background-issue-updater レシピ

長い実装中に、進捗コメントを GitHub issue に淡々と投げる background sub-agent
の雛形。

## いつ使う

- 本体 session で数十分以上かかる実装をしていて、複数ファイル/複数 crate を
  またぐ場合。
- レビュアーや並走している他 session が、tracking issue を見て進捗を把握
  したい場合。
- checkpoint が事前に列挙できる場合 (例: 「protocol 変更 → broker 配線 →
  test 追加」)。本体側はコードに集中し、コメント投稿は background に逃がす。

## いつ使わない

- 5 分以内に終わる小さな修正。コメントの方がノイズになる。
- 既にコメントが多い issue で、これ以上人間のレビュー負荷を上げたくない時。
- 本体 session が PR を頻繁に push していて、PR 側の commit log が
  事実上の進捗ログになっている場合。重複は避ける。
- checkpoint が動的に変わる作業 (探索的なデバッグなど)。background では
  追従しきれない。

## 起動方法

本体 session の Agent tool を以下のように呼ぶ:

- `subagent_type`: `"general-purpose"`
- `run_in_background`: `true`
- `description`: `"issue #<n> に進捗コメントを投稿"`
- `prompt`: 下の「sub-agent prompt テンプレ」をコピペし、プレースホルダを
  実値に置き換える。

`run_in_background: true` なので、sub-agent の完了は非同期で本体に戻る。
本体は待たずに実装を継続する。

## sub-agent prompt テンプレ

```
あなたは GitHub issue に進捗コメントを投稿するだけの background agent です。

対象 issue:
- repo: <owner/repo>             (例: ippoan/cc-relay)
- issue 番号: <issue#>
- 作業の 1 行サマリ: <summary>

投稿する checkpoint (上から順):
<checkpoints>
  例:
  1. protocol.rs に <Foo> イベントを追加した
  2. agent-broker 側で <Foo> をハンドリングするコードを配線した
  3. integration test を追加して green になった

ルール:
- 各 checkpoint について、mcp__github__add_issue_comment で 1 件ずつ
  短いコメント (1-3 行、日本語、絵文字なし) を投稿する。
- 投稿前に mcp__github__issue_read で既存コメントを取得し、同じ本文が
  既に存在するなら skip する (重複防止)。
- issue title や body は変更しない。コメント追加だけ。ラベルもいじらない。
- checkpoint の間で sleep / polling をしない。リストを順に上から投げ、
  最後の 1 件を投稿し終えたら直ちに exit する。
- 失敗 (API エラー、権限エラー等) が起きた場合は、それ以降の checkpoint を
  投稿せず、エラー内容と「どの checkpoint で落ちたか」を 5 行以内で
  サマリして親 agent に返す。本体 session を panic させない。
- gh CLI は使わない。GitHub 操作はすべて mcp__github__* tool 経由。

完了したら、投稿した checkpoint の番号を 1 行で返して終了する。
```

## アンチパターン

- **`sleep` で polling しない。** 本 agent は checkpoint リストを投げ切ったら
  即 exit する設計。イベント駆動で待ち受けたい場合は
  `mcp__github__subscribe_pr_activity` 等を別 sub-agent で使う。
- **本体と二重投稿しない。** 本体 session が同じ issue にコメントするなら、
  background 側の checkpoint リストから重複分を抜くか、background を起動
  しない。
- **issue title や body を書き換えない。** これは `mcp__github__issue_write`
  の update 系を呼ばないという意味。コメント追加 (`add_issue_comment`) のみ。
- **checkpoint を動的に増やさない。** 本 agent は与えられたリストを
  そのまま順に投げるだけ。途中で本体の状況を「察して」コメントを増やそうと
  しない。必要なら本体側から別 sub-agent を再度起動する。
- **秘密情報をコメント本文に含めない。** App の private key、token、
  社内 URL など。issue は public な可能性がある。

## 完了条件

- checkpoint リストの全項目について、`add_issue_comment` が成功するか、
  既存コメントと重複していて skip されたかのいずれかで処理が終わっている。
- sub-agent は最後に投稿した checkpoint 番号 (または skip した番号) を
  1 行で親に返して exit する。
- エラーで途中終了した場合は、落ちた checkpoint 番号とエラーサマリを
  返す。本体は戻り値を見て、必要なら手動で残りをコメントする。

---

このレシピは、本体 session を実装に集中させたまま、tracking issue 側の
「今どこ?」を低コストで埋めるための型紙。checkpoint が事前列挙できる
作業に限り効く。探索的な作業では本体から都度コメントする方が確実。
