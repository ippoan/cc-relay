# CLAUDE.md

このリポジトリで Claude Code セッションを動かす時の作業ガイド。

詳細 (アーキテクチャ・経緯・gotcha) は cc-relay-map skill を参照。

## ビルド / テスト / lint

```
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

CI は `.github/workflows/ci.yml` で上の 3 つを matrix で回す。

## ブランチ

- 命名規則: `claude/issue-<n>` または `claude/<topic>-<n>`
- **`main` に直接 push しない。** PR を開く → CI が green になれば auto-merge ワークフローが自動で merge する。

## やってはいけないこと

- daemon を panic させない。runtime 経路の `Result` は全て log して捨てる、`main` まで bubble させない。
- MVP が Linux x86_64 で green になる前に macOS / Windows 向けの分岐コードを足さない。
- 秘密鍵 (`.pem`) をリポジトリにコミットしない。`.gitignore` が守っているが、`git add -f` で強制追加しないこと。
- `main` に直 push しない (proxy 側でも block されるが、習慣として)。
