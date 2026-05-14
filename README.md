# cc-relay

> **ステータス: WIP — P3 設計転換中。** 使えるバイナリはまだ無い。
> README の本格的な書き直しは P8 (#20) で行う。

**Claude Code on Web** 用の、エージェント間調整レイヤ。各セッションが
小さな stdio MCP サーバ (`rust-mcp-agent`) を動かし、共有 **broker** を
介して通知と共有プランを他エージェントとやり取りする。

broker は `Broker` トレイトでプラガブル (P4 / #16)。MVP では **GitHub**
が backend — 指定したリポジトリ / issue がメッセージ列とプランを保持
する。Web サンドボックスから個別インフラ無しに到達できる唯一の汎用
永続サービスだから (詳細は [`ARCHITECTURE.md`](./ARCHITECTURE.md) の
ADR-001)。

## 構成

| パス                   | 中身                                                                |
| ---------------------- | ------------------------------------------------------------------- |
| `crates/agent-core/`   | wire protocol の値型 (Rust)                                         |
| `crates/agent-mcp/`    | stdio MCP サーバ (`notify_agent` / `get_inbox` / plan ツール群)     |
| `crates/agent-cli/`    | clap dispatcher: `rust-mcp-agent stdio`                             |
| `hooks/`               | `.claude/hooks` スクリプト (bootstrap / inbox / notify / cleanup)   |
| `docs/github-app.md`   | broker 用 GitHub App `cc-relay-agent` の設定記録                    |

後続フェーズで増える crate:

| パス                   | 中身                                                                |
| ---------------------- | ------------------------------------------------------------------- |
| `crates/agent-broker/` | `Broker` トレイト + `GitHubBroker` 実装 (P4 / #16)                  |

## 設計

設計判断は [`ARCHITECTURE.md`](./ARCHITECTURE.md) を参照。特に ADR-001 が、
当初の Cloudflare-DO + WebSocket 設計を捨てた理由と置換設計を記録して
いる。

## ロードマップ

[project #7](https://github.com/orgs/ippoan/projects/7) の
[`Epic #1`](https://github.com/ippoan/cc-relay/issues/1) MVP で管理。

フェーズ:

- **P0** — scaffolding + CI スケルトン (#2, #3) ✅
- **P1** — agent-core protocol (#4) ✅
- **P2** — agent-mcp stdio サーバ (#5, #6) ✅
- **P3** — ADR-001 + ワークスペース整理 (#15) ← 現在
- **P4** — `agent-broker` crate (#16)
- **P5** — `agent-mcp` を broker 経由にリファクタ (#17)
- **P6** — `agent-cli` 簡素化 + `--broker` フラグ (#18)
- **P7** — 実 GitHub リポジトリでの end-to-end テスト (#19)
- **P8** — README 書き直し + `.mcp.json` テンプレート (#20)
- **#10** — リリースパイプライン仕上げ
- **#11** — 認証 / observability / 設定

## ライセンス

以下のいずれかのデュアルライセンス、選択可能:

- [Apache License, Version 2.0](./LICENSE-APACHE)
- [MIT License](./LICENSE-MIT)
