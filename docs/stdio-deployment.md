# CCoW stdio deployment

Claude Code on Web (CCoW) で cc-relay を **stdio variant** として配布・運用する
ための設計メモ。現行の HTTP transport (ADR-003, auth-worker DO 経由) +
`relay` / `probe` 常駐に対する代替経路で、ADR-001 の "stdio-only" への回帰に
あたる。

> Status: **proposal** (#72 / ADR-008 候補)。install hook の実コードは
> `yhonda-ohishi/claude-hooks` 側で別途実装する。本ドキュメントは契約 (何を
> どこに置き、どう認証し、いつ反映されるか) を定義する。

## なぜ stdio か

```
HTTP版 : Claude.ai connector → auth-worker DO(mcp.ippoan.org) → WSS → relay常駐 → GitHubBroker → api.github.com
stdio版: Claude Code → (local spawn) rust-mcp-agent stdio ───────────────────────→ GitHubBroker → api.github.com
```

- 中間の **auth-worker DO + WSS ホップを除去** (障害点・レイテンシ削減、GitHub 直結)。
- **常駐プロセス管理が不要** — `relay`/`probe` のような `nohup` 常駐は Claude Code
  本体が spawn/監督/kill する子プロセスに置き換わり、hook 側の pid 管理・生死監視・
  double-start ガードが消える。
- #69 (ADR-007 予定) で re-wake が「PR comment + `cc-relay-agent[bot]` +
  `subscribe_pr_activity`」= GitHub webhook 駆動の push に寄ったため、常駐 WSS の
  必要性が下がり、**stdio + harness subscribe** の構成と整合する。
- **skills / hook 配布と好相性** — binary + `.mcp.json` エントリだけで完結する。

## §1. 配布 (binary 取得)

- GitHub Releases の `rust-mcp-agent-x86_64-linux-musl` を install hook が取得する。
- 取得ロジック (latest / tag pin・sha256 検証・idempotent・`CCR_AGENT_FORCE_REFRESH`)
  は既存の `session-start-cc-relay-broker.sh` に実装済み。これを共通関数
  **`fetch-cc-relay-agent.sh`** に切り出し、broker / stdio 両 hook で共有する。
- **固定パス** `~/.cache/cc-relay/bin/rust-mcp-agent` に **in-place 上書き**する。
  これにより `.mcp.json` の `command` が不変になり、binary 更新で定義の書き換えが
  不要になる (§4 参照)。

## §2. 認証 (CCoW、PAT 持ち込み不要)

stdio mode は `--broker-token` (= GitHub token) を要求する
([README configuration reference](../README.md))。CCoW container には GitHub PAT が
無いが、token は持ち込まずに取得できる:

```
Anthropic OAT                          ← /home/claude/.claude/remote/.oauth_token (CCoW が注入)
   │  auth-worker: grant-via-oat
   ▼
binding JWT (24h)
   │  auth-worker: POST /mcp/introspect   (Authorization: Bearer <jwt>, ADR-003 §4 mode 1)
   ▼
github_token (repo scope)
```

- これは `docs/credentials.md` の end-user 経路と同じ仕組みで、mcp-relay が既に自動
  実行している (`INTERNAL_SHARED_SECRET` 不要、JWT 自体が認証)。
- install hook が OAT→introspect で github_token を取得し、`~/.claude.json` の
  `env.CC_RELAY_BROKER_TOKEN` に **`${CC_RELAY_BROKER_TOKEN}` 参照**で注入する。
  実値は session env から展開され、tool-call JSON / ファイル / transcript には
  **実値を残さない**。

## §3. 登録 (`~/.claude.json`)

CCoW が読む MCP config は `~/.claude.json` の `mcpServers`。stdio エントリを
upsert する:

```jsonc
"<name>": {
  "type": "stdio",
  "command": "~/.cache/cc-relay/bin/rust-mcp-agent",
  "args": ["stdio", "--broker-repo", "ippoan/cc-relay",
           "--broker-issue", "<N>", "--agent-id", "<id>"],
  "env": { "CC_RELAY_BROKER_TOKEN": "${CC_RELAY_BROKER_TOKEN}" }
}
```

既存 http エントリ (`cc-relay` = `{type: http, url: https://mcp.ippoan.org/mcp}`)
との関係には2案があり、**未決定** (本 proposal のスコープ外):

| 案 | 内容 |
|----|------|
| **併存 (別名 `cc-relay-stdio`)** | http(remote broker) と stdio(local spawn) を並べて検証できる。tool 名衝突を避けるため別名必須 |
| **上書き** | `cc-relay` を stdio に置換。経路一本化。auth-worker 経由 (ADR-003) の動線を捨てる |

## §4. 反映タイミング (kill フロー)

Claude Code の stdio MCP は以下の制約を持つ (公式 `mcp-servers.md` "Automatic
reconnection" で確認):

| 対象 | いつ反映 | 理由 |
|------|---------|------|
| `mcpServers` 定義 | 次セッション | 定義は session start でのみ読まれる (README Troubleshooting "Tools list empty" と同根) |
| binary cache の差し替え (DL) | 当該セッションで即 | hook が固定パスに上書きするだけ |
| 走行中の stdio プロセス | 次に spawn される時 (= 次セッション) | プロセスは spawn 時の binary イメージで動き続ける |

走行中セッションへ **即反映**したい場合:

- **`kill` 単独では再 spawn されない。** stdio はローカルプロセスで、HTTP/SSE の
  ような自動再接続 (exponential backoff) の対象外
  ("Stdio servers are local processes and are not reconnected automatically")。
  kill すると `disconnected` / `failed` のまま放置される。
- **`kill` + `/mcp` Reconnect** で、セッション切り替え無しに新 binary が spawn
  される (CCoW でも利用可)。
- 何もしなければ **次セッションで自動的に新版が spawn** される。

```
[1] hook が cache の binary を新版に in-place 上書き   (即)
[2] 走行中の stdio を kill                              (これ単独では failed になるだけ)
[3] /mcp で該当サーバーを Reconnect                     (ここで新 binary が再 spawn)
        ↑ セッション切り替え不要・CCoW でも可
```

固定パス (§1) + in-place 上書きにしておけば、binary 更新で `.mcp.json` の
再読込は不要。`cache 差し替え` + (即反映したい時だけ) `/mcp Reconnect` で完結する。
常駐 `relay` 版のような明示的な kill→再起動の儀式は要らない。

## 非目標 (別 issue / 別判断)

- http エントリの **置換 vs 併存** の最終決定 (運用判断)。
- install hook の **実コード** (`yhonda-ohishi/claude-hooks` 側で実装)。
- musl release の **tagless 化** (本件とは独立)。

## Refs

- #72 (本 proposal), #69 (ADR-007 / identity-distinct wake)
- ADR-001 (GitHub-as-broker + stdio-only MCP server)
- ADR-003 (Sandbox auth via auth-worker)
- [`docs/credentials.md`](./credentials.md) — token flows
