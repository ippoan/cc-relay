# cc-relay

> **Status: WIP — P3 architecture pivot.** No usable binaries yet.
> Full README rewrite lands with P8 (#20).

Claude Code agent-to-agent coordination layer for **Claude Code on Web**.
Each session runs a tiny stdio MCP server (`rust-mcp-agent`) that talks to
a shared **broker** to route notifications and a shared plan between
agents working on the same task.

The broker is pluggable via a `Broker` trait (P4 / #16). The MVP backend
is **GitHub** — a designated repo / issue carries the message stream and
the plan — chosen because it is the only general-purpose persistent service
reachable from the Web sandbox without per-user infrastructure setup. See
[`ARCHITECTURE.md`](./ARCHITECTURE.md) ADR-001 for the rationale.

## Components

| Path                   | What                                                              |
| ---------------------- | ----------------------------------------------------------------- |
| `crates/agent-core/`   | Wire protocol value types (Rust)                                  |
| `crates/agent-mcp/`    | stdio MCP server (`notify_agent` / `get_inbox` / plan tools)      |
| `crates/agent-cli/`    | thin clap dispatcher: `rust-mcp-agent stdio`                      |
| `hooks/`               | `.claude/hooks` scripts (bootstrap, inbox, notify, cleanup)       |

Crates landing in later phases:

| Path                   | What                                                              |
| ---------------------- | ----------------------------------------------------------------- |
| `crates/agent-broker/` | `Broker` trait + `GitHubBroker` impl (P4 / #16)                   |

## Design

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the design decisions, in
particular ADR-001 which records why the original Cloudflare-DO + WebSocket
plan was abandoned and what replaced it.

## Roadmap

Tracked on [project #7](https://github.com/orgs/ippoan/projects/7) under
[`Epic #1`](https://github.com/ippoan/cc-relay/issues/1) MVP.

Phases:

- **P0** — scaffolding + CI skeleton (#2, #3) ✅
- **P1** — agent-core protocol (#4) ✅
- **P2** — agent-mcp stdio server (#5, #6) ✅
- **P3** — ADR-001 + workspace cleanup (#15) ← current
- **P4** — `agent-broker` crate (#16)
- **P5** — `agent-mcp` refactor onto the broker (#17)
- **P6** — `agent-cli` simplification + `--broker` flag (#18)
- **P7** — end-to-end integration test against a real GitHub repo (#19)
- **P8** — README rewrite + `.mcp.json` template (#20)
- **#10** — release pipeline polish
- **#11** — auth / observability / config

## License

Dual-licensed under either of

- [Apache License, Version 2.0](./LICENSE-APACHE)
- [MIT License](./LICENSE-MIT)

at your option.
