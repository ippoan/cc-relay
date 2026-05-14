# cc-relay

> **Status: WIP — P0 scaffolding.** No usable binaries yet.

Claude Code agent-to-agent coordination layer. Each Claude Code on Web session
runs a tiny Rust daemon that connects to a shared Cloudflare Durable Object via
WebSocket. The DO fans out file-change events, agent-to-agent notifications,
and a shared plan across every agent attached to a session.

```
  Claude Code A                          Claude Code B
       │                                      │
       │ MCP (stdio)                          │ MCP (stdio)
       ▼                                      ▼
  rust-mcp-agent stdio              rust-mcp-agent stdio
       │ HTTP loopback                        │ HTTP loopback
       ▼                                      ▼
  rust-mcp-agent daemon  ◀── notify-rs       rust-mcp-agent daemon
       │                       file watcher        │
       │ WebSocket (wss://)                       │ WebSocket (wss://)
       └────────────┬─────────────────────────────┘
                    ▼
         Cloudflare Worker + SessionDO
             (WebSocket Hibernation)
```

## Components

| Path                   | What                                                              |
| ---------------------- | ----------------------------------------------------------------- |
| `crates/agent-core/`   | WireProtocol types (Rust source of truth) + ts-rs TS export       |
| `crates/agent-daemon/` | notify-rs watcher + WS client + axum HTTP server                  |
| `crates/agent-mcp/`    | stdio MCP server that relays to the local daemon                  |
| `crates/agent-cli/`    | thin clap dispatcher: `rust-mcp-agent daemon` / `... stdio`       |
| `coordinator/`         | Cloudflare Worker + SessionDO (TypeScript, outside cargo workspace) |
| `hooks/`               | `.claude/hooks` scripts (bootstrap, inbox, notify, cleanup)       |

## Design

See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the design decisions
(ts-rs as source of truth, SessionDO with hibernation, protocol
versioning, hook layout, etc.).

## Roadmap

Tracked on [project #7](https://github.com/orgs/ippoan/projects/7) under
the [`Epic #1`](https://github.com/ippoan/cc-relay/issues/1) MVP.

Phases:

- **P0** — scaffolding + CI skeleton
- **P1** — agent-core protocol + ts-rs TS generation
- **P2** — agent-daemon + agent-mcp
- **P3** — coordinator (SessionDO)
- **P4** — end-to-end integration test
- **P5** — `.claude/hooks` scripts + v0.1.0 musl release
- **P6** — auth (shared secret) + observability + config

## License

Dual-licensed under either of

- [Apache License, Version 2.0](./LICENSE-APACHE)
- [MIT License](./LICENSE-MIT)

at your option.
