# CLAUDE.md

Working guidelines for Claude Code sessions on this repository.

## Read these first

- [`README.md`](./README.md) — what cc-relay is, at a glance.
- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — design rationale.
- [Project #7](https://github.com/orgs/ippoan/projects/7) — phased roadmap.
  Each phase issue (#2–#11) has a "完了条件" section that defines done.

## Branching and worktrees

- All work happens on a short-lived branch off `main`. The convention is
  `claude/issue-<n>` or `claude/<topic>-<n>`.
- Prefer separate `git worktree` per issue so concurrent sessions on different
  crates don't collide. Example:
  ```
  git worktree add ../cc-relay-issue-4 claude/issue-4
  ```
- Never push directly to `main`. Open a PR when the work is ready for review.

## Build / test / lint

Once P0 lands you can run:

```
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

`cargo test` is what runs `ts-rs` to regenerate
`coordinator/src/generated/*.ts`. CI checks that the generated tree matches
what's committed, so re-run tests and commit the generated files before
pushing if you changed any protocol type.

The coordinator is **outside** the cargo workspace. To work on it:

```
cd coordinator
npm install
npx wrangler dev
```

## Wire protocol changes

Anything in `crates/agent-core/src/protocol.rs` is the source of truth for
both Rust and TypeScript. Changing a `WireMessage` variant means:

1. Edit the Rust struct/enum.
2. Run `cargo test -p agent-core` to regenerate
   `coordinator/src/generated/*.ts`.
3. Commit *both* the Rust change and the generated TS in the same commit.
4. If the change is backwards-incompatible, bump
   `PROTOCOL_VERSION` (currently `1`) so old daemons close with `4001`.

## Hooks

`hooks/` ships shell scripts that the user wires into their
`.claude/settings.json` (`SessionStart`, `UserPromptSubmit`, `PostToolUse`,
`SessionEnd`). They are versioned and sha256-pinned; see issue #9.

## What not to do

- Don't add a second source of truth for protocol types. Don't hand-write
  TypeScript types in `coordinator/src/generated/`.
- Don't add platform-specific code paths (macOS, Windows) before the MVP is
  green on Linux x86_64.
- Don't let the daemon panic. Every `Result` in the runtime path should be
  logged and discarded, not bubbled to `main`.
