# coordinator

Cloudflare Worker + Durable Object that fans out cc-relay messages.

Empty until P3 (issue #7). The `src/generated/` directory is populated by
`cargo test -p agent-core` (ts-rs) in P1 (issue #4); CI checks that the
committed tree matches the regenerated output.

This package lives **outside** the cargo workspace.
