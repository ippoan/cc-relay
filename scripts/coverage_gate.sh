#!/usr/bin/env bash
# Coverage gate: enforce that no source line in the workspace has zero
# execution count.
#
# `cargo-llvm-cov`'s region-based percentage can dip below 100% even
# when every source line *did* execute, because llvm-cov assigns
# multi-region lines (e.g. `.map_err(|e| ...)` closure bodies, or the
# closing `}` of an `if let Some(...) { }` chained off `?`) to a single
# line and reports the line as partial when only one region fires. The
# user-visible question — "is there a source line we never ran?" — is
# best answered by `cargo llvm-cov report --text`, where each source
# line gets one count column. We grep for `0|` in that column.
#
# Files on the IGNORE_REGEX list are skipped:
#   - `crates/agent-cli/src/main.rs` — 3-line shim (`fn main() { run() }`).
#   - `crates/agent-cli/src/runners.rs` — the runtime shims that call
#     `agent_mcp::stdio::run` (real stdin loop), `agent_mcp::relay::run`
#     (real WebSocket), `auth::start_device_authorization` (real HTTP).
#     The bodies are dependency construction; the testable cores live in
#     `agent-mcp/src/{stdio,channel,relay}.rs` and `agent-broker/src/auth.rs`.
#
# Lines on the per-file allowlist (`scripts/coverage_allowlist.txt`)
# are also tolerated. Each entry is "<relpath>:<line>" with a `# reason`
# comment. The allowlist is the documented record of "this line cannot
# be hit by a unit test"; reviewers should push back when it grows.

set -euo pipefail

cd "$(dirname "$0")/.."

IGNORE_REGEX='crates/agent-cli/src/(main|runners)\.rs'

cargo llvm-cov --workspace --all-features --no-fail-fast \
    --ignore-filename-regex "$IGNORE_REGEX" \
    --text \
    > coverage.txt

cargo llvm-cov report --summary-only \
    --ignore-filename-regex "$IGNORE_REGEX" \
    | tee coverage_summary.txt

echo
echo "=== uncovered source lines (line count == 0) ==="

ALLOWLIST="${PWD}/scripts/coverage_allowlist.txt"
export ALLOWLIST

python3 - <<'PY'
import os
import re
import sys

allowlist = set()
allowlist_path = os.environ.get("ALLOWLIST", "")
if allowlist_path and os.path.exists(allowlist_path):
    with open(allowlist_path) as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            # Format: "<rel-path>:<line>" — single line numbers only.
            allowlist.add(line)

with open("coverage.txt") as f:
    raw = f.read()

# Split by per-file headers — `cargo llvm-cov ... --text` prefixes each
# file's annotated source with `<absolute_path>:` on its own line.
parts = re.split(r'^(/[^\n:]+\.rs):\n', raw, flags=re.M)

uncovered_total = 0
files_with_uncov = []

for i in range(1, len(parts), 2):
    path = parts[i]
    body = parts[i + 1] if i + 1 < len(parts) else ""

    if "/registry/" in path or "/.cargo/" in path:
        continue

    rel = path.split("/cc-relay/")[-1] if "/cc-relay/" in path else path

    uncov = []
    for line in body.split("\n"):
        m = re.match(r"^\s+(\d+)\|\s+0\|", line)
        if m:
            n = int(m.group(1))
            key = f"{rel}:{n}"
            if key in allowlist:
                continue
            uncov.append(n)

    if uncov:
        uncovered_total += len(uncov)
        files_with_uncov.append((rel, uncov))

if files_with_uncov:
    for rel, uncov in files_with_uncov:
        head = ", ".join(str(n) for n in uncov[:25])
        more = f" (+{len(uncov) - 25} more)" if len(uncov) > 25 else ""
        print(f"::error file={rel}::uncovered lines: {head}{more}")
    print(
        f"\n{uncovered_total} uncovered source lines across "
        f"{len(files_with_uncov)} files — coverage gate FAILED"
    )
    sys.exit(1)

print(
    f"all source lines executed at least once "
    f"({len(allowlist)} allowlist entries) — coverage gate PASSED"
)
PY
