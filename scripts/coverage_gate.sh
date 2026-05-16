#!/usr/bin/env bash
# Coverage gate: enforce that no source line in the workspace has zero
# execution count.
#
# Two-layer check (matches `ippoan/ci-workflows`'s `coverage_100.toml`
# convention adapted for Rust):
#
#   1. Files listed in `coverage_100.toml` MUST be at 100% — every
#      source line in each listed file has to execute at least once
#      under `cargo test --workspace`.
#   2. The per-line allowlist (`scripts/coverage_allowlist.txt`) is the
#      explicit, reviewer-visible record of "this line cannot be hit by
#      a unit test" (llvm-cov brace-attribution artifacts, defensive
#      transport-error arms that reqwest never produces, etc.).
#
# Files NOT in `coverage_100.toml` are excluded entirely by
# `--ignore-filename-regex`. They are 1-3 line shims that wire real I/O
# (`tokio::io::stdin`, `tokio_tungstenite::connect_async`, the binary
# entry point) into a testable inner core that lives in another file.

set -euo pipefail

cd "$(dirname "$0")/.."

# Files NOT enforced — shim files only.
IGNORE_REGEX='crates/agent-cli/src/(main|runners)\.rs|crates/agent-mcp/src/(relay_ws|channel_ws)\.rs'

# Two-call pattern: run tests + collect raw profdata first, then ask
# `cargo llvm-cov report` for both the human-readable summary and the
# per-line annotated source. Combining `--text` with the test run on
# the first call only emits per-line text into stdout interleaved with
# test runner output on some `cargo-llvm-cov` versions — invoking
# `report --text` separately produces a deterministic file.
cargo llvm-cov --workspace --all-features --no-fail-fast --no-report

cargo llvm-cov report --summary-only \
    --ignore-filename-regex "$IGNORE_REGEX" \
    | tee coverage_summary.txt

cargo llvm-cov report --text \
    --ignore-filename-regex "$IGNORE_REGEX" \
    > coverage.txt

echo
echo "=== uncovered source lines (line count == 0) ==="

ALLOWLIST="${PWD}/scripts/coverage_allowlist.txt"
COVERAGE_100="${PWD}/coverage_100.toml"
export ALLOWLIST COVERAGE_100

python3 - <<'PY'
import os
import re
import sys

# ---- parse allowlist ----
allowlist = set()
allowlist_path = os.environ.get("ALLOWLIST", "")
if allowlist_path and os.path.exists(allowlist_path):
    with open(allowlist_path) as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            allowlist.add(line)

# ---- parse coverage_100.toml ----
required_files = set()
cov100_path = os.environ.get("COVERAGE_100", "")
if cov100_path and os.path.exists(cov100_path):
    with open(cov100_path) as f:
        for raw in f:
            m = re.match(r'^\s*path\s*=\s*"(.+)"\s*$', raw)
            if m:
                required_files.add(m.group(1))

# ---- parse llvm-cov text ----
with open("coverage.txt") as f:
    raw = f.read()
parts = re.split(r'^(/[^\n:]+\.rs):\n', raw, flags=re.M)

uncovered_total = 0
files_with_uncov = []
files_seen = set()

for i in range(1, len(parts), 2):
    path = parts[i]
    body = parts[i + 1] if i + 1 < len(parts) else ""

    if "/registry/" in path or "/.cargo/" in path:
        continue

    rel = path.split("/cc-relay/")[-1] if "/cc-relay/" in path else path
    files_seen.add(rel)

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

# ---- check coverage_100.toml files were seen ----
missing_required = required_files - files_seen
if missing_required:
    for rel in sorted(missing_required):
        print(
            f"::error file={rel}::registered in coverage_100.toml but "
            f"not present in coverage.txt — has the file been deleted "
            f"or moved?"
        )
    sys.exit(1)

# ---- report ----
if files_with_uncov:
    fail_required = []
    fail_other = []
    for rel, uncov in files_with_uncov:
        head = ", ".join(str(n) for n in uncov[:25])
        more = f" (+{len(uncov) - 25} more)" if len(uncov) > 25 else ""
        msg = f"::error file={rel}::uncovered lines: {head}{more}"
        if rel in required_files:
            fail_required.append(msg)
        else:
            fail_other.append(msg)
    for m in fail_required + fail_other:
        print(m)
    print(
        f"\n{uncovered_total} uncovered source lines across "
        f"{len(files_with_uncov)} files — coverage gate FAILED"
    )
    sys.exit(1)

print(
    f"all {len(required_files)} coverage_100.toml files at 100%, "
    f"with {len(allowlist)} documented allowlist entries — coverage gate PASSED"
)
PY
