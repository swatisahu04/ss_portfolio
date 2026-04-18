#!/usr/bin/env bash
#
# AlphaAgent end-to-end smoke test.
#
# Runs the full happy-path locally:
#   1. syntax-check every Python module
#   2. dry-run eval harness (parses reference SQL for all 30 questions)
#   3. unit tests (API routes + anything in tests/unit)
#   4. if Docker is available: stand up services, seed, transform, eval, serve
#
# Exit non-zero on any step failure.
#
# Usage:
#   bash scripts/smoke_test.sh                # fast path (no docker)
#   bash scripts/smoke_test.sh --full         # full path (requires docker)

set -euo pipefail

FULL=${1:-}
cd "$(dirname "$0")/.."

say() { printf "\n\033[36m==> %s\033[0m\n" "$*"; }
ok()  { printf "\033[32m✓ %s\033[0m\n" "$*"; }
die() { printf "\033[31m✗ %s\033[0m\n" "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
say "1/5 · Python syntax check"
python3 - <<'PY'
import ast, pathlib, sys
root = pathlib.Path(".")
skip = {".venv", "node_modules", ".git", "dbt_project/target", "dbt_project/dbt_packages"}
files = [p for p in root.rglob("*.py")
         if not any(s in str(p) for s in skip)]
bad = []
for p in files:
    try:
        ast.parse(p.read_text())
    except SyntaxError as e:
        bad.append(f"{p}: {e}")
if bad:
    print("\n".join(bad)); sys.exit(1)
print(f"{len(files)} files OK")
PY
ok "syntax clean"

# ---------------------------------------------------------------------------
say "2/5 · Unit tests"
python3 -m pytest tests/unit -v --tb=short 2>&1 | tail -30
ok "unit tests pass"

# ---------------------------------------------------------------------------
say "3/5 · Eval harness — dry run (no DB, no LLM)"
python3 -m evals.run --dry-run --out /tmp/alphaagent-scorecard.md --csv /tmp/alphaagent-results.csv
grep -q "AlphaAgent Eval Scorecard" /tmp/alphaagent-scorecard.md \
  && ok "scorecard generated" \
  || die "scorecard missing or malformed"

# ---------------------------------------------------------------------------
say "4/5 · SQL safety validator — fuzz the banned-node list"
python3 - <<'PY'
from agent.safe_exec import validate_sql
cases = [
    ("SELECT 1", True),
    ("SELECT * FROM marts.fct_portfolio_performance_daily LIMIT 1", True),
    ("DROP TABLE marts.foo", False),
    ("DELETE FROM marts.foo", False),
    ("INSERT INTO marts.foo VALUES (1)", False),
    ("UPDATE marts.foo SET x = 1", False),
    ("TRUNCATE TABLE marts.foo", False),
    ("ALTER TABLE marts.foo ADD COLUMN x INT", False),
    ("SELECT * FROM raw.trades", False),  # non-mart schema
    ("SELECT * FROM portfolios", False),  # unqualified
]
fail = []
for sql, want_ok in cases:
    ok_, errs = validate_sql(sql)
    if ok_ != want_ok:
        fail.append(f"  {sql!r:60} expected ok={want_ok} got ok={ok_} errors={errs}")
if fail:
    print("FAIL:"); print("\n".join(fail)); raise SystemExit(1)
print(f"{len(cases)} safety cases passed")
PY
ok "SQL validator behaves correctly on the 10-case fuzz"

# ---------------------------------------------------------------------------
if [[ "$FULL" != "--full" ]]; then
  say "5/5 · (skipping docker smoke — pass --full to include)"
  ok "fast smoke complete"
  exit 0
fi

# ---------------------------------------------------------------------------
say "5/5 · Full docker smoke (requires Docker Desktop)"
command -v docker >/dev/null || die "docker not installed"

make up
sleep 15
make seed || die "seed failed"
make transform || die "dbt build failed"
make ge || die "data quality checks failed"

# start API in background, hit health, stop it
make api &
API_PID=$!
trap "kill $API_PID 2>/dev/null" EXIT
sleep 5
curl -fsS localhost:8000/v1/health | python3 -m json.tool
curl -fsS localhost:8000/v1/portfolios | python3 -c 'import json,sys; d=json.load(sys.stdin); print(f"{len(d[\"portfolios\"])} portfolios")'

AGENT_LLM_MOCK=1 make eval || die "eval regressed"
ok "full smoke test passed"
