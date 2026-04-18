"""
Eval harness for AlphaAgent.

Scores every question on:
  - parse_ok           : Did the generated SQL parse as PostgreSQL?
  - critic_passed      : Did the critic accept it (safe + schema-valid)?
  - contains_keywords  : Do expected substrings appear in generated SQL?
  - executes           : Did the SQL run without error?
  - result_matches_ref : Do result values agree with reference SQL (within tolerance)?

Produces:
  evals/scorecard.md     — human-readable summary
  evals/results.csv      — per-question results for regression tracking

Modes:
  --dry-run   : skip LLM calls, skip DB — only parses reference SQL (CI sanity).
"""
from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime
from pathlib import Path

import psycopg
import yaml

from agent.config import get_agent_settings
from agent.safe_exec import validate_sql

# Lazy — only imported when we actually run the agent (not in --dry-run).
def _lazy_run_agent(question: str):
    from agent.graph import run_agent
    return run_agent(question)


run_agent = _lazy_run_agent

EVALS_DIR = Path(__file__).resolve().parent


def load_questions(path: Path) -> list[dict]:
    return yaml.safe_load(path.read_text())


def run_reference_sql(sql: str) -> tuple[bool, list[tuple], str | None]:
    s = get_agent_settings()
    try:
        with psycopg.connect(s.pg_dsn_readonly, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return True, cur.fetchall() if cur.description else [], None
    except Exception as e:
        return False, [], str(e)


def results_match(ref: list[tuple], agent: list[dict], tolerance: float = 1e-4) -> bool:
    """
    Loose compare: same row count + numeric cells match within tolerance.
    String cells must match exactly (case-insensitive).
    """
    if len(ref) != len(agent):
        return False
    if not ref:
        return True
    agent_rows = [tuple(r.values()) for r in agent]
    # sort both by first column (or all) to be order-agnostic
    try:
        ref_sorted = sorted(ref, key=lambda r: tuple(str(c) for c in r))
        agent_sorted = sorted(agent_rows, key=lambda r: tuple(str(c) for c in r))
    except TypeError:
        ref_sorted, agent_sorted = ref, agent_rows
    for r, a in zip(ref_sorted, agent_sorted):
        if len(r) != len(a):
            return False
        for rc, ac in zip(r, a):
            if isinstance(rc, (int, float)) and isinstance(ac, (int, float)):
                if abs(float(rc) - float(ac)) > tolerance * max(1.0, abs(float(rc))):
                    return False
            else:
                if str(rc).strip().lower() != str(ac).strip().lower():
                    return False
    return True


def check_keywords(sql: str, required: list[str]) -> bool:
    if not required:
        return True
    low = sql.lower()
    return all(kw.lower() in low for kw in required)


def run_eval(questions: list[dict], dry_run: bool = False) -> list[dict]:
    results = []
    for q in questions:
        row = {"id": q["id"], "difficulty": q["difficulty"], "category": q["category"]}

        # Reference SQL parse check (pure)
        ref_ok, ref_errors = validate_sql(q["reference_sql"])
        row["reference_sql_parses"] = ref_ok

        if dry_run:
            results.append(row | {"parse_ok": None, "critic_passed": None,
                                   "keywords_ok": None, "executes": None,
                                   "result_matches_ref": None, "cost_usd": 0.0,
                                   "latency_ms": 0})
            continue

        # Reference result
        ref_success, ref_rows, ref_err = run_reference_sql(q["reference_sql"])

        # Agent run
        t0 = time.perf_counter()
        try:
            state = run_agent(q["question"])
            latency_ms = int((time.perf_counter() - t0) * 1000)
        except Exception as e:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            results.append(row | {"parse_ok": False, "critic_passed": False,
                                   "keywords_ok": False, "executes": False,
                                   "result_matches_ref": False, "cost_usd": 0.0,
                                   "latency_ms": latency_ms, "error": str(e)})
            continue

        gen_sql = state.get("generated_sql", "")
        parse_ok = False
        try:
            import sqlglot
            sqlglot.parse_one(gen_sql, read="postgres")
            parse_ok = True
        except Exception:
            parse_ok = False

        critic_passed = state.get("sql_valid", False)
        keywords_ok = check_keywords(gen_sql, q.get("expected_sql_contains", []))
        executes = state.get("final_status") == "success" and not state.get("error")
        match = (ref_success and executes
                 and results_match(ref_rows, state.get("result_rows", [])))

        results.append(row | {
            "parse_ok": parse_ok,
            "critic_passed": critic_passed,
            "keywords_ok": keywords_ok,
            "executes": executes,
            "result_matches_ref": match,
            "cost_usd": round(state.get("llm_cost_usd", 0.0), 6),
            "latency_ms": latency_ms,
            "generated_sql": gen_sql[:500],
            "answer": state.get("answer", "")[:300],
        })
    return results


def write_scorecard(results: list[dict], out_path: Path) -> None:
    total = len(results)

    def pct(key: str) -> str:
        n = sum(1 for r in results if r.get(key))
        return f"{n}/{total} ({100 * n / total:.0f}%)"

    by_diff = {}
    for r in results:
        by_diff.setdefault(r["difficulty"], []).append(r)

    total_cost = sum(r.get("cost_usd", 0) for r in results)
    avg_latency = sum(r.get("latency_ms", 0) for r in results) / max(total, 1)

    md = f"""# AlphaAgent Eval Scorecard

**Run at**: {datetime.utcnow().isoformat()}Z
**Questions**: {total}
**Total LLM cost**: ${total_cost:.4f}
**Avg latency**: {avg_latency:.0f}ms

## Overall Metrics

| Metric | Score |
|---|---|
| Parses as SQL | {pct('parse_ok')} |
| Critic accepts | {pct('critic_passed')} |
| Contains required keywords | {pct('keywords_ok')} |
| Executes successfully | {pct('executes')} |
| **Result matches reference** | **{pct('result_matches_ref')}** |

## By Difficulty

| Difficulty | N | Executes | Matches Ref |
|---|---|---|---|
"""
    for diff in ["easy", "medium", "hard"]:
        sub = by_diff.get(diff, [])
        if not sub:
            continue
        ex = sum(1 for r in sub if r.get("executes"))
        m = sum(1 for r in sub if r.get("result_matches_ref"))
        md += f"| {diff} | {len(sub)} | {ex}/{len(sub)} | {m}/{len(sub)} |\n"

    md += "\n## Per-Question Results\n\n"
    md += "| ID | Difficulty | Parse | Critic | KW | Exec | Match | Cost | ms |\n"
    md += "|---|---|---|---|---|---|---|---|---|\n"
    for r in results:
        def _m(k: str) -> str:
            v = r.get(k)
            return "✓" if v else ("–" if v is None else "✗")
        md += (f"| {r['id']} | {r['difficulty']} | {_m('parse_ok')} | "
               f"{_m('critic_passed')} | {_m('keywords_ok')} | "
               f"{_m('executes')} | {_m('result_matches_ref')} | "
               f"${r.get('cost_usd', 0):.4f} | {r.get('latency_ms', 0)} |\n")

    out_path.write_text(md)


def write_csv(results: list[dict], out_path: Path) -> None:
    if not results:
        return
    # stable column set
    keys = ["id", "difficulty", "category", "parse_ok", "critic_passed",
            "keywords_ok", "executes", "result_matches_ref", "cost_usd", "latency_ms"]
    with out_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in keys})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--questions", type=Path, default=EVALS_DIR / "golden.yaml")
    ap.add_argument("--out", type=Path, default=EVALS_DIR / "scorecard.md")
    ap.add_argument("--csv", type=Path, default=EVALS_DIR / "results.csv")
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip LLM calls and DB; parse reference SQL only.")
    ap.add_argument("--mock-llm", action="store_true",
                    help="Use deterministic mock LLM (no API cost).")
    args = ap.parse_args()

    if args.mock_llm:
        os.environ["AGENT_LLM_MOCK"] = "1"

    questions = load_questions(args.questions)
    print(f"→ running eval on {len(questions)} questions (dry_run={args.dry_run})")

    results = run_eval(questions, dry_run=args.dry_run)
    write_scorecard(results, args.out)
    write_csv(results, args.csv)

    print(f"✓ scorecard: {args.out}")
    print(f"✓ csv:       {args.csv}")

    if not args.dry_run:
        match = sum(1 for r in results if r.get("result_matches_ref"))
        pct = 100 * match / max(len(results), 1)
        print(f"\nResult-match accuracy: {match}/{len(results)} ({pct:.0f}%)")


if __name__ == "__main__":
    main()
