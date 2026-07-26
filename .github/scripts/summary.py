"""Unified test dashboard: EVERY engine's dbt test results in ONE table -> $GITHUB_STEP_SUMMARY.

The project's thesis is "the engine doesn't matter, the output does": one neutral reference suite
(DuckDB SQL over each engine's OneLake Delta tables) grades all four outputs. So a test row should
read identically across all four columns — the `parity` column flags a test whose verdict DIFFERS
between engines, which is the only genuinely interesting signal here.

Replaces the old per-engine summary: four separate step summaries (plus two more from the `dbt run`
legs) made it impossible to see at a glance whether a failure was engine-specific or systemic.

Reads the rr-<engine>.json snapshots the verify workflow's test matrix uploads as artifacts.
Usage: python summary.py <dir-of-rr-json>
"""
import json
import os
import sys

# Canonical column order (matches the verify.yml test matrix).
ENGINES = ["duckrun", "iceberg", "dwh", "spark"]
ICON = {"pass": "✅", "warn": "⚠️", "fail": "❌", "error": "💥", "skipped": "⏭️"}

RESULTS_DIR = sys.argv[1] if len(sys.argv) > 1 else "."


def load(engine):
    """{test_name: (status, failures)} for one engine, or None if it produced no results."""
    try:
        with open(os.path.join(RESULTS_DIR, f"rr-{engine}.json")) as fh:
            results = json.load(fh)["results"]
    except Exception:
        return None
    return {r["unique_id"].split(".")[2]: (r["status"], r.get("failures") or 0)
            for r in results if r["unique_id"].startswith("test.")}


def cell(v):
    if v is None:
        return "—"
    status, failures = v
    icon = ICON.get(status, status)
    # A row count is only meaningful for a test that actually returned rows.
    return f"{icon} {failures:,}" if status in ("fail", "warn", "error") and failures else icon


def main():
    per = {e: load(e) for e in ENGINES}
    # ALWAYS render every engine as a column. A leg that died before writing run_results.json
    # uploads no artifact, and dropping its column would hide the failure completely — the same
    # silent-omission trap as a skipped verification job. Missing legs are named explicitly below.
    live = ENGINES
    missing = [e for e in ENGINES if per[e] is None]

    print("## 🧪 Test results\n")
    if len(missing) == len(ENGINES):
        print("> **No test results at all** — every leg failed before dbt produced results.\n")
        return
    print("<sub>One neutral DuckDB suite grades all four outputs; `parity` marks rows where the "
          "engines disagree.</sub>\n")
    if missing:
        print(f"> ⚠️ **No results from {', '.join(f'`{e}`' for e in missing)}** — "
              f"{'that leg' if len(missing) == 1 else 'those legs'} failed before dbt wrote "
              f"run_results.json. The column{'' if len(missing) == 1 else 's'} below "
              f"{'is' if len(missing) == 1 else 'are'} blank, not passing.\n")

    # --- per-engine totals -------------------------------------------------
    print("| | " + " | ".join(live) + " |")
    print("| --- | " + " | ".join("--:" for _ in live) + " |")
    for label, key in (("✅ pass", "pass"), ("⚠️ warn", "warn"),
                       ("❌ fail", "fail"), ("💥 error", "error")):
        counts = [None if per[e] is None else sum(1 for s, _ in per[e].values() if s == key)
                  for e in live]
        if any(c for c in counts if c):      # hide rows that are zero/absent everywhere
            print(f"| {label} | " + " | ".join("—" if c is None else str(c) for c in counts) + " |")
    print("| **total** | "
          + " | ".join("—" if per[e] is None else f"**{len(per[e])}**" for e in live) + " |")
    print()

    # --- one row per test --------------------------------------------------
    names = sorted({n for e in live if per[e] for n in per[e]})
    bad = {"fail", "error"}

    def statuses_of(n):
        return {per[e][n][0] for e in live if per[e] and n in per[e]}

    def rank(n):
        # Anything actionable first: hard failures, then warns, then disagreements.
        statuses = statuses_of(n)
        return (0 if statuses & bad else 1 if "warn" in statuses else 2,
                0 if len(statuses) > 1 else 1, n)

    print("| test | " + " | ".join(live) + " | parity |")
    print("| --- | " + " | ".join(":--:" for _ in live) + " | :--: |")
    for n in sorted(names, key=rank):
        vals = [None if per[e] is None else per[e].get(n) for e in live]
        # Disagreement OR a blank cell both count as "not in parity" — a missing leg is unknown,
        # not agreement.
        agree = len(statuses_of(n)) == 1 and None not in vals
        print(f"| `{n}` | " + " | ".join(cell(v) for v in vals) + f" | {'' if agree else '⚠️'} |")
    print()

    warned = sorted(n for n in names if "warn" in statuses_of(n))
    if warned:
        print("<sub>`severity: warn` tests report rows but do not fail the build: "
              + ", ".join(f"`{n}`" for n in warned) + ".</sub>\n")


if __name__ == "__main__":
    main()
