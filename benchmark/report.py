"""One JSON file per run — every script merges into it, nothing is passed between steps as text.

Ported from duckrun's `tests/parquet_layout/aemo/report.py`. The build-phase inputs
(`row_limit`, `opt_sort`, `rebuild`) are gone: this project has nothing to build, so the run's
knobs are only the measurement ones plus the engine set.

Env in: RUN_REPORT (path, default ./run_report.json).
"""
import json
import os
import sys
from datetime import datetime, timezone


def _path(path=None):
    return path or os.environ.get("RUN_REPORT", "run_report.json")


def _deep_update(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            _deep_update(a[k], v)
        else:
            a[k] = v
    return a


def merge(obj, path=None):
    path = _path(path)
    cur = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            cur = json.load(f)
    _deep_update(cur, obj)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cur, f, indent=2, default=str)


def _int(name):
    try:
        return int(os.environ.get(name, "") or 0)
    except ValueError:
        return 0


def _init():
    try:
        from importlib.metadata import version
        dv = version("duckrun")
    except Exception:
        dv = None
    merge({"run": {
        "sha": os.environ.get("GITHUB_SHA"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "date": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "engines": os.environ.get("BENCH_ENGINES"),
            # PASSES over the suite, not hot repetitions: pass 1 cold, pass 2 warm, the rest hot.
            "runs": _int("BENCH_RUNS"),
            "gap_seconds": _int("BENCH_GAP_SECONDS"),
        },
        "duckrun_version": dv,
    }})


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "init":
        _init()
        print(f"initialized {_path()}")
