"""One JSON document per dispatch — every stage merges into it, nothing is passed as text.

**The point of this file is the item GUID.** Before it, nothing recorded which Fabric items a run
created: `provision.py` resolved every GUID and wrote it to stderr, `stats.py` resolved all four and
dropped them on the floor, and the notebook's was never seen on the runner at all. CU could then only
be attributed by matching item DISPLAY NAMES, which is why `cu/` had an `engine_of()` substring
matcher, a lagging `'Items'` snapshot join, and a `shared` column for everything it could not name.

A run now writes down every GUID it touched. Combined with the teardown — every item except
`dbt_landing` is deleted when the run finishes — a GUID belongs to exactly one run, and attributing
CU becomes a dictionary lookup instead of a heuristic.

Env in: `RUN_RECORD` (path to the fragment this stage writes). **Unset is a no-op**, deliberately:
`provision.py` and `stats.py` must stay runnable by hand to reproduce a CI failure, and neither
should need a record path to do it.

Each stage writes its OWN fragment file — separate jobs run on separate runners and cannot share one
— and a final job merges them by basename order, exactly as `benchmark/merge_reports.py` already does
for the benchmark's fragments. That is also why `items` is a dict keyed by GUID rather than a list:
a deep merge unions dicts and REPLACES lists, so the teardown fragment's `{guid: {deleted: ...}}`
lands on top of the provision fragment's `{guid: {created: ...}}` without either knowing about the
other.

This is a 20-line copy of `benchmark/report.py`'s helper rather than an import of it. `.github/scripts/`
and `benchmark/` cannot import each other without a sys.path hack, and `benchmark/` being deletable by
removing one directory is a property worth more than twenty lines.
"""
import glob
import json
import os
import sys
from datetime import datetime, timezone


def path(p=None):
    """The fragment this stage writes, or None when there is no record to write."""
    return p or os.environ.get("RUN_RECORD") or None


def now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def deep_update(a, b):
    """Recursive dict union, b winning. Lists and scalars are replaced, not appended."""
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            deep_update(a[k], v)
        else:
            a[k] = v
    return a


def merge(obj, p=None):
    """Deep-merge `obj` into the fragment. No RUN_RECORD set -> nothing happens, silently."""
    p = path(p)
    if not p:
        return None
    cur = {}
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            cur = json.load(f)
    deep_update(cur, obj)
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cur, f, indent=1, sort_keys=True, default=str)
    return p


def item(guid, role, kind, name, **extra):
    """Record one Fabric item under its GUID.

    `role` is what replaces name matching downstream, so keep the vocabulary closed:
    `landing` | `output` | `dwh_src` | `folder` | `compute` | `semantic_model`.
    `kind` is Fabric's own item type (`Lakehouse`, `Warehouse`, `Notebook`, `SemanticModel`).

    A GUID is written down even when this run only FOUND the item (`created: false`) — `dbt_landing`
    is the case that matters, since its CU is a shared input cost that must be attributable to the
    runs that read it without ever being charged to an engine.
    """
    if not guid:
        return None
    rec = {"role": role, "kind": kind, "name": name, **extra}
    return merge({"items": {str(guid).upper(): rec}})


def fragments(paths):
    """Every *.json under each path, sorted by BASENAME.

    Basename, not full path: `actions/download-artifact` nests each artifact in its own directory,
    so the full paths sort by artifact name and the intended `record-00-run.json` first ordering is
    lost. Ordering only decides who wins a scalar collision — the dicts union either way.
    """
    found = []
    for p in paths:
        if os.path.isdir(p):
            found += glob.glob(os.path.join(p, "**", "*.json"), recursive=True)
        elif os.path.exists(p):
            found.append(p)
    return sorted(found, key=os.path.basename)


def combine(paths, dest):
    """Merge every fragment into `dest`. Returns the list merged, so the caller can log it."""
    got = fragments(paths)
    for f in got:
        with open(f, encoding="utf-8") as fh:
            merge(json.load(fh), dest)
    return got


def _init():
    """Seed the run block from the workflow environment. `RUN_*` env vars are set by all.yml."""
    merge({
        "schema": 1,
        "run": {
            "id": os.environ.get("GITHUB_RUN_ID"),
            "sha": os.environ.get("GITHUB_SHA"),
            "started": now(),
            "url": (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
                    f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/"
                    f"{os.environ.get('GITHUB_RUN_ID', '')}"),
        },
        "engine": os.environ.get("RUN_ENGINE") or None,
        "inputs": {k.lower()[6:]: v for k, v in os.environ.items()
                   if k.startswith("RUNIN_") and v != ""},
    })


def finish(frag_dir, bench, dest):
    """Close the record: merge every fragment, fold in the benchmark's raw report, stamp finished.

    `bench` is `run_report.json` or a path that does not exist — a build-only dispatch has no
    benchmark, and the record then simply has no `benchmark` key rather than an empty one.
    """
    got = combine([frag_dir], dest)
    if bench and os.path.exists(bench):
        with open(bench, encoding="utf-8") as f:
            merge({"benchmark": json.load(f)}, dest)
        got.append(bench)
    with open(dest, encoding="utf-8") as f:
        doc = json.load(f)
    # DERIVED from the run's own items, not from a dispatch input. The teardown deletes every output
    # item, so a build normally starts from nothing — but only if the previous run's teardown
    # actually ran, and a dispatch with `teardown: false` leaves tables behind for the next one to
    # build on incrementally. `created: true` on the output item is the fact that settles it; the
    # input only ever stated an intention.
    merge({"run": {"finished": now()},
           "full_load": any(it.get("role") == "output" and it.get("created")
                            for it in (doc.get("items") or {}).values())}, dest)
    with open(dest, encoding="utf-8") as f:
        doc = json.load(f)
    sys.stderr.write(
        f"  merged {len(got)} document(s): "
        + ", ".join(os.path.basename(g) for g in got) + "\n"
        f"  {dest}: {len(doc.get('items') or {})} item GUIDs, top-level keys {sorted(doc)}\n")
    for guid, it in sorted((doc.get("items") or {}).items(),
                           key=lambda kv: (kv[1].get("role", ""), kv[1].get("name", ""))):
        sys.stderr.write(f"    {it.get('role', '?'):<15} {it.get('name', '?'):<24} {guid}"
                         + ("  (deleted)" if it.get("deleted") else "") + "\n")
    reindex(os.path.dirname(dest))
    return dest


def reindex(run_dir):
    """Write `<run_dir>/index.json` — the record filenames, sorted. Best-effort.

    WHY: the page listed this directory through the GitHub CONTENTS API, which is 60 requests per
    hour per IP unauthenticated and returns 403 when that runs out — one wasted page load per
    reader, and the page then shows an error instead of the data. raw.githubusercontent.com serves
    repo files with no such limit, but it serves FILES and not indexes, so the index has to be a
    file. Committed beside the records it names, in the same commit, so it cannot lag them.

    `legacy/` is excluded here exactly as the loader excluded directories: those records predate the
    item GUIDs and cannot be joined to a ledger.

    A record moved by hand without re-running this leaves the index stale; the loader drops a name it
    cannot fetch and falls back to the contents API when the index is missing entirely, so the
    failure is a slightly short page rather than an error.
    """
    try:
        names = sorted(f for f in os.listdir(run_dir)
                       if f.endswith(".json") and f != "index.json"
                       and os.path.isfile(os.path.join(run_dir, f)))
        with open(os.path.join(run_dir, "index.json"), "w", encoding="utf-8") as f:
            json.dump(names, f, indent=1)
        sys.stderr.write(f"  indexed {len(names)} record(s) -> {run_dir}/index.json\n")
    except Exception as e:                              # noqa: BLE001 — never fail the record job
        sys.stderr.write(f"  index not written ({type(e).__name__}: {e})\n")


if __name__ == "__main__":
    argv = sys.argv[1:]
    if not argv:
        raise SystemExit("usage: record.py init | merge '<json>' | combine <dir>... <dest> "
                         "| finish <fragment dir> <bench report|-> <dest>")
    if argv[0] == "init":
        _init()
        print(path() or "(no RUN_RECORD set)")
    elif argv[0] == "merge":
        merge(json.loads(argv[1]))
        print(path() or "(no RUN_RECORD set)")
    elif argv[0] == "combine":
        *srcs, dest = argv[1:]
        got = combine(srcs, dest)
        sys.stderr.write(f"merged {len(got)} fragment(s) into {dest}\n")
        print(dest)
    elif argv[0] == "finish":
        frag, bench, dest = argv[1], argv[2], argv[3]
        print(finish(frag, None if bench == "-" else bench, dest))
    else:
        raise SystemExit(f"unknown command {argv[0]}")
