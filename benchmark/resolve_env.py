"""Resolve the benchmark's workspace + per-engine Fabric items into $GITHUB_ENV via duckrun.

No deploy_config.yml and no pyyaml: this project already carries the workspace as `WS_ID` (a
workflow-level env in .github/workflows/ci.yml), and the engine -> item mapping lives in
benchmark/engines.py alongside .github/scripts/stats.py's copy of it. The item GUIDs are resolved
the same way .github/scripts/item_path.py does — the `duckrun` profile addresses items by GUID, not
display name, and so does Direct Lake.

Emits, for $GITHUB_ENV:
  WS_ID          — echoed back so downstream steps need only this file's output
  PBI_WORKSPACE  — the workspace DISPLAY name; the XMLA endpoint addresses by name, not GUID
  BENCH_ITEMS    — one JSON line, {engine: {item, kind, guid, writer}}

And the same three plus `matrix` to $GITHUB_OUTPUT, because the workflow runs ONE JOB
PER ENGINE — this file is the single job that resolves the workspace, so it also emits the engine
matrix those jobs fan out over. `matrix` carries an `index` per engine: the idle gap between models
is a step in each job that skips itself at index 0.

Resolving every engine here, once, is also the cheap early failure: a renamed item raises before any
capacity is spent, instead of on the third job of a serialized matrix.

Diagnostics go to stderr: CLAUDE.md's rule is that anything writing $GITHUB_ENV keeps stdout clean.
"""
import json
import os
import sys

import requests

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engines as E  # noqa: E402
import report  # noqa: E402

FAB = "https://api.fabric.microsoft.com/v1"


def main():
    ws = duckrun.workspace(os.environ["WS_ID"])
    token = duckrun.auth.get_fabric_token()
    picked = E.selected()

    # One list call per item KIND, not per engine: /lakehouses covers three of the four.
    listing = {}
    for kind in sorted({E.KIND[e] for e in picked}):
        r = requests.get(f"{FAB}/workspaces/{ws.id}/{kind}",
                         headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
        listing[kind] = {i["displayName"]: i["id"] for i in r.json().get("value", [])}

    out = {}
    for e in picked:
        item, kind = E.ITEM[e], E.KIND[e]
        guid = listing[kind].get(item)
        if not guid:
            have = ", ".join(sorted(listing[kind])) or "(none)"
            raise SystemExit(f"{kind[:-1]} {item!r} (engine {e}) not found in workspace {ws.id}; "
                             f"have: {have}")
        out[e] = {"item": item, "kind": kind, "guid": guid, "writer": E.WRITER[e]}
        sys.stderr.write(f"  {e:8s} -> {item} ({kind[:-1]}, wrote by {E.WRITER[e]}) {guid}\n")

    env = {"WS_ID": ws.id,
           "PBI_WORKSPACE": ws.display_name,
           "BENCH_ITEMS": json.dumps(out, separators=(",", ":"))}
    gh = os.environ.get("GITHUB_ENV")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write("".join(f"{k}={v}\n" for k, v in env.items()))
    for k, v in env.items():
        print(f"{k}={v}")

    # Job outputs: the same values, plus the per-engine matrix the bench jobs fan out over. Order is
    # preserved from BENCH_ENGINES, and it decides only the order they are MEASURED in: index 0 is
    # the job that skips the idle gap. No number in the report depends on it — there is no reference
    # engine, so a different order gives the same report with the jobs in a different sequence.
    matrix = {"include": [{"engine": e, "item": out[e]["item"], "writer": out[e]["writer"],
                           "index": i} for i, e in enumerate(picked)]}
    gho = os.environ.get("GITHUB_OUTPUT")
    if gho:
        with open(gho, "a", encoding="utf-8") as f:
            for k, v in env.items():
                f.write(f"{k.lower()}={v}\n")
            f.write(f"matrix={json.dumps(matrix, separators=(',', ':'))}\n")
    sys.stderr.write(f"  matrix: {', '.join(m['engine'] for m in matrix['include'])}\n")

    # The engine metadata belongs in the report too — the render layer reads `writer` from there,
    # which is the axis under test: the same DAX over four identical semantic models, and the only
    # thing that differs is which adapter wrote the parquet underneath.
    report.merge({"engines": out, "run": {"workspace": ws.display_name}})


if __name__ == "__main__":
    main()
