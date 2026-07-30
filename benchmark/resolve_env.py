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

    # The engine metadata belongs in the report too — the render layer reads `writer` from there,
    # which is the axis under test: the same DAX over four identical semantic models, and the only
    # thing that differs is which adapter wrote the parquet underneath.
    report.merge({"engines": out,
                  "run": {"reference": E.reference(picked), "workspace": ws.display_name}})


if __name__ == "__main__":
    main()
