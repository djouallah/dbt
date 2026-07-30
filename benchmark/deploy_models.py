"""Deploy one semantic model per engine over that engine's own `mart.fct_summary`.

The experiment is: **one DAX suite, four identical semantic models, four dbt adapters.** The adapter
that wrote the parquet is the only variable; everything on top of it is held constant on purpose. So
there is ONE `.bim`, deployed four times, and every knob that could differ per engine has been
removed rather than left configurable.

`ws.deploy()` takes two arguments here, and only the first varies:

  `lakehouse=` / `warehouse=`  — WHICH item holds the tables, from the item's kind (engines.KIND).
                                 duckrun raises rather than silently pointing elsewhere if the wrong
                                 one is passed.
  `mode=`                      — HOW it is read: `engines.DEPLOY_MODE`, one constant, Direct Lake.
                                 duckrun rewrites every table to an entity partition over one
                                 AzureStorage.DataLake expression on the item's OneLake root and sets
                                 directLakeBehavior=directLakeOnly, so a query Direct Lake cannot
                                 serve FAILS rather than falling back to the SQL endpoint and logging
                                 a pushdown time that would read as a slow layout.

Requires duckrun >= 0.4.36, which made `mode=` independent of the item kind. Before it a warehouse
could only be read by DirectQuery, which is why `dwh` used to be measured differently from the other
three — a second hand-authored template, hot-only, no reframe, scoped out of every COLD table. A
warehouse's Tables are Delta in OneLake like any other item's, so that asymmetry was never about the
storage, and it is gone: all four are now the same measurement.

Every model is Direct Lake, so every deploy REFRESHES (a reframe onto the latest Delta) and returns
only once the model is live. Nothing is written to any lakehouse — the models read tables the dbt run
already produced.

Env in: WS_ID, BENCH_ITEMS (from resolve_env.py), BENCH_ENGINES, BENCH_FOLDER (optional).
"""
import os
import sys
import time

import requests

import duckrun

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engines as E  # noqa: E402
import report  # noqa: E402

FAB = "https://api.fabric.microsoft.com/v1"

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "fct_summary.SemanticModel", "model.bim")

# Workspace folder the models are grouped under, so a benchmark dispatch does not scatter four items
# across the workspace root next to the lakehouses. duckrun creates it if absent (and raises if an
# explicitly named folder cannot be resolved, rather than silently landing at the root).
#
# NOTE: placement happens when an item is CREATED. `overwrite=True` on an existing model updates its
# definition in place and leaves it wherever it already lives — so models deployed before this was
# set stay at the workspace root until they are deleted once and recreated.
FOLDER = os.environ.get("BENCH_FOLDER", "benchmark")


def deploy_kwargs(meta):
    """The `ws.deploy()` kwargs for one engine's BENCH_ITEMS entry.

    Only the item argument varies, and it follows the item's KIND — independent of the storage mode
    since duckrun 0.4.36. Passing `lakehouse=` for a warehouse (or the reverse) raises rather than
    deploying something that points elsewhere. The mode is the same constant for every engine, which
    is the point: four adapters, one way of reading what they wrote.

    Extracted from main() so the pairing is testable without Fabric — getting it wrong costs a
    deploy failure partway through a paid run."""
    kw = {"warehouse": meta["item"]} if meta["kind"] == "warehouses" else {"lakehouse": meta["item"]}
    kw["mode"] = E.DEPLOY_MODE
    return kw


def _reparent(ws, item_id, name):
    """Move an ALREADY-EXISTING model into FOLDER.

    `deploy(folder=...)` only places an item when it CREATES it — an `overwrite` updates the
    definition in place and leaves the item wherever it already lives. Without this, a model first
    deployed before FOLDER was set stays at the workspace root for good, and the only fix is deleting
    it by hand. Best-effort by design: placement is cosmetic, so a failure here warns and the
    benchmark carries on.
    """
    h = {"Authorization": f"Bearer {duckrun.auth.get_fabric_token()}"}
    try:
        r = requests.get(f"{FAB}/workspaces/{ws.id}/folders", headers=h)
        r.raise_for_status()
        fid = next((f["id"] for f in r.json().get("value", [])
                    if f.get("displayName") == FOLDER and not f.get("parentFolderId")), None)
        if not fid:
            return                        # deploy() creates it; nothing to move into yet
        r = requests.get(f"{FAB}/workspaces/{ws.id}/items/{item_id}", headers=h)
        r.raise_for_status()
        if r.json().get("folderId") == fid:
            return                        # already there — the common case after the first run
        r = requests.post(f"{FAB}/workspaces/{ws.id}/items/{item_id}/move",
                          headers=h, json={"targetFolderId": fid})
        if r.status_code in (200, 201, 202):
            print(f"  moved {name} into folder {FOLDER!r}", flush=True)
        else:
            print(f"  note: could not move {name} into {FOLDER!r} "
                  f"(HTTP {r.status_code}) — it stays where it is", flush=True)
    except Exception as ex:
        print(f"  note: folder placement for {name} skipped "
              f"({type(ex).__name__}: {str(ex).splitlines()[0][:120]})", flush=True)


def main():
    items = E.items()
    picked = [e for e in E.selected() if e in items]
    ws = duckrun.workspace(os.environ["WS_ID"])

    deployed, failed = {}, {}
    for e in picked:
        meta = items[e]
        item, kind = meta["item"], meta["kind"]
        name = E.model_name(e)
        kwargs = deploy_kwargs(meta)
        print(f"deploying {name} -> {item} ({kind[:-1]}, {E.DEPLOY_MODE}) into folder {FOLDER!r} ...",
              flush=True)
        t0 = time.perf_counter()
        try:
            item_id = ws.deploy(TEMPLATE, name=name, overwrite=True, folder=FOLDER, **kwargs)
        except Exception as ex:
            # One engine failing to deploy must not cost the others their run: record it and carry
            # on. xmla_compare.py benchmarks whatever actually deployed.
            failed[e] = f"{type(ex).__name__}: {str(ex).splitlines()[0][:200]}"
            print(f"  FAILED {name}: {failed[e]}", flush=True)
            continue
        secs = round(time.perf_counter() - t0, 1)
        deployed[e] = {"model": name, "item_id": item_id, "seconds": secs, "folder": FOLDER}
        # Direct Lake everywhere, so deploy always reframes onto the latest Delta and returns only
        # once the model is live.
        print(f"  ok {name} ({item_id}) in {secs}s — refreshed", flush=True)
        _reparent(ws, item_id, name)

    report.merge({"deploy": {"deployed": deployed, "failed": failed}})

    if not deployed:
        sys.exit("no semantic model deployed — nothing to benchmark")
    # The reference engine is the one every ratio is taken against; without it there is no
    # comparison to draw, only a column of absolute numbers.
    ref = E.reference(picked)
    if ref not in deployed:
        sys.exit(f"reference engine {ref!r} failed to deploy ({failed.get(ref, 'unknown')}) — "
                 "every comparison is measured against it, so there is nothing to report")
    print(f"\ndeployed {len(deployed)}/{len(picked)}: {', '.join(deployed)}"
          + (f" (failed: {', '.join(failed)})" if failed else ""))
    # Hand the survivors on, so a failed deploy can't make xmla_compare wait 16 retries on a model
    # that was never created.
    gh = os.environ.get("GITHUB_ENV")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(f"BENCH_ENGINES={','.join(e for e in picked if e in deployed)}\n")


if __name__ == "__main__":
    main()
