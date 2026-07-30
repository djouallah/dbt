"""Deploy one semantic model per engine over that engine's own `mart.fct_summary`.

Two templates, picked by the engine's mode (benchmark/engines.py):

  directLake  — fct_summary.SemanticModel/model.bim, deployed with `lakehouse=<item>`. duckrun
                rewrites the workspace/lakehouse GUIDs baked into the M expression and then
                REFRESHES the model, so deploy() returns only once the reframe succeeded.
  directQuery — fct_summary_dq.SemanticModel/model.bim, deployed with `warehouse=<item>`. duckrun
                resolves this workspace's SQL endpoint and rewrites both halves of the
                Sql.Database(...) reference. There is deliberately NO refresh: a DirectQuery model
                has nothing to reframe, it queries live. That is not a partial deploy.

Nothing is written to any lakehouse — the models read tables the dbt run already produced.

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
TEMPLATE = {"directLake": os.path.join(HERE, "fct_summary.SemanticModel", "model.bim"),
            "directQuery": os.path.join(HERE, "fct_summary_dq.SemanticModel", "model.bim")}

# Workspace folder the models are grouped under, so a benchmark dispatch does not scatter four items
# across the workspace root next to the lakehouses. duckrun creates it if absent (and raises if an
# explicitly named folder cannot be resolved, rather than silently landing at the root).
#
# NOTE: placement happens when an item is CREATED. `overwrite=True` on an existing model updates its
# definition in place and leaves it wherever it already lives — so models deployed before this was
# set stay at the workspace root until they are deleted once and recreated.
FOLDER = os.environ.get("BENCH_FOLDER", "benchmark")


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
        mode, item = meta["mode"], meta["item"]
        name = E.model_name(e)
        bim = TEMPLATE[mode]
        # lakehouse= drives the Direct Lake OneLake-GUID rewrite; warehouse= drives the DirectQuery
        # Sql.Database rewrite. Passing the wrong one raises in duckrun rather than deploying
        # something that silently points elsewhere, which is why the mode picks both at once.
        kwargs = {"lakehouse": item} if mode == "directLake" else {"warehouse": item}
        print(f"deploying {name} -> {item} ({mode}) into folder {FOLDER!r} ...", flush=True)
        t0 = time.perf_counter()
        try:
            item_id = ws.deploy(bim, name=name, overwrite=True, folder=FOLDER, **kwargs)
        except Exception as ex:
            # One engine failing to deploy must not cost the others their run: record it and carry
            # on. xmla_compare.py benchmarks whatever actually deployed.
            failed[e] = f"{type(ex).__name__}: {str(ex).splitlines()[0][:200]}"
            print(f"  FAILED {name}: {failed[e]}", flush=True)
            continue
        secs = round(time.perf_counter() - t0, 1)
        deployed[e] = {"model": name, "item_id": item_id, "seconds": secs, "folder": FOLDER}
        refreshed = "refreshed" if mode == "directLake" else "no refresh (queries live)"
        print(f"  ok {name} ({item_id}) in {secs}s — {refreshed}", flush=True)
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
