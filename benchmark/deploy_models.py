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

**Each model is DELETED before it is deployed**, so the deploy creates a new item rather than
updating one in place. That is the benchmark's cold guarantee: a newly created dataset has an empty
VertiPaq store, so pass 1 of the session pays the full transcode. `overwrite=True` alone keeps the
item id and can inherit the previous dispatch's resident columns. See `_delete_existing` for the
alternatives that were checked and rejected, and for the costs.

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


def _delete_existing(ws, name):
    """Delete the semantic model called `name` so the deploy below CREATES a new item.

    **This is the benchmark's cold guarantee, and the only reset in the run.** A newly created
    dataset has an empty VertiPaq store — "After the initial semantic model load, no column data is
    resident in memory yet. Direct Lake is cold." — so pass 1 of the measurement pays the full
    Delta->memory transcode. `overwrite=True` on its own does NOT give that: it updates the
    definition in place and keeps the item id, so residency can survive from the previous dispatch.

    There is no non-destructive way to force cold, and each alternative was checked and rejected:
    TMSL `clearCache` clears query caches but leaves resident columns alone (it is a hot->warm
    lever); reframing is *incremental* and retains dictionaries, so it lands at semiwarm at best;
    memory pressure and node reassignment do produce cold state but neither is commandable. See
    benchmark/README.md.

    The accepted cost is one extra item GUID per dispatch in the Capacity Metrics app's item list.
    It does not break `cu/` — that tool resolves names live from the REST API precisely because a
    recreate mints a new GUID — and the display name never changes, so CU stays attributed.

    The other trade, stated because it is a real regression: if this delete succeeds and the deploy
    then fails, the engine is left with NO model, where an overwrite failure used to leave the
    previous one standing. Acceptable here because the model is rebuilt from the template every run
    and nothing between runs depends on it.

    Best-effort on the lookup, deliberate on the delete: if the item cannot be listed we let the
    deploy overwrite as before (and say so), because a benchmark that refuses to run is worse than
    one that warns its first pass may not be cold.

    Returns the deleted item id, or None if there was nothing to delete.
    """
    h = {"Authorization": f"Bearer {duckrun.auth.get_fabric_token()}"}
    try:
        # Paginated: a workspace with enough items returns a continuationToken, and stopping at the
        # first page would report "no existing model" for one that is simply on page two — which
        # reads as the cold guarantee holding while it silently does not.
        hit, token, page = None, None, 0
        while hit is None and page < 20:
            url = f"{FAB}/workspaces/{ws.id}/items?type=SemanticModel"
            if token:
                url += f"&continuationToken={token}"
            r = requests.get(url, headers=h)
            r.raise_for_status()
            body = r.json()
            hit = next((i for i in body.get("value", []) if i.get("displayName") == name), None)
            token = body.get("continuationToken")
            page += 1
            if not token:
                break
    except Exception as ex:
        print(f"  note: could not list semantic models ({type(ex).__name__}: "
              f"{str(ex).splitlines()[0][:120]}) — deploying over whatever is there, so pass 1 "
              f"may not be cold", flush=True)
        return None
    if not hit:
        print(f"  no existing {name} — the deploy creates it (cold by construction)", flush=True)
        return None
    old = hit["id"]
    r = requests.delete(f"{FAB}/workspaces/{ws.id}/items/{old}", headers=h)
    if r.status_code in (200, 202, 204):
        print(f"  deleted the previous {name} ({old}) — the deploy creates a new item", flush=True)
        return old
    print(f"  note: DELETE {name} returned HTTP {r.status_code} — deploying over it instead, so "
          f"pass 1 may not be cold", flush=True)
    return None


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
        old_id = _delete_existing(ws, name)
        t0 = time.perf_counter()
        item_id = None
        # Retry the CREATE: the workspace can still hold the deleted name for a moment, and the
        # create then 409s. Only a conflict is retried — anything else is a real failure and must
        # not be masked by three more attempts.
        for attempt in range(1, 4):
            try:
                item_id = ws.deploy(TEMPLATE, name=name, overwrite=True, folder=FOLDER, **kwargs)
                break
            except Exception as ex:
                msg = str(ex)
                conflict = "409" in msg or "conflict" in msg.lower() or "already exists" in msg.lower()
                if attempt < 3 and conflict:
                    wait = 10 * attempt
                    print(f"  create conflicted (the delete is still propagating); "
                          f"retrying in {wait}s ({attempt}/3)", flush=True)
                    time.sleep(wait)
                    continue
                # One engine failing to deploy must not cost the others their run: record it and
                # carry on. xmla_compare.py benchmarks whatever actually deployed.
                failed[e] = f"{type(ex).__name__}: {msg.splitlines()[0][:200]}"
                print(f"  FAILED {name}: {failed[e]}", flush=True)
                break
        if item_id is None:
            continue
        secs = round(time.perf_counter() - t0, 1)
        deployed[e] = {"model": name, "item_id": item_id, "previous_item_id": old_id,
                       "seconds": secs, "folder": FOLDER}
        # Direct Lake everywhere, so deploy always reframes onto the latest Delta and returns only
        # once the model is live. The item id is printed against the one it replaced because that
        # difference IS the evidence the model is new, and therefore that pass 1 is cold.
        print(f"  ok {name} ({item_id}) in {secs}s — refreshed"
              + (f"; replaced {old_id}" if old_id else "; created fresh"), flush=True)
        _reparent(ws, item_id, name)

    report.merge({"deploy": {"deployed": deployed, "failed": failed}})

    # A deploy failure fails THIS engine's job and nothing else: the matrix is not fail-fast, the
    # other engines' jobs are unaffected, and the report names whoever is missing. There is no
    # reference engine whose absence would invalidate the run.
    if not deployed:
        sys.exit("no semantic model deployed — nothing to benchmark")
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
