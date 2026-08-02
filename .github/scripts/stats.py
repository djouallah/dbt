"""Table layout + row-count parity: duckrun.get_stats() over EVERY engine's output, pivoted to
$GITHUB_STEP_SUMMARY. Run by the `layout` job of .github/workflows/dbt.yml.

The project's thesis is: same raw data -> four engines (duckrun/Delta, iceberg, Fabric Warehouse,
Spark) -> identical output. So the final row counts should line up column-for-column. get_stats reads
each item's Delta log, and OneLake surfaces every item (including the native Iceberg lakehouse and the
Warehouse) with a Delta representation, so ONE reader covers all four. Diagnostics -> stderr.

**`STATS_JSON` makes this run's result reusable, and that matters because this is EXPENSIVE and
NEARLY STATIC.** Reading four Delta logs over OneLake takes ~10 minutes (the iceberg item alone 12m+),
while files/row groups/size/v-order only move when the tables are rewritten — which is why this is a
dispatch-only workflow rather than a job in every build. A step summary could not be reused: the
markdown goes to stdout and into `$GITHUB_STEP_SUMMARY`, readable by a human on the run page and by
NOTHING else — not in the job log (stdout is redirected), and GitHub exposes no REST endpoint for it.
Set `STATS_JSON=<path>` and the same numbers are also written as JSON, which the workflow uploads as
the `stats` artifact; `cu/` downloads it from this workflow's latest successful run so a CU report can
sit next to the layout that produced it, WITHOUT a second reader of the same Delta logs and without
duckrun or a storage token anywhere near `cu/`. A cached reading is sound precisely because the layout
is near-static.

The same document is also merged into the RUN RECORD under `layout` (see record.py) — two sinks, one
document. The artifact is how a run's layout is read back without a checkout; the record is what
outlives artifact retention and what the page joins against the CU ledger. `engines[e].guid` is the
join key, and it was resolved here and discarded one line later until this was written.

That JSON is a data contract with a consumer outside this file. Its shape is
`{"run": {...}, "config": {...}, "engines": {...}, "tables": [...], "stats": {engine: {table:
{detail}}}}` and the detail keys are DETAIL_KEYS below. `config` is what the build ran ON — vCores,
Spark resource profile, native execution engine — read from the env the legs were actually given, so
the page can state the hardware instead of asserting it. Adding a key is safe; renaming or removing one breaks `cu/`'s layout
table, which degrades to a note rather than an error — so a rename fails QUIETLY over there. Change
both together.
"""
import json
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
import duckrun

import record

WS = os.environ["WS_ID"]
FAB = "https://api.fabric.microsoft.com/v1"
TRANSPORT = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "curl")

# (engine label, Fabric item name, item kind)
ALL_ENGINES = [("duckrun", "dbt_delta", "lakehouses"),
               ("iceberg", "dbt_iceberg", "lakehouses"),
               ("spark", "dbt_spark", "lakehouses"),
               ("dwh", "dbt_dwh", "warehouses")]

# Narrowed to what the dispatch actually built. Reading an item this run did not touch would record
# an older generation's layout under this run's id — and each read is 10+ minutes over OneLake.
_want = [e.strip() for e in os.environ.get("BUILD_ENGINES", "").split(",") if e.strip()]
_unknown = [e for e in _want if e not in {n for n, _i, _k in ALL_ENGINES}]
if _unknown:
    raise SystemExit(f"BUILD_ENGINES names unknown engine(s) {_unknown}")
ENGINES = [t for t in ALL_ENGINES if not _want or t[0] in _want]

# Every shared table each engine emits, in pipeline order — inputs first, mart last.
#
# It was briefly cut to the three mart tables on the argument that the facts are inputs whose rows
# are implied by fct_summary's. They are not, diagnostically: when fct_summary disagrees across
# engines, the ONLY way to tell an input difference from a summary-logic difference is to read the
# fact counts on the row above it. A mart-only table shows the symptom and hides the cause.
#
# This table IS the cross-engine check. The only assertion left in the dbt suite is a grain test
# that reads fct_summary and nothing else, so a disagreement between engines shows up here or
# nowhere: a ⚠️ on a row means the four outputs are not the same, and it is the one signal that
# can say so.
TABLES = ["stg_csv_archive_log", "dim_calendar", "dim_duid",
          "fct_price", "fct_scada", "fct_price_today", "fct_scada_today", "fct_summary"]

# The get_stats() detail carried per table (see stats_for) and how each column is rendered.
DETAIL_KEYS = ("schema", "total_rows", "num_files", "num_row_groups",
               "avg_row_group", "size_mb", "vorder", "compression")
DETAIL_COLS = [("total_rows", "rows", "num"), ("num_files", "files", "num"),
               ("num_row_groups", "row groups", "num"), ("avg_row_group", "avg RG rows", "num"),
               ("size_mb", "size MB", "num"), ("vorder", "vorder", "bool"),
               ("compression", "compression", "left")]

# What actually wrote the parquet behind each engine's Delta log — the interesting axis when two
# engines produce the same rows in a very different physical layout.
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)",
          "spark": "spark", "dwh": "warehouse"}


def fabric_token():
    try:
        from duckrun.auth import get_fabric_token
        return get_fabric_token()
    except Exception:
        return subprocess.check_output(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
             "--query", "accessToken", "-o", "tsv"], text=True).strip()


H = {"Authorization": "Bearer " + fabric_token()}


def find_guid(kind, name):
    r = requests.get(f"{FAB}/workspaces/{WS}/{kind}", headers=H)
    r.raise_for_status()
    it = next((i for i in r.json().get("value", []) if i["displayName"] == name), None)
    return it["id"] if it else None


def tables_path(guid):
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{guid}/Tables"


def reader(guid):
    con = duckrun.connect(tables_path(guid), read_only=True)
    try:
        con.con.sql(f"SET GLOBAL azure_transport_option_type='{TRANSPORT}'")
    except Exception:
        pass
    return con


def stats_for(guid):
    """{table: {schema, total_rows, num_files, num_row_groups, avg_row_group, size_mb,
    vorder, compression}} for one item's Tables — the full get_stats() detail, not just rows."""
    rows = reader(guid).get_stats().fetchall()
    # get_stats() column order: catalog, schema, table, total_rows, num_files, num_row_groups,
    # avg_row_group, size_mb, vorder, compression
    return {r[2]: dict(zip(DETAIL_KEYS, (r[1], r[3], r[4], r[5], r[6], r[7], r[8], r[9])))
            for r in rows}


def fmt(v, kind):
    if v is None:
        return "—"
    if kind == "num":
        return f"{v:,.1f}".rstrip("0").rstrip(".") if isinstance(v, float) else f"{v:,}"
    if kind == "bool":
        return "✅" if v else "·"
    return f"`{v}`" if kind == "left" else str(v)


def parity_table(per_engine, engines):
    """Row counts side by side — the thesis check. ⚠️ = differs or missing across engines.

    The last two rows fold in what used to be a separate per-engine totals table:
    total rows carries the parity ⚠️ (counts must line up); total MB doesn't (physical
    size legitimately differs by writer/compression)."""
    print("## 🧮 Row-count parity\n")
    print("<sub>Every shared table, in pipeline order. ⚠️ = differs or missing "
          "across engines.</sub>\n")
    print("| table | " + " | ".join(engines) + " |")
    print("| --- | " + " | ".join("--:" for _ in engines) + " |")
    for t in TABLES:
        vals = [(per_engine[e].get(t) or {}).get("total_rows") for e in engines]
        present = [v for v in vals if v is not None]
        match = len(present) == len(engines) and len(set(present)) == 1
        print(f"| `{t}`{'' if match else ' ⚠️'} | "
              + " | ".join(fmt(v, "num") for v in vals) + " |")

    def total(e, key):
        # An engine whose stats fetch failed has an empty dict: render "—", not 0.
        # Summed over EVERYTHING the item holds, not just TABLES: the total is the item's size,
        # and a table present on one engine only would otherwise be invisible in both the rows
        # above and the total.
        if not per_engine[e]:
            return None
        return sum(d.get(key) or 0 for d in per_engine[e].values())

    rows = [total(e, "total_rows") for e in engines]
    present = [v for v in rows if v is not None]
    match = len(present) == len(engines) and len(set(present)) == 1
    print(f"| **total rows**{'' if match else ' ⚠️'} | "
          + " | ".join(fmt(v, "num") for v in rows) + " |")
    mbs = [total(e, "size_mb") for e in engines]
    print("| **total MB** | "
          + " | ".join(fmt(None if v is None else round(v, 1), "num") for v in mbs) + " |")
    print()


def detail_tables(per_engine, engines):
    """Full get_stats() detail as ONE flat table: a row per (table, engine).

    Deliberately flat and un-collapsed. The previous shape — a <details> block per engine, each
    holding its own table — meant comparing how two engines wrote the SAME table required opening
    four blocks and scrolling between them, and a collapsed block reads as prose, not data. Rows
    are grouped by table so the engines sit directly under each other, which is the only layout in
    which "same rows, wildly different files/row-groups" is visible at a glance.

    The parity table above only proves the row counts agree, not that the engines wrote comparable
    physical layouts — this is the "why is my table slow / full of small files" view.
    """
    print("## 🔬 Physical layout\n")
    heads = ["table", "engine", "writer"] + [h for _, h, _ in DETAIL_COLS]
    aligns = ["---", "---", "---"] + ["--:" if k == "num" else "---" for _, _, k in DETAIL_COLS]
    print("| " + " | ".join(heads) + " |")
    print("| " + " | ".join(aligns) + " |")

    for t in TABLES:
        vals = [(per_engine.get(e) or {}).get(t) for e in engines]
        counts = [d.get("total_rows") for d in vals if d is not None]
        agree = len(counts) == len(engines) and len(set(counts)) == 1
        for i, (e, d) in enumerate(zip(engines, vals)):
            # Name the table once per group; ⚠️ flags a group whose row counts don't line up.
            name = f"`{t}`{'' if agree else ' ⚠️'}" if i == 0 else ""
            cells = [fmt(None if d is None else d.get(key), kind) for key, _, kind in DETAIL_COLS]
            print(f"| {name} | {e} | `{WRITER.get(e, e)}` | " + " | ".join(cells) + " |")
    print()
    # Per-engine totals now live as the last two rows of the parity table above.


def build_doc(per_engine, engines, guids=None):
    """The layout document: run stamp, hardware config, per-engine item + GUID, per-table detail.

    Carries the run stamp too: a consumer reading this out of an artifact needs to know WHICH dbt run
    it came from and when, or it will quote a layout from a run three days older than the CU it sits
    beside.
    """
    guids = guids or {}
    doc = {
        # No `workspace` key. It was the WS_ID GUID, which is now a repo secret, and this document
        # is uploaded as a public-repo ARTIFACT — anyone can download it. Nothing ever read it back
        # (`cu/` takes `id` and `sha` only), so recording it only widened where the value lives.
        "run": {"id": os.environ.get("GITHUB_RUN_ID"),
                "sha": os.environ.get("GITHUB_SHA"),
                "written": datetime.now(timezone.utc).isoformat()},
        # What the build actually ran ON, read from the env the legs were given rather than from a
        # doc that can drift. A layout number means little without it: "4 files, 999 MB" is a
        # different achievement at 8 vCores than at 64.
        #
        # `None` where the workflow set nothing, and the reader must print that as "not recorded"
        # rather than filling in a default — the whole point is that this reports the run, not the
        # repo's intentions. dwh is absent on purpose: Fabric Warehouse exposes no knob here, and an
        # invented row would imply one exists.
        # Scoped to ENGINES, like `stats` and `engines` above: a `BUILD_ENGINES=spark` dispatch
        # never set `FABRIC_CORES` for a notebook it did not run, so recording a vCore count under
        # `duckrun` there states a hardware choice that no leg made. The reader prints this as the
        # hardware the run RAN ON, and an engine the run did not build has none.
        "config": {e: cfg for e, cfg in (
            ("duckrun", {"vcores": os.environ.get("FABRIC_CORES") or None}),
            ("iceberg", {"vcores": os.environ.get("FABRIC_CORES") or None}),
            ("spark", {"resource_profile": os.environ.get("SPARK_RESOURCE_PROFILE") or None,
                       "native_execution_engine": os.environ.get("SPARK_NATIVE_ENABLED") or None}),
        ) if any(e == n for n, _i, _k in ENGINES)},
        # `guid` is the join key to the CU ledger, and it used to be resolved here and thrown away
        # one line later (`_, per_engine[engine] = ...`). It is the item's identity; the display
        # name is only how a human finds it, and matching on the name is exactly what `cu/` had to
        # do for want of this field.
        "engines": {e: {"item": item, "kind": kind, "writer": WRITER.get(e, e),
                        "guid": guids.get(e)}
                    for e, item, kind in ENGINES},
        "tables": list(TABLES),
        "detail_keys": list(DETAIL_KEYS),
        "stats": {e: per_engine.get(e) or {} for e in engines},
    }
    return doc


def write_json(doc, engines):
    """Write the layout doc where STATS_JSON names a path, and into the run record either way.

    Two sinks, one document. STATS_JSON is the per-run artifact (kept: it is how a failed run's
    layout is read back without a checkout); the run record is what survives artifact retention and
    is what the page joins against the CU ledger.
    """
    record.merge({"layout": doc})
    path = os.environ.get("STATS_JSON", "").strip()
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    have = sum(len(v) for v in doc["stats"].values())
    sys.stderr.write(f"  wrote {path}: {have} (engine, table) rows for {len(engines)} engines\n")


def one_engine(item, kind):
    """(guid, stats) for one Fabric item; exceptions propagate to the caller."""
    guid = find_guid(kind, item)
    return guid, (stats_for(guid) if guid else {})


def main():
    per_engine, guids = {}, {}
    # The four items are independent and the iceberg one alone can take >10 minutes to read
    # over OneLake, so fetch them concurrently: wall-clock = slowest engine, not the sum.
    with ThreadPoolExecutor(max_workers=len(ENGINES)) as pool:
        futures = {engine: pool.submit(one_engine, item, kind) for engine, item, kind in ENGINES}
    for engine, item, kind in ENGINES:
        try:
            guids[engine], per_engine[engine] = futures[engine].result()
            sys.stderr.write(f"  {engine} ({item} {guids[engine]}): "
                             f"{sum(d.get('total_rows') or 0 for d in per_engine[engine].values()):,}"
                             f" rows total (all tables)\n")
        except Exception as e:
            per_engine[engine] = {}
            sys.stderr.write(f"  {engine} ({item}) FAILED: {e}\n")

    engines = [e for e, _, _ in ENGINES]
    parity_table(per_engine, engines)
    detail_tables(per_engine, engines)
    write_json(build_doc(per_engine, engines, guids), engines)


if __name__ == "__main__":
    main()
