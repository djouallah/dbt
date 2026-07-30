"""Parity dashboard: duckrun.get_stats() over EVERY engine's output, pivoted to $GITHUB_STEP_SUMMARY.

The project's thesis is: same raw data -> four engines (duckrun/Delta, iceberg, Fabric Warehouse,
Spark) -> identical output. So the final row counts should line up column-for-column. get_stats reads
each item's Delta log, and OneLake surfaces every item (including the native Iceberg lakehouse and the
Warehouse) with a Delta representation, so ONE reader covers all four. Diagnostics -> stderr.

**`STATS_JSON` makes this step's result reusable, and that is the point of it existing.** The markdown
goes to stdout and, in ci.yml, straight into `$GITHUB_STEP_SUMMARY` — where it is readable by a human
on the run page and by NOTHING else. It is not in the job log (stdout is redirected), there is no REST
endpoint for a step summary, so the layout numbers this reads off four Delta logs were unreachable
from any other workflow. Set `STATS_JSON=<path>` and the same numbers are also written as JSON, which
ci.yml uploads as the `stats` artifact; `cu/` downloads it from the latest successful `dbt` run so a CU
report can sit next to the layout that produced it, WITHOUT a second reader of the same Delta logs and
without duckrun or a storage token anywhere near `cu/`.

That JSON is a data contract with a consumer outside this file. Its shape is
`{"run": {...}, "engines": {...}, "tables": [...], "stats": {engine: {table: {detail}}}}` and the
detail keys are DETAIL_KEYS below. Adding a key is safe; renaming or removing one breaks `cu/`'s layout
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

WS = os.environ["WS_ID"]
FAB = "https://api.fabric.microsoft.com/v1"
TRANSPORT = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "curl")

# (engine label, Fabric item name, item kind)
ENGINES = [("duckrun", "dbt_delta", "lakehouses"),
           ("iceberg", "dbt_iceberg", "lakehouses"),
           ("spark", "dbt_spark", "lakehouses"),
           ("dwh", "dbt_dwh", "warehouses")]

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


def write_json(per_engine, engines):
    """Write the same numbers as JSON when STATS_JSON names a path. No-op otherwise.

    Carries the run stamp too: a consumer reading this out of an artifact needs to know WHICH dbt run
    it came from and when, or it will quote a layout from a run three days older than the CU it sits
    beside. `cu/` prints that provenance line for exactly that reason.
    """
    path = os.environ.get("STATS_JSON", "").strip()
    if not path:
        return
    doc = {
        "run": {"id": os.environ.get("GITHUB_RUN_ID"),
                "sha": os.environ.get("GITHUB_SHA"),
                "workspace": WS,
                "written": datetime.now(timezone.utc).isoformat()},
        "engines": {e: {"item": item, "kind": kind, "writer": WRITER.get(e, e)}
                    for e, item, kind in ENGINES},
        "tables": list(TABLES),
        "detail_keys": list(DETAIL_KEYS),
        "stats": {e: per_engine.get(e) or {} for e in engines},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    have = sum(len(v) for v in doc["stats"].values())
    sys.stderr.write(f"  wrote {path}: {have} (engine, table) rows for {len(engines)} engines\n")


def one_engine(item, kind):
    """(guid, stats) for one Fabric item; exceptions propagate to the caller."""
    guid = find_guid(kind, item)
    return guid, (stats_for(guid) if guid else {})


def main():
    per_engine = {}
    # The four items are independent and the iceberg one alone can take >10 minutes to read
    # over OneLake, so fetch them concurrently: wall-clock = slowest engine, not the sum.
    with ThreadPoolExecutor(max_workers=len(ENGINES)) as pool:
        futures = {engine: pool.submit(one_engine, item, kind) for engine, item, kind in ENGINES}
    for engine, item, kind in ENGINES:
        try:
            _, per_engine[engine] = futures[engine].result()
            sys.stderr.write(f"  {engine} ({item}): "
                             f"{sum(d.get('total_rows') or 0 for d in per_engine[engine].values()):,}"
                             f" rows total (all tables)\n")
        except Exception as e:
            per_engine[engine] = {}
            sys.stderr.write(f"  {engine} ({item}) FAILED: {e}\n")

    engines = [e for e, _, _ in ENGINES]
    parity_table(per_engine, engines)
    detail_tables(per_engine, engines)
    write_json(per_engine, engines)


if __name__ == "__main__":
    main()
