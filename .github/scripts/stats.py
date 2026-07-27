"""Parity dashboard: duckrun.get_stats() over EVERY engine's output, pivoted to $GITHUB_STEP_SUMMARY.

The project's thesis is: same raw data -> four engines (duckrun/Delta, iceberg, Fabric Warehouse,
Spark) -> identical output. So the final row counts should line up column-for-column. get_stats reads
each item's Delta log, and OneLake surfaces every item (including the native Iceberg lakehouse and the
Warehouse) with a Delta representation, so ONE reader covers all four. Diagnostics -> stderr.
"""
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor

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

# The shared final tables every engine emits (order = pipeline order).
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


def duid_probe(guid, schemas):
    """Why does the fct_scada.DUID -> dim_duid relationships test report ~100% orphans?

    Prints the facts needed to tell the three candidate causes apart:
      - dim_duid is empty            -> dim rows == 0
      - the join key is dirty        -> exact overlap == 0 but trim/upper overlap > 0
      - fct_scada.DUID holds the
        wrong CSV field (misaligned
        positional read_csv layout)  -> both overlaps 0 and the fct samples don't look like DUIDs
    Probes fct_scada_today (small) rather than fct_scada (~70M rows).
    """
    base = tables_path(guid)
    con = reader(guid)

    def scan(table):
        return f"delta_scan('{base}/{schemas[table]}/{table}')"

    def one(sql):
        return con.con.sql(sql).fetchall()

    dim, fct = scan("dim_duid"), scan("fct_scada_today")
    d_rows, d_distinct = one(f"SELECT count(*), count(DISTINCT DUID) FROM {dim}")[0]
    f_rows, f_distinct = one(f"SELECT count(*), count(DISTINCT DUID) FROM {fct}")[0]
    exact = one(f"SELECT count(*) FROM (SELECT DISTINCT DUID d FROM {fct}) c "
                f"JOIN (SELECT DISTINCT DUID d FROM {dim}) p USING (d)")[0][0]
    loose = one(f"SELECT count(*) FROM (SELECT DISTINCT upper(trim(DUID)) d FROM {fct}) c "
                f"JOIN (SELECT DISTINCT upper(trim(DUID)) d FROM {dim}) p USING (d)")[0][0]
    d_sample = [r[0] for r in one(f"SELECT DISTINCT DUID FROM {dim} ORDER BY 1 LIMIT 8")]
    f_sample = [r[0] for r in one(f"SELECT DISTINCT DUID FROM {fct} ORDER BY 1 LIMIT 8")]

    # Mirror to stderr as well: $GITHUB_STEP_SUMMARY is renderable only in the web UI and is NOT
    # retrievable from the API, so a summary-only diagnostic cannot be read from the job log.
    sys.stderr.write(
        f"  [duid_probe] dim_duid={d_rows} rows/{d_distinct} distinct  "
        f"fct_scada_today={f_rows} rows/{f_distinct} distinct  "
        f"exact_overlap={exact}  loose_overlap={loose}\n"
        f"  [duid_probe] dim sample={d_sample}\n"
        f"  [duid_probe] fct sample={f_sample}\n")

    print("## 🔎 DUID join probe (`fct_scada_today` → `dim_duid`)\n")
    print("| probe | value |")
    print("| --- | --- |")
    print(f"| `dim_duid` rows / distinct DUID | {d_rows:,} / {d_distinct:,} |")
    print(f"| `fct_scada_today` rows / distinct DUID | {f_rows:,} / {f_distinct:,} |")
    print(f"| distinct DUIDs matching **exactly** | {exact:,} |")
    print(f"| distinct DUIDs matching after `upper(trim())` | {loose:,} |")
    print(f"| `dim_duid` DUID sample | {', '.join(f'`{v}`' for v in d_sample) or '—'} |")
    print(f"| `fct_scada_today` DUID sample | {', '.join(f'`{v}`' for v in f_sample) or '—'} |")
    print()
    if d_rows == 0:
        print("> **Cause: `dim_duid` is empty.** The facts have nothing to join to — look at the "
              "`JOIN states ON a.Region = states.RegionID` inner join and the `has_new_duids` "
              "incremental gate in `dim_duid.sql`.\n")
    elif exact == 0 and loose > 0:
        print(f"> **Cause: dirty join key.** {loose:,} DUIDs match once case/whitespace is normalised "
              "but 0 match exactly — `dim_duid` does not `trim()` DUID (it only trims "
              "`FuelSourceDescriptor`).\n")
    elif exact == 0:
        print("> **Cause: the two columns are not the same field.** Neither exact nor normalised "
              "matching finds anything, so compare the samples above — a positional `read_csv` "
              "layout in `fct_scada.sql` that is off by one would put the wrong CSV field in `DUID`.\n")
    else:
        print(f"> {exact:,} DUIDs join cleanly; the orphans are a genuine subset "
              "(e.g. retired units absent from the current AEMO registration list).\n")


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
    print("<sub>⚠️ = differs or missing across engines.</sub>\n")
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


def one_engine(item, kind):
    """(guid, stats) for one Fabric item; exceptions propagate to the caller."""
    guid = find_guid(kind, item)
    return guid, (stats_for(guid) if guid else {})


def main():
    per_engine, probe = {}, None
    # The four items are independent and the iceberg one alone can take >10 minutes to read
    # over OneLake, so fetch them concurrently: wall-clock = slowest engine, not the sum.
    with ThreadPoolExecutor(max_workers=len(ENGINES)) as pool:
        futures = {engine: pool.submit(one_engine, item, kind) for engine, item, kind in ENGINES}
    for engine, item, kind in ENGINES:
        try:
            guid, per_engine[engine] = futures[engine].result()
            if engine == "duckrun" and {"dim_duid", "fct_scada_today"} <= per_engine[engine].keys():
                probe = (guid, {t: d["schema"] for t, d in per_engine[engine].items()})
            sys.stderr.write(f"  {engine} ({item}): "
                             f"{sum(d.get('total_rows') or 0 for d in per_engine[engine].values()):,}"
                             f" rows total\n")
        except Exception as e:
            per_engine[engine] = {}
            sys.stderr.write(f"  {engine} ({item}) FAILED: {e}\n")

    engines = [e for e, _, _ in ENGINES]
    parity_table(per_engine, engines)
    detail_tables(per_engine, engines)

    # Diagnostic only — must never fail the parity dashboard.
    if probe:
        try:
            duid_probe(*probe)
        except Exception as e:
            sys.stderr.write(f"  duid_probe FAILED: {e}\n")


if __name__ == "__main__":
    main()
