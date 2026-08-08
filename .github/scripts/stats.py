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

It also reports the INPUT side — `landing`: how many files and how many bytes sit in the archive
every engine reads. Everything else here describes what came out; without that, the record can say a
run wrote 143,980,961 rows and not say from how much. It is read by listing the store rather than
querying it, because DuckDB's `glob()` returns paths and no sizes and the archive is uncompressed
CSV whose bytes are the point.

It also reports whether the writer PHYSICALLY REORDERED THE ROWS — `ordering`, see `ordering_for`.
V-Order is documented as a row-reordering pass and nothing here could tell whether it happened: the
`vorder` detail key is a table PROPERTY nobody sets, so it reads false for spark whatever the files
contain. That is measured from the parquet itself now, plus the per-file Delta `VORDER` tag this
file's own comments have called the honest check for months without anything reading it.

**NEITHER of those two sees a WAREHOUSE, and reading them as "dwh writes no V-Order" is wrong.** The
property is a `TBLPROPERTIES` key and the tag is a Spark-writer marker; the warehouse engine sets
neither and V-Orders **by default** on every new warehouse (irreversible once disabled). So
`ordering_for` skips the tag read for `kind == "warehouses"` — absent, not `tagged: 0` — and dwh's
answer comes from `sys.databases.is_vorder_enabled`, recorded by the dwh leg as
`layout.ordering.dwh.vorder_enabled`.

That JSON is a data contract with a consumer outside this file. Its shape is
`{"run": {...}, "config": {...}, "engines": {...}, "tables": [...], "landing": {...},
"ordering": {engine: {...}}, "stats": {engine: {table: {detail}}}}` and the detail keys are
DETAIL_KEYS below. `config` is what the build ran ON — vCores,
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
from urllib.parse import unquote

import requests
import duckrun

import record

WS = os.environ["WS_ID"]
FAB = "https://api.fabric.microsoft.com/v1"
TRANSPORT = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "curl")

# (engine label, Fabric item name, item kind)
LANDING = "dbt_landing"
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

# The one table the query benchmark touches, the layout chart is about, and `encodings_from` reads.
MART = "fct_summary"

# How many physical rows `run_lengths` reads from the sample file. A FIXED ROW BUDGET rather than
# "the first row group", because the engines' row-group sizes differ by two orders of magnitude
# (duckrun's default ~122K against spark readHeavyForPBI's 16M) and a per-row-group sample would
# compare a 122K-row window on one engine with a 16M-row window on another — the run count is a
# fraction OF THE ROWS READ, so the windows have to be the same size for the numbers to be one
# measurement. 4M is small enough to stay minutes on the slowest item and large enough that a
# high-cardinality column cannot look clustered by luck.
ORDERING_SAMPLE_ROWS = 4_000_000

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

# Item kind per engine, so the renderers can say WHY a signal is absent rather than dashing it out.
# `vorder_files` is a Spark-writer marker and a warehouse writes none, so `—` there would read as
# "not V-Ordered" on the one engine that V-Orders by default. See `vorder_tags`.
KIND = {name: kind for name, _item, kind in ALL_ENGINES}


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


def landing_stats():
    """How much raw data went IN: files and bytes under `dbt_landing/Files`, and per folder.

    Everything else in this document describes what came OUT. Without this the record says a run
    wrote 143,980,961 rows and cannot say from how much input — which is the number that makes a
    duration, a file count or a CU total mean anything, and the one that moves when `skip_download`
    is turned off.

    Read by LISTING, not by querying: DuckDB's `glob()` returns paths and no sizes, and the archive
    is uncompressed CSV whose bytes are the whole point. `obstore.list` is one paginated listing over
    the same store `download_aemo.py` already writes through, so this adds no dependency the `land`
    job does not have.

    Best-effort: any failure returns `{}` and the record simply has no `landing` key. A layout report
    is worth having without it, and this is the one part of stats.py that reads an item no engine
    owns.
    """
    try:
        import obstore
        from dbt.adapters.duckrun import objectstore, secret
        guid = find_guid("lakehouses", LANDING)
        if not guid:
            sys.stderr.write(f"  {LANDING} not found — no landing stats\n")
            return {}
        base = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{guid}/Files"
        dr = duckrun.connect(base, read_only=True)
        store = objectstore.build_store(base, secret.refreshed(dr.storage_options))
        folders, files, size = {}, 0, 0
        for batch in obstore.list(store):
            for o in batch:
                path, n = o["path"], int(o["size"] or 0)
                files += 1
                size += n
                # The directory holding the file: `csv_raw/<source>` for the archive,
                # `(root)` for the archive log parquet that sits beside it.
                folder = path.rsplit("/", 1)[0] if "/" in path else "(root)"
                f = folders.setdefault(folder, {"files": 0, "size_mb": 0.0})
                f["files"] += 1
                f["size_mb"] += n / 1048576
        for f in folders.values():
            f["size_mb"] = round(f["size_mb"], 2)
        doc = {"item": LANDING, "guid": guid, "files": files,
               "size_mb": round(size / 1048576, 2),
               "folders": dict(sorted(folders.items()))}
        sys.stderr.write(f"  {LANDING}: {files:,} files, {doc['size_mb']:,.2f} MB "
                         f"across {len(folders)} folder(s)\n")
        return doc
    except Exception as e:                              # noqa: BLE001 — never fail the layout job
        sys.stderr.write(f"  landing stats unavailable ({type(e).__name__}: {e})\n")
        return {}


def landing_table(doc):
    """The input side of the step summary, above the parity table."""
    if not doc:
        return
    print(f"### Input archive — `{doc['item']}`\n")
    print("| folder | files | size MB |")
    print("|---|--:|--:|")
    for name, f in doc["folders"].items():
        print(f"| `{name}` | {f['files']:,} | {f['size_mb']:,.2f} |")
    print(f"| **total** | **{doc['files']:,}** | **{doc['size_mb']:,.2f}** |")
    print()


def reader(guid):
    con = duckrun.connect(tables_path(guid), read_only=True)
    try:
        con.con.sql(f"SET GLOBAL azure_transport_option_type='{TRANSPORT}'")
    except Exception:
        pass
    return con


def stats_for(con):
    """{table: {schema, total_rows, num_files, num_row_groups, avg_row_group, size_mb,
    vorder, compression}} for one item's Tables — the full get_stats() detail, not just rows.

    Takes a CONNECTION, not a guid: `one_engine` now asks this item four questions (aggregate
    stats, encodings, row ordering, the Delta log) and a guid argument would mean four
    `duckrun.connect` attaches to the same Tables path per engine.
    """
    rows = con.get_stats().fetchall()
    # get_stats() column order: catalog, schema, table, total_rows, num_files, num_row_groups,
    # avg_row_group, size_mb, vorder, compression
    return {r[2]: dict(zip(DETAIL_KEYS, (r[1], r[3], r[4], r[5], r[6], r[7], r[8], r[9])))
            for r in rows}


def mart_chunks(con, table):
    """`(name -> index, rows)` of `get_stats(table, detailed=True)` — ONE footer read for everything.

    This is DuckDB's raw `parquet_metadata()` over the mart's live files: one row per (row group,
    column chunk), carrying the encodings, the physical type, the compressed size, the row group's
    id and row count, and its per-column min/max statistics. THREE questions are answered from this
    single fetch — `encodings_from` (what Power BI has to transcode), `rg_ordering` (whether the row
    groups carve the domain up or all span it) and `run_lengths`'s choice of sample file — because a
    second `get_stats(detailed=True)` would re-read EVERY parquet footer over OneLake, on an item
    that already reads at 12m+.

    `(None, [])` on any failure: every consumer treats that as "not measured" and the layout job
    stays green. `table` MUST BE SCHEMA-QUALIFIED — see `encodings_from`.
    """
    try:
        # `description` and `fetchall` off the SAME relation object — see above.
        rel = con.get_stats(table, detailed=True)
        at = {name: i for i, name in enumerate(d[0] for d in rel.description)}
        return at, rel.fetchall()
    except Exception as e:                              # noqa: BLE001 — never fail the layout job
        sys.stderr.write(f"  parquet metadata unavailable for {table} ({type(e).__name__}: {e})\n")
        return None, []


def encodings_from(at, rows):
    """`{column: {encodings, type, dict_pages, chunks, mb}}` for one engine's MART parquet.

    Pure: it aggregates the rows `mart_chunks` fetched and reads nothing itself.

    The table `mart_chunks` was given MUST BE SCHEMA-QUALIFIED (`mart.fct_summary`). A bare name does
    not resolve — `get_stats()` with no argument sweeps every attached catalog and keys the result by
    table name, but `get_stats('fct_summary')` raises `'fct_summary' is neither a known table nor a
    schema in any attached catalog (['data'])`, because a one-part name is looked up in the CURRENT
    schema and dbt writes the mart to `mart`. That is what run 31008858454 hit: the layout job was
    green, the record simply had no `encodings`. `one_engine` passes the schema `stats_for` already
    read, so the name cannot drift from the one the rest of the document reports.

    WHY THIS EXISTS. Every other lever on the layout chart is confounded, and the one hypothesis the
    record could not test was the interesting one: whether the engines differ in what Power BI has to
    transcode, i.e. PER-COLUMN PARQUET ENCODING. `compression` was captured (SNAPPY everywhere except
    dwh, which is UNCOMPRESSED) and encoding was not, so a 2.6x gap between spark's two resource
    profiles at the same row-group band had no measurable explanation. Bytes-per-row rules out size
    as the cause — duckrun writes the DENSEST parquet on the page (5.63 B/row) and does not win, and
    the smallest of all (4.74, the date,time,DUID sort) has that engine's worst CU.

    No pyarrow: `get_stats(detailed=True)` returns DuckDB's raw `parquet_metadata()`, one row per
    column chunk, carrying `encodings`, `type`, `dictionary_page_offset` and the compressed size. The
    footers are already being read for the aggregate call, so the marginal cost is one more read of
    the same files.

    SCOPED TO THE MART, and that is a cost decision rather than a preference. The layout job already
    runs ~10 minutes (the iceberg item alone reads at 12m+ over OneLake), a full pass would be one
    chunk row per column per row group across all eight tables — iceberg's 1,172 row groups times
    `fct_price`'s ~130 columns is six figures of rows for a question nobody asked — and `fct_summary`
    is the only table the query benchmark touches, the only one the layout chart is about, and the
    only one at row-count parity by construction.

    Aggregated to one row per COLUMN, never per chunk: the distinct encodings across every chunk
    (sorted, so two engines are string-comparable), whether a dictionary page was written, and the
    compressed megabytes. That is what answers "same encoding or not" and it stays a handful of keys.

    Best-effort, like `landing_stats`: any failure returns `{}` and the record simply has no
    `encodings`. An absent key reads as "not measured"; `{}` per column would read as "no encodings",
    which is not a thing parquet can be.
    """
    if at is None or not rows:
        return {}
    cols = {}
    # `parquet_metadata()` column order is stable across DuckDB versions but not worth trusting by
    # index alone — resolved by NAME above.
    need = ("path_in_schema", "type", "encodings", "dictionary_page_offset", "total_compressed_size")
    if any(k not in at for k in need):
        sys.stderr.write(f"  parquet_metadata is missing {[k for k in need if k not in at]}\n")
        return {}
    for r in rows:
        c = cols.setdefault(r[at["path_in_schema"]],
                            {"encodings": set(), "type": r[at["type"]], "dict_pages": 0,
                             "chunks": 0, "mb": 0.0})
        for enc in str(r[at["encodings"]] or "").split(","):
            if enc.strip():
                c["encodings"].add(enc.strip())
        c["dict_pages"] += 1 if r[at["dictionary_page_offset"]] else 0
        c["chunks"] += 1
        c["mb"] += (r[at["total_compressed_size"]] or 0) / 1048576
    return {name: {**c, "encodings": sorted(c["encodings"]), "mb": round(c["mb"], 2)}
            for name, c in sorted(cols.items())}


def _stat(v):
    """A row-group min/max coerced to something SORTABLE, tagged so mixed coercions still compare.

    `parquet_metadata()` renders `stats_min_value`/`stats_max_value` as VARCHAR whatever the physical
    type is, so comparing them raw is lexicographic — and lexicographic is WRONG for every numeric
    column: `"9" > "10000"`, which would report a perfectly ascending `time` column as fully
    overlapping. Numbers are cast; everything else stays a string, which is correct for `date`
    (rendered ISO, so lexicographic IS chronological) and for `DUID`.

    The leading tag keeps a column that coerces both ways (a NULL-ish rendering among numbers, say)
    comparable instead of raising TypeError mid-measurement — ordering between the two groups is
    arbitrary but total, which is all the overlap count needs.
    """
    for cast in (int, float):
        try:
            return (0, cast(v))
        except (TypeError, ValueError):
            pass
    return (1, str(v))


def _file_rows(at, rows):
    """`{file: rows}` from the chunk metadata — row counts summed over DISTINCT row groups.

    The fetch is one row per (row group, COLUMN CHUNK), so summing `row_group_num_rows` blindly
    multiplies every file's row count by its column count.
    """
    seen, out = set(), {}
    for r in rows:
        key = (r[at["file_name"]], r[at["row_group_id"]])
        if key in seen:
            continue
        seen.add(key)
        out[key[0]] = out.get(key[0], 0) + int(r[at["row_group_num_rows"]] or 0)
    return out


def _columns(at, rows):
    """The mart's top-level column names, in file order. Nested paths (`a.b`) are skipped — the mart
    has none, and a leaf of a struct is not a column anyone can reason about here."""
    out = []
    for r in rows:
        c = r[at["path_in_schema"]]
        if c and "." not in c and c not in out:
            out.append(c)
    return out


def rg_ordering(at, rows):
    """`{column: {rg_overlap_pct, rgs, [inexact]}}` — do the row groups CARVE UP the domain or all
    span it? The free half of the row-ordering question.

    For each column: one `[min, max]` range per row group, sorted by min, then the percentage of
    CONSECUTIVE PAIRS that overlap. 0% means the row groups partition the column's range — every
    value lives in essentially one row group, which is what a global sort (or any real clustering
    pass) produces and what lets Direct Lake and every predicate pushdown skip segments. ~100% means
    every row group spans the whole domain, i.e. the rows arrived in no order at all.

    THE COMPARISON IS STRICT (`min_i < max_{i-1}`) AND THAT IS NOT A ROUNDING CHOICE. A row group
    boundary almost never lands on a value boundary, so under a PERFECT sort the last row of one row
    group and the first row of the next hold the SAME value — `min_i == max_{i-1}` — and a
    touch-counts-as-overlap rule scores a flawlessly sorted column 100%. Measured on synthetic
    fct_summary-shaped data before this was strict: `date` sorted, one value straddling each
    boundary, read 100% overlap on every column of every case, i.e. the metric saturated and said
    nothing. Strict counts only ranges that genuinely INTERLEAVE, which is the property being asked
    about; the cost is that a column whose every row group holds one single value cannot be
    distinguished from a sorted one, which is true and uninteresting.

    Sorted by min rather than left in file order on purpose: file order is an artifact of the writer
    and would make a descending sort look as unclustered as a shuffle. Sorting first asks "COULD
    these ranges be laid out disjointly", which is the property that matters and is direction-blind.

    Free: it reads the rows `mart_chunks` already fetched, so it costs no OneLake traffic at all.

    WHAT IT CANNOT SEE, and why `run_lengths` exists beside it: this is a statement about ordering
    ACROSS row groups. A writer that assigns rows to row groups by range and then shuffles within
    each one scores 0% here and is not sorted in any sense Power BI benefits from.

    NULL stats are dropped rather than treated as a range (parquet omits stats for an all-NULL
    chunk); a column left with fewer than two row groups is ABSENT, because "no consecutive pairs"
    is not 0% overlap. `inexact` appears only when a writer TRUNCATED a string statistic — that can
    move a boundary either way, so it is surfaced for the reader to discount rather than silently
    computed over.
    """
    need = ("file_name", "row_group_id", "path_in_schema", "stats_min_value", "stats_max_value")
    if at is None or not rows or any(k not in at for k in need):
        if rows:
            sys.stderr.write(f"  parquet_metadata is missing "
                             f"{[k for k in need if at is None or k not in at]} — no rg ordering\n")
        return {}
    per = {}
    for r in rows:
        lo, hi = r[at["stats_min_value"]], r[at["stats_max_value"]]
        if lo is None or hi is None:
            continue
        exact = not any(k in at and r[at[k]] is False for k in ("min_is_exact", "max_is_exact"))
        per.setdefault(r[at["path_in_schema"]], []).append((_stat(lo), _stat(hi), exact))
    out = {}
    for col, ent in per.items():
        if "." in col or len(ent) < 2:
            continue
        ent.sort(key=lambda e: (e[0], e[1]))
        # STRICT: touching at a shared boundary value is what a perfect sort looks like. See above.
        overlaps = sum(1 for i in range(1, len(ent)) if ent[i][0] < ent[i - 1][1])
        d = {"rg_overlap_pct": round(100 * overlaps / (len(ent) - 1), 1), "rgs": len(ent)}
        if not all(e[2] for e in ent):
            d["inexact"] = True
        out[col] = d
    return out


def run_lengths(con, at, rows):
    """`{file, rows, runs: {column: n}}` — adjacent equal-value RUNS in physical row order.

    THIS IS THE QUESTION `rg_ordering` CANNOT ANSWER: whether the writer reordered rows INSIDE a row
    group. V-Order is documented as a row-reordering pass, and nothing in this repo could tell
    whether it does anything — the `vorder` detail column is a table property nobody sets. A run is a
    maximal span of equal adjacent values, so `runs` ≈ `rows` means the column's values are
    interleaved (nothing reordered them) and `runs` ≪ `rows` means equal values were brought
    together, which is exactly what makes RLE and dictionary encoding pay and what Power BI
    transcodes cheaply.

    Read it as a RATIO of the rows sampled, never as an absolute: a near-unique column (`mw`,
    `price`) is the built-in control — it CANNOT score low unless the file really was reordered, so a
    run where every column drops together is measuring something real rather than low cardinality.

    ONE FILE, `ORDERING_SAMPLE_ROWS` rows of it — the largest live file, ties broken by name so two
    dispatches of the same layout sample the same place. The whole mart is 143M rows across up to 80
    files and this job already runs 10-15 minutes; the ordering of one file's first 4M rows answers
    the question, and a full pass would answer it four times over on paid capacity.

    ORDERED BY `file_row_number` EXPLICITLY. That is the physical row index within the file, so the
    count does not depend on DuckDB's scan order, its thread count, or `preserve_insertion_order` —
    the one way this measurement could have been silently wrong.

    Best-effort: `{}` on any failure, and the record simply carries no runs.
    """
    need = ("file_name", "row_group_id", "row_group_num_rows", "path_in_schema")
    if at is None or not rows or any(k not in at for k in need):
        return {}
    try:
        files = _file_rows(at, rows)
        cols = _columns(at, rows)
        if not files or not cols:
            return {}
        path = sorted(files, key=lambda f: (-files[f], f))[0]
        span = min(files[path], ORDERING_SAMPLE_ROWS)
        # `IS DISTINCT FROM` counts NULL == NULL as one run, and the first row's LAG is NULL and so
        # opens a run — the sum is exactly the number of maximal equal-value spans.
        chg = ",\n           ".join(
            f'CASE WHEN "{c}" IS DISTINCT FROM LAG("{c}") OVER w THEN 1 ELSE 0 END AS chg_{i}'
            for i, c in enumerate(cols))
        sel = ", ".join(f"SUM(chg_{i}) AS runs_{i}" for i in range(len(cols)))
        sql = (f"SELECT COUNT(*) AS n, {sel} FROM (\n"
               f"    SELECT {chg}\n"
               f"    FROM read_parquet(['{path.replace(chr(39), chr(39) * 2)}'], "
               f"file_row_number=true)\n"
               f"    WHERE file_row_number < {int(span)}\n"
               f"    WINDOW w AS (ORDER BY file_row_number)\n) t")
        res = con.con.sql(sql).fetchone()
        if not res or not res[0]:
            return {}
        return {"file": path.rsplit("/", 1)[-1], "rows": int(res[0]),
                "runs": {c: int(res[i + 1] or 0) for i, c in enumerate(cols)}}
    except Exception as e:                              # noqa: BLE001 — never fail the layout job
        sys.stderr.write(f"  run lengths unavailable ({type(e).__name__}: {e})\n")
        return {}


def _vorder_from_log(actions, live_files):
    """`{tagged, files, unknown}` — how many LIVE parquet files carry Fabric's `VORDER` add tag.

    THE CHECK THIS FILE'S OWN COMMENTS HAVE CALLED THE HONEST ONE and nothing implemented. The
    `vorder` detail column comes from duckrun's `get_stats`, which reads the TABLE PROPERTY
    `delta.parquet.vorder.enabled` off the Delta metadata — nothing in this repo or in Fabric's
    writer sets that property, so it reads false for spark no matter what the files contain. Spark
    records V-Order per file, as an `add.tags` entry, and delta-rs's `get_add_actions` does not
    surface tags, which is why this reads the commit JSON itself.

    Pure, so the parsing is testable without a store. Last `add` per path wins (a file re-added by a
    later commit is described by that commit) and REMOVES ARE NOT REPLAYED — the live set comes from
    the file list `mart_chunks` already read, which duckrun derived from the Delta log with
    tombstones excluded. That is the simplification that makes this cheap AND keeps it from
    disagreeing with the rest of the document about which files exist.

    Matched on BASENAME: `file_name` is a full abfss URI and `add.path` is table-relative and
    URL-encoded. The names are GUID-bearing, so basename equality is unambiguous.

    `unknown` counts live files no JSON commit describes — their add was folded into a checkpoint.
    Reported rather than guessed at, and structurally rare here: the teardown means every dispatch
    builds its tables from nothing, so the log is a handful of commits.
    """
    live = {str(f).rsplit("/", 1)[-1] for f in live_files}
    tags = {}
    for a in actions:
        add = (a or {}).get("add")
        if not isinstance(add, dict) or not add.get("path"):
            continue
        tags[unquote(str(add["path"])).rsplit("/", 1)[-1]] = add.get("tags") or {}
    return {"tagged": sum(1 for f in live
                          if str((tags.get(f) or {}).get("VORDER", "")).lower() == "true"),
            "files": len(live),
            "unknown": sum(1 for f in live if f not in tags)}


def vorder_tags(con, guid, schema, table, live_files):
    """`_vorder_from_log` over the table's `_delta_log/*.json`, read with obstore.

    Same store-building path as `landing_stats` — `objectstore.build_store` plus
    `secret.refreshed(...)` — so it adds no dependency this job does not already have. The commit
    files are zero-padded 20-digit, so a lexicographic sort IS commit order.

    **ONLY MEANINGFUL FOR A SPARK-WRITTEN TABLE, which is why `ordering_for` does not call this for a
    Warehouse.** `add.tags.VORDER` is a marker the Fabric SPARK writer stamps. The warehouse engine
    V-Orders by default (`is_vorder_enabled`, on for every new warehouse and irreversible once off)
    and stamps no such tag, so this returned `{tagged: 0, files: 77, unknown: 0}` against dwh — a
    successful read of a log that simply does not carry the marker, which is indistinguishable on the
    page from "the writer did not V-Order". That is the false negative this guard exists to prevent;
    the authoritative dwh signal is `layout.ordering.dwh.vorder_enabled`, read from `sys.databases`
    by the dwh leg itself.

    Best-effort: `{}` on anything at all.
    """
    try:
        import obstore
        from dbt.adapters.duckrun import objectstore, secret
        base = f"{tables_path(guid)}/{schema}/{table}/_delta_log"
        store = objectstore.build_store(base, secret.refreshed(con.storage_options))
        keys = sorted(o["path"].rsplit("/", 1)[-1]
                      for batch in obstore.list(store) for o in batch
                      if str(o["path"]).endswith(".json"))
        actions = []
        for k in keys:
            body = bytes(obstore.get(store, k).bytes()).decode("utf-8")
            actions += [json.loads(line) for line in body.splitlines() if line.strip()]
        return _vorder_from_log(actions, live_files)
    except Exception as e:                              # noqa: BLE001 — never fail the layout job
        sys.stderr.write(f"  vorder tags unavailable for {schema}.{table} "
                         f"({type(e).__name__}: {e})\n")
        return {}


def ordering_for(con, guid, schema, at, rows, kind="lakehouses"):
    """DID THE WRITER PHYSICALLY REORDER THE ROWS? Three signals over the mart, one document.

    V-Order is documented as a row-reordering plus encoding pass, and until this existed the record
    could say a run asked for `readHeavyForPBI` and never say whether the parquet came out any
    different. The three answer different halves and none of them subsumes another:

    - `columns[c].rg_overlap_pct` — ordering ACROSS row groups, free from metadata already fetched.
    - `columns[c].runs` — ordering WITHIN a file, from a bounded read of one sample file. This is
      the intra-file reordering V-Order claims and nothing else here can see.
    - `vorder_files` — whether Fabric TAGGED the files as V-Ordered, read from the Delta log. A
      measured answer to the question the `vorder` detail column pretends to answer — **for a
      Spark-written table only**. `kind == "warehouses"` skips it, because the tag is a Spark-writer
      marker and the warehouse writes none: it read `0/77` against dwh's own V-Ordered parquet, which
      is a successful read of an absent marker, not a measurement of an absent optimisation. See
      `vorder_tags`. The dwh answer arrives as `vorder_enabled` from the leg's own `sys.databases`
      query instead, so an ABSENT `vorder_files` here is the honest "this probe cannot see it".

    All three land under `layout.ordering.<engine>`, a sibling of `stats` and `encodings` and
    deliberately NOT in `layout.config` — the dashboard's `variant()` walks every key of that block
    into a column name, so a measurement that moves run to run would split an engine's column and
    its layout bar on a difference in what was MEASURED rather than what was configured.

    Each part fails on its own: a missing one is absent, and `{}` when nothing was measured at all —
    never a zero, which would read as "nothing was reordered".
    """
    if at is None or not rows or not schema:
        return {}
    doc = {"table": f"{schema}.{MART}"}
    cols = rg_ordering(at, rows)
    rl = run_lengths(con, at, rows)
    if rl:
        doc["sample"] = {"file": rl["file"], "rows": rl["rows"]}
        for c, n in rl["runs"].items():
            cols.setdefault(c, {})["runs"] = n
    if "file_name" in at and kind != "warehouses":
        vt = vorder_tags(con, guid, schema, MART, {r[at["file_name"]] for r in rows})
        if vt:
            doc["vorder_files"] = vt
    if cols:
        doc["columns"] = dict(sorted(cols.items()))
    return doc if len(doc) > 1 else {}


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


def _nonbaseline(var, baseline):
    """The env value when `sorted` is on AND it differs from `baseline`, else `None`.

    Absence is what keeps a run in the same dashboard column as the history that wrote the same
    parquet — the same rule `sorted` itself follows, and for the same reason: identical parquet, so
    splitting the column would claim two layouts where there is one.

    THE BASELINE IS THE GEOMETRY THE RECORDED HISTORY WAS WRITTEN UNDER, NOT THE DISPATCH'S CURRENT
    DEFAULT, and the two have already diverged — `row_group_size` defaulted to 16000000 for the 13+
    runs now in `history/`, and defaults to 2000000 since. This was called
    `_nondefault` and read the live default, which is a trap that fires the moment a default moves:
    a 2M run would record `None`, land in the same `(engine, config)` column as the 16M history, and
    `columnsFor` — which takes the LATEST run per column — would HIDE six runs of 9-RG history
    behind one 24-RG run. The bars would still separate (`layoutKey` bands the MEASURED file and row
    group counts), so nothing would look broken; the CU and sources tables would just quietly report
    the wrong geometry's numbers. Pin the baseline to what history holds and let every new default
    record itself explicitly.
    """
    if os.environ.get("DUCKDB_SORTED") != "true":
        return None
    v = (os.environ.get(var) or "").strip()
    return v if v and v != baseline else None


def declared_sort_key():
    """`{table: [cols]}` for the sort the duckrun run DECLARED, or `{}` when it declared none.

    WHY THIS IS RECORDED AT ALL: the key is a property of the RUN — the model declared
    `['date','time','DUID']` for a while and `['date','time']` since, and it is now a dispatch input
    that can be anything — so anything downstream holding one constant is right only by luck. The
    dashboard held exactly that constant and captioned run 30955591822, a DUID sort, `by date, time`.

    READ FROM THE ENV, not from the model. It used to regex a literal list out of
    `fct_summary.sql`; the model now renders `sort_by` from `DUCKDB_SORT_BY`, so there is no literal
    left to match and that regex would silently return `{}` — the same quiet gap this exists to
    close. The env var is what the model itself reads, so the two cannot disagree.

    Gated on `DUCKDB_SORTED`: an unsorted run declares no key, and recording one would caption an
    unsorted bar `by date, time`. Absent, never empty.
    """
    if os.environ.get("DUCKDB_SORTED") != "true":
        return {}
    # Fallback MUST match the model's own `env_var('DUCKDB_SORT_BY', ...)` default, or a hand run
    # with the var unset records a key it did not write. CI always sets it from the input.
    cols = [c.strip()
            for c in os.environ.get("DUCKDB_SORT_BY", "date,time,price").split(",") if c.strip()]
    return {MART: cols} if cols else {}


def encoding_table(encodings, engines):
    """`fct_summary`'s per-column parquet encoding, engines side by side.

    The question this answers is whether two engines hand Power BI the same thing to transcode. It
    sits beside the layout table because that one reports SHAPE — files, row groups, size — and shape
    turned out not to explain the CU: duckrun writes the densest parquet on the page and does not
    win, and dwh writes UNCOMPRESSED and beats a SNAPPY spark build.
    """
    have = [e for e in engines if encodings.get(e)]
    if not have:
        return
    print(f"## 🔤 `{MART}` column encoding\n")
    print("| column | type | " + " | ".join(have) + " |")
    print("| --- | --- | " + " | ".join("---" for _ in have) + " |")
    for col in sorted({c for e in have for c in encodings[e]}):
        # The type is the PARQUET physical type and the engines can legitimately disagree (a DATE is
        # INT32 to one writer and INT64 to another), which is itself worth seeing — so it is printed
        # from whichever engine has it and any disagreement shows up in the cells beside it.
        typ = next((encodings[e][col]["type"] for e in have if col in encodings[e]), "—")
        cells = []
        for e in have:
            c = encodings[e].get(col)
            cells.append("—" if not c else
                         f"`{'+'.join(c['encodings'])}`"
                         f"{'' if c['dict_pages'] else ' ⚠️ no dict'} · {c['mb']:,.1f} MB")
        print(f"| `{col}` | `{typ}` | " + " | ".join(cells) + " |")
    print()


def ordering_table(ordering, engines):
    """`fct_summary`'s physical row order, engines side by side — the V-Order reality check.

    Two blocks. The per-engine one answers "did Fabric tag these files V-Ordered, and what did we
    sample"; the per-column one puts the two ordering measurements in one cell, because they are one
    question asked at two scales and reading them apart invites concluding a table is sorted from
    row-group ranges alone.
    """
    have = [e for e in engines if ordering.get(e)]
    if not have:
        return
    print(f"## 🔀 `{MART}` physical row order\n")
    print("<sub>Is the data actually reordered on disk? <b>RG overlap</b>: consecutive row-group "
          "[min,max] ranges, sorted by min, that overlap — 0% = the row groups partition the "
          "column's range, ~100% = every row group spans everything. <b>runs</b>: adjacent "
          "equal-value spans in the first rows of the largest file, in physical order — runs ≪ rows "
          "means equal values were brought together, which is the intra-file reordering V-Order "
          "claims and the row-group ranges cannot see. A near-unique column (`mw`, `price`) is the "
          "control: it can only drop if the file really was reordered. `*` = a truncated string "
          "statistic, so that boundary is approximate. <b>V-Order files</b> is the per-file Delta "
          "<code>add.tags.VORDER</code>, which only the Fabric SPARK writer stamps — a Warehouse "
          "reads <code>n/a</code> rather than 0/N, because it writes no such tag while V-Ordering by "
          "default; its state is <code>layout.ordering.dwh.vorder_enabled</code> in the run "
          "record.</sub>\n")
    print("| engine | V-Order files | sample |")
    print("| --- | --- | --- |")
    for e in have:
        d = ordering[e]
        v, s = d.get("vorder_files") or {}, d.get("sample") or {}
        # `n/a`, never `—`, for a warehouse: the tag is a Spark-writer marker and the warehouse writes
        # none while V-Ordering BY DEFAULT, so a dash here reads as "not V-Ordered" on the one engine
        # that always is. The real answer is `vorder_enabled` in the record, from the build leg's own
        # `sys.databases` query — this job has no T-SQL connection and cannot print it.
        vc = ("n/a (warehouse)" if not v and KIND.get(e) == "warehouses"
              else "—" if not v else (f"{v['tagged']:,}/{v['files']:,}"
                                      + (f" +{v['unknown']:,}?" if v.get("unknown") else "")))
        sc = "—" if not s else f"`{s['file']}` · {s['rows']:,} rows"
        print(f"| {e} | {vc} | {sc} |")
    print()
    print("| column | " + " | ".join(have) + " |")
    print("| --- | " + " | ".join("---" for _ in have) + " |")
    for col in sorted({c for e in have for c in (ordering[e].get("columns") or {})}):
        cells = []
        for e in have:
            c = (ordering[e].get("columns") or {}).get(col)
            if not c:
                cells.append("—")
                continue
            pct = c.get("rg_overlap_pct")
            runs = c.get("runs")
            cells.append(
                ("—" if pct is None else f"{pct:,.0f}%{'*' if c.get('inexact') else ''} RG overlap")
                + (f" · {runs:,} runs" if runs is not None else ""))
        print(f"| `{col}` | " + " | ".join(cells) + " |")
    print()


def build_doc(per_engine, engines, guids=None, landing=None, encodings=None, ordering=None):
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
        # `sorted` is on DUCKRUN ONLY and is recorded ONLY when it is on. Both halves of that are
        # deliberate and neither is symmetry worth restoring. iceberg parses the same model and has
        # no `sort_by` config at all, so recording the flag there would split iceberg's dashboard
        # column between two runs whose parquet is byte-identical — the page would claim two
        # layouts where there is one; what the DISPATCH asked for is still in the record's `inputs`
        # block, which is what that block is for. And off is the same parquet as never-offered, so
        # an explicit "false" would fragment 13 runs of history for a difference that does not
        # exist — the same reason `variantTag` can read absence as off here but not for NEE.
        # The geometry keys follow `sorted`'s rule exactly: recorded ONLY when they are in force AND
        # differ from the default, because a default dispatch writes the parquet every earlier run
        # wrote and must key to the same dashboard column. `variant()` skips null, so a `None` here
        # costs nothing; a value SPLITS the column, which is what a different geometry deserves.
        # They only bite while `sorted` is on — that is where the model declares geometry at all.
        "config": {e: cfg for e, cfg in (
            ("duckrun", {"vcores": os.environ.get("FABRIC_CORES") or None,
                         "sorted": "true" if os.environ.get("DUCKDB_SORTED") == "true" else None,
                         # 16000000 / 1024 are what `history/` was written under, NOT today's
                         # dispatch defaults — see `_nonbaseline`. Moving these moves 13+ runs
                         # into the wrong column, silently.
                         "row_group_size": _nonbaseline("DUCKDB_ROW_GROUP_SIZE", "16000000"),
                         "file_size_mb": _nonbaseline("DUCKDB_FILE_SIZE_MB", "1024")}),
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
        # `fct_summary`'s per-column parquet encoding — the one thing about what Power BI transcodes
        # that nothing measured, and therefore the only untested explanation left for the CU gaps.
        # Absent rather than `{}` when nothing was profiled, same rule as `landing`: an empty dict
        # would read as "no encodings", which is not a state parquet can be in.
        **({"encodings": {e: encodings[e] for e in engines if encodings.get(e)}}
           if any((encodings or {}).get(e) for e in engines) else {}),
        # Whether the writer physically REORDERED the rows — row-group range overlap, intra-file
        # run lengths, and the per-file Delta `VORDER` tag. See `ordering_for`. Absent rather than
        # `{}`, same rule as `encodings` and `landing`: "not measured" and "nothing is ordered" are
        # different claims and only one of them is ever true here.
        **({"ordering": {e: ordering[e] for e in engines if ordering.get(e)}}
           if any((ordering or {}).get(e) for e in engines) else {}),
        # The INPUT side: files and bytes in the landing archive every engine reads. Absent, never
        # empty, when the listing failed — `{}` would read as "an empty archive", which is a
        # different statement from "not measured".
        **({"landing": landing} if landing else {}),
    }
    return doc


def write_json(doc, engines):
    """Write the layout doc where STATS_JSON names a path, and into the run record either way.

    Two sinks, one document. STATS_JSON is the per-run artifact (kept: it is how a failed run's
    layout is read back without a checkout); the run record is what survives artifact retention and
    is what the page joins against the CU ledger.
    """
    record.merge({"layout": doc})
    # WHICH columns a sorted duckrun run ordered by, as a SIBLING of the layout doc rather than a
    # branch of it. It belongs beside `fabric_run.py`'s `sort_by_auto` — `dbt.<engine>` is where a
    # fact about the dbt run lives — and it must stay out of `layout.config`, whose every entry the
    # dashboard's `variant()` walks: a key that changes commit to commit would split an engine's
    # column and its layout bar. Gated on the same env as `config.sorted`, and duckrun-only for the
    # same reason: iceberg parses the same model and has no `sort_by` config at all.
    sort_key = declared_sort_key() if os.environ.get("DUCKDB_SORTED") == "true" else {}
    if sort_key:
        record.merge({"dbt": {"duckrun": {"sort_by": sort_key}}})
    path = os.environ.get("STATS_JSON", "").strip()
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    have = sum(len(v) for v in doc["stats"].values())
    sys.stderr.write(f"  wrote {path}: {have} (engine, table) rows for {len(engines)} engines\n")


def one_engine(item, kind):
    """(guid, stats, mart encodings, mart ordering) for one Fabric item; exceptions propagate.

    Everything rides along in the SAME worker rather than a second pass: this function already owns
    a reader for that item, and the pool is sized one thread per engine, so a separate pass would
    either serialise behind the slowest engine again or multiply the connections.

    ONE connection, and one `mart_chunks` fetch off it. Four questions are asked of each item now
    and each of them used to imply its own `duckrun.connect`; more to the point, the encodings and
    both ordering metrics all read the SAME parquet footers, which is the expensive part.
    """
    guid = find_guid(kind, item)
    if not guid:
        return guid, {}, {}, {}
    con = reader(guid)
    st = stats_for(con)
    # Qualified from the schema `stats_for` just read, never hardcoded: a bare name does not resolve
    # (see `encodings_from`), and deriving it here means the profiled table is by construction the
    # one the rest of this document reports on.
    schema = (st.get(MART) or {}).get("schema")
    at, chunks = mart_chunks(con, f"{schema}.{MART}") if schema else (None, [])
    return guid, st, encodings_from(at, chunks), ordering_for(con, guid, schema, at, chunks, kind)


def main():
    per_engine, guids, encodings, orderings = {}, {}, {}, {}
    # The items are independent and the iceberg one alone can take >10 minutes to read over
    # OneLake, so fetch them concurrently: wall-clock = slowest engine, not the sum. The landing
    # listing rides along in the same pool — it is a different item and a different question, and
    # doing it in series would add its minute to a job that is already the slowest in the run.
    with ThreadPoolExecutor(max_workers=len(ENGINES) + 1) as pool:
        landing = pool.submit(landing_stats)
        futures = {engine: pool.submit(one_engine, item, kind) for engine, item, kind in ENGINES}
    for engine, item, kind in ENGINES:
        try:
            (guids[engine], per_engine[engine],
             encodings[engine], orderings[engine]) = futures[engine].result()
            v = (orderings[engine].get("vorder_files") or {}) if orderings[engine] else {}
            sys.stderr.write(f"  {engine} ({item} {guids[engine]}): "
                             f"{sum(d.get('total_rows') or 0 for d in per_engine[engine].values()):,}"
                             f" rows total (all tables), "
                             f"{len(encodings[engine])} {MART} column(s) profiled"
                             + (f", {v['tagged']}/{v['files']} V-Ordered file(s)" if v else "")
                             + "\n")
        except Exception as e:
            per_engine[engine] = {}
            encodings[engine] = {}
            orderings[engine] = {}
            sys.stderr.write(f"  {engine} ({item}) FAILED: {e}\n")

    engines = [e for e, _, _ in ENGINES]
    land = landing.result()
    landing_table(land)
    parity_table(per_engine, engines)
    detail_tables(per_engine, engines)
    encoding_table(encodings, engines)
    ordering_table(orderings, engines)
    write_json(build_doc(per_engine, engines, guids, land, encodings, orderings), engines)


if __name__ == "__main__":
    main()
