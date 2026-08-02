"""The page. Reads the run records and the CU ledger, joins them on the ITEM GUID, renders markdown.

    python cu/dashboard.py > dashboard.md
    python cu/report_html.py dashboard.md "footer" > index.html

**Two JSON documents, joined on one key.** `history/runs/<ts>-<run id>.json` is written by the
`Benchmark` workflow and names every Fabric item GUID that run created, with its role, plus the
layout, the input archive and the raw query timings. `history/cu.json` is the cumulative ledger
`measure.py` builds, `{item GUID: {operation: {hour: CU}}}`. Nothing else passes between them.

That join replaces the whole apparatus the old page needed. Attribution used to be substring matching
on item DISPLAY NAMES, with a `shared` column for everything ambiguous, a lagging `'Items'` snapshot
for kinds, and heuristics — idle-hour gaps, repeated model names — to guess where one run ended and
the next began. Now every item bar the landing lakehouse is created and destroyed inside one run, so
a GUID belongs to exactly one run and the class comes from the role WE recorded. There is no
`shared`, no `engine_of`, no sessionize.

Properties worth keeping:

- **No token, no network, no third-party package** — the standard library and `report_html.py`. This
  cannot fail for a reason that has anything to do with Fabric, which is what makes republishing a
  free, repeatable act rather than a measurement.
- **It renders what the records CONTAIN.** One engine, two, a dispatch that skipped the benchmark and
  so has no analytics CU: the columns come from the records, never from a configured list. An engine
  nothing ever measured has no zero to print.
- **The page is composed from EVERY record** — each engine's latest run, once per config. One
  dispatch builds one engine, so rendering the newest record alone would give a comparison page with
  one column. `CU_RECORD` pins one run when reproducing an old page.
"""
import json
import os
import sys

HISTORY_DIR = os.environ.get("CU_HISTORY_DIR", "history").strip()
RUNS_DIR = os.environ.get("CU_RUNS_DIR", os.path.join(HISTORY_DIR, "runs")).strip()
LEDGER = os.environ.get("CU_LEDGER", os.path.join(HISTORY_DIR, "cu.json")).strip()
# Render ONE run alone. A substring of the filename, so a run id or a date both work.
PICK = os.environ.get("CU_RECORD", "").strip()

REPO = os.environ.get("GITHUB_REPOSITORY", "djouallah/fabric-dbt-benchmark")
SERVER = os.environ.get("GITHUB_SERVER_URL", "https://github.com")

# Engine order wherever one is needed. Not a filter — an engine outside this list still renders, it
# just sorts to the end.
ENGINES = ["duckrun", "iceberg", "spark", "dwh"]

# What each engine IS, for the chart captions and the layout table. `iceberg` beside `duckrun` reads
# as an engine difference and it is a WRITER difference — the same DuckDB, in the same notebook, at
# the same vCores. The third entry matches stats.py's WRITER map exactly, so the layout table reads
# identically whether it came from that artifact or from a record.
STACK = {
    "landing": ("download_aemo.py", "the shared AEMO archive every leg reads", "—"),
    "duckrun": ("dbt-duckrun", "DuckDB → delta-rs", "delta-rs"),
    "iceberg": ("dbt-duckdb", "DuckDB → Iceberg REST catalog", "duckdb (iceberg)"),
    "spark": ("dbt-fabricspark", "Fabric Spark (Livy) → Delta", "spark"),
    "dwh": ("dbt-fabric-samdebruyn", "Fabric Warehouse (T-SQL)", "warehouse"),
}

# A column is an engine (`spark`) or an engine under one CONFIG (`spark·readHeavyForPBI+NEE`), which
# is what puts the same engine's two resource profiles side by side. A tag joins its own parts with
# `+`, never with this, so the split back to the engine is unambiguous.
COL_SEP = "·"

# Role -> which half of the page an item's CU belongs to. Everything that is not a semantic model is
# work done to BUILD the tables; a semantic model is only ever queried. This replaces classification
# by Fabric item kind, read out of a snapshot that had usually not catalogued a minutes-old item.
ANALYTICS_ROLES = {"semantic_model"}
# Not an engine and never a column: the landing lakehouse is the shared archive every engine reads,
# so its CU is an input cost, not one engine's. `folder` holds nothing and costs nothing.
NON_ENGINE_ROLES = {"landing", "folder"}


def log(msg):
    sys.stderr.write(msg + "\n")


def base_engine(col):
    """`spark·readHeavyForPBI+NEE` → `spark`; `spark` → `spark`."""
    return str(col).split(COL_SEP, 1)[0].strip()


def run_url(run_id):
    return f"{SERVER}/{REPO}/actions/runs/{run_id}"


# --------------------------------------------------------------------------------------- loading

def load_runs(directory=None):
    """Every readable run record, oldest first. A missing directory is an empty list, not an
    exception: a checkout without `history/runs/` is a normal thing to be, and the caller says so on
    the page rather than crashing on it."""
    directory = RUNS_DIR if directory is None else directory
    out = []
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return out
    for n in names:
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(directory, n), encoding="utf-8") as f:
                rec = json.load(f)
        except Exception as ex:                            # noqa: BLE001
            log(f"  skipping {n}: unreadable ({type(ex).__name__}: {ex})")
            continue
        rec["_file"] = n
        out.append(rec)
    out.sort(key=lambda r: (((r.get("run") or {}).get("started") or ""), r["_file"]))
    return out


def load_ledger(path=None):
    path = LEDGER if path is None else path
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return {"items": {}, "reads": []}
    doc.setdefault("items", {})
    doc.setdefault("reads", [])
    return doc


def item_cu(ledger, guid):
    """Total CU for one Fabric item. `None` when the ledger has never seen it.

    `None` and `0.0` are different claims — "not measured yet" against "cost nothing" — and the
    sources table has to be able to say which.
    """
    v = ledger["items"].get(guid)
    return None if v is None else float(v)


# ------------------------------------------------------------------------------------- the join

def run_cu(rec, ledger):
    """`({class: {item label: CU}}, landing CU, unmeasured items)` for one run.

    THE join, and it is a dictionary lookup: every GUID the run recorded, looked up in the ledger,
    filed under the class its ROLE implies. No allocation and no heuristic, because the teardown
    means a GUID belongs to exactly one run.

    Broken down by ITEM rather than by operation type. The item names come from the run record, so
    they cost nothing and say more: `dbt-duckrun-*` at 29,571 CU beside `dbt_delta` at 1,509 is the
    whole story of where a DuckDB leg's cost goes, and no operation name carries that.

    `dbt_landing` is the exception — never deleted, read by every engine — so its total is cumulative
    over the measured window rather than this run's share. It is returned separately and never added
    to an engine.
    """
    cells, landing, unmeasured = {}, None, []
    for guid, item in (rec.get("items") or {}).items():
        role = item.get("role") or "?"
        if role == "folder":                     # a workspace folder never accrues a capacity unit
            continue
        value = item_cu(ledger, guid)
        if role == "landing":
            landing = (landing or 0.0) + value if value is not None else landing
            continue
        if value is None:
            unmeasured.append(f"{role}/{item.get('name') or guid}")
            continue
        cls = "analytics" if role in ANALYTICS_ROLES else "etl"
        label = f"{item.get('name') or guid} ({role})"
        cells.setdefault(cls, {})[label] = cells.setdefault(cls, {}).get(label, 0.0) + value
    return cells, landing, unmeasured


def class_total(cells, cls):
    return sum((cells.get(cls) or {}).values())


def still_accruing(rec, hours=2.0):
    """True when this run finished recently enough that its CU can still rise.

    DERIVED, never stored. An hour's CU keeps growing for ~70 minutes after the fact, so a number
    read minutes after a run is a lower bound — but that is a property of the clock, not a fact worth
    writing into a file and keeping in step.
    """
    from datetime import datetime, timezone
    stamp = (rec.get("run") or {}).get("finished")
    if not stamp:
        return False
    try:
        t = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return False
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - t).total_seconds() < hours * 3600


def variant(rec):
    """The config signature this run ran under, hashable. `()` when it recorded none — which is dwh
    always, since Fabric Warehouse exposes no per-run knob."""
    cfg = (rec.get("layout") or {}).get("config") or {}
    engine = rec.get("engine")
    c = cfg.get(engine) or {}
    return tuple(sorted((k, str(v)) for k, v in c.items() if v is not None))


def variant_tag(sig):
    """The short label separating one config from another in a column header. Compact on purpose: it
    sits in a table head, and the full reading is in the layout section."""
    d = dict(sig)
    bits = []
    if d.get("vcores"):
        bits.append(f"{d['vcores']}c")
    if d.get("resource_profile"):
        bits.append(d["resource_profile"])
    nee = d.get("native_execution_engine")
    if nee is not None:
        bits.append("NEE" if nee.lower() == "true" else "noNEE")
    # `+`, never COL_SEP — base_engine splits on that, and a tag containing one would make
    # `spark·readHeavyForPBI+NEE` unparseable back to `spark`.
    return "+".join(bits) or "unrecorded"


def columns_for(runs):
    """`[(column id, engine, record)]` — each engine's LATEST run, once per configuration.

    This is what the page is for. One dispatch builds ONE engine, so rendering the newest record
    alone gives a comparison page with a single column. The key is (engine, config) rather than
    engine, because spark under `readHeavyForPBI` answers a different question from spark under
    `writeHeavy` and one number cannot stand for both; and an engine nobody has rebuilt keeps showing
    its last real measurement instead of vanishing.

    The cost is that columns are different dispatches, days apart — which `render_sources` states
    per column rather than smoothing over.
    """
    latest = {}
    for rec in runs:                                    # oldest first, so later runs win their key
        engine = rec.get("engine")
        if not engine:
            continue
        latest[(engine, variant(rec))] = rec
    per_engine = {}
    for e, _sig in latest:
        per_engine[e] = per_engine.get(e, 0) + 1
    cols = []
    for (e, sig), rec in latest.items():
        cols.append((e if per_engine[e] < 2 else f"{e}{COL_SEP}{variant_tag(sig)}", e, rec))
    order = {e: i for i, e in enumerate(ENGINES)}
    cols.sort(key=lambda c: (order.get(c[1], len(order)), c[1], c[0]))
    return cols


# ------------------------------------------------------------------------------------ rendering

def engine_caption(rec, col):
    """The "what this bar actually is" line: the adapter, then whatever the run RECORDED about the
    compute it was given. Never a default — an unrecorded profile is simply absent, because a
    filled-in one reads exactly like a measurement."""
    adapter = (STACK.get(base_engine(col)) or ("",))[0]
    bits = [adapter] if adapter else []
    c = ((rec.get("layout") or {}).get("config") or {}).get(rec.get("engine")) or {}
    if c.get("vcores"):
        bits.append(f"{c['vcores']} vCores")
    if c.get("resource_profile"):
        bits.append(str(c["resource_profile"]))
    nee = c.get("native_execution_engine")
    if nee is not None:
        bits.append("NEE on" if str(nee).lower() == "true" else "NEE off")
    return " · ".join(bits)


def chart(title, subtitle, rows):
    """Emit a chart spec for the HTML renderer, as an HTML COMMENT.

    One data path, two outputs. The same markdown goes to the GitHub job summary and to the page, and
    GitHub SANITISES inline SVG — so the chart cannot be drawn here. A comment is invisible in the
    summary and `report_html.py` picks it up and draws the bars. The numbers below it are the same
    numbers, so the summary loses nothing but the picture.

    Sorted CHEAPEST FIRST, because "lower is better" makes the ranking the finding. A ZERO sorts to
    the BOTTOM: zero means "this engine did no such work", and at the top under that caption it would
    read as the winner — the one value whose rank would lie.
    """
    rows = sorted(rows, key=lambda r: (r[1] == 0, r[1]))
    if not any(r[1] for r in rows):
        return
    print(f"\n<!--chart:{json.dumps({'title': title, 'subtitle': subtitle, 'rows': rows})}-->")


def engine_table(per_col, cols):
    """Engines across, ITEMS down, grouped by class — the shape the whole repo reads in.

    ENGINE-MAJOR, and that orientation is what makes the width work: item-major (one column per
    item) would need a column per Fabric item and every run creates different ones. Turned ninety
    degrees those are rows, which markdown handles fine.

    **No total column and no grand-total row.** Both would sum ACROSS engines, which is the one sum
    on this page that answers nothing — the engines are alternatives to each other. The class
    subtotals stay: they sum DOWN a column, which is "what this engine spent building".
    """
    names = [c for c, _e, _r in cols]
    labels = {}
    for cls in ("etl", "analytics"):
        seen = {}
        for col in names:
            for label, value in ((per_col.get(col) or {}).get(cls) or {}).items():
                seen[label] = seen.get(label, 0.0) + value
        labels[cls] = sorted(seen, key=lambda k: -seen[k])
    # The corner cell names the measure. Every number in the table is one, and a matrix whose values
    # carry no unit gets quoted as "26,128" with no idea what of.
    print("| CU (s) | " + " | ".join(names) + " |")
    print("|:--|" + "---:|" * len(names))
    for cls in ("etl", "analytics"):
        if not labels[cls]:
            continue
        print(f"| **{cls}** | "
              + " | ".join(f"**{class_total(per_col.get(c) or {}, cls):,.1f}**" for c in names)
              + " |")
        for label in labels[cls]:
            row = []
            for col in names:
                v = ((per_col.get(col) or {}).get(cls) or {}).get(label)
                # An em dash, not 0.0: this engine never created an item of that name, which is a
                # different statement from one that cost nothing.
                row.append("—" if v is None else f"{v:,.1f}")
            print(f"| `{label}` | " + " | ".join(row) + " |")
    print("\n<sub>Broken down by the Fabric ITEM that was billed, named from the run record. `etl` "
          "against `analytics` comes from each item's recorded role, not from an operation name: a "
          "semantic model is only ever queried, everything else is work done to build the "
          "tables.</sub>")


def render_sources(cols, ledger, unmeasured):
    """Which dispatch each column came from, and whether its CU can still rise.

    The one thing a composed page owes the reader that a single-run page did not: the columns are
    different dispatches, so a column can be days older than the one beside it. The other half is
    that a run measured minutes ago is a LOWER BOUND — an hour's CU keeps growing for ~70 minutes
    after the fact — so the reader is told to dispatch again rather than left to wonder.
    """
    print("\n<sub>Each column is that engine's latest run. They are different dispatches:</sub>\n")
    print("| column | run | built | items | CU |")
    print("|:--|:--|:--|--:|:--|")
    for col, _engine, rec in cols:
        rid = (rec.get("run") or {}).get("id")
        link = f"[{rid}]({run_url(rid)})" if rid else "—"
        items = [g for g, it in (rec.get("items") or {}).items()
                 if (it.get("role") or "") not in NON_ENGINE_ROLES]
        started = ((rec.get("run") or {}).get("started") or "?")[:16].replace("T", " ")
        missing = unmeasured.get(col) or []
        if missing:
            state = f"{len(items) - len(missing)}/{len(items)} items measured"
        elif still_accruing(rec):
            state = "may still rise"
        else:
            state = "settled"
        load = "full" if rec.get("full_load") else "incremental"
        print(f"| {col} | {link} | {started} ({load}) | {len(items)} | {state} |")
    print("\n<sub>An hour's CU keeps growing for up to ~70 minutes after the work happened, so a run "
          "measured just now is a lower bound — dispatch **Dashboard** again and the numbers rise to "
          "their final value. Every item a run creates is deleted when it finishes, which is what "
          "makes a Fabric item GUID belong to exactly one run and the attribution exact.</sub>")


def render_landing(cols, per_landing):
    """The shared input cost, kept OUT of every engine column.

    `dbt_landing` holds the downloaded AEMO archive and is the one item no run deletes, so its total
    is cumulative over the measured window rather than any single run's share. It is a stage every
    engine reads from, not a fifth competitor.
    """
    rows = [(col, per_landing.get(col)) for col, _e, _r in cols]
    if not any(v for _c, v in rows if v):
        return
    print("\n<sub>`dbt_landing` — the shared archive every engine reads. Cumulative over the "
          "measured window, and NOT part of any engine's column:</sub>\n")
    print("| | " + " | ".join(c for c, _v in rows) + " |")
    print("|:--|" + "---:|" * len(rows))
    print("| landing | " + " | ".join("—" if v is None else f"{v:,.1f}" for _c, v in rows) + " |")


def render_input(cols):
    """How much data went IN. Every other number on this page describes what came out."""
    have = [(col, ((rec.get("layout") or {}).get("landing") or {})) for col, _e, rec in cols]
    have = [(c, d) for c, d in have if d]
    if not have:
        return
    print("\n### Input archive\n")
    print("| column | files | size MB |")
    print("|:--|--:|--:|")
    for col, d in have:
        print(f"| {col} | {d.get('files', 0):,} | {float(d.get('size_mb') or 0):,.2f} |")
    print("\n<sub>The landed AEMO CSV archive as each run read it, from `stats.py`'s listing of "
          "`dbt_landing/Files`. Every other number on this page is about what came OUT; this is what "
          "went in, and it is the one that makes a duration or a CU total mean anything. It moves "
          "only when a dispatch runs with `skip_download` off.</sub>")


LAYOUT_TABLE = os.environ.get("CU_LAYOUT_TABLE", "fct_summary").strip()


def render_layouts(cols, analytics):
    """Every shared table's physical layout, one block each, the mart first.

    The mart leads because it is the table the benchmark's queries land on, and it is the only block
    carrying the CU column — the analytics CU is one number per engine, not per table, so printing it
    in every block would read as one measurement per table.
    """
    stats = {col: ((rec.get("layout") or {}).get("stats") or {}).get(rec.get("engine")) or {}
             for col, _e, rec in cols}
    writers = {col: (STACK.get(base_engine(col)) or ("", "", "—"))[2] for col, _e, _r in cols}
    tables, schema = [], {}
    for col, _e, rec in cols:
        for t in ((rec.get("layout") or {}).get("tables") or []):
            if t not in tables:
                tables.append(t)
        for t, d in (stats.get(col) or {}).items():
            schema.setdefault(t, (d or {}).get("schema"))
            if t not in tables:
                tables.append(t)
    if not tables:
        return
    mart = LAYOUT_TABLE if LAYOUT_TABLE in tables else tables[0]
    order = [mart] + [t for t in tables if t != mart]
    metrics = [("total_rows", "rows", 0), ("num_files", "files", 0),
               ("num_row_groups", "row groups", 0), ("avg_row_group", "avg RG rows", 0),
               ("size_mb", "size MB", 1)]
    print("\n### Table layout\n")
    for t in order:
        present = [(c, (stats.get(c) or {}).get(t)) for c, _e, _r in cols]
        present = [(c, d) for c, d in present if d]
        if not present:
            continue
        show_cu = t == mart
        head = (f"`{t}` in detail — the mart the queries land on" if t == mart
                else f"`{(schema.get(t) + '.') if schema.get(t) else ''}{t}`")
        print(f"\n#### {head}\n")
        print("| engine | writer | " + ("CU | " if show_cu else "")
              + " | ".join(h for _k, h, _d in metrics) + " | vorder |")
        print("|:--|:--|" + ("--:|" if show_cu else "") + "--:|" * len(metrics) + ":--|")
        for col, d in present:
            cells = ["—" if d.get(k) is None else f"{float(d[k]):,.{dp}f}" for k, _h, dp in metrics]
            cu_cell = (f"{analytics.get(col, 0.0):,.1f} | " if show_cu else "")
            print(f"| {col} | `{writers.get(col, '—')}` | {cu_cell}" + " | ".join(cells)
                  + f" | {'yes' if d.get('vorder') else 'no'} |")
    print("\n<sub>Every shared table the project writes, in pipeline order, as `stats.py` read the "
          "Delta log in that run's **layout** job. Sizes are what the tables held at that moment; "
          "the CU beside the mart is the engine's ANALYTICS total — what querying it cost, not what "
          "building it did — and the queries read all of these. Nothing here re-read a Delta "
          "log.</sub>")


def render(cols, runs, ledger):
    """The whole page, on stdout, as the markdown subset `report_html.py` renders."""
    per_col, per_landing, analytics, unmeasured = {}, {}, {}, {}
    for col, _engine, rec in cols:
        cells, landing, missing = run_cu(rec, ledger)
        per_col[col] = cells
        per_landing[col] = landing
        unmeasured[col] = missing
        analytics[col] = class_total(cells, "analytics")

    newest = max((((r.get("run") or {}).get("started") or "") for _c, _e, r in cols), default="")
    print(f"## Capacity units — the latest run per engine, as of "
          f"{(ledger.get('updated') or newest or '?')[:16].replace('T', ' ')}\n")

    n = len({base_engine(c) for c, _e, _r in cols})
    print("**Every number on this page is capacity units (CU-seconds)** — Fabric's own billing "
          "measure, read from the Capacity Metrics model. Not milliseconds and not rows: what the "
          f"work COST. One dbt project, {n} engine{'s' if n != 1 else ''}, one landed copy of the "
          "data: this is what each engine charged to build the same tables and to answer the same "
          "queries. Attribution is by Fabric ITEM GUID — each run records what it created and then "
          "deletes it — so no number here is a guess about which engine an item belonged to.\n")

    reads = len(ledger.get("reads") or [])
    print(" · ".join([f"[source]({SERVER}/{REPO})",
                      f"`{RUNS_DIR}/` — {len(runs)} run(s), {len(cols)} on this page",
                      f"`{LEDGER}` — {len(ledger['items'])} item GUID(s) over {reads} read(s)"])
          + "\n")

    render_sources(cols, ledger, unmeasured)

    chart("ETL — what building the tables cost", "capacity units, lower is better",
          [[col, round(class_total(per_col[col], "etl"), 1), engine_caption(rec, col)]
           for col, _e, rec in cols])
    chart("Analytics — what querying them cost", "capacity units, lower is better",
          [[col, round(analytics.get(col, 0.0), 1), engine_caption(rec, col)]
           for col, _e, rec in cols])

    print("\nEvery engine's latest run, summed:\n")
    engine_table(per_col, cols)
    render_landing(cols, per_landing)
    render_input(cols)
    render_layouts(cols, analytics)


def render_empty(runs_dir, ledger_path):
    """Nothing to render, so say what the contract is rather than printing an empty page. This is the
    dashboard's only failure mode, and it is always the same one: nothing has been measured yet."""
    print("## Capacity units\n")
    print(f"**No run records in `{runs_dir}/`.** This page renders what a run filed and what the "
          f"capacity ledger (`{ledger_path}`) says those items cost. It reads nothing else and "
          "spends no capacity, so an empty directory means nothing has been recorded yet — not that "
          "the capacity was idle.\n")
    print(f"Dispatch **Benchmark** ([{REPO}]({SERVER}/{REPO}/actions)). It builds one engine, "
          "benchmarks it, deletes what it created and commits one record; this workflow then reads "
          "the capacity for those item GUIDs and renders.")


def main(argv=None):
    runs = load_runs()
    ledger = load_ledger()
    if not runs:
        render_empty(RUNS_DIR, LEDGER)
        return 0
    if PICK:
        hits = [r for r in runs if PICK in r["_file"]]
        if not hits:
            log(f"  no run record matches {PICK!r}; rendering the newest instead")
            hits = runs[-1:]
        rec = hits[-1]
        log(f"  rendering {rec['_file']} alone (CU_RECORD={PICK!r}) of {len(runs)} record(s)")
        render([(rec.get("engine") or "?", rec.get("engine"), rec)], runs, ledger)
        return 0
    cols = columns_for(runs)
    log(f"  composing {len(cols)} column(s) from {len(runs)} run(s): "
        + ", ".join(f"{c} <- {r['_file']}" for c, _e, r in cols))
    render(cols, runs, ledger)
    return 0


if __name__ == "__main__":
    sys.exit(main())
