"""The page. Reads the run records and the CU ledger, joins them on the ITEM GUID, renders markdown.

    python cu/dashboard.py > dashboard.md
    python cu/report_html.py dashboard.md "footer" > index.html

**Two JSON documents, joined on one key.** `history/runs/<ts>-<run id>.json` is written by the
`Benchmark` workflow and names every Fabric item GUID that run created, with its role, plus the
layout, the input archive and the raw query timings. `history/cu.json` is the cumulative ledger
`measure.py` builds, `{item GUID: {operation: CU}}`. Nothing else passes between them.

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
# OPERATION -> bucket. `OneLake …` is storage; everything else is compute. Measured against the live
# model 2026-08-02, and it is the only split that works, because compute and storage share an ITEM:
#
#   dbt_spark  [Lakehouse]  High Concurrency Session Livy Run  188,636   <- compute
#                           OneLake Write via Redirect          20,268   <- storage
#   dbt_dwh    [Warehouse]  Warehouse Query                    129,177   <- compute
#                           OneLake Write via Redirect           1,640   <- storage
#
# Bucketing by the item's ROLE was wrong for exactly that reason and this replaces it. Checked
# against every operation name on the capacity: the `OneLake` prefix separates them cleanly.
STORAGE_PREFIX = "OneLake"


# Skipped entirely — not a column, not a row, not a footnote. This page compares ENGINES. The
# landing lakehouse is the ingestion staging area that no run deletes and every run reads, so its CU
# is one cumulative figure belonging to no engine; a workspace `folder` never accrues a capacity unit
# at all. The archive's SIZE is still reported (render_input) — that is the input volume, which is a
# different question from what ingesting it cost.
NON_ENGINE_ROLES = {"landing", "folder"}


def bucket(op):
    return "storage" if str(op).startswith(STORAGE_PREFIX) else "compute"


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
        why = incomplete(rec)
        if why:
            log(f"  skipping {n}: {why}")
            continue
        out.append(rec)
    out.sort(key=lambda r: (((r.get("run") or {}).get("started") or ""), r["_file"]))
    return out


# Roles the teardown must have deleted. If one is still alive, that run's items are STILL ACCRUING
# and its numbers are not a measurement of that run — they are a measurement of everything since.
DELETABLE_ROLES = {"output", "dwh_src", "compute", "semantic_model"}


def incomplete(rec):
    """Why this run cannot go on the page, or `None` if it can.

    The page compares generations, so a run has to be a WHOLE generation: built, benchmarked, and
    torn down. A partial one is not a smaller answer, it is a misleading one —

    - **no benchmark** means an empty analytics column, which reads as "querying this engine was
      free" rather than "nobody measured it". Run 30743411308 is exactly that: the `bench` job was
      skipped by a `needs` bug and only the ETL half exists.
    - **no layout** means the build half never reported.

    A run that was never TORN DOWN is not rejected — see `drifting()`. Its numbers do keep creeping,
    but the creep is small and a missing column costs more than a caveated one; the page says so
    instead of hiding the run.

    Non-compliant records are skipped and NAMED, never silently dropped — and `measure.py` still
    reads them, because their items really did cost capacity and the ledger is the ledger.
    """
    if not rec.get("engine"):
        return "no engine recorded"
    run = rec.get("run") or {}
    if not (run.get("started") and run.get("finished")):
        return "no start/finish stamp"
    items = rec.get("items") or {}
    if not any((it.get("role") or "") == "output" for it in items.values()):
        return "no output item"
    if not ((rec.get("layout") or {}).get("stats") or {}).get(rec["engine"]):
        return "no layout recorded — the build half did not report"
    if not ((rec.get("benchmark") or {}).get("timings") or {}):
        return "no benchmark timings — the query half did not run"
    return None


def drifting(rec):
    """Items this run created and never deleted — so its CU has no upper bound.

    A run whose teardown ran has a FINAL cost: every item is gone, nothing can be charged to it
    again. One whose teardown did not (run 30733912205 predates the job) leaves its lakehouse and
    semantic model alive, and Fabric keeps billing them — background OneLake reads against an idle
    lakehouse, a Direct Lake model that gets refreshed. Its number is therefore "that run, plus
    whatever those items have done since", and it grows every time the ledger is topped up.

    Reported rather than rejected. The drift is small in practice and a column that disappears is
    worse than one carrying a caveat — but the caveat has to be there, because "settled" and "still
    climbing" are different claims and only one of them is comparable to a torn-down run.
    """
    return sorted(f"{it.get('role')}/{it.get('name') or g}"
                  for g, it in (rec.get("items") or {}).items()
                  if (it.get("role") or "") in DELETABLE_ROLES and not it.get("deleted"))


def load_ledger(path=None):
    path = LEDGER if path is None else path
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return {"items": {}, "seconds": {}, "reads": []}
    doc.setdefault("items", {})
    # Absent on every ledger written before `measure.py` read duration, and absent again on any read
    # where the model had no duration column. Empty is the honest state for both, and the time
    # section renders NOTHING rather than a table of zeros.
    doc.setdefault("seconds", {})
    doc.setdefault("reads", [])
    return doc


def item_cu(ledger, guid, key="items"):
    """`{operation: CU}` — or `{operation: seconds}` — for one Fabric item. `None` when the ledger has
    never seen it.

    `None` and `{}` are different claims — "not measured yet" against "cost nothing" — and the
    sources table has to be able to say which.
    """
    v = (ledger.get(key) or {}).get(guid)
    if v is None:
        return None
    # An older ledger stored one NUMBER per item, before the operation was needed to split compute
    # from storage. It cannot be bucketed, so it is reported as unsplit rather than guessed into the
    # wrong half; `measure.py` drops such entries on its next read and they come back in full.
    return dict(v) if isinstance(v, dict) else {"(operation not recorded)": float(v)}


# ------------------------------------------------------------------------------------- the join

def run_cu(rec, ledger, key="items"):
    """`({class: {bucket: CU}}, unmeasured items)` for one run. `key="seconds"` gives the same
    breakdown in billed SECONDS, off the ledger's sibling dict — same GUIDs, same roles, same
    compute/storage split, because it is the same read.

    THE join, and it is a dictionary lookup: every GUID the run recorded, looked up in the ledger,
    filed under the class its ROLE implies. No allocation and no heuristic, because the teardown
    means a GUID belongs to exactly one run.

    Broken down by ITEM rather than by operation type. The item names come from the run record, so
    they cost nothing and say more: `dbt-duckrun-*` at 29,571 CU beside `dbt_delta` at 1,509 is the
    whole story of where a DuckDB leg's cost goes, and no operation name carries that.

    **`landing` and `folder` are skipped entirely, not reported apart.** The page compares ENGINES.
    `dbt_landing` is the ingestion staging area — no run deletes it, every run reads it, so its CU is
    one cumulative figure that belongs to no engine and answers no question this page asks. It was
    briefly given a row of its own; the same number repeated under every column read as "each of them
    spent this", which is the opposite of what it meant. The archive's SIZE is still reported (see
    `render_input`) — that is the input volume, not the cost of ingesting it.
    """
    cells, unmeasured = {}, []
    for guid, item in (rec.get("items") or {}).items():
        role = item.get("role") or "?"
        if role in NON_ENGINE_ROLES:
            continue
        value = item_cu(ledger, guid, key)
        if value is None:
            unmeasured.append(f"{role}/{item.get('name') or guid}")
            continue
        cls = "analytics" if role in ANALYTICS_ROLES else "etl"
        for op, cu in value.items():
            label = bucket(op)
            cells.setdefault(cls, {})[label] = cells.setdefault(cls, {}).get(label, 0.0) + cu
    return cells, unmeasured


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


def spread_for(runs, ledger, cls, key_of, key="items"):
    """`{column: [CU, …]}` — every run's total for `cls`, not just the latest. `key="seconds"` reads
    the ledger's duration dict instead, which is what puts a range on the ETL-time chart.

    One run is one sample of a SHARED capacity, so a single number is a reading rather than a
    result. Collecting every run of a column is what lets the chart show a mean and a range, and the
    range is the honest part: when two engines' averages sit closer together than either one's own
    spread, the ranking between them means nothing and the reader can see it.
    """
    out = {}
    for rec in runs:
        col = key_of(rec)
        if col is None:
            continue
        cells, _missing = run_cu(rec, ledger, key)
        value = class_total(cells, cls)
        if value:
            out.setdefault(col, []).append(value)
    return out


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


def chart_rows(cols, spread, latest, captions):
    """`[label, mean, min, max, caption]` per column, from every run that column has had.

    Sorted by the MEAN, which is what a ranking should be built on — one dispatch of a shared
    capacity is a sample. A column with no history falls back to its latest run, so a first-ever
    engine still charts.
    """
    out = []
    for col, _engine, _rec in cols:
        vals = spread.get(col) or ([latest[col]] if latest.get(col) else [])
        if not vals:
            out.append([col, 0, 0, 0, captions.get(col, "")])
            continue
        out.append([col, round(sum(vals) / len(vals), 1), round(min(vals), 1),
                    round(max(vals), 1), captions.get(col, "")])
    return out


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
        # Decompose a class ONLY when it decomposes something: some column has to hold more than one
        # item in it. `analytics` is always exactly one semantic model per engine, so its item rows
        # would repeat the subtotal and add a row of em dashes for every other engine — three rows
        # carrying one row's information. `etl` splits because a DuckDB leg really is a notebook
        # plus a lakehouse.
        deepest = max((len((per_col.get(c) or {}).get(cls) or {}) for c in names), default=0)
        labels[cls] = sorted(seen, key=lambda k: -seen[k]) if deepest > 1 else []
    # The corner cell names the measure. Every number in the table is one, and a matrix whose values
    # carry no unit gets quoted as "26,128" with no idea what of.
    print("| CU (s) | " + " | ".join(names) + " |")
    print("|:--|" + "---:|" * len(names))
    for cls in ("etl", "analytics"):
        if not any((per_col.get(c) or {}).get(cls) for c in names):
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
    print("\n<sub>`etl` against `analytics` comes from each item's recorded ROLE — a semantic model "
          "is only ever queried, everything else is work done to build the tables. `compute` against "
          "`storage` comes from the OPERATION, which is the only thing that can separate them: they "
          "share an ITEM. Spark bills its Livy session and its OneLake reads against the same "
          "lakehouse; a warehouse bills `Warehouse Query` and its OneLake writes against the same "
          "warehouse. Every `OneLake …` operation is storage; everything else — Livy runs, warehouse "
          "queries, notebook runs, SQL-endpoint queries — is compute. A dash means no operation of "
          "that kind was billed there at all.</sub>")


def render_sources(cols, ledger, unmeasured):
    """Which dispatch each column came from, and whether its CU can still rise.

    The one thing a composed page owes the reader that a single-run page did not: the columns are
    different dispatches, so a column can be days older than the one beside it. The other half is
    that a run measured minutes ago is a LOWER BOUND — an hour's CU keeps growing for ~70 minutes
    after the fact — so the reader is told to dispatch again rather than left to wonder.
    """
    print("\n<sub>Each column is that engine's latest run. They are different dispatches, "
          "newest first:</sub>\n")
    print("| column | run | built | items | CU |")
    print("|:--|:--|:--|--:|:--|")
    # NEWEST DISPATCH FIRST. Everywhere else on the page the order is the engine order, which is
    # what makes columns comparable across two renders; here the point of the table is precisely
    # that the columns are NOT contemporaneous, so it sorts on the thing it is reporting.
    for col, _engine, rec in sorted(
            cols, key=lambda c: ((c[2].get("run") or {}).get("started") or ""), reverse=True):
        rid = (rec.get("run") or {}).get("id")
        link = f"[{rid}]({run_url(rid)})" if rid else "—"
        items = [g for g, it in (rec.get("items") or {}).items()
                 if (it.get("role") or "") not in NON_ENGINE_ROLES]
        started = ((rec.get("run") or {}).get("started") or "?")[:16].replace("T", " ")
        missing = unmeasured.get(col) or []
        live = drifting(rec)
        if live:
            # Loudest of the three, because it is the only one that never resolves: the other two
            # are "wait and read again", this one is "the number has no upper bound until someone
            # deletes these".
            state = f"**still billing** — {len(live)} item(s) never deleted"
        elif missing:
            state = f"{len(items) - len(missing)}/{len(items)} items measured"
        elif still_accruing(rec):
            state = "may still rise"
        else:
            state = "settled"
        load = "full" if rec.get("full_load") else "incremental"
        print(f"| {col} | {link} | {started} ({load}) | {len(items)} | {state} |")
    drifters = {c: drifting(r) for c, _e, r in cols}
    print("\n<sub>An hour's CU keeps growing for up to ~70 minutes after the work happened, so a run "
          "measured just now is a lower bound — dispatch **Dashboard** again and the numbers rise to "
          "their final value. Every item a run creates is deleted when it finishes, which is what "
          "makes a Fabric item GUID belong to exactly one run and the attribution exact."
          + ("".join(f" **{c}** predates that teardown and still owns {', '.join(f'`{x}`' for x in v)}"
                     f" — Fabric keeps billing them, so its total creeps upward and is an upper "
                     f"bound on that run rather than a measurement of it. Delete them and it "
                     f"settles." for c, v in drifters.items() if v))
          + "</sub>")


def render_input(cols):
    """How much data went IN — ONE archive, not one per engine.

    `dbt_landing` holds a single copy of the AEMO CSVs and every engine reads the same bytes, so a
    column per engine repeated one number across the page and invited the reading that each engine
    had its own input. It is broken down by FOLDER instead, which is a real decomposition and comes
    free in the record.

    Taken from the most recent run that listed it. If an older column read a different archive — a
    dispatch with `skip_download` off extends it — that is stated rather than averaged away, because
    the two runs then did genuinely different amounts of work.
    """
    have = [(col, ((rec.get("layout") or {}).get("landing") or {})) for col, _e, rec in cols]
    have = [(c, d) for c, d in have if d]
    if not have:
        return
    col, latest = have[-1]
    folders = latest.get("folders") or {}
    print("\n### Input archive\n")
    print("| folder | files | size MB |")
    print("|:--|--:|--:|")
    for name, f in sorted(folders.items(), key=lambda kv: -(kv[1].get("size_mb") or 0)):
        print(f"| `{name}` | {f.get('files', 0):,} | {float(f.get('size_mb') or 0):,.2f} |")
    print(f"| **total** | **{latest.get('files', 0):,}** | "
          f"**{float(latest.get('size_mb') or 0):,.2f}** |")
    differ = sorted({round(float(d.get("size_mb") or 0), 1) for _c, d in have})
    print("\n<sub>The landed AEMO archive `stats.py` listed in `dbt_landing/Files` — **one copy, read "
          "by every engine**, so this is not per column. Every other number on this page is about "
          "what came OUT; this is what went in, and it is what makes a duration or a CU total mean "
          "anything. It moves only when a dispatch runs with `skip_download` off."
          + (f" The runs on this page did not all read the same archive: sizes ranged "
             f"{differ[0]:,.1f}–{differ[-1]:,.1f} MB, so they did different amounts of work."
             if len(differ) > 1 else "") + "</sub>")


LAYOUT_TABLE = os.environ.get("CU_LAYOUT_TABLE", "fct_summary").strip()


def render_layouts(cols, analytics):
    """Every shared table's physical layout, one block each, the mart first.

    The mart leads because it is the table the benchmark's queries land on, and it is the only block
    carrying the CU column — the analytics CU is one number per engine, not per table, so printing it
    in every block would read as one measurement per table. That block's rows are ordered by that CU,
    cheapest first; the rest keep the engine order.
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
        if show_cu:
            # CHEAPEST FIRST, like `chart()` — the CU column is the finding on this block, and
            # "lower is better" only reads as a ranking if the rows are in that order. A 0 means
            # nothing was measured, not that querying was free, so it sorts to the END. The other
            # blocks carry no CU and keep the engine order.
            present.sort(key=lambda cd: (analytics.get(cd[0], 0.0) == 0, analytics.get(cd[0], 0.0)))
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


# ------------------------------------------------------------------------------------ query time

# Pass POSITION, which is what cold/warm/hot mean here — the first visit to a freshly deployed
# semantic model, the second, then the median of the rest. NOT the record's own `tier` field, which
# is the query CATEGORY (`probe`/`composite`/`raw`/`hot_only`) and names four different things.
TIERS = [("cold", "cold_ms"), ("warm", "warm_ms"), ("hot", "hot_median_ms")]


def bench_timings(rec):
    """`{query: {metric: ms}}` for one run. One record measured ONE engine, so there is one semantic
    model in it; a record holding two would merge, last wins."""
    out = {}
    for _model, queries in (((rec.get("benchmark") or {}).get("timings") or {})).items():
        for q, t in (queries or {}).items():
            if isinstance(t, dict):
                out[q] = t
    return out


def bench_totals(per_col, metric):
    """`({column: summed ms}, n queries)` over the query set EVERY column carries at this metric.

    The common set, not each column's own, because a total over different queries is not a
    comparison — and it genuinely differs by metric here, not just by engine: the selectivity-ladder
    queries `sel_1duid`/`sel_1duid_1mo` have no `cold_ms` at all, since the top DUID is only resolved
    after pass 1. Cold is therefore summed over two fewer queries than warm and hot, which is why the
    count is returned and printed rather than left to be inferred from a total that looks small.
    """
    if not per_col:
        return {}, 0
    sets = [{q for q, t in (timings or {}).items() if t.get(metric) is not None}
            for timings in per_col.values()]
    common = set.intersection(*sets) if sets else set()
    if not common:
        return {}, 0
    return ({col: round(sum(float(timings[q][metric]) for q in common), 1)
             for col, timings in per_col.items()}, len(common))


def render_query_time(cols, runs):
    """How long the SAME 25 DAX queries took on each engine, cold, warm and hot.

    The one thing on this page that is not capacity units. Every run record already carries it —
    `benchmark.timings.<model>.<query>` — and `benchmark/render_report.py` renders it per dispatch,
    but a dispatch builds ONE engine, so that report always has a single column and its ranking is
    degenerate. Composed here from every engine's latest run, this is the only place the three tiers
    can be read ACROSS engines at all.

    Deliberately reimplemented rather than imported. `render_report._totals`/`rank` take exactly this
    shape, and `cu/` importing `benchmark/` would end the isolation that makes this directory
    deletable by removing one folder and one workflow file. It is twenty lines of arithmetic.
    """
    per_col = {col: bench_timings(rec) for col, _e, rec in cols}
    per_col = {c: t for c, t in per_col.items() if t}
    if not per_col:
        return
    names = [c for c, _e, _r in cols if c in per_col]
    rows = [(label,) + bench_totals(per_col, metric) for label, metric in TIERS]
    rows = [(label, tot, n) for label, tot, n in rows if n]
    if not rows:
        return

    print("\n### Query time — cold, warm, hot\n")
    # The COLD tier gets the chart. It is the one the layout moves: a first visit transcodes the
    # columns out of parquet into VertiPaq, so it is where V-Order, file count and row-group size
    # show up. Warm and hot converge on what the model holds in memory, which is the same shape
    # whatever wrote it.
    cold = next((r for r in rows if r[0] == "cold"), None)
    if cold:
        spread = bench_spread(runs, cols, per_col, "cold_ms")
        n = max((len(v) for v in spread.values()), default=1)
        chart("Query time — the first visit",
              "milliseconds over the whole suite, lower is better — the tier the table layout moves"
              + (f", mean of {n} runs with the range" if n > 1 else ""),
              chart_rows([c for c in cols if c[0] in per_col], spread, cold[1],
                         {c: engine_caption(r, c) for c, _e, r in cols}))
    print("| ms | " + " | ".join(names) + " |")
    print("|:--|" + "---:|" * len(names))
    for label, totals, n in rows:
        # Only when there is something to win. A lone column is trivially its own fastest, and
        # bolding every cell of a one-column table states a ranking that was never run.
        best = min((totals[c] for c in names if c in totals), default=None) if len(names) > 1 else None
        cells = []
        for c in names:
            v = totals.get(c)
            cells.append("—" if v is None else
                         (f"**{v:,.0f}**" if v == best else f"{v:,.0f}"))
        print(f"| **{label}** — {n} queries | " + " | ".join(cells) + " |")
    spreads = hot_spreads(per_col)
    if spreads:
        print("| `hot spread` | "
              + " | ".join("—" if names[i] not in spreads else f"{spreads[names[i]]:,.1f}%"
                           for i in range(len(names))) + " |")
    print("\n<sub>The same 25 DAX queries against the same semantic model on every engine — one "
          "`.bim`, one storage mode, Direct Lake — so the adapter that wrote the parquet is the only "
          "variable. **cold** is the first visit to a freshly deployed model, when nothing is "
          "resident and every column has to be transcoded out of parquet; **warm** is the second "
          "visit; **hot** is the median of the passes after that. Each tier is summed over the "
          "queries EVERY column carries at that tier, and the count says how many — cold is two "
          "short because the selectivity-ladder queries only exist once the top DUID has been "
          "resolved, which happens after pass 1. `hot spread` is the median per-query spread across "
          "the hot passes: where two columns sit closer together than that, the gap between them "
          "means nothing. Fastest per row in bold.</sub>")


def hot_spreads(per_col):
    """Median per-query `hot_spread_pct` per column, over the queries every column has hot.

    The honesty row. A total is a ranking only if the samples behind it are tight; this is the number
    that says whether they are, and it is measured per query and already in the record.
    """
    sets = [{q for q, t in (timings or {}).items() if t.get("hot_spread_pct") is not None}
            for timings in per_col.values()]
    common = set.intersection(*sets) if sets else set()
    if not common:
        return {}
    out = {}
    for col, timings in per_col.items():
        vals = sorted(float(timings[q]["hot_spread_pct"]) for q in common)
        mid = len(vals) // 2
        out[col] = vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2
    return out


def bench_spread(runs, cols, per_col, metric):
    """`{column: [total ms, …]}` over every run of that column, so the chart carries a range.

    A run only counts if it covers the WHOLE common query set — otherwise its total is smaller for a
    reason that has nothing to do with speed, and it would widen the range downward as if the engine
    had once been fast. A column with no qualifying history simply gets its latest run, which is what
    `chart_rows` falls back to anyway.
    """
    sets = [{q for q, t in (timings or {}).items() if t.get(metric) is not None}
            for timings in per_col.values()]
    common = set.intersection(*sets) if sets else set()
    if not common:
        return {}
    key_by_variant = {(base_engine(c), variant(r)): c for c, _e, r in cols}
    out = {}
    for rec in runs:
        col = key_by_variant.get((rec.get("engine"), variant(rec)))
        if col is None:
            continue
        timings = bench_timings(rec)
        if not all((timings.get(q) or {}).get(metric) is not None for q in common):
            continue
        out.setdefault(col, []).append(round(sum(float(timings[q][metric]) for q in common), 1))
    return out


# ------------------------------------------------------------------------------------- build time

def render_time(cols, runs, ledger):
    """What the work TOOK, beside what it cost — and how hard it drew while it ran.

    Free: `measure.py` reads `Duration (s)` from the same Capacity Metrics row as `CU (s)`, in the
    same query. So this is the same join as the CU table — item GUID, role, compute/storage — read
    off the ledger's `seconds` dict instead of `items`.

    **Seconds here are BILLED OPERATION seconds, not wall clock**, and the difference is not small on
    every engine. A duckrun leg is one long notebook run, so the two nearly agree; spark opens five
    concurrent Livy REPLs under one session and their durations sum to more than the clock ever
    showed. That is why CU stays the page's lead measure and this is second.

    **The rate is the robust number of the two.** `CU ÷ seconds` is the average capacity the work drew
    while it was running, and the overlap that makes spark's seconds hard to read appears in the
    numerator and the denominator alike — both are summed over the same operations — so it cancels.
    A high rate is a wide engine, not a slow one; read it beside the seconds, not instead of them.

    Renders NOTHING when the ledger has no seconds. That is the correct output for a ledger written
    before the duration read, or one whose model does not expose the column: an absent section says
    "not measured", a table of zeros would say "instant".
    """
    if not (ledger.get("seconds") or {}):
        return
    per_col = {}
    for col, _e, rec in cols:
        cells, _missing = run_cu(rec, ledger, "seconds")
        if any(cells.values()):
            per_col[col] = cells
    if not per_col:
        return
    names = [c for c, _e, _r in cols if c in per_col]
    cu_col = {col: run_cu(rec, ledger)[0] for col, _e, rec in cols}

    print("\n### Time — how long the work took, and how hard it drew\n")
    key_by_variant = {(base_engine(c), variant(r)): c for c, _e, r in cols}
    key_of = lambda rec: key_by_variant.get((rec.get("engine"), variant(rec)))
    etl_spread = spread_for(runs, ledger, "etl", key_of, "seconds")
    n = max((len(v) for v in etl_spread.values()), default=1)
    chart("ETL — how long building the tables took",
          "billed operation seconds, lower is better — not wall clock, see below"
          + (f", mean of {n} runs with the range" if n > 1 else ""),
          chart_rows([c for c in cols if c[0] in per_col], etl_spread,
                     {c: class_total(per_col[c], "etl") for c in per_col},
                     {col: engine_caption(rec, col) for col, _e, rec in cols}))
    print("| seconds | " + " | ".join(names) + " |")
    print("|:--|" + "---:|" * len(names))
    for cls in ("etl", "analytics"):
        if not any(per_col[c].get(cls) for c in names):
            continue
        print(f"| **{cls}** | "
              + " | ".join(f"**{class_total(per_col[c], cls):,.1f}**" for c in names) + " |")
        rates = []
        for c in names:
            secs = class_total(per_col[c], cls)
            cu = class_total(cu_col.get(c) or {}, cls)
            rates.append("—" if not secs or not cu else f"{cu / secs:,.1f}")
        print("| `CU per second` | " + " | ".join(rates) + " |")
    print("\n<sub>Read from `Duration (s)` in the same Capacity Metrics row as the CU above, in the "
          "same query — it costs no extra request and no capacity. These are **billed operation "
          "seconds, not wall clock**: a duckrun leg is one long notebook run so the two nearly "
          "agree, while spark's five concurrent Livy REPLs bill against one session and sum to more "
          "than the clock ever showed. **`CU per second` is the sturdier of the two** — it is the "
          "average capacity the work drew while it ran, and the concurrency that makes spark's "
          "seconds hard to read is in the numerator and the denominator alike, so it cancels. A high "
          "rate is a WIDE engine, not a slow one; the seconds beside it say whether that width "
          "finished sooner.</sub>")


def render(cols, runs, ledger):
    """The whole page, on stdout, as the markdown subset `report_html.py` renders."""
    per_col, analytics, unmeasured = {}, {}, {}
    for col, _engine, rec in cols:
        cells, missing = run_cu(rec, ledger)
        per_col[col] = cells
        unmeasured[col] = missing
        analytics[col] = class_total(cells, "analytics")

    newest = max((((r.get("run") or {}).get("started") or "") for _c, _e, r in cols), default="")
    print(f"## Capacity units — the latest run per engine, as of "
          f"{(ledger.get('updated') or newest or '?')[:16].replace('T', ' ')}\n")

    # NUMBERS FIRST. What this page is for is the charts and the table under them; a reader who
    # already knows what a capacity unit is should not have to scroll past a paragraph explaining it
    # and a provenance table to reach them.
    #
    # AND ANALYTICS FIRST OF THE TWO, which is the point of the whole project. Fabric smooths
    # BACKGROUND operations — the build — over 24 hours, so a heavy ETL leg is absorbed. Query CU is
    # INTERACTIVE, smoothed over minutes, and it is what throttles: it is the CU a user waits behind
    # and a capacity admin notices. An engine that builds cheaply and queries expensively has
    # optimised the half that does not hurt.
    # EVERY run maps to its column, not just the one the column was named after: the chart's mean is
    # over an engine's whole history at that configuration, and matching on the chosen record's
    # filename would have collapsed every sample but the newest.
    key_by_variant = {(base_engine(c), variant(r)): c for c, _e, r in cols}
    key_of = lambda rec: key_by_variant.get((rec.get("engine"), variant(rec)))
    captions = {col: engine_caption(rec, col) for col, _e, rec in cols}
    runs_for = {col: len(spread_for(runs, ledger, "analytics", key_of).get(col) or []) or 1
                for col, _e, _r in cols}
    n_runs = max(runs_for.values(), default=1)
    over = f", mean of {n_runs} runs with the range" if n_runs > 1 else ""
    chart("Analytics — what querying the tables cost",
          f"capacity units, lower is better — this is the INTERACTIVE CU that throttles{over}",
          chart_rows(cols, spread_for(runs, ledger, "analytics", key_of), analytics, captions))
    chart("ETL — what building them cost",
          f"capacity units, lower is better — background CU, smoothed over 24h{over}",
          chart_rows(cols, spread_for(runs, ledger, "etl", key_of),
                     {c: class_total(per_col[c], "etl") for c in per_col}, captions))

    print("\nEvery engine's latest run, summed:\n")
    engine_table(per_col, cols)
    render_input(cols)
    render_layouts(cols, analytics)
    # The two non-CU axes, and they come AFTER the layout because that is the order the question is
    # asked in: what did it cost, what shape are the tables, then how long did querying and building
    # them take. Both render nothing when their input is absent — a record with no benchmark, a
    # ledger with no duration column — which is what keeps the page a report of what exists.
    render_query_time(cols, runs)
    render_time(cols, runs, ledger)

    n = len({base_engine(c) for c, _e, _r in cols})
    print("\n### About these numbers\n")
    print("**Capacity units (CU-seconds) are what this page leads with** — Fabric's own billing "
          "measure, read from the Capacity Metrics model. Not milliseconds and not rows: what the "
          f"work COST. One dbt project, {n} engine{'s' if n != 1 else ''}, one landed copy of the "
          "data: this is what each engine charged to build the same tables and to answer the same "
          "queries. Attribution is by Fabric ITEM GUID — each run records what it created and then "
          "deletes it — so no number here is a guess about which engine an item belonged to.\n")
    print("**The CU columns are directly comparable, and the two time sections need reading with "
          "more care.** The engines were handed different compute — a 64-vCore notebook, a Livy "
          "pool, a warehouse — and a capacity unit already prices that in, which is the whole reason "
          "to lead with cost. Duration does not: billed operation seconds SUM across concurrent "
          "operations, so spark's five Livy REPLs total more than the clock they ran on, and query "
          "milliseconds are one sample of a shared capacity rather than a bill. They are on the page "
          "because they answer a question CU cannot — how long a person waits, and how hard the "
          "engine drew while they did — and each section says where its own number bends.\n")
    print("**Analytics is the half that matters**, and it leads for that reason. Fabric smooths "
          "BACKGROUND operations — everything the build does — over 24 hours, so a heavy ETL leg is "
          "absorbed and nobody waits for it. Query CU is INTERACTIVE, smoothed over minutes, and it "
          "is what THROTTLES: the CU a user sits behind and a capacity admin asks about. An engine "
          "that builds cheaply and queries expensively has optimised the half that does not hurt.\n")

    render_sources(cols, ledger, unmeasured)

    reads = len(ledger.get("reads") or [])
    print("\n" + " · ".join([f"[source]({SERVER}/{REPO})",
                             f"`{RUNS_DIR}/` — {len(runs)} run(s), {len(cols)} on this page",
                             f"`{LEDGER}` — {len(ledger['items'])} item GUID(s) over "
                             f"{reads} read(s)"]))


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
