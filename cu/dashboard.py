"""The dashboard. Reads `history/`, writes a page. Nothing else. `python cu/dashboard.py`.

**The contract is the JSON.** A measurement workflow does the expensive, credentialed, capacity
spending work and files ONE record in `history/`; this reads those records and renders. There is no
other channel between them — no artifact, no env var, no shared module state, no `needs:`. So:

- The dashboard needs **no token, no network and no third-party package** — the standard library and
  `report_html.py`. It cannot fail for a reason that has anything to do with Fabric.
- It is **dispatched by a human**, never chained off a measurement. Publishing is a decision, and a
  page that republishes itself whenever a measurement lands will eventually publish one nobody
  looked at. (`capacity_cu.py` still prints its own markdown to the job summary — that is the
  measurement's log, not the dashboard.)
- A record **need not cover four engines**. One engine, two, a dispatch that skipped the benchmark
  and so has no analytics CU at all: the columns come from the records, not from a configured list.
  An engine nothing ever measured has no zero to print.
- The page is therefore **composed from every record — each engine's latest measurement, once per
  CONFIG** (see columns_for). Rendering only the newest record was the old behaviour and it failed
  the moment dispatches became partial: `engines=spark` filed a record naming one engine, the page
  went down to one column, and a comparison page with nothing to compare is the failure this whole
  directory exists to avoid. `CU_RECORD` pins one generation when reproducing an old page.
- Re-rendering is **free and repeatable**. Every published page can be rebuilt years later from the
  file it came from, which is not true of anything that depends on a 90-day artifact or the metrics
  app's ~14-day retention.

It imports `capacity_cu` for the renderers they genuinely share — the charts, the engine table, the
layout tables, the hardware table — rather than growing a second copy that drifts. That import must
stay side-effect-free at module scope; `requests` is optional there for exactly this reason.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import capacity_cu as cu                                          # noqa: E402


HISTORY_DIR = os.environ.get("CU_HISTORY_DIR", "history").strip()
# Which record to render ALONE. Blank = compose the page from every record (see columns_for). A
# substring of the filename, so a run id ("30691610866") or a date ("2026-08-01") both work — the
# names are `<timestamp>-<run id>.json`. Setting it is how you reproduce the page for one generation.
PICK = os.environ.get("CU_RECORD", "").strip()


def load_records(directory=None):
    """Every readable record, oldest first. A directory that is not there is an empty list, not an
    exception: a checkout without `history/` is a normal thing to be, and the caller says so on the
    page rather than crashing on it."""
    directory = HISTORY_DIR if directory is None else directory
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
        except Exception as ex:
            cu.log(f"  skipping {n}: unreadable ({type(ex).__name__}: {ex})")
            continue
        if rec.get("schema") not in cu.SCHEMAS:
            cu.log(f"  skipping {n}: schema {rec.get('schema')!r} is not one of {cu.SCHEMAS}")
            continue
        rec["_file"] = n
        out.append(rec)
    out.sort(key=lambda r: (r.get("written") or "", r["_file"]))
    return out


def pick(records, which=None):
    """The record to render: the newest, or the one whose FILENAME contains `which`.

    Matched on the filename rather than on a field because the filename is what a reader has in
    front of them — it carries both the timestamp and the run id. An ambiguous match takes the
    newest and says so, because refusing to render is worse than rendering the wrong one of two
    candidates while naming both."""
    which = (PICK if which is None else which).strip()
    if not records:
        return None
    if not which:
        return records[-1]
    hits = [r for r in records if which in r["_file"]]
    if not hits:
        cu.log(f"  no record matches {which!r}; rendering the newest instead")
        return records[-1]
    if len(hits) > 1:
        cu.log(f"  {len(hits)} records match {which!r}: {', '.join(h['_file'] for h in hits)}"
               f" — rendering the newest")
    return hits[-1]


def engines_in(record):
    """The engines this record actually measured, in the repo's canonical order with anything
    unrecognised appended. NOT the configured engine list: a record is a closed document, and an
    engine it never saw has no zero to print."""
    seen = {e for engines in (record.get("cu") or {}).values() for e in engines}
    seen |= set(record.get("layout") or {})
    seen.discard("shared")
    ordered = [e for e in cu.ENGINES if e in seen]
    return ordered + sorted(seen - set(ordered))


def variant(record, engine):
    """The CONFIG this record ran `engine` under, as a hashable signature. `()` when the record
    recorded none — which is dwh always (Fabric Warehouse exposes no per-run knob) and any engine a
    schema-1 record measured."""
    c = (record.get("config") or {}).get(engine) or {}
    return tuple(sorted((k, str(v)) for k, v in c.items() if v is not None))


def variant_tag(sig):
    """The short label that separates one config from another in a column header. Compact on
    purpose: it goes in a table head beside seven other columns, and the full reading is in the
    hardware table at the foot of the page."""
    d = dict(sig)
    bits = []
    if d.get("vcores"):
        bits.append(f"{d['vcores']}c")
    if d.get("resource_profile"):
        bits.append(d["resource_profile"])
    nee = d.get("native_execution_engine")
    if nee is not None:
        bits.append("NEE" if nee.lower() == "true" else "noNEE")
    # `+`, never the column separator — `base_engine` splits on that and a tag containing one would
    # make `spark·readHeavyForPBI+NEE` unparseable back to `spark`.
    return "+".join(bits) or "unrecorded"


def columns_for(records):
    """`[(column id, engine, record)]` — each engine's LATEST measurement, once per configuration.

    This is what the page is for. A record is one dispatch, and dispatches are increasingly partial:
    `engines=spark` builds one leg, and a spark leg run under `readHeavyForPBI` answers a different
    question from one run under `writeHeavy`. Rendering only the newest record therefore threw away
    every engine that dispatch did not run — the page went down to one column and stopped being a
    comparison, which is the entire reason it exists.

    So the column key is (engine, config), not engine: the same engine under two resource profiles is
    two findings and gets two columns, and an engine nobody has rebuilt keeps showing its last real
    measurement rather than vanishing. Later records win their key outright — one config, one number,
    the most recent one.

    The cost is real and is why `render_sources` exists: columns no longer come from one dispatch, so
    they can be days apart and sit on different `since` floors. That is stated per column rather than
    smoothed over. Pin `CU_RECORD` to get the old one-generation page back.
    """
    latest = {}
    for rec in records:                                  # oldest first, so later records win
        for e in engines_in(rec):
            # `landing` is not one of the things being compared — it is the shared archive every leg
            # reads, and on a page whose columns are now "engine under a config" it is the one column
            # that answers a different question. The measurement's own report still carries it.
            if e == "landing":
                continue
            latest[(e, variant(rec, e))] = rec
    per_engine = {}
    for e, _sig in latest:
        per_engine[e] = per_engine.get(e, 0) + 1
    cols = []
    for (e, sig), rec in latest.items():
        col = e if per_engine[e] < 2 else f"{e}{cu.COL_SEP}{variant_tag(sig)}"
        cols.append((col, e, rec))
    order = {e: i for i, e in enumerate(cu.ENGINES)}
    cols.sort(key=lambda c: (order.get(c[1], len(order)), c[1], c[0]))
    return cols


def compose(cols, records):
    """One synthetic record built from `cols`, in the shape every renderer already takes.

    Not a measurement — a VIEW. Its `cu`, `layout` and `config` are keyed by column id rather than by
    engine, which is exactly what the column id is for. `runs.build` names every dispatch that
    contributed, because the footnotes downstream quote it and naming one of three would be a
    statement this page cannot support.
    """
    newest = records[-1]
    out = {"schema": 2, "unit": newest.get("unit"), "_file": newest["_file"],
           "written": newest.get("written"), "since": newest.get("since"),
           "cu": {}, "layout": {}, "config": {}, "tables": [], "layout_written": None}
    builds, shas, written = [], [], []
    for col, engine, rec in cols:
        for cls, engines in (rec.get("cu") or {}).items():
            ops = (engines or {}).get(engine)
            if ops:
                out["cu"].setdefault(cls, {})[col] = dict(ops)
        if (rec.get("layout") or {}).get(engine):
            out["layout"][col] = rec["layout"][engine]
        if (rec.get("config") or {}).get(engine):
            out["config"][col] = rec["config"][engine]
        for lst, value in ((builds, (rec.get("runs") or {}).get("build")),
                           (shas, (rec.get("runs") or {}).get("build_sha")),
                           (written, rec.get("layout_written") or rec.get("written"))):
            if value and value not in lst:
                lst.append(value)
        if not out["tables"] and rec.get("tables"):
            out["tables"] = list(rec["tables"])
    # `shared` and `landing` are deliberately absent. Neither is a column this page compares: shared
    # is CU nothing could attribute, so it has no (engine, config) key to be the latest FOR, and
    # landing is the archive every leg reads. Both are still measured, still recorded, and still in
    # the measurement's own report — this page is the comparison, not the ledger.
    out["runs"] = {"build": ", ".join(builds) if builds else None,
                   "build_sha": shas[0] if len(shas) == 1 else None,
                   "measure": (newest.get("runs") or {}).get("measure")}
    # The oldest contributing layout, not the newest: the provenance line under the layout table is a
    # caveat, and a caveat is only useful at its worst case.
    out["layout_written"] = min(written) if written else None
    return out


def render_sources(cols):
    """Which dispatch each column came from. The one thing the composed page owes the reader that a
    single-record page did not: its columns are no longer one measurement, so a column can be days
    older than the one beside it, and the CU in it is cumulative from ITS OWN floor. Printed as a
    table rather than a footnote because it is a lookup — you check it when a number surprises you.
    """
    print("\n<sub>Each column is that engine's latest measurement, and they are different "
          "dispatches:</sub>\n")
    print("| column | dbt build | measured | CU cumulative since |")
    print("|:--|:--|:--|:--|")
    for col, _engine, rec in cols:
        runs = rec.get("runs") or {}
        build = runs.get("build")
        link = f"[{build}]({cu.run_url(build)})" if build else "—"
        floor = (rec.get("since") or "everything retained").replace("T", " ")[:16]
        print(f"| {col} | {link} | {(rec.get('written') or '?')[:16].replace('T', ' ')} | {floor} |")
    print("\n<sub>A column is only comparable to another when both were measured over a window of "
          "the same shape — a `since` floor is a floor, so the CU is cumulative from it. Two columns "
          "from one dispatch compare cleanly; two from different dispatches compare the engines as "
          "they were then. `CU_RECORD=<run id>` renders one generation on its own.</sub>")


def cells_meta(record):
    """`(cells, meta)` in the shape `capacity_cu`'s table renderers already take.

    The "item" here is an `<engine>|<class>` pair rather than a Fabric item: the record has already
    folded items into engines, and one engine carries both classes. Nothing downstream reads the key
    itself — it is only a lookup handle — so the pair is free, and it keeps `_engine_table` usable
    without a second implementation.
    """
    cells, meta = {}, {}
    for cls, engines in (record.get("cu") or {}).items():
        for eng, ops in (engines or {}).items():
            key = f"{eng}|{cls}"
            meta[key] = {"label": eng, "cls": cls,
                         "engine": None if eng == "shared" else eng}
            for op, value in (ops or {}).items():
                cells[(key, op)] = float(value)
    return cells, meta


def as_doc(record, engines=None):
    """The record's layout half, in the shape `render_tables`/`render_layout`/`render_hardware`
    expect from `stats.py`'s artifact. They were written against that document, and the record is a
    subset of it — so this is a rename, not a conversion.

    `writer` comes from `STACK`, not from the record: it is a fact of this repo's `profiles.yml`,
    which means an old record still labels correctly and a record never has to carry it.
    """
    # The caller's column order when it has one: a composite's ids (`spark·writeHeavy`) are not
    # engine names, so engines_in() sorts them to the end and every layout table below comes out in a
    # different order from the CU table above it.
    engines = engines_in(record) if engines is None else list(engines)
    layout = record.get("layout") or {}
    tables = [t for t in (record.get("tables") or []) if any(t in (layout.get(e) or {})
                                                             for e in engines)]
    if not tables:                       # schema 1 carried no order; alphabetical is what is left
        tables = sorted({t for e in engines for t in (layout.get(e) or {})})
    return {"run": {"id": (record.get("runs") or {}).get("build"),
                    "sha": (record.get("runs") or {}).get("build_sha"),
                    # When the LAYOUT was read, which is the dbt build's clock, not this record's.
                    # A schema-1 record did not carry it; the record's own timestamp is the closest
                    # honest answer and is never later than the truth by more than the lag.
                    "written": record.get("layout_written") or record.get("written")},
            "config": record.get("config") or {},
            "engines": {e: {"writer": (cu.STACK.get(cu.base_engine(e)) or ("", "", "—"))[2]}
                        for e in engines},
            "tables": tables,
            "stats": {e: layout.get(e) or {} for e in engines if layout.get(e)}}


def render(record, records, engines=None, sources=None):
    """The whole page, on stdout, as the markdown subset `report_html.py` renders.

    `sources` present means the record is a COMPOSITE (see compose): its columns are each engine's
    latest measurement and come from different dispatches, which changes the heading, adds the
    provenance table, and makes the generations table below key on engines rather than on columns.
    """
    engines = engines_in(record) if engines is None else engines
    runs = record.get("runs") or {}
    floor = record.get("since") or "everything retained"
    if sources:
        print(f"## Capacity units — the latest measurement per engine, "
              f"as of {(record.get('written') or '?')[:16].replace('T', ' ')}\n")
    else:
        print(f"## Capacity units — {floor.replace('T', ' ')}, "
              f"measured {record.get('written', '?')}\n")

    # Counted WITHOUT `landing`: it is a stage every engine reads from, not a fifth competitor, and
    # a headline saying "5 engines" over a four-bar chart is the exact confusion the column
    # footnote spends a paragraph undoing. Counted by ENGINE, not by column: spark under two
    # resource profiles is two columns and one engine, and "5 engines" over four names would be the
    # same confusion wearing a different hat.
    n = len({cu.base_engine(e) for e in engines if cu.base_engine(e) != "landing"})
    print("**Every number on this page is capacity units (CU-seconds)** — Fabric's own billing "
          "measure, read from the Capacity Metrics model's `CU (s)` column. Not milliseconds and "
          "not rows: what the work COST. One dbt project, "
          f"{n} engine{'s' if n != 1 else ''}, one landed copy of the data: "
          "this is what each engine charged to build the same tables and to answer the same "
          "queries.\n")

    links = [f"[source]({cu.SERVER}/{cu.REPO})"]
    if not sources:
        if runs.get("build"):
            links.append(f"[dbt build {runs['build']}]({cu.run_url(runs['build'])})")
        if runs.get("measure"):
            links.append(f"[measured by run {runs['measure']}]({cu.run_url(runs['measure'])})")
        links.append(f"`{cu.HISTORY_DIR}/{record['_file']}`")
    else:
        links.append(f"`{cu.HISTORY_DIR}/` — {len(records)} record(s), "
                     f"{len({r['_file'] for _c, _e, r in sources})} on this page")
    print(" · ".join(links) + "\n")
    if sources:
        render_sources(sources)

    cells, meta = cells_meta(record)
    per_cls = {}
    for (k, _op), value in cells.items():
        info = meta[k]
        if info["engine"] in (None, "landing"):
            continue
        per_cls[(info["cls"], info["engine"])] = per_cls.get(
            (info["cls"], info["engine"]), 0.0) + value
    doc = as_doc(record, engines)
    cfg = doc["config"]
    bars = [e for e in engines if e != "landing"]
    cu._chart("ETL — what building the tables cost", "capacity units, lower is better",
              [[e, round(per_cls.get(("etl", e), 0.0), 1), cu.engine_caption(cfg, e)]
               for e in bars])
    cu._chart("Analytics — what querying them cost", "capacity units, lower is better",
              [[e, round(per_cls.get(("analytics", e), 0.0), 1), cu.engine_caption(cfg, e)]
               for e in bars])

    print(("Every engine's latest measurement, summed:\n" if sources
           else "Everything this record measured, summed:\n"))
    cu._engine_table(cells, meta, engines=engines)

    if doc["stats"]:
        analytics = {}
        for (k, _op), value in cells.items():
            info = meta[k]
            if info["cls"] == "analytics" and info["engine"]:
                analytics[info["engine"]] = analytics.get(info["engine"], 0.0) + value
        render_layouts(doc, analytics)


def render_layouts(doc, analytics):
    """The per-engine layout of EVERY table, one block each, the mart first.

    This replaced a two-part section: a files·MB summary of all eight tables, then the mart broken
    out in detail. The summary was the half that got dropped — it said less per row than the detail
    it sat above, and a reader comparing engines wants rows, row groups and V-Order for whichever
    table they are looking at, not for the one the page chose. So every table now gets the detailed
    reading, and nothing is summarised away.

    The mart leads because it is the table the benchmark's queries land on, and it is the only block
    carrying the CU column — the analytics CU is one number per engine, not per table, so printing it
    in all eight blocks would read as eight measurements of eight different things.
    """
    tables = list(doc.get("tables") or [])
    mart = cu.LAYOUT_TABLE if cu.LAYOUT_TABLE in tables else (tables[0] if tables else None)
    if not mart:
        return
    order = [mart] + [t for t in tables if t != mart]
    schema = {}
    for per_table in (doc.get("stats") or {}).values():
        for t, d in (per_table or {}).items():
            schema.setdefault(t, (d or {}).get("schema"))
    print("\n### The layout that CU was spent on\n")
    for t in order:
        head = (f"`{t}` in detail — the mart the queries land on" if t == mart
                else f"`{(schema.get(t) + '.') if schema.get(t) else ''}{t}`")
        cu.render_layout(doc, analytics if t == mart else {}, table=t, heading=head, note=False)
    run = doc.get("run") or {}
    print(f"\n<sub>Every shared table the project writes, in pipeline order, as `stats.py` read the "
          f"Delta log in the **layout** job of `dbt` run `{run.get('id') or '?'}` — **a different run "
          f"from the CU above**, so read it as \"the layout as of that dispatch\". Sizes are "
          f"CUMULATIVE — what the tables hold today — while the CU is what one window's dispatches "
          f"spent, which for an incremental build is not the cost of the rows here. The CU column is "
          f"on the mart alone: it is the engine's ANALYTICS total, one number per engine, and the "
          f"queries read all of these.</sub>")


def render_empty(directory):
    """No record, so say what the contract is rather than printing an empty page. This is the only
    failure mode the dashboard has, and it is always the same one: nothing has measured yet."""
    print("## Capacity units\n")
    print(f"**No records in `{directory}/`.** This page renders what a measurement filed; it "
          "reads nothing else and spends no capacity, so an empty directory means no measurement "
          "has been committed yet — not that the capacity was idle.\n")
    print(f"Dispatch **Build, benchmark, measure** ([{cu.REPO}]({cu.SERVER}/{cu.REPO}/actions)). "
          "It writes one record per generation and commits it; this workflow then renders it on "
          "demand.")


def main(argv=None):
    records = load_records()
    if not records:
        render_empty(HISTORY_DIR)
        return 0
    if PICK:
        rec = pick(records)
        cu.log(f"  rendering {rec['_file']} alone (CU_RECORD={PICK!r}) of {len(records)} record(s)")
        render(rec, records)
        return 0
    cols = columns_for(records)
    cu.log(f"  composing {len(cols)} column(s) from {len(records)} record(s) in {HISTORY_DIR}/: "
           + ", ".join(f"{c} <- {r['_file']}" for c, _e, r in cols))
    render(compose(cols, records), records, engines=[c for c, _e, _r in cols], sources=cols)
    return 0


if __name__ == "__main__":
    sys.exit(main())
