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
  and so has no analytics CU at all: the columns come from the record, not from a configured list.
  An engine a record never measured has no zero to print — it simply is not in that document.
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
# Which record to render. Blank = the newest. Otherwise a substring of the filename, so a run id
# ("30691610866") or a date ("2026-08-01") both work — the names are `<timestamp>-<run id>.json`.
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


def as_doc(record):
    """The record's layout half, in the shape `render_tables`/`render_layout`/`render_hardware`
    expect from `stats.py`'s artifact. They were written against that document, and the record is a
    subset of it — so this is a rename, not a conversion.

    `writer` comes from `STACK`, not from the record: it is a fact of this repo's `profiles.yml`,
    which means an old record still labels correctly and a record never has to carry it.
    """
    engines = engines_in(record)
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
            "engines": {e: {"writer": (cu.STACK.get(e) or ("", "", "—"))[2]} for e in engines},
            "tables": tables,
            "stats": {e: layout.get(e) or {} for e in engines if layout.get(e)}}


def history_cols(records, current, engines):
    """`[(label, build id, {(class, engine): cu})]` oldest first, ending with the rendered record.

    Same collapse rule as the measurement's own page and for the same reason: a `since` floor is a
    floor, so several records sharing one are re-reads of one accumulating window, and printed as
    separate columns they read as dispatches getting steadily more expensive. The latest read of a
    floor wins; the record being RENDERED wins its own floor outright, whether or not it is the
    latest one on it — the page must agree with the table it is showing.
    """
    by_floor = {}
    for rec in records:
        if rec["_file"] == current["_file"] or rec.get("since") == current.get("since"):
            continue
        per = {}
        for cls, engs in (rec.get("cu") or {}).items():
            for eng, ops in (engs or {}).items():
                per[(cls, eng)] = per.get((cls, eng), 0.0) + sum(ops.values())
        by_floor[rec.get("since")] = (rec, per)
    cols = [((floor or "everything retained").replace("T", " ")[:16],
             (rec.get("runs") or {}).get("build"), per)
            for floor, (rec, per) in sorted(by_floor.items(),
                                            key=lambda kv: kv[1][0].get("written") or "")]
    if cu.HISTORY_COLS > 0 and len(cols) > cu.HISTORY_COLS - 1:
        dropped = len(cols) - (cu.HISTORY_COLS - 1)
        cu.log(f"  history: {dropped} older generation(s) not shown (CU_HISTORY_COLS="
               f"{cu.HISTORY_COLS})")
        cols = cols[-(cu.HISTORY_COLS - 1):]
    per_now = {}
    for cls, engs in (current.get("cu") or {}).items():
        for eng, ops in (engs or {}).items():
            per_now[(cls, eng)] = per_now.get((cls, eng), 0.0) + sum(ops.values())
    cols.append(((current.get("since") or "everything retained").replace("T", " ")[:16]
                 + " · **this record**", (current.get("runs") or {}).get("build"), per_now))
    return cols


def render(record, records, engines=None):
    """The whole page, on stdout, as the markdown subset `report_html.py` renders."""
    engines = engines_in(record) if engines is None else engines
    runs = record.get("runs") or {}
    floor = record.get("since") or "everything retained"
    print(f"## Capacity units — {floor.replace('T', ' ')}, measured {record.get('written', '?')}\n")

    # Counted WITHOUT `landing`: it is a stage every engine reads from, not a fifth competitor, and
    # a headline saying "5 engines" over a four-bar chart is the exact confusion the column
    # footnote spends a paragraph undoing.
    n = len([e for e in engines if e != "landing"])
    print("**Every number on this page is capacity units (CU-seconds)** — Fabric's own billing "
          "measure, read from the Capacity Metrics model's `CU (s)` column. Not milliseconds and "
          "not rows: what the work COST. One dbt project, "
          f"{n} engine{'s' if n != 1 else ''}, one landed copy of the data: "
          "this is what each engine charged to build the same tables and to answer the same "
          "queries.\n")

    links = [f"[source]({cu.SERVER}/{cu.REPO})"]
    if runs.get("build"):
        links.append(f"[dbt build {runs['build']}]({cu.run_url(runs['build'])})")
    if runs.get("measure"):
        links.append(f"[measured by run {runs['measure']}]({cu.run_url(runs['measure'])})")
    links.append(f"`{cu.HISTORY_DIR}/{record['_file']}`")
    print(" · ".join(links) + "\n")

    cells, meta = cells_meta(record)
    per_cls = {}
    for (k, _op), value in cells.items():
        info = meta[k]
        if info["engine"] in (None, "landing"):
            continue
        per_cls[(info["cls"], info["engine"])] = per_cls.get(
            (info["cls"], info["engine"]), 0.0) + value
    doc = as_doc(record)
    cfg = doc["config"]
    bars = [e for e in engines if e != "landing"]
    cu._chart("ETL — what building the tables cost", "capacity units, lower is better",
              [[e, round(per_cls.get(("etl", e), 0.0), 1), cu.engine_caption(cfg, e)]
               for e in bars])
    cu._chart("Analytics — what querying them cost", "capacity units, lower is better",
              [[e, round(per_cls.get(("analytics", e), 0.0), 1), cu.engine_caption(cfg, e)]
               for e in bars])

    print("Everything this record measured, summed:\n")
    cu._engine_table(cells, meta, engines=engines)

    if doc["stats"]:
        analytics = {}
        for (k, _op), value in cells.items():
            info = meta[k]
            if info["cls"] == "analytics" and info["engine"]:
                analytics[info["engine"]] = analytics.get(info["engine"], 0.0) + value
        cu.render_tables(doc)
        cu.render_layout(doc, analytics)
    cu.render_history(history_cols(records, record, engines), engines=engines)
    cu.render_hardware(doc, engines=engines)


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
    rec = pick(records)
    if rec is None:
        render_empty(HISTORY_DIR)
        return 0
    cu.log(f"  rendering {rec['_file']} of {len(records)} record(s) in {HISTORY_DIR}/")
    render(rec, records)
    return 0


if __name__ == "__main__":
    sys.exit(main())
