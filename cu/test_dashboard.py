"""Offline tests for the page. No token, no network, no Fabric — which is the property being kept.

What matters here is the JOIN. Attribution used to be substring matching on display names with a
`shared` bucket for anything ambiguous; it is now a dictionary lookup on the item GUID, and the class
comes from the role the run itself recorded. If that join is wrong the page prints a confident number
under the wrong engine, which is the failure this directory exists to avoid.

    python -m pytest cu/test_dashboard.py -q
"""
import io
import json
import os
import sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dashboard as d  # noqa: E402


def ago(hours):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def gone(role, name):
    """An item the teardown deleted — the normal case, and the one that is not `drifting`."""
    return {"role": role, "name": name, "deleted": ago(1)}


def rec(file, engine, items, config=None, stats=None, tables=None, landing=None,
        full_load=True, finished_hours_ago=48):
    return {"_file": file, "schema": 1, "engine": engine, "full_load": full_load,
            "run": {"id": file.split("-")[-1].split(".")[0],
                    "started": ago(finished_hours_ago + 1), "finished": ago(finished_hours_ago)},
            "items": items,
            "layout": {"config": config or {}, "stats": stats or {}, "tables": tables or [],
                       **({"landing": landing} if landing else {})}}


def ledger(items):
    """`{guid: {operation: CU}}`. A bare number is taken as one compute operation, for brevity."""
    return {"items": {g: (v if isinstance(v, dict) else {"Warehouse Query": v})
                      for g, v in items.items()},
            "reads": [{"at": "2026-08-02T20:00:00+00:00"}],
            "updated": "2026-08-02T20:00:00+00:00"}


def test_the_role_decides_the_class_not_the_fabric_item_kind():
    """A semantic model is only ever queried; everything else is work done to BUILD the tables. This
    replaced classification from the metrics app's item-kind snapshot, which routinely had not
    catalogued a minutes-old item at all."""
    r = rec("r-1.json", "spark", {
        "OUT": {"role": "output", "name": "dbt_spark"},
        "NB": {"role": "compute", "name": "dbt-spark-ab12"},
        "SEM": {"role": "semantic_model", "name": "aemo_spark"},
    })
    cells, _missing = d.run_cu(r, ledger({
        "OUT": {"OneLake Write via Redirect": 10.0},
        "NB": {"Jupyter Notebook Scheduled Run": 900.0},
        "SEM": {"XMLA Read Operation": 40.0}}))
    assert cells == {"etl": {"storage": 10.0, "compute": 900.0},
                     "analytics": {"compute": 40.0}}
    assert d.class_total(cells, "etl") == 910.0
    assert d.class_total(cells, "analytics") == 40.0


def test_landing_cu_is_not_on_the_page_at_all():
    """The page compares ENGINES. `dbt_landing` is the ingestion staging area — no run deletes it and
    every run reads it, so its CU is one cumulative figure belonging to no engine. It is skipped
    outright, not given a row: the same number repeated under every column read as "each of them
    spent this". The archive's SIZE still appears — input volume is a different question from what
    ingesting it cost."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"},
                                  "LAND": {"role": "landing", "name": "dbt_landing"}})
    cells, missing = d.run_cu(r, ledger({"OUT": 10.0, "LAND": 507.0}))
    assert d.class_total(cells, "etl") == 10.0, "landing must not be added to the engine's own CU"
    assert missing == [], "landing is not an item whose CU could be missing"


def test_the_dbt_folder_costs_nothing_and_is_skipped():
    r = rec("r-1.json", "dwh", {"F": {"role": "folder", "name": "dbt"},
                                "OUT": {"role": "output", "name": "dbt_dwh"}})
    cells, missing = d.run_cu(r, ledger({"OUT": {"OneLake Read via Redirect": 1.0}}))
    assert cells == {"etl": {"storage": 1.0}}
    assert missing == [], "a folder is not an item whose CU could be missing"


def test_an_item_the_ledger_has_never_seen_is_unmeasured_not_zero():
    """'not measured yet' and 'cost nothing' are different claims, and the sources table has to be
    able to say which."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"},
                                  "SEM": {"role": "semantic_model", "name": "aemo_spark"}})
    cells, missing = d.run_cu(r, ledger({"OUT": {"OneLake Read via Redirect": 5.0}}))
    assert cells == {"etl": {"storage": 5.0}}
    assert missing == ["semantic_model/aemo_spark"]


def test_still_accruing_is_derived_from_the_clock_not_stored():
    """An hour's CU keeps growing for ~70 minutes after the fact. That is a property of the clock,
    not a fact worth writing into a file and keeping in step."""
    assert d.still_accruing(rec("a.json", "dwh", {}, finished_hours_ago=0.5))
    assert not d.still_accruing(rec("a.json", "dwh", {}, finished_hours_ago=48))
    assert not d.still_accruing({"run": {}}), "no finished stamp, no claim"


def test_columns_are_the_latest_run_per_engine_and_config():
    """One dispatch builds ONE engine, so rendering the newest record alone gives a comparison page
    with a single column. And spark under readHeavyForPBI answers a different question from spark
    under writeHeavy: one number cannot stand for both."""
    runs = [
        rec("a-1.json", "spark", {}, config={"spark": {"resource_profile": "writeHeavy"}},
            finished_hours_ago=72),
        rec("b-2.json", "spark", {}, config={"spark": {"resource_profile": "writeHeavy"}},
            finished_hours_ago=48),
        rec("c-3.json", "spark", {}, config={"spark": {"resource_profile": "readHeavyForPBI"}},
            finished_hours_ago=24),
        rec("d-4.json", "dwh", {}, finished_hours_ago=12),
    ]
    cols = d.columns_for(runs)
    assert [c for c, _e, _r in cols] == ["spark·readHeavyForPBI", "spark·writeHeavy", "dwh"]
    by_col = {c: r["_file"] for c, _e, r in cols}
    assert by_col["spark·writeHeavy"] == "b-2.json", "the LATER run of a config wins its column"


def test_one_config_per_engine_gets_a_bare_column_name():
    assert [c for c, _e, _r in d.columns_for([rec("a-1.json", "dwh", {})])] == ["dwh"]


def test_a_variant_tag_never_contains_the_column_separator():
    """base_engine splits on COL_SEP; a tag containing one would make the column id unparseable back
    to its engine, and STACK lookups would silently miss."""
    tag = d.variant_tag((("native_execution_engine", "true"),
                         ("resource_profile", "readHeavyForPBI"), ("vcores", "64")))
    assert d.COL_SEP not in tag
    assert d.base_engine(f"spark{d.COL_SEP}{tag}") == "spark"


def _render(runs, led):
    buf = io.StringIO()
    with redirect_stdout(buf):
        d.render(d.columns_for(runs), runs, led)
    return buf.getvalue()


def test_the_page_renders_end_to_end_with_charts_and_a_layout():
    runs = [rec("a-1.json", "duckrun",
                {"OUT": {"role": "output", "name": "dbt_delta"},
                 "NB": {"role": "compute", "name": "dbt-duckrun-baf95ac5"},
                 "SEM": {"role": "semantic_model", "name": "aemo_duckrun"}},
                config={"duckrun": {"vcores": "64"}},
                stats={"duckrun": {"fct_summary": {"total_rows": 143980961, "num_files": 4,
                                                   "num_row_groups": 79, "avg_row_group": 1822544,
                                                   "size_mb": 998.9, "vorder": False,
                                                   "schema": "mart"}}},
                tables=["fct_summary"], landing={"files": 8167, "size_mb": 12345.6})]
    out = _render(runs, ledger({"OUT": {"OneLake Write via Redirect": 1509.0},
                                "NB": {"Jupyter Notebook Scheduled Run": 29571.0},
                                "SEM": {"XMLA Read Operation": 2041.0}}))
    assert out.count("<!--chart:") == 2
    assert "| **etl** |" in out and "| **analytics** |" in out
    # Item-major: the notebook and the lakehouse are separate rows, which is where a DuckDB leg's
    # cost actually goes.
    assert "`compute`" in out and "29,571.0" in out
    assert "`storage`" in out and "1,509.0" in out
    assert "fct_summary" in out and "delta-rs" in out
    assert "8,167" in out and "12,345.60" in out, "the input archive should be on the page"
    spec = json.loads(out.split("<!--chart:")[1].split("-->")[0])
    assert spec["rows"][0][1] == 31080.0, "etl chart is the sum of the etl items"
    assert spec["rows"][0][2] == "dbt-duckrun · 64 vCores"


def test_a_column_with_no_operations_of_a_kind_prints_a_dash_not_a_zero():
    """A dash says "nothing of that kind was billed here"; 0.0 would say "it was billed and cost
    nothing". Real case: an iceberg lakehouse bills 40,832 CU and every operation of it is OneLake —
    its compute is the notebook, a different item entirely."""
    runs = [rec("a-1.json", "duckrun", {"NB": {"role": "compute", "name": "dbt-duckrun-ab12"},
                                        "OUT": {"role": "output", "name": "dbt_delta"}}),
            rec("b-2.json", "iceberg", {"OUT2": {"role": "output", "name": "dbt_iceberg"}})]
    out = _render(runs, ledger({"NB": {"Jupyter Notebook Scheduled Run": 29571.0},
                                "OUT": {"OneLake Write via Redirect": 1509.0},
                                "OUT2": {"OneLake Iterative Read via Proxy": 40831.8}}))
    rows = [ln for ln in out.splitlines() if ln.startswith("| `compute`")]
    assert rows and "—" in rows[0], "iceberg's lakehouse bills no compute operation at all"


def test_the_page_says_when_a_column_can_still_rise():
    fresh = [rec("a-1.json", "dwh", {"OUT": gone("output", "dbt_dwh")}, finished_hours_ago=0.5)]
    assert "may still rise" in _render(fresh, ledger({"OUT": 5.0}))
    old = [rec("a-1.json", "dwh", {"OUT": gone("output", "dbt_dwh")}, finished_hours_ago=48)]
    assert "may still rise" not in _render(old, ledger({"OUT": 5.0}))


def test_no_records_explains_the_contract_rather_than_printing_an_empty_page():
    buf = io.StringIO()
    with redirect_stdout(buf):
        d.render_empty("history/runs", "history/cu.json")
    out = buf.getvalue()
    assert "No run records" in out and "Benchmark" in out
    assert "not that the capacity was idle" in out


def test_a_missing_directory_is_an_empty_list_not_an_exception(tmp_path):
    assert d.load_runs(str(tmp_path / "nope")) == []
    assert d.load_ledger(str(tmp_path / "nope.json"))["items"] == {}


def test_the_rendered_page_mentions_no_landing_cu_anywhere():
    """Belt and braces on the whole render path, not just the join."""
    runs = [rec("a-1.json", "duckrun", {"OUT": {"role": "output", "name": "dbt_delta"},
                                        "L": {"role": "landing", "name": "dbt_landing"}}),
            rec("b-2.json", "spark", {"OUT2": {"role": "output", "name": "dbt_spark"},
                                      "L": {"role": "landing", "name": "dbt_landing"}})]
    out = _render(runs, ledger({"OUT": 1.0, "OUT2": 2.0, "L": 70.2}))
    assert "70.2" not in out and "dbt_landing (" not in out


def _full(file, engine, **kw):
    """A record that IS a whole generation: torn down, built, benchmarked."""
    r = rec(file, engine, {"OUT": {"role": "output", "name": f"dbt_{engine}", "deleted": ago(1)},
                           "SEM": {"role": "semantic_model", "name": f"aemo_{engine}",
                                   "deleted": ago(1)},
                           "L": {"role": "landing", "name": "dbt_landing"}},
            stats={engine: {"fct_summary": {"total_rows": 1}}}, tables=["fct_summary"], **kw)
    r["benchmark"] = {"timings": {f"aemo_{engine}": {"q": {"ms_by_pass": [1]}}}}
    return r


def test_a_whole_generation_is_accepted():
    assert d.incomplete(_full("a-1.json", "spark")) is None


def test_a_run_that_was_not_torn_down_is_caveated_not_rejected():
    """Its items are still alive and Fabric keeps billing them, so its total creeps upward — but the
    creep is small, and a column that disappears costs more than one carrying a caveat. Run
    30733912205 predates the teardown and is exactly this."""
    r = _full("a-1.json", "duckrun")
    del r["items"]["OUT"]["deleted"]
    assert d.incomplete(r) is None, "it still renders"
    assert d.drifting(r) == ["output/dbt_duckrun"], "and it is named as still billing"


def test_a_torn_down_run_is_not_drifting():
    assert d.drifting(_full("a-1.json", "spark")) == []


def test_the_sources_table_says_which_column_is_still_billing():
    """"settled" and "still climbing" are different claims and only one is comparable to a
    torn-down run, so the loudest of the three states is the one that never resolves on its own."""
    good = _full("a-1.json", "spark")
    bad = _full("b-2.json", "duckrun")
    del bad["items"]["OUT"]["deleted"]
    out = _render([good, bad], ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "**still billing** — 1 item(s) never deleted" in out
    assert "predates that teardown and still owns `output/dbt_duckrun`" in out
    assert "upper bound on that run rather than a measurement of it" in out


def test_a_run_with_no_benchmark_is_rejected():
    """An empty analytics column reads as "querying this engine was free" rather than "nobody
    measured it". Run 30743411308 is exactly this — the bench job was skipped by a needs bug."""
    r = _full("a-1.json", "spark")
    r["benchmark"] = {}
    assert "query half did not run" in d.incomplete(r)


def test_a_run_with_no_layout_is_rejected():
    r = _full("a-1.json", "spark")
    r["layout"]["stats"] = {}
    assert "build half did not report" in d.incomplete(r)


def test_incomplete_records_are_skipped_by_the_loader_and_named(tmp_path, capsys):
    """Skipped, never silently dropped: a page that quietly ignores a record is indistinguishable
    from one that never had it."""
    good, bad = _full("a-1.json", "spark"), _full("b-2.json", "dwh")
    bad["benchmark"] = {}          # the query half never ran — that is still a rejection
    for r in (good, bad):
        (tmp_path / r["_file"]).write_text(json.dumps(r), encoding="utf-8")
    loaded = d.load_runs(str(tmp_path))
    assert [r["_file"] for r in loaded] == ["a-1.json"]
    assert "skipping b-2.json" in capsys.readouterr().err


def test_the_table_says_where_the_compute_storage_split_comes_from():
    """It comes from the operation, and the page has to say so: compute and storage share an item,
    so a reader who assumes the rows are per-item will misread every column."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 34046.3, "SEM": 1514.0}))
    assert "comes from the OPERATION" in out
    assert "share an ITEM" in out
    assert "Every `OneLake …` operation is storage" in out


def test_a_class_with_one_item_per_engine_is_not_decomposed():
    """analytics is always exactly one semantic model per engine, so item rows there would repeat
    the subtotal and add a row of em dashes for every other engine — three rows carrying one row's
    information. etl splits because a DuckDB leg really is a notebook plus a lakehouse."""
    runs = [rec("a-1.json", "duckrun", {"NB": gone("compute", "dbt-duckrun-ab12"),
                                        "OUT": gone("output", "dbt_delta"),
                                        "SEM": gone("semantic_model", "aemo_duckrun")}),
            rec("b-2.json", "spark", {"OUT2": gone("output", "dbt_spark"),
                                      "SEM2": gone("semantic_model", "aemo_spark")})]
    out = _render(runs, ledger({"NB": 26403.5, "OUT": 2463.9, "SEM": 2157.8,
                                "OUT2": 34046.3, "SEM2": 1514.0}))
    assert "| **analytics** |" in out and "2,157.8" in out and "1,514.0" in out
    assert "semantic_model" not in out, "no per-item analytics rows"
    # etl still decomposes: duckrun is genuinely a notebook plus a lakehouse.
    assert "`compute`" in out and "`storage`" in out


def test_compute_and_storage_come_from_the_operation_not_the_item():
    """They share an ITEM: spark bills its Livy session AND its OneLake reads against one lakehouse,
    a warehouse bills Warehouse Query AND its OneLake writes against one warehouse. Bucketing by the
    item's role could never separate them — measured against the live model 2026-08-02."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"}})
    cells, _m = d.run_cu(r, ledger({"OUT": {
        "High Concurrency Session Livy Run": 188635.8,
        "OneLake Write via Redirect": 20267.9,
        "OneLake Read via Redirect": 5737.4}}))
    assert cells["etl"]["compute"] == 188635.8
    assert round(cells["etl"]["storage"], 1) == 26005.3


def test_every_measured_operation_name_buckets_the_way_it_should():
    """The names are the real ones off the capacity, not invented."""
    for op in ("OneLake Write via Redirect", "OneLake Iterative Read via Proxy",
               "OneLake Other Operations", "OneLake Read via Proxy"):
        assert d.bucket(op) == "storage", op
    for op in ("High Concurrency Session Livy Run", "Warehouse Query", "SQL Endpoint Query",
               "Jupyter Notebook Scheduled Run", "XMLA Read Operation", "Dataset On-Demand Refresh"):
        assert d.bucket(op) == "compute", op


def test_the_input_archive_is_one_table_not_a_column_per_engine():
    """dbt_landing holds ONE copy of the CSVs and every engine reads the same bytes. A column per
    engine repeated one number across the page and invited the reading that each had its own input.
    Broken down by folder instead, which is a real decomposition."""
    land = {"files": 8338, "size_mb": 170491.40,
            "folders": {"csv_raw/daily": {"files": 3042, "size_mb": 170004.56},
                        "csv_raw/price_today": {"files": 2550, "size_mb": 381.24}}}
    runs = [_full("a-1.json", "duckrun", landing=land), _full("b-2.json", "spark", landing=land)]
    out = _render(runs, ledger({"OUT": 1.0, "SEM": 2.0}))
    block = out.split("### Input archive")[1].split("###")[0]
    assert "| folder | files | size MB |" in block
    assert "duckrun" not in block and "spark" not in block, "no engine column"
    assert "`csv_raw/daily`" in block and "170,004.56" in block
    assert "**8,338**" in block and "**170,491.40**" in block
    assert block.count("170,491.40") == 1, "the total is stated once, not per engine"


def test_a_changed_archive_between_runs_is_stated_not_averaged():
    """skip_download off extends the archive, and then the two runs did genuinely different amounts
    of work — which is a caveat, not something to smooth over."""
    runs = [_full("a-1.json", "duckrun", landing={"files": 8000, "size_mb": 150000.0, "folders": {}}),
            _full("b-2.json", "spark", landing={"files": 8338, "size_mb": 170491.4, "folders": {}})]
    out = _render(runs, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "did not all read the same archive" in out and "150,000.0" in out


def test_the_numbers_come_before_the_methodology():
    """The charts and the table are what the page is for. A reader who already knows what a capacity
    unit is should not have to scroll past a paragraph explaining it, and a provenance table, to
    reach them — that material reads better as what you check after a number surprises you."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 34046.3, "SEM": 1514.0}))
    first_chart = out.index("<!--chart:")
    assert first_chart < out.index("**Every number on this page is capacity units")
    assert first_chart < out.index("### About these numbers")
    assert first_chart < out.index("Each column is that engine's latest run")
    assert first_chart < out.index("[source](")
    # ...and the heading still leads.
    assert out.index("## Capacity units") < first_chart


def test_the_page_says_the_columns_are_comparable():
    """A capacity unit already prices in how much compute an engine was given — that is the whole
    reason to measure cost rather than wall-clock. Seconds would need a hardware caveat; CU is the
    bill, so the page says so rather than leaving a reader to invent one."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "The columns are directly comparable" in out
    assert "CU is the bill" in out
