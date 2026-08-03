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
    assert [c for c, _e, _r in cols] == ["spark·V-Order", "spark·default", "dwh"]
    by_col = {c: r["_file"] for c, _e, r in cols}
    assert by_col["spark·default"] == "b-2.json", "the LATER run of a config wins its column"


def test_one_config_per_engine_gets_a_bare_column_name():
    assert [c for c, _e, _r in d.columns_for([rec("a-1.json", "dwh", {})])] == ["dwh"]


def test_a_variant_tag_never_contains_the_column_separator():
    """base_engine splits on COL_SEP; a tag containing one would make the column id unparseable back
    to its engine, and STACK lookups would silently miss."""
    tag = d.variant_tag((("native_execution_engine", "true"),
                         ("resource_profile", "readHeavyForPBI"), ("vcores", "64")))
    assert d.COL_SEP not in tag
    assert d.base_engine(f"spark{d.COL_SEP}{tag}") == "spark"


def test_a_column_header_names_a_profile_by_its_effect():
    """`spark·readHeavyForPBI+noNEE` is Microsoft's name for an intended workload plus a double
    negative, in a header repeated across every table and both charts. The same PROFILE_LABEL the
    layout captions use gets it to `spark·V-Order`, so a profile reads the same wherever it appears."""
    assert d.variant_tag((("resource_profile", "readHeavyForPBI"),)) == "V-Order"
    assert d.variant_tag((("resource_profile", "writeHeavy"),)) == "default"
    # An unmapped profile keeps its own name — guessing at readHeavyForSpark would be wrong.
    assert d.variant_tag((("resource_profile", "readHeavyForSpark"),)) == "readHeavyForSpark"


def test_a_flag_that_is_off_is_absent_from_the_header_rather_than_negated():
    """`+noNEE` spends header width saying nothing happened. Absence carries it, and the contrast
    with the run that DID enable it is what the reader is looking for."""
    on = (("native_execution_engine", "true"), ("resource_profile", "writeHeavy"))
    off = (("native_execution_engine", "false"), ("resource_profile", "writeHeavy"))
    assert d.variant_tag(on) == "default+NEE"
    assert d.variant_tag(off) == "default"


def test_two_configs_that_would_share_a_header_are_spelled_out_instead():
    """Absence-means-off is only unambiguous while every config of the engine RECORDS the flag. A
    record predating the dispatch input has no key at all, which would collide with an explicit
    `false` — and a page printing one column name twice is unreadable and says nothing about why."""
    runs = [rec("a-1.json", "spark", {}, config={"spark": {"resource_profile": "writeHeavy"}},
                finished_hours_ago=48),
            rec("b-2.json", "spark", {},
                config={"spark": {"resource_profile": "writeHeavy",
                                  "native_execution_engine": "false"}}, finished_hours_ago=24)]
    names = [c for c, _e, _r in d.columns_for(runs)]
    assert len(set(names)) == 2, names
    assert names == ["spark·default", "spark·default+noNEE"]


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
    assert "fct_summary" in out and "1.8M" in out, "the layout block, with row-group size abbreviated"
    assert "8,167" in out and "12,345.60" in out, "the input archive should be on the page"
    # ANALYTICS leads: it is the interactive CU that throttles, which is the point of the project.
    first, second = [json.loads(b.split("-->")[0]) for b in out.split("<!--chart:")[1:3]]
    assert "Analytics" in first["title"] and "INTERACTIVE" in first["subtitle"]
    assert "ETL" in second["title"] and "background" in second["subtitle"]
    # [label, mean, min, max, caption] — one run, so the range collapses onto the mean.
    # ANALYTICS is labelled by the LAYOUT and captioned by the writer; ETL is labelled by the column
    # and captioned by the adapter and its compute. That asymmetry is the point of both charts.
    assert first["rows"][0][:4] == ["duckrun", 2041.0, 2041.0, 2041.0]
    assert first["rows"][0][4] == "4 files · 79 RG", "the shape is the sub-label"
    assert second["rows"][0][:4] == ["duckrun", 31080.0, 31080.0, 31080.0]
    assert second["rows"][0][4] == "dbt-duckrun · 64 vCores"


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


def _full(file, engine, timings=None, **kw):
    """A record that IS a whole generation: torn down, built, benchmarked.

    The DEFAULT `timings` carries no tier keys at all — only `ms_by_pass`, which is what
    `incomplete()` checks for and nothing a tier column can read. That is deliberate: it keeps every
    pre-existing test exercising the "no timings, no columns" path, and `timings=` is how the
    query-time tests opt in.
    """
    r = rec(file, engine, {"OUT": {"role": "output", "name": f"dbt_{engine}", "deleted": ago(1)},
                           "SEM": {"role": "semantic_model", "name": f"aemo_{engine}",
                                   "deleted": ago(1)},
                           "L": {"role": "landing", "name": "dbt_landing"}},
            **{"stats": {engine: {"fct_summary": {"total_rows": 1}}},
               "tables": ["fct_summary"], **kw})
    r["benchmark"] = {"timings": {f"aemo_{engine}": timings or {"q": {"ms_by_pass": [1]}}}}
    return r


def _timings(**per_query):
    """`{query: {cold_ms, warm_ms, hot_median_ms, hot_spread_pct}}` from `q=(cold, warm, hot)`.
    A `None` cold is the real ladder-query shape: no first-pass sample at all."""
    out = {}
    for q, (cold, warm, hot) in per_query.items():
        t = {"warm_ms": warm, "hot_median_ms": hot, "hot_spread_pct": 5.0}
        if cold is not None:
            t["cold_ms"] = cold
        out[q] = t
    return out


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
    assert first_chart < out.index("**Capacity units (CU-seconds) are what this page leads with")
    assert first_chart < out.index("### About these numbers")
    assert first_chart < out.index("Each column is that engine's latest run")
    assert first_chart < out.index("[source](")
    # ...and the heading still leads.
    assert out.index("## Capacity units") < first_chart


def test_the_page_says_which_of_its_measures_is_the_comparable_one():
    """A capacity unit already prices in how much compute an engine was given — that is the whole
    reason CU leads. The two time sections do NOT have that property: billed seconds sum across
    concurrent operations and milliseconds are a sample of a shared capacity. The page has to say
    which is which, rather than presenting three measures as equally footed."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "The CU columns are directly comparable" in out
    assert "reason to lead with cost" in out
    assert "sample of a shared capacity" in out, "the ms caveat has to be stated, not implied"


def test_the_chart_shows_the_mean_and_the_range_across_runs():
    """One dispatch is one sample of a SHARED capacity, so a single number is a reading rather than a
    result. The bar is the mean because that is what a ranking should be built on; the range is what
    tells a reader when two averages are closer together than either engine's own spread."""
    runs = [_full("a-1.json", "spark", finished_hours_ago=72),
            _full("b-2.json", "spark", finished_hours_ago=48),
            _full("c-3.json", "spark", finished_hours_ago=24)]
    # Distinct GUIDs per run so each contributes its own sample.
    for i, r in enumerate(runs):
        r["items"] = {f"S{i}": gone("semantic_model", "aemo_spark"),
                      f"O{i}": gone("output", "dbt_spark")}
    led = ledger({"S0": {"XMLA Read Operation": 1000.0}, "O0": {"Warehouse Query": 1.0},
                  "S1": {"XMLA Read Operation": 2000.0}, "O1": {"Warehouse Query": 1.0},
                  "S2": {"XMLA Read Operation": 1500.0}, "O2": {"Warehouse Query": 1.0}})
    out = _render(runs, led)
    spec = json.loads(out.split("<!--chart:")[1].split("-->")[0])
    assert spec["rows"][0][1:4] == [1500.0, 1000.0, 2000.0]
    assert spec["rows"][0][0] == "spark", "the analytics bar is NAMED for its writer"
    assert "mean of 3 runs" in spec["subtitle"]


def test_the_chart_sorts_by_the_mean():
    runs = [_full("a-1.json", "spark"), _full("b-2.json", "dwh")]
    runs[0]["items"] = {"S0": gone("semantic_model", "aemo_spark"),
                        "O0": gone("output", "dbt_spark")}
    runs[1]["items"] = {"S1": gone("semantic_model", "aemo_dwh"),
                        "O1": gone("output", "dbt_dwh")}
    out = _render(runs, ledger({"S0": {"XMLA Read Operation": 9.0}, "O0": {"Warehouse Query": 1.0},
                                "S1": {"XMLA Read Operation": 3.0}, "O1": {"Warehouse Query": 1.0}}))
    spec = json.loads(out.split("<!--chart:")[1].split("-->")[0])
    assert [r[0] for r in spec["rows"]] == ["dwh", "spark"], "cheapest mean first"


# ------------------------------------------------------------- one bar per LAYOUT, not per engine

def _lay(engine, files, rgs, vorder=False, cfg=None, **kw):
    """A record whose mart layout is spelled out, so grouping has something to group on."""
    return _full(kw.pop("file", "x.json"), engine, config={engine: cfg or {}},
                 stats={engine: {"fct_summary": {"total_rows": 143980961, "num_files": files,
                                                 "num_row_groups": rgs, "avg_row_group": 1,
                                                 "size_mb": 1.0, "vorder": vorder,
                                                 "schema": "mart"}}}, **kw)


def _analytics_chart(out):
    return json.loads(out.split("<!--chart:")[1].split("-->")[0])


def test_the_same_parquet_is_one_bar_however_many_engines_wrote_it():
    """Power BI never sees the engine — it opens parquet through Direct Lake and transcodes row
    groups. duckrun at 64 cores and at 32 wrote 4 files and 27 row groups either way, so two bars
    50% apart was not a comparison: it was one layout measured twice, presented as two results."""
    runs = [_lay("duckrun", 4, 27, cfg={"vcores": "64"}, file="a-1.json", finished_hours_ago=72),
            _lay("duckrun", 4, 27, cfg={"vcores": "32"}, file="b-2.json", finished_hours_ago=48)]
    runs[0]["items"] = {"S0": gone("semantic_model", "aemo_duckrun"),
                        "O0": gone("output", "dbt_delta")}
    runs[1]["items"] = {"S1": gone("semantic_model", "aemo_duckrun"),
                        "O1": gone("output", "dbt_delta")}
    out = _render(runs, ledger({"S0": {"XMLA Read Operation": 1000.0}, "O0": 1.0,
                                "S1": {"XMLA Read Operation": 2000.0}, "O1": 1.0}))
    rows = _analytics_chart(out)["rows"]
    assert len(rows) == 1, "one layout, one bar"
    assert rows[0][:4] == ["duckrun", 1500.0, 1000.0, 2000.0],         "not `duckrun·64c` and `duckrun·32c` — the cores never reached the parquet"
    assert rows[0][4] == "4 files · 27 RG", "the shape it grouped on sits underneath"
    # ...while the ETL chart keeps BOTH columns, because there the writer and the compute it was
    # given are the entire subject. That asymmetry is the change, and it must not be tidied away.
    etl = json.loads(out.split("<!--chart:")[2].split("-->")[0])
    assert [r[0] for r in etl["rows"]] == ["duckrun·32c", "duckrun·64c"]


def test_an_engine_is_named_for_who_writes_when_the_target_name_misleads():
    """`iceberg` reads as a format beside three engines, when the writer is the same DuckDB that
    duckrun uses — pointed at an Iceberg REST catalog instead of delta-rs. On a page whose subject is
    what got written, that is the entire reason the pair exists."""
    assert d.producer(_lay("iceberg", 357, 1172)) == "duckdb iceberg"
    assert d.producer(_lay("duckrun", 4, 27)) == "duckrun", "only where the name misleads"


def test_seconds_are_rows_of_the_engine_table_not_a_section():
    """They come off the SAME Capacity Metrics row as the CU above them — same GUIDs, same roles,
    same compute/storage split — so a table of their own restated the whole join to add two numbers
    per class, and split "what it cost" from "how long it took" across two tables. And no third
    chart: the page carries two bars and both are capacity units, the measure it can defend."""
    runs = [_lay("spark", 11, 11, file="a-1.json")]
    led = ledger({"OUT": {"High Concurrency Session Livy Run": 900.0},
                  "SEM": {"XMLA Read Operation": 40.0}})
    led["seconds"] = _secs({"OUT": {"High Concurrency Session Livy Run": 30.0},
                            "SEM": {"XMLA Read Operation": 4.0}})
    out = _render(runs, led)
    assert "### Time" not in out, "no section of its own"
    body = out.split("Every engine's latest run")[1].split("###")[0]
    assert "| **etl** | **900.0** |" in body
    assert "| `compute CU per second` | 30.0 |" in body, "the rate, under the class it belongs to"
    assert "| `seconds` |" not in body, "the raw seconds are not shown — see the rate's docstring"
    assert out.count("<!--chart:") == 2, "and it brought no bar with it"


def test_v_order_never_merges_with_anything():
    """The sharpest experiment on the page: the same file band with V-Order on and off. Merging
    those two would erase the one comparison the layout job exists to make."""
    runs = [_lay("spark", 11, 11, vorder=True, cfg={"resource_profile": "readHeavyForPBI"},
                 file="a-1.json"),
            _lay("spark", 14, 14, vorder=False, cfg={"resource_profile": "writeHeavy"},
                 file="b-2.json")]
    assert d.layout_key(runs[0]) != d.layout_key(runs[1])
    assert d.layout_key(runs[0])[1] == d.layout_key(runs[1])[1], "same file band, on purpose"


def test_a_band_absorbs_drift_but_not_a_real_difference():
    """78 files and 80 are the same writer with the same settings and one more incremental run.
    Exact equality would split dwh from itself; a power-of-two band does not. 27 row groups and
    1,172 are four bands apart and stay apart."""
    assert d.layout_band(78) == d.layout_band(80)
    assert d.layout_band(10) == d.layout_band(11) == d.layout_band(14)
    assert d.layout_band(27) != d.layout_band(1172) != d.layout_band(4)
    assert d.layout_band(0) == d.layout_band(None) == -1


def test_an_unmeasured_layout_is_never_grouped_with_another_one():
    """Two records carrying no file count are not two identical layouts, they are two unmeasured
    ones. Merging them would claim Power BI cannot tell apart two things nobody looked at."""
    a, b = _full("a-1.json", "spark"), _full("b-2.json", "dwh")   # stats carry total_rows only
    assert d.layout_key(a) is None and d.layout_key(b) is None
    assert len(d.layout_groups(d.columns_for([a, b]))) == 2


def test_the_producer_name_drops_what_never_reached_the_parquet():
    """`spark V-Order` / `spark default`, not `spark·readHeavyForPBI+NEE`. The profile is named by
    what it DOES, and the core count and NEE flag are gone because two runs each showed they do not
    change what is written."""
    assert d.producer(_lay("spark", 11, 11, cfg={"resource_profile": "readHeavyForPBI",
                                                 "native_execution_engine": "true"})) \
        == "spark V-Order"
    assert d.producer(_lay("spark", 14, 14, cfg={"resource_profile": "writeHeavy",
                                                 "native_execution_engine": "false"})) \
        == "spark default"
    assert d.producer(_lay("duckrun", 4, 27, cfg={"vcores": "64"})) == "duckrun"
    # An unmapped profile keeps its own name rather than being guessed at — `readHeavyForSpark`
    # reads like it enables V-Order and sets no vorder at all.
    assert d.producer(_lay("spark", 4, 4, cfg={"resource_profile": "readHeavyForSpark"})) \
        == "spark readHeavyForSpark"


def test_a_group_of_genuinely_different_writers_names_both():
    """The case worth reading: two engines that produced parquet Power BI cannot tell apart."""
    members = [("duckrun·64c", _lay("duckrun", 4, 27, cfg={"vcores": "64"})),
               ("duckrun·32c", _lay("duckrun", 4, 27, cfg={"vcores": "32"})),
               ("spark·writeHeavy", _lay("spark", 4, 27, cfg={"resource_profile": "writeHeavy"}))]
    assert d.producers(members) == "duckrun, spark default", "deduplicated, and both kept"


def test_the_layout_table_is_one_row_per_writer_and_agrees_with_the_chart():
    """The table groups by the DECLARED producer and the chart by the MEASURED parquet — two
    directions onto the same rows. And both quote the same CU: a page printing 1,916 in a bar and
    1,960 in the row under it would be asking the reader which one it meant."""
    runs = [_lay("duckrun", 4, 27, cfg={"vcores": "64"}, file="a-1.json", finished_hours_ago=72),
            _lay("duckrun", 4, 27, cfg={"vcores": "32"}, file="b-2.json", finished_hours_ago=48)]
    runs[0]["items"] = {"S0": gone("semantic_model", "aemo_duckrun"),
                        "O0": gone("output", "dbt_delta")}
    runs[1]["items"] = {"S1": gone("semantic_model", "aemo_duckrun"),
                        "O1": gone("output", "dbt_delta")}
    out = _render(runs, ledger({"S0": {"XMLA Read Operation": 1000.0}, "O0": 1.0,
                                "S1": {"XMLA Read Operation": 2000.0}, "O1": 1.0}))
    block = out.split("#### `fct_summary`")[1].split("\n###")[0]
    body = [ln for ln in block.splitlines() if ln.startswith("| ") and not ln.startswith("|:")]
    assert len(body) == 2, "a header and ONE row — duckrun, not duckrun twice"
    assert body[1].startswith("| duckrun | 1,500 |")
    assert _analytics_chart(out)["rows"][0][1] == 1500.0, "the same number as the bar"


def test_the_row_count_lives_in_the_heading_until_the_engines_disagree():
    """It is identical on every row by design — that is the parity statement the project rests on —
    so a 143,980,961 repeated down the table is a wide column carrying one fact. When they DISAGREE
    it comes back as a column, because that is the loudest signal this page has."""
    same = [_lay("duckrun", 4, 27, file="a-1.json"), _lay("dwh", 78, 78, file="b-2.json")]
    out = _render(same, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "143,980,961 rows on every engine" in out
    assert "| rows |" not in out
    drifted = [_lay("duckrun", 4, 27, file="a-1.json"), _lay("dwh", 78, 78, file="b-2.json")]
    drifted[1]["layout"]["stats"]["dwh"]["fct_summary"]["total_rows"] = 143980960
    out = _render(drifted, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "row counts DISAGREE" in out
    assert "| rows |" in out, "and the numbers come back so the gap can be read"


# ---------------------------------------------------------------- query time, in the mart block

def test_a_tier_is_summed_over_the_queries_every_column_has():
    """A total over different queries is not a comparison. A query one engine never ran is dropped
    from EVERY column's total, not counted for the engines that have it."""
    runs = [_full("a-1.json", "duckrun", timings=_timings(a=(10, 5, 4), b=(100, 50, 40))),
            _full("b-2.json", "dwh", timings=_timings(a=(20, 6, 5)))]
    per_col = {"duckrun": d.bench_timings(runs[0]), "dwh": d.bench_timings(runs[1])}
    totals, n = d.bench_totals(per_col, "cold_ms")
    assert n == 1, "`b` is duckrun's alone and must not inflate its total"
    assert totals == {"duckrun": 10.0, "dwh": 20.0}


def test_the_three_tiers_are_columns_of_the_mart_block_not_a_section():
    """They were briefly a table of their own, which put the layout and the speed it produced on two
    different tables — and whether one explains the other is the only question worth asking of these
    numbers. On one row, `files`/`row groups`/`size MB`/`vorder` sit beside the ms they produced."""
    t = _timings(a=(10, 5, 4), b=(20, 6, 5))
    runs = [_full("a-1.json", "duckrun", timings=t,
                  stats={"duckrun": {"fct_summary": {"total_rows": 1, "num_files": 4}}}),
            _full("b-2.json", "dwh", timings=t,
                  stats={"dwh": {"fct_summary": {"total_rows": 1, "num_files": 78}}})]
    out = _render(runs, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "### Query time" not in out, "no section of its own"
    block = out.split("#### `fct_summary`")[1].split("####")[0]
    assert "| layout | CU | cold ms | warm ms | hot ms | files |" in block
    row = next(ln for ln in block.splitlines() if ln.startswith("| duckrun |"))
    assert "| 30 | 11 | 9 |" in row, "cold/warm/hot beside the layout that produced them"
    assert row.rstrip().endswith("| 4 | — | — | — | · |"), "and the file count on the same row"
    assert "| writer |" not in block, "the row label IS the writer now"


def test_the_tiers_appear_on_the_mart_block_alone():
    """One number per ENGINE, not per table — on every block it would read as one measurement per
    table, which is the same reason the CU column is mart-only."""
    t = _timings(a=(10, 5, 4))
    runs = [_full("a-1.json", "duckrun", timings=t,
                  stats={"duckrun": {"fct_summary": {"total_rows": 1},
                                     "fct_scada": {"total_rows": 9, "schema": "landing"}}},
                  tables=["fct_summary", "fct_scada"])]
    out = _render(runs, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "cold ms" in out.split("#### `fct_summary`")[1].split("####")[0]
    assert "cold ms" not in out.split("fct_scada`")[1]


def test_cold_covers_fewer_queries_than_hot_and_the_note_says_so():
    """The selectivity-ladder queries have NO cold sample — the top DUID is resolved after pass 1 —
    so cold is genuinely summed over a smaller set, and the note counts each tier rather than leaving
    a suspiciously small total to be explained."""
    t = _timings(probe=(10, 5, 4), sel_1duid=(None, 7, 6))
    runs = [_full("a-1.json", "duckrun", timings=t), _full("b-2.json", "dwh", timings=t)]
    out = _render(runs, ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "cold over 1, warm over 2, hot over 2" in out


def test_a_record_with_no_tier_timings_adds_no_columns():
    """`_full`'s default carries `ms_by_pass` and nothing else — enough for `incomplete()`, nothing a
    tier can read. Absent columns say "not measured"; zeros would say "instant"."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "cold ms" not in out and "### Query time" not in out
    assert "| layout | CU | files |" in out, "the block itself still renders"


# ------------------------------------------------------------------------------------- build time

def _secs(items):
    return {g: (v if isinstance(v, dict) else {"Warehouse Query": v}) for g, v in items.items()}


def test_seconds_split_by_role_exactly_like_cu():
    """Same GUIDs, same roles, same read — the duration rides in the same Capacity Metrics row, so
    the join cannot disagree with the CU one."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"},
                                  "SEM": {"role": "semantic_model", "name": "aemo_spark"},
                                  "L": {"role": "landing", "name": "dbt_landing"}})
    led = ledger({"OUT": {"High Concurrency Session Livy Run": 900.0},
                  "SEM": {"XMLA Read Operation": 40.0}, "L": {"Warehouse Query": 70.2}})
    led["seconds"] = _secs({"OUT": {"High Concurrency Session Livy Run": 30.0},
                            "SEM": {"XMLA Read Operation": 4.0}, "L": {"Warehouse Query": 9.9}})
    cells, _missing = d.run_cu(r, led, "seconds")
    assert d.class_total(cells, "etl") == 30.0
    assert d.class_total(cells, "analytics") == 4.0, "landing is skipped here as it is for CU"


def test_a_class_the_ledger_has_not_read_yet_is_a_dash_not_a_zero():
    """A run committed minutes ago whose CU has not been read. `**0.0**` on its subtotal says the
    engine did that work for FREE, which is the one reading this whole page is built to prevent —
    and it is the same distinction the item rows already make. Live case: a record landed from CI
    mid-render and printed 0.0 down an entire column."""
    runs = [_lay("duckrun", 4, 27, file="a-1.json"), _lay("dwh", 78, 78, file="b-2.json")]
    runs[0]["items"] = {"O0": gone("output", "dbt_delta"), "S0": gone("semantic_model", "aemo")}
    runs[1]["items"] = {"O1": gone("output", "dbt_dwh"), "S1": gone("semantic_model", "aemo_dwh")}
    led = ledger({"O0": {"Jupyter Notebook Scheduled Run": 900.0},
                  "S0": {"XMLA Read Operation": 40.0}})       # nothing for dwh at all
    led["seconds"] = _secs({"O0": {"Jupyter Notebook Scheduled Run": 30.0},
                            "S0": {"XMLA Read Operation": 4.0}})
    body = _render(runs, led).split("Every engine's latest run")[1].split("###")[0]
    assert "| **etl** | **900.0** | — |" in body, "measured, then not-yet-measured"
    assert "| `compute CU per second` | 30.0 | — |" in body
    assert "| 0.0 |" not in body and "**0.0**" not in body, "no cell reads as free"


def test_a_ledger_with_no_seconds_renders_no_rate_row():
    """Every ledger written before the duration read, and any read where the model had no duration
    column. Absent is the correct output for both — zeros would say the work was instant."""
    out = _render([_full("a-1.json", "spark")], ledger({"OUT": 1.0, "SEM": 2.0}))
    assert "| `compute CU per second` |" not in out, "no ROW; the note may still explain it"
    assert out.count("<!--chart:") == 2, "no seconds, no ETL-time chart"


def test_the_landing_lakehouses_sql_endpoint_is_not_an_engines_cu():
    """Fabric pairs every lakehouse with a SQL analytics endpoint — a separate billable `Warehouse`
    item with its own GUID and the role `sql_endpoint`, not `landing`. So landing CU reached the page
    through the one door the role check does not cover: the SAME endpoint item appears in every run
    record and charged every engine 130.4 CU it did not spend. Caught by NAME against the record's own
    landing items, so an engine's OWN endpoint is untouched."""
    r = rec("r-1.json", "spark", {
        "L": {"role": "landing", "name": "dbt_landing"},
        "LEP": {"role": "sql_endpoint", "name": "dbt_landing"},        # landing's — not this engine's
        "OEP": {"role": "sql_endpoint", "name": "dbt_spark"},          # the engine's own — keep
        "OUT": {"role": "output", "name": "dbt_spark"}})
    assert d.landing_guids(r) == {"LEP"}
    cells, missing = d.run_cu(r, ledger({"L": {"Warehouse Query": 70.2},
                                         "LEP": {"SQL Endpoint Query": 130.4},
                                         "OEP": {"SQL Endpoint Query": 306.3},
                                         "OUT": {"High Concurrency Session Livy Run": 900.0}}))
    assert d.class_total(cells, "etl") == 1206.3, "900 + the engine's own endpoint, and nothing else"
    assert missing == [], "landing's endpoint is not an item whose CU could be missing"


def test_the_rate_is_compute_over_compute_never_total_over_total():
    """A storage operation bills real CU over a duration of essentially nothing — 383.25 CU in
    0.049 s, measured — so putting it in the ratio does not dilute the rate, it detonates it, by an
    amount that tracks only how much OneLake traffic the engine happened to make. Live symptom: the
    same DuckDB in the same 64-vCore notebook read 36.1 for iceberg and 31.2 for duckrun. Compute
    against compute, both read 32.0."""
    runs = [_full("a-1.json", "duckrun")]
    runs[0]["items"] = {"NB": gone("compute", "dbt-duckrun-ab12"),
                        "OUT": gone("output", "dbt_delta"),
                        "SEM": gone("semantic_model", "aemo_duckrun")}
    led = ledger({"NB": {"Jupyter Notebook Scheduled Run": 20665.6},
                  "OUT": {"OneLake Write via Redirect": 384.1},
                  "SEM": {"XMLA Read Operation": 1287.2}})
    led["seconds"] = _secs({"NB": {"Jupyter Notebook Scheduled Run": 645.79},
                            "OUT": {"OneLake Write via Redirect": 0.031},
                            "SEM": {"XMLA Read Operation": 25.93}})
    body = _render(runs, led).split("Every engine's latest run")[1].split("###")[0]
    rate = [ln for ln in body.splitlines() if ln.startswith("| `compute CU per second`")]
    assert rate[0] == "| `compute CU per second` | 32.0 |", "the node's own draw, not a blend"
    # And the SECONDS row still counts storage — it is the rate alone that must not.
    assert "| `compute` | 20,665.6 |" in body, "the compute CU the rate divides"


def test_the_rate_scales_with_the_cores_the_column_was_given():
    """It is `cores` ÷ 2 for a single-node Python notebook — 32 at 64 vCores, 16 at 32 — NOT the
    constant 32 it is tempting to read it as, because `cores` is a dispatch input. The invariant is
    that two legs at the SAME cores agree; comparing a 32-core column against a 64-core one compares
    node sizes. The page can only ever show them apart: `vcores` is in `variant()`, so they are
    separate columns, and the caption names each."""
    big = _full("a-1.json", "duckrun", config={"duckrun": {"vcores": "64"}})
    small = _full("b-2.json", "duckrun", config={"duckrun": {"vcores": "32"}})
    big["items"] = {"NB": gone("compute", "dbt-duckrun-big")}
    small["items"] = {"NB2": gone("compute", "dbt-duckrun-small")}
    led = ledger({"NB": {"Jupyter Notebook Scheduled Run": 3200.0},
                  "NB2": {"Jupyter Notebook Scheduled Run": 1600.0}})
    led["seconds"] = _secs({"NB": {"Jupyter Notebook Scheduled Run": 100.0},
                            "NB2": {"Jupyter Notebook Scheduled Run": 100.0}})
    cols = d.columns_for([big, small])
    assert [c for c, _e, _r in cols] == ["duckrun·32c", "duckrun·64c"], "never one blended column"
    out = _render([big, small], led)
    rate = next(ln for ln in out.splitlines() if ln.startswith("| `compute CU per second`"))
    assert rate == "| `compute CU per second` | 16.0 | 32.0 |", "cores ÷ 2, per column"
    assert "64 vCores" in out and "32 vCores" in out, "the caption has to name the size"


def test_the_rate_is_computed_per_class():
    """The rate is the average capacity that class's compute drew while it ran, and the concurrency
    that makes a spark leg's billed seconds exceed its wall clock is in the numerator and the
    denominator alike, so it cancels."""
    runs = [_full("a-1.json", "spark")]
    led = ledger({"OUT": {"High Concurrency Session Livy Run": 900.0},
                  "SEM": {"XMLA Read Operation": 40.0}})
    led["seconds"] = _secs({"OUT": {"High Concurrency Session Livy Run": 30.0},
                            "SEM": {"XMLA Read Operation": 4.0}})
    out = _render(runs, led)
    body = out.split("Every engine's latest run")[1].split("###")[0]
    assert "| **etl** | **900.0** |" in body
    assert "| `compute CU per second` | 30.0 |" in body, "900 CU over 30 s"
    assert "| **analytics** | **40.0** |" in body
    assert "| `compute CU per second` | 10.0 |" in body, "40 CU over 4 s"
    assert out.count("<!--chart:") == 2, "the two CU charts and no third — seconds stay a table"


def test_the_svg_draws_a_whisker_only_when_there_is_a_range():
    """A single run is a point, and drawing a zero-width whisker on it would suggest a spread that
    was never measured."""
    import report_html as R
    wide = R.chart_svg({"title": "t", "subtitle": "s",
                        "rows": [["spark", 1500.0, 1000.0, 2000.0, "cap"]]})
    assert wide.count('class="whisker"') == 1 and wide.count("whisker-cap") == 2
    assert "(1,000–2,000)" in wide
    flat = R.chart_svg({"title": "t", "subtitle": "s",
                        "rows": [["dwh", 1853.5, 1853.5, 1853.5, "cap"]]})
    assert 'class="whisker"' not in flat and "(" not in flat.split("bar-value")[1][:40]


def test_the_svg_still_takes_the_older_three_field_row():
    """`[label, value, caption]` — so a chart spec from an artifact rendered months ago still draws."""
    import report_html as R
    svg = R.chart_svg({"title": "t", "subtitle": "s", "rows": [["spark", 42.0, "cap"]]})
    assert "42.0" in svg and "cap" in svg and 'class="whisker"' not in svg
