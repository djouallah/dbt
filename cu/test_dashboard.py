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

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dashboard as d  # noqa: E402


def rec(file, engine, started, finished, items, config=None, stats=None, tables=None,
        landing=None, full_load=True):
    return {"_file": file, "schema": 1, "engine": engine, "full_load": full_load,
            "run": {"id": file.split("-")[-1].split(".")[0], "started": started,
                    "finished": finished},
            "items": items,
            "layout": {"config": config or {}, "stats": stats or {}, "tables": tables or [],
                       **({"landing": landing} if landing else {})}}


def ledger(items, settled=(), runs=None):
    """The cumulative shape measure.py keeps: `{guid: {cu: {op: total}, settled: ...}}`."""
    return {"items": {g: {"cu": ops, "settled": ("quiet" if g in settled else None)}
                      for g, ops in items.items()},
            "runs": runs or {}, "reads": [{"at": "2026-08-02T20:00:00+00:00"}],
            "updated": "2026-08-02T20:00:00+00:00"}


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setenv("CU_MODEL_OFFSET_HOURS", "10")


def test_the_role_decides_the_class_not_the_fabric_item_kind():
    """A semantic model is only ever queried; everything else is work done to BUILD the tables. This
    replaced classification from the metrics app's item-kind snapshot, which routinely had not
    catalogued a minutes-old item at all."""
    r = rec("r-1.json", "spark", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00", {
        "OUT": {"role": "output", "name": "dbt_spark"},
        "NB": {"role": "compute", "name": "dbt-spark-ab12"},
        "SEM": {"role": "semantic_model", "name": "aemo_spark"},
    })
    led = ledger({"OUT": {"OneLake Write": 10.0}, "NB": {"Livy Run": 900.0},
                  "SEM": {"XMLA Read": 40.0}})
    cells, _landing, _open = d.run_cu(r, led)
    assert cells == {("etl", "OneLake Write"): 10.0, ("etl", "Livy Run"): 900.0,
                     ("analytics", "XMLA Read"): 40.0}


def test_landing_comes_from_the_per_run_rollup_and_never_joins_the_engine_total():
    """`dbt_landing` is never deleted, so it has no total of its own — measure.py attributes it per
    run over that run's window, and the page reads THAT rather than re-deriving it. Two sides
    deriving the same allocation independently is two chances to disagree."""
    r = rec("r-1.json", "spark", "2026-08-02T05:00:00+00:00", "2026-08-02T06:30:00+00:00", {
        "OUT": {"role": "output", "name": "dbt_spark"},
        "LAND": {"role": "landing", "name": "dbt_landing"},
    })
    led = ledger({"OUT": {"Write": 10.0}, "LAND": {"Write": 507.0}},
                 runs={"1": {"landing": {"Write": 7.0}, "settled": True}})
    cells, landing, open_items = d.run_cu(r, led)
    assert landing == {"Write": 7.0}
    assert cells == {("etl", "Write"): 10.0}, "landing must not be added to the engine's own CU"
    assert open_items == ["output/dbt_spark"], "landing is never an item that settles"


def test_the_dbt_folder_costs_nothing_and_is_skipped():
    r = rec("r-1.json", "dwh", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
            {"F": {"role": "folder", "name": "dbt"}, "OUT": {"role": "output", "name": "dbt_dwh"}})
    cells, _l, open_items = d.run_cu(r, ledger({"OUT": {"Q": 1.0}}))
    assert cells == {("etl", "Q"): 1.0}
    assert open_items == ["output/dbt_dwh"], "the folder is not an item whose CU can settle"


def test_an_item_with_no_ledger_rows_yet_is_reported_as_open_not_as_zero():
    """A run measured minutes ago is still accruing. 'not measured' and 'cost nothing' are different
    claims, and the sources table has to be able to say which."""
    r = rec("r-1.json", "spark", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
            {"OUT": {"role": "output", "name": "dbt_spark"}})
    cells, _l, open_items = d.run_cu(r, ledger({}))
    assert cells == {} and open_items == ["output/dbt_spark"]


def test_a_settled_item_still_reports_its_total_after_its_hours_are_gone():
    """The whole reason the ledger keeps cumulative totals: measure.py drops the hour detail when an
    item settles, because the app forgets the window within 14 days. A reader that summed hours
    would show a settled item as having cost nothing."""
    r = rec("r-1.json", "dwh", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
            {"OUT": {"role": "output", "name": "dbt_dwh"}})
    cells, _l, open_items = d.run_cu(r, ledger({"OUT": {"Q": 31063.6}}, settled={"OUT"}))
    assert cells == {("etl", "Q"): 31063.6}
    assert open_items == []


def test_columns_are_the_latest_run_per_engine_and_config():
    """One dispatch builds ONE engine, so rendering the newest record alone gives a comparison page
    with a single column. And spark under readHeavyForPBI answers a different question from spark
    under writeHeavy: one number cannot stand for both."""
    runs = [
        rec("a-1.json", "spark", "2026-08-01T05:00:00+00:00", "2026-08-01T06:00:00+00:00",
            {}, config={"spark": {"resource_profile": "writeHeavy"}}),
        rec("b-2.json", "spark", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
            {}, config={"spark": {"resource_profile": "writeHeavy"}}),
        rec("c-3.json", "spark", "2026-08-02T07:00:00+00:00", "2026-08-02T08:00:00+00:00",
            {}, config={"spark": {"resource_profile": "readHeavyForPBI"}}),
        rec("d-4.json", "dwh", "2026-08-02T09:00:00+00:00", "2026-08-02T10:00:00+00:00", {}),
    ]
    cols = d.columns_for(runs)
    assert [c for c, _e, _r in cols] == ["spark·readHeavyForPBI", "spark·writeHeavy", "dwh"]
    by_col = {c: r["_file"] for c, _e, r in cols}
    assert by_col["spark·writeHeavy"] == "b-2.json", "the LATER run of a config wins its column"


def test_one_config_per_engine_gets_a_bare_column_name():
    runs = [rec("a-1.json", "dwh", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00", {})]
    assert [c for c, _e, _r in d.columns_for(runs)] == ["dwh"]


def test_a_variant_tag_never_contains_the_column_separator():
    """base_engine splits on COL_SEP; a tag containing one would make the column id unparseable back
    to its engine, and STACK lookups would silently miss."""
    tag = d.variant_tag((("native_execution_engine", "true"), ("resource_profile", "readHeavyForPBI"),
                         ("vcores", "64")))
    assert d.COL_SEP not in tag
    assert d.base_engine(f"spark{d.COL_SEP}{tag}") == "spark"


def _render(runs, led):
    buf = io.StringIO()
    with redirect_stdout(buf):
        d.render(d.columns_for(runs), runs, led)
    return buf.getvalue()


def test_the_page_renders_end_to_end_with_charts_and_a_layout():
    runs = [rec("a-1.json", "duckrun", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
                {"OUT": {"role": "output", "name": "dbt_delta"},
                 "SEM": {"role": "semantic_model", "name": "aemo_duckrun"}},
                config={"duckrun": {"vcores": "64"}},
                stats={"duckrun": {"fct_summary": {"total_rows": 143980961, "num_files": 4,
                                                   "num_row_groups": 79, "avg_row_group": 1822544,
                                                   "size_mb": 998.9, "vorder": False,
                                                   "schema": "mart"}}},
                tables=["fct_summary"], landing={"files": 8167, "size_mb": 12345.6})]
    led = ledger({"OUT": {"OneLake Write": 1088.0}, "SEM": {"XMLA Read": 1891.0}})
    out = _render(runs, led)
    assert "<!--chart:" in out and out.count("<!--chart:") == 2
    assert "| **etl** |" in out and "| **analytics** |" in out
    assert "1,088.0" in out and "1,891.0" in out
    assert "fct_summary" in out and "delta-rs" in out
    assert "8,167" in out and "12,345.60" in out, "the input archive should be on the page"
    # Charts carry the adapter and the compute: `iceberg` beside `duckrun` reads as an engine
    # difference when it is a writer difference.
    spec = json.loads(out.split("<!--chart:")[1].split("-->")[0])
    assert spec["rows"][0][2] == "dbt-duckrun · 64 vCores"


def test_the_page_says_when_a_column_is_still_accruing():
    runs = [rec("a-1.json", "dwh", "2026-08-02T05:00:00+00:00", "2026-08-02T06:00:00+00:00",
                {"OUT": {"role": "output", "name": "dbt_dwh"}})]
    open_page = _render(runs, ledger({"OUT": {"Q": 5.0}}))
    assert "still accruing" in open_page
    done_page = _render(runs, ledger({"OUT": {"Q": 5.0}}, settled={"OUT"}))
    assert "still accruing" not in done_page


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
