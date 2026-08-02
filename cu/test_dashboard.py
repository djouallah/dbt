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


def rec(file, engine, items, config=None, stats=None, tables=None, landing=None,
        full_load=True, finished_hours_ago=48):
    return {"_file": file, "schema": 1, "engine": engine, "full_load": full_load,
            "run": {"id": file.split("-")[-1].split(".")[0],
                    "started": ago(finished_hours_ago + 1), "finished": ago(finished_hours_ago)},
            "items": items,
            "layout": {"config": config or {}, "stats": stats or {}, "tables": tables or [],
                       **({"landing": landing} if landing else {})}}


def ledger(items):
    """The whole shape: one number per item GUID."""
    return {"items": dict(items), "reads": [{"at": "2026-08-02T20:00:00+00:00"}],
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
    cells, _landing, _missing = d.run_cu(r, ledger({"OUT": 10.0, "NB": 900.0, "SEM": 40.0}))
    assert cells == {"etl": {"dbt_spark (output)": 10.0, "dbt-spark-ab12 (compute)": 900.0},
                     "analytics": {"aemo_spark (semantic_model)": 40.0}}
    assert d.class_total(cells, "etl") == 910.0
    assert d.class_total(cells, "analytics") == 40.0


def test_landing_is_reported_apart_and_never_joins_an_engine_total():
    """`dbt_landing` is the one item no run deletes, so its total is cumulative over the measured
    window rather than any one run's share. It is a stage every engine reads from, not a competitor."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"},
                                  "LAND": {"role": "landing", "name": "dbt_landing"}})
    cells, landing, _missing = d.run_cu(r, ledger({"OUT": 10.0, "LAND": 507.0}))
    assert landing == 507.0
    assert d.class_total(cells, "etl") == 10.0, "landing must not be added to the engine's own CU"


def test_the_dbt_folder_costs_nothing_and_is_skipped():
    r = rec("r-1.json", "dwh", {"F": {"role": "folder", "name": "dbt"},
                                "OUT": {"role": "output", "name": "dbt_dwh"}})
    cells, _l, missing = d.run_cu(r, ledger({"OUT": 1.0}))
    assert cells == {"etl": {"dbt_dwh (output)": 1.0}}
    assert missing == [], "a folder is not an item whose CU could be missing"


def test_an_item_the_ledger_has_never_seen_is_unmeasured_not_zero():
    """'not measured yet' and 'cost nothing' are different claims, and the sources table has to be
    able to say which."""
    r = rec("r-1.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"},
                                  "SEM": {"role": "semantic_model", "name": "aemo_spark"}})
    cells, _l, missing = d.run_cu(r, ledger({"OUT": 5.0}))
    assert cells == {"etl": {"dbt_spark (output)": 5.0}}
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
    out = _render(runs, ledger({"OUT": 1509.0, "NB": 29571.0, "SEM": 2041.0}))
    assert out.count("<!--chart:") == 2
    assert "| **etl** |" in out and "| **analytics** |" in out
    # Item-major: the notebook and the lakehouse are separate rows, which is where a DuckDB leg's
    # cost actually goes.
    assert "`dbt-duckrun-baf95ac5 (compute)`" in out and "29,571.0" in out
    assert "`dbt_delta (output)`" in out and "1,509.0" in out
    assert "fct_summary" in out and "delta-rs" in out
    assert "8,167" in out and "12,345.60" in out, "the input archive should be on the page"
    spec = json.loads(out.split("<!--chart:")[1].split("-->")[0])
    assert spec["rows"][0][1] == 31080.0, "etl chart is the sum of the etl items"
    assert spec["rows"][0][2] == "dbt-duckrun · 64 vCores"


def test_an_item_missing_from_one_column_prints_a_dash_not_a_zero():
    """A dash says 'this engine never made an item of that name'; 0.0 would say 'it cost nothing'."""
    runs = [rec("a-1.json", "duckrun", {"NB": {"role": "compute", "name": "dbt-duckrun-ab12"}}),
            rec("b-2.json", "spark", {"OUT": {"role": "output", "name": "dbt_spark"}})]
    out = _render(runs, ledger({"NB": 29571.0, "OUT": 24903.0}))
    rows = [ln for ln in out.splitlines() if ln.startswith("| `dbt-duckrun-ab12")]
    assert rows and "—" in rows[0]


def test_the_page_says_when_a_column_can_still_rise():
    fresh = [rec("a-1.json", "dwh", {"OUT": {"role": "output", "name": "dbt_dwh"}},
                 finished_hours_ago=0.5)]
    assert "may still rise" in _render(fresh, ledger({"OUT": 5.0}))
    old = [rec("a-1.json", "dwh", {"OUT": {"role": "output", "name": "dbt_dwh"}},
               finished_hours_ago=48)]
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
