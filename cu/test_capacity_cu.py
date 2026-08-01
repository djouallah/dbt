"""Offline tests for capacity_cu.py. No network, no token, no Fabric — `python -m pytest cu/ -q`.

Why this exists at all, in a directory whose whole point is being deletable: the report is now built
from two different run-allocation rules (exact by GUID for a redeployed item, by hour for one that
outlives a run), and both of them fail the same way when they are wrong — a number that is plausible,
printed with confidence, and off. Everything here is pure post-processing of rows already in hand, so
it is testable without spending a single CU, and the alternative to testing it is dispatching against
paid capacity to find out.

It imports capacity_cu and nothing else, so `rm -rf cu/` still removes every trace.
"""
import importlib
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

H = datetime(2026, 8, 1, 10, 0)


def load(**env):
    """Import capacity_cu with a given environment. Its config is module-level, so a test that
    changes CU_ETL has to reload rather than set an attribute."""
    for k in [k for k in os.environ if k.startswith("CU_")] + ["PBI_TOKEN", "FABRIC_TOKEN",
                                                               "STATS_JSON"]:
        os.environ.pop(k, None)
    os.environ.update({"PBI_TOKEN": "tok", "CU_METRICS_WORKSPACE_ID": "ws",
                       "CU_METRICS_MODEL_ID": "mdl", "CU_CAPACITY_ID": "cap",
                       "CU_REFRESH": "0", **env})
    import capacity_cu
    return importlib.reload(capacity_cu)


def hours(*offsets):
    return [H + timedelta(hours=o) for o in offsets]


# --------------------------------------------------------------------------- sessionize

def test_generational_items_still_split_inside_one_hour():
    """The rule this file was built on: two benchmark dispatches in the same hour are two runs,
    because each redeploys and so mints a new GUID for the same name."""
    m = load()
    runs = m.sessionize([
        ("G1", "aemo_duckrun", H, True),
        ("G2", "aemo_duckrun", H, True),   # same name, new GUID => next run
    ], gap_hours=2)
    assert len(runs) == 2
    assert [r["items"] for r in runs] == [{"G1"}, {"G2"}]


def test_long_lived_etl_item_is_split_across_runs_by_hour():
    """A lakehouse has ONE GUID for years, so the generation rule can say nothing about it. Its
    hours must follow the run windows instead — the failure mode being that all of its CU lands in
    whichever run happened to be read first."""
    m = load()
    runs = m.sessionize([
        ("G1", "aemo_duckrun", H, True),
        ("G2", "aemo_duckrun", H + timedelta(hours=8), True),
        ("LH", "dbt_delta", H, False),
        ("LH", "dbt_delta", H + timedelta(hours=8), False),
    ], gap_hours=2)
    assert len(runs) == 2
    assert ("LH", H) in runs[0]["pairs"]
    assert ("LH", H + timedelta(hours=8)) in runs[1]["pairs"]
    assert ("LH", H + timedelta(hours=8)) not in runs[0]["pairs"]


def test_etl_only_activity_forms_its_own_run():
    """A dbt build with no benchmark beside it — no semantic model activity at all, so no window to
    attach to. It must still be a run, or the build's CU is reported in no column."""
    m = load()
    runs = m.sessionize([("LH", "dbt_delta", h, False) for h in hours(0, 1)]
                        + [("WH", "dbt_dwh", h, False) for h in hours(0, 1)], gap_hours=2)
    assert len(runs) == 1
    assert runs[0]["labels"] == {"dbt_delta", "dbt_dwh"}
    assert len(runs[0]["pairs"]) == 4


def test_etl_only_activity_splits_on_the_gap():
    m = load()
    runs = m.sessionize([("LH", "dbt_delta", h, False) for h in hours(0, 1, 9, 10)], gap_hours=2)
    assert len(runs) == 2
    assert [len(r["hours"]) for r in runs] == [2, 2]


def test_gap_zero_is_one_run_holding_everything():
    m = load()
    runs = m.sessionize([("G1", "aemo_duckrun", H, True), ("LH", "dbt_delta", H, False)],
                        gap_hours=0)
    assert len(runs) == 1
    assert runs[0]["pairs"] == {("G1", H), ("LH", H)}


def test_every_event_lands_in_exactly_one_run():
    """The conservation property the two tables are read against: no pair dropped, none duplicated.
    A duplicated pair double-counts CU; a dropped one makes the run columns silently total less than
    the aggregate above them."""
    m = load()
    events = ([("G1", "aemo_duckrun", h, True) for h in hours(0, 1)]
              + [("G2", "aemo_duckrun", h, True) for h in hours(9, 10)]
              + [("LH", "dbt_delta", h, False) for h in hours(0, 1, 5, 9, 10, 30)]
              + [("WH", "dbt_dwh", h, False) for h in hours(30,)])
    runs = m.sessionize(events, gap_hours=2)
    seen = [p for r in runs for p in r["pairs"]]
    assert len(seen) == len(set(seen))
    assert set(seen) == {(iid, h) for iid, _l, h, _g in events}


def test_runs_come_back_oldest_first():
    m = load()
    runs = m.sessionize([("LH", "dbt_delta", h, False) for h in hours(30, 0, 60)], gap_hours=2)
    assert [r["hours"][0] for r in runs] == sorted(r["hours"][0] for r in runs)


# --------------------------------------------------------------------------- tables

META = {
    "aemo_duckrun": {"label": "aemo_duckrun", "cls": "analytics", "kind": "SemanticModel",
                     "gen": True},
    "dbt_delta": {"label": "dbt_delta", "cls": "etl", "kind": "Lakehouse", "gen": False},
    "duckrun-py-*": {"label": "duckrun-py-*", "cls": "etl", "kind": "Notebook", "gen": False},
}


def _cells():
    return {("aemo_duckrun", "Query"): 100.0, ("aemo_duckrun", "Refresh"): 10.0,
            ("dbt_delta", "OneLake Write"): 50.0, ("dbt_delta", "OneLake Read"): 5.0,
            ("duckrun-py-*", "Spark Job"): 25.0}


def test_op_table_totals_and_class_subtotals(capsys):
    m = load(CU_MODELS="", CU_ETL="1")
    m._op_table(_cells(), META)
    out = capsys.readouterr().out
    assert "| **analytics** |" in out and "| **etl** |" in out
    assert "**110.0**" in out          # analytics subtotal
    assert "**80.0**" in out           # etl subtotal
    assert "**190.0**" in out          # grand total


def test_op_columns_fold_past_the_cap(capsys):
    """The readability guard, and it must be honest: folded columns are named and counted."""
    m = load(CU_MODELS="", CU_OP_COLS="2")
    cells = {("dbt_delta", f"op{i}"): float(10 - i) for i in range(6)}
    m._op_table(cells, {"dbt_delta": {"label": "dbt_delta", "cls": "etl", "kind": "Lakehouse",
                                      "gen": False}})
    out = capsys.readouterr().out
    assert "other (4 ops)" in out
    assert "folded into `other`" in out
    assert "**45.0**" in out           # 10+9+8+7+6+5, nothing lost to the fold


def test_named_models_are_printed_even_with_no_activity(capsys):
    """A model that vanishes from the table looks identical to one that was never deployed."""
    m = load(CU_MODELS="aemo_duckrun,aemo_spark")
    m._op_table(_cells(), META)
    out = capsys.readouterr().out
    assert "| aemo_spark | " in out and "**0.0**" in out


def test_display_label_collapses_the_throwaway_notebooks():
    m = load()
    assert m.display_label("duckrun-py-3f2a1b") == "duckrun-py-*"
    assert m.display_label("dbt-duckrun-9c1e") == "dbt-duckrun-*"
    assert m.display_label("dbt-iceberg-9c1e") == "dbt-iceberg-*"
    assert m.display_label("dbt_delta") == "dbt_delta"


def test_a_fabric_notebook_is_etl_under_every_spelling():
    """Measured on run 30676341725: the app calls a Fabric notebook `SynapseNotebook` for most of
    the CU and `Notebook` for the rest. Missing the first put 103,157 CU in `other`."""
    m = load()
    assert m.classify("SynapseNotebook") == "etl"
    assert m.classify("Notebook") == "etl"
    assert m.classify("JupyterNotebook") == "etl"


def test_a_collapsed_group_takes_the_known_class_not_the_first_one(capsys):
    """Same measurement, other half: one collapsed group held both spellings, so first-wins made the
    group's class depend on row order. A known class must beat `other`."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    rows = [_row("NB1", "Jupyter Notebook Scheduled Run", 100.0, H),
            _row("NB2", "Jupyter Notebook Scheduled Run", 50.0, H)]
    items = [{"Id": "NB1", "Name": "duckrun-py-aaa", "Kind": "SomethingUnmapped"},
             {"Id": "NB2", "Name": "duckrun-py-bbb", "Kind": "Notebook"}]
    _stub(m, rows, items)
    m.main()
    out = capsys.readouterr().out
    assert "| **etl** |" in out and "| **other** |" not in out
    # Both notebooks are `duckrun-py-*`, which no engine can claim, so the whole 150 is `shared`.
    assert "| **etl** | **0.0** | **0.0** | **0.0** | **0.0** | **0.0** | **150.0** |" in out


def test_classify_is_case_and_space_insensitive_and_keeps_strangers():
    m = load()
    assert m.classify("Semantic Model") == "analytics"
    assert m.classify("SemanticModel") == "analytics"
    assert m.classify("Lakehouse") == "etl"
    assert m.classify("Notebook") == "etl"
    assert m.classify("Warehouse") == "etl"
    assert m.classify("SomethingFabricInventedLastWeek") == "other"
    assert m.classify("") == "other"


def test_run_table_class_rows_agree_with_the_engine_rows(capsys):
    """The class subtotal is computed from the same `per` as the engine rows under it; if the two
    ever disagree the report contradicts itself on one page."""
    m = load(CU_MODELS="")
    hourly = {}
    for key, iid, h, cu in [
            ("aemo_duckrun", "G1", H, 100.0),
            ("aemo_duckrun", "G2", H + timedelta(hours=8), 120.0),
            ("dbt_delta", "LH", H, 40.0),
            ("dbt_delta", "LH", H + timedelta(hours=8), 60.0)]:
        hourly[(key, "op", h, iid)] = cu
    meta = {k: dict(v, engine=m.engine_of(k)) for k, v in META.items()}
    runs = m.sessionize(((iid, k, h, meta[k]["gen"]) for (k, _o, h, iid) in hourly), gap_hours=2)
    m.render_runs(hourly, runs, meta, cells={k[:2]: v for k, v in hourly.items()})
    out = capsys.readouterr().out
    assert "Runs detected: 2" in out
    assert "| **etl** | **40.0** | **60.0** |" in out
    assert "| **analytics** | **100.0** | **120.0** |" in out
    # Both the lakehouse and the model are duckrun's, so each class has exactly one engine row.
    assert out.count("| duckrun | 40.0 | 60.0 |") == 1
    assert out.count("| duckrun | 100.0 | 120.0 |") == 1
    assert "**total**" not in out          # summing across runs answers nothing


def test_engine_of_maps_every_item_this_repo_creates():
    m = load()
    assert m.engine_of("dbt_delta") == "duckrun"          # the alias that matters
    assert m.engine_of("aemo_duckrun") == "duckrun"
    assert m.engine_of("dbt-duckrun-*") == "duckrun"
    assert m.engine_of("dbt_iceberg") == "iceberg"
    assert m.engine_of("dbt_spark") == "spark"
    assert m.engine_of("aemo_dwh") == "dwh"
    assert m.engine_of("dbt_landing") == "landing"        # a stage, not an engine, but a column
    # dwh reads the landed CSVs through a shortcut in this lakehouse — the only item the shortcut
    # scheme adds, because a warehouse has no `Files` of its own (provision.py DWH_SRC). The other
    # three legs put the shortcut inside the output lakehouse they already have, so they need no
    # new name here. THIS is why it is `_src` and not `_landing`: engine_of tries CU_ENGINES in
    # order, `landing` is first, and `dbt_dwh_landing` would put dwh's landing reads straight back
    # into the `landing` column — the exact hole the shortcut exists to close.
    assert m.engine_of("dbt_dwh_src") == "dwh"
    assert m.engine_of("dbt_dwh_landing") == "landing"    # the trap, pinned so it stays known
    # Genuinely ambiguous — a wrong column is worse than an honest `shared`.
    assert m.engine_of("duckrun-py-*") is None
    assert m.engine_of("") is None


def test_landing_column_says_it_is_a_stage_not_an_engine(capsys):
    """It gets a column so the download's WRITE is visible, not so it can be compared with an
    engine's, and the table has to say so where the number is read. The note used to claim the
    column could never be split; the legs' reads now sit in the engine columns, via a shortcut
    each, so it has to say where they went instead of repeating a fact that stopped being one."""
    m = load(CU_MODELS="")
    meta = {"dbt_landing": {"label": "dbt_landing", "cls": "etl", "kind": "Lakehouse",
                            "gen": False, "engine": m.engine_of("dbt_landing")}}
    m._engine_table({("dbt_landing", "OneLake Write"): 40.0}, meta)
    out = capsys.readouterr().out
    assert out.splitlines()[0].startswith("| CU (s) | landing | duckrun |")
    assert "| **etl** | **40.0** |" in out
    assert "`landing` is a STAGE, not an engine" in out
    assert "cannot be split between them" not in out
    assert "`Files/landing` shortcut in its own lakehouse" in out


def test_engine_table_puts_operations_down_and_engines_across(capsys):
    m = load(CU_MODELS="")
    meta = {k: dict(v, engine=m.engine_of(k)) for k, v in META.items()}
    m._engine_table(_cells(), meta)
    out = capsys.readouterr().out
    head = out.splitlines()[0]
    assert head == "| CU (s) | landing | duckrun | iceberg | spark | dwh | shared |"
    assert "| **etl** |" in out and "| **analytics** |" in out
    assert "| OneLake Write | 0.0 | 50.0 | 0.0 | 0.0 | 0.0 | 0.0 |" in out
    assert "| Spark Job | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 25.0 |" in out   # ambiguous notebook
    # duckrun 110 analytics + 55 etl, shared 25 — as class subtotals, never as a grand total.
    assert "| **analytics** | **0.0** | **110.0** | **0.0** | **0.0** | **0.0** | **0.0** |" in out
    assert "| **etl** | **0.0** | **55.0** | **0.0** | **0.0** | **0.0** | **25.0** |" in out
    assert "**total**" not in out           # summing across engines answers nothing
    assert "`shared` is CU no engine can be given" in out
    assert "genuinely ambiguous" in out          # the duckrun-py-* explanation, since it is present
    assert "dbt_landing" not in out              # ...and not the one that is absent


# --------------------------------------------------------------------------- hardware

def test_hardware_reports_what_the_run_recorded(capsys):
    m = load(CU_MODELS="")
    m.render_hardware({"run": {"id": "123"}, "config": {
        "duckrun": {"vcores": "64"}, "iceberg": {"vcores": "64"},
        "spark": {"resource_profile": "readHeavyForPBI", "native_execution_engine": "true"}}})
    out = capsys.readouterr().out
    assert "| duckrun | 64 vCores (Fabric Python notebook) |" in out
    assert "resource profile `readHeavyForPBI`, native execution engine ON" in out
    assert "Fabric Warehouse exposes no per-run compute knob" in out


def test_hardware_says_not_recorded_rather_than_guessing(capsys):
    """A default filled in here would read exactly like a measurement — which is the whole failure
    this section exists to avoid."""
    m = load(CU_MODELS="")
    m.render_hardware({"run": {"id": "123"}, "config": {}})
    out = capsys.readouterr().out
    assert out.count("not recorded by dbt run 123") == 3      # duckrun, iceberg, spark
    assert "64" not in out and "writeHeavy" not in out


# --------------------------------------------------------------------------- history

def test_history_record_round_trips_the_numbers(tmp_path, capsys):
    """The record is what survives retention, so it has to hold the same numbers the page printed —
    not a rendering of them. Written, read back, compared against the table."""
    out_file = tmp_path / "h" / "rec.json"
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1",
             CU_HISTORY_JSON=str(out_file))
    rows = [_row("LH", "OneLake Write", 40.0, H), _row("G1", "XMLA Read Operation", 100.0, H)]
    items = [{"Id": "LH", "Name": "dbt_delta", "Kind": "Lakehouse"},
             {"Id": "G1", "Name": "aemo_duckrun", "Kind": "SemanticModel"}]
    _stub(m, rows, items)
    m.main()
    capsys.readouterr()
    import json as _json
    rec = _json.loads(out_file.read_text(encoding="utf-8"))
    assert rec["schema"] == 1
    assert rec["cu"]["etl"]["duckrun"]["OneLake Write"] == 40.0
    assert rec["cu"]["analytics"]["duckrun"]["XMLA Read Operation"] == 100.0
    assert "since" in rec and "runs" in rec


def test_no_history_is_written_without_the_env(tmp_path, capsys):
    """A standalone dispatch measures an arbitrary window; filing that as a generation would poison
    the history with records that mean something else."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    _stub(m, [_row("LH", "OneLake Write", 40.0, H)],
          [{"Id": "LH", "Name": "dbt_delta", "Kind": "Lakehouse"}])
    m.main()
    capsys.readouterr()
    assert not list(tmp_path.iterdir())


# --------------------------------------------------------------------------- empty report

def test_empty_report_explains_itself_instead_of_raising(capsys):
    """render_empty was CALLED and never defined — the diagnosis path raised NameError. An empty
    report and an idle capacity print the same otherwise, which is the wrong conclusion drawn
    confidently."""
    m = load()
    m.render({}, {}, {}, None, datetime(2026, 8, 1, 12, 0), seen=7,
             dropped={"workspace": 7, "workspace_blank": 0, "name": 0, "kind": 0},
             active={("some-model", "SemanticModel", "OTHER-WS"): 12.0},
             near={("aemo_spark", "SemanticModel", "OTHER-WS"): 30.0})
    out = capsys.readouterr().out
    assert "No item activity" in out
    assert "7" in out and "aemo_spark" in out


def test_empty_with_no_rows_at_all_says_the_capacity_was_idle(capsys):
    m = load()
    m.render({}, {}, {}, None, datetime(2026, 8, 1, 12, 0), seen=0)
    assert "no rows at all" in capsys.readouterr().out


# --------------------------------------------------------------------------- end to end

SCHEMA = ([{"Table": "Metrics By Item Operation And Hour", "Name": n}
           for n in ("Item Id", "Workspace Id", "Operation name", "CU (s)", "Datetime")]
          + [{"Table": "Items", "Name": n} for n in ("Item Id", "Item name", "Item kind")])

WS_ID = "AAAA1111-0000-0000-0000-000000000000"


def _row(iid, op, cu, h):
    return {"Item Id": iid, "Workspace Id": WS_ID, "Operation name": op, "CU": cu,
            "Datetime": h.strftime("%Y-%m-%dT%H:%M:%S")}


def _stub(m, rows, items):
    def execute_dax(dax, tries=4, fatal=True):
        if "INFO.VIEW.COLUMNS" in dax:
            return SCHEMA
        if "'Items'" in dax:
            return items
        return rows
    m.execute_dax = execute_dax
    m.datasets_in_workspace = lambda ws: {}
    m.fabric_items = lambda ws: {}


def test_end_to_end_reports_etl_and_analytics(capsys):
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="aemo_duckrun", CU_ETL="1")
    rows = [_row("G1", "Query", 100.0, H),
            _row("LH", "OneLake Write", 40.0, H),
            _row("NB1", "Spark Job", 7.0, H),
            _row("NB2", "Spark Job", 3.0, H)]
    items = [{"Id": "G1", "Name": "aemo_duckrun", "Kind": "SemanticModel"},
             {"Id": "LH", "Name": "dbt_delta", "Kind": "Lakehouse"},
             {"Id": "NB1", "Name": "duckrun-py-aaa", "Kind": "Notebook"},
             {"Id": "NB2", "Name": "duckrun-py-bbb", "Kind": "Notebook"}]
    _stub(m, rows, items)
    m.main()
    out = capsys.readouterr().out
    assert "Capacity units" in out
    # landing | duckrun's lakehouse (40) | ... | the two ambiguous notebooks (10, in `shared`).
    assert "| **etl** | **0.0** | **40.0** | **0.0** | **0.0** | **0.0** | **10.0** |" in out
    assert "| **analytics** | **0.0** | **100.0** | **0.0** | **0.0** | **0.0** | **0.0** |" in out
    assert "**total**" not in out


def test_end_to_end_etl_off_reproduces_the_old_scope(capsys):
    """CU_ETL=0 has to be a true revert, or an older dispatch's numbers cannot be compared."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="aemo_duckrun", CU_ETL="0")
    rows = [_row("G1", "Query", 100.0, H), _row("LH", "OneLake Write", 40.0, H)]
    items = [{"Id": "G1", "Name": "aemo_duckrun", "Kind": "SemanticModel"},
             {"Id": "LH", "Name": "dbt_delta", "Kind": "Lakehouse"}]
    _stub(m, rows, items)
    m.main()
    out = capsys.readouterr().out
    assert "Capacity units — semantic models only" in out
    assert "dbt_delta" not in out
    assert "**100.0**" in out


def test_the_two_duckdb_legs_land_in_their_own_columns(capsys):
    """The point of naming the notebooks per engine: duckrun's default `duckrun-py-<runid>` was
    identical for both legs, so their compute could only ever be one undivided `shared` number."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    rows = [_row("NB1", "Notebook Run", 300.0, H), _row("NB2", "Notebook Run", 200.0, H)]
    items = [{"Id": "NB1", "Name": "dbt-duckrun-aaaa", "Kind": "Notebook"},
             {"Id": "NB2", "Name": "dbt-iceberg-bbbb", "Kind": "Notebook"}]
    _stub(m, rows, items)
    m.main()
    out = capsys.readouterr().out
    assert "| **etl** | **0.0** | **300.0** | **200.0** | **0.0** | **0.0** |" in out
    assert "shared" not in out       # both attributable, so no shared column at all


def test_a_collapsed_name_repeating_is_the_next_run(capsys):
    """A throwaway notebook is recreated per dbt build, so a repeated collapsed name dates a run
    exactly the way a redeployed semantic model does — without it, two builds an hour apart would
    be one column."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    later = H + timedelta(hours=1)
    rows = [_row("NB1", "Notebook Run", 300.0, H), _row("NB2", "Notebook Run", 200.0, later),
            _row("LH", "OneLake Write", 10.0, H), _row("LH", "OneLake Write", 20.0, later)]
    items = [{"Id": "NB1", "Name": "dbt-duckrun-aaaa", "Kind": "Notebook"},
             {"Id": "NB2", "Name": "dbt-duckrun-bbbb", "Kind": "Notebook"},
             {"Id": "LH", "Name": "dbt_delta", "Kind": "Lakehouse"}]
    _stub(m, rows, items)
    m.main()
    out = capsys.readouterr().out
    assert "Runs detected: 2" in out
    # Notebook + lakehouse are both duckrun's, and the lakehouse follows the windows the notebooks
    # formed — an hour each — so the column totals are 310 and 220, not 510 in one.
    assert "| duckrun | 310.0 | 220.0 |" in out


def test_end_to_end_keeps_an_unnamed_item_under_its_guid(capsys):
    """The lagging-'Items' trap from the other side: an item nothing can name must keep its CU."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    _stub(m, [_row("MYSTERY-GUID", "Spark Job", 12.0, H)], [])
    m.main()
    out = capsys.readouterr().out
    assert "mystery-guid" in out.lower()
    assert "**12.0**" in out


# --------------------------------------------------------------------------- the page

def _html_of(md):
    import report_html
    return report_html.page(md, footer="run 1 (`abc123`)")


def test_page_renders_the_report_and_needs_no_network():
    """The page is a build artifact: it has to open off a local disk with no network, years later.
    So nothing external — no CDN, no font, no image, no script."""
    md = ("## Capacity units — since 2026-08-01 10:00\n\n"
          "| | landing | duckrun | total |\n|:--|---:|---:|---:|\n"
          "| **etl** | **300.0** | **215.0** | **515.0** |\n"
          "| OneLake Write | 240.0 | 120.0 | 360.0 |\n")
    out = _html_of(md)
    assert "<h2>Capacity units — since 2026-08-01 10:00</h2>" in out
    assert '<th class="right">landing</th>' in out
    assert '<tr class="sub">' in out                       # the bold subtotal row is marked
    assert '<td class="right">240.0</td>' in out           # numbers right-aligned, unreformatted
    for forbidden in ("http://", "https://", "<script", "src=", "@import"):
        assert forbidden not in out


def test_chart_marker_is_invisible_in_markdown_but_draws_in_html(capsys):
    """The same markdown goes to the GitHub job summary, which sanitises inline SVG. A comment
    keeps the summary clean and still carries the numbers to the page."""
    m = load(CU_MODELS="")
    m._chart("ETL", "lower is better", [["duckrun", 10.0], ["dwh", 5.0]])
    md = capsys.readouterr().out
    assert md.strip().startswith("<!--chart:") and md.strip().endswith("-->")

    import report_html
    svg = report_html.to_html(md)
    assert "<svg" in svg and 'class="bar"' in svg
    # Bars start at zero and carry their value at the tip; the tooltip needs no script.
    assert svg.count('<path class="bar" d="M0,0') == 2
    assert ">10.0<" in svg and "<title>duckrun: 10.0 CU</title>" in svg


def test_chart_ranks_cheapest_first_and_sinks_the_zeros(capsys):
    """`lower is better` makes the ranking the finding. A zero is the exception: it means the engine
    did no such work, and at the top under that caption it would read as the winner."""
    m = load(CU_MODELS="")
    m._chart("ETL", "lower is better",
             [["spark", 30.0], ["duckrun", 10.0], ["iceberg", 0.0], ["dwh", 20.0]])
    import json as _json
    spec = _json.loads(capsys.readouterr().out.strip()[len("<!--chart:"):-len("-->")])
    assert [r[0] for r in spec["rows"]] == ["duckrun", "dwh", "spark", "iceberg"]


def test_a_chart_of_all_zeros_is_not_drawn(capsys):
    """Four bars of length zero is a picture of nothing, and it reads as a broken chart rather than
    as an idle engine. The table still carries the zeros."""
    m = load(CU_MODELS="")
    m._chart("ETL", "lower is better", [["duckrun", 0.0], ["dwh", 0.0]])
    assert capsys.readouterr().out.strip() == ""


def test_bar_geometry_is_square_at_the_baseline_and_rounded_at_the_tip():
    import report_html
    assert report_html._bar_path(100, 18).startswith("M0,0 H96.0 A4,4")
    # Shorter than the corner radius: no arc, or the path would curl back on itself.
    assert "A" not in report_html._bar_path(2, 18)


def test_page_escapes_before_it_formats():
    """Item names come from a REST API. A `<` in one must not become markup."""
    import report_html
    out = report_html.to_html("| item |\n|:--|\n| <img src=x> |\n")
    assert "&lt;img src=x&gt;" in out
    assert "<img" not in out


def test_page_keeps_multiline_notes_as_one_paragraph():
    """The report wraps its <sub> notes at ~100 columns; the page must not print five paragraphs."""
    import report_html
    out = report_html.to_html("<sub>first line\nsecond line\nthird line</sub>\n")
    assert out.count('<p class="note">') == 1
    assert "first line second line third line" in out


def test_page_survives_the_whole_report(capsys):
    """End to end: the real renderer over the real report, not a hand-written fragment."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_ETL="1")
    _stub(m, [_row("LH", "OneLake Write", 40.0, H)],
          [{"Id": "LH", "Name": "dbt_landing", "Kind": "Lakehouse"}])
    m.main()
    out = _html_of(capsys.readouterr().out)
    assert out.startswith("<!doctype html>")
    assert "<table>" in out and "</html>" in out
    assert "landing" in out and "STAGE, not an engine" in out
    assert "<footer>" in out


def test_end_to_end_dies_if_the_since_filter_did_not_bind(capsys):
    """A DAX filter that is accepted and then ignored is this tool's worst failure: a plausible
    wrong total. It has happened once already."""
    m = load(CU_WORKSPACE_FILTER=WS_ID, CU_MODELS="", CU_SINCE="2026-08-01T09:00:00")
    _stub(m, [_row("G1", "Query", 5.0, H - timedelta(hours=5))],
          [{"Id": "G1", "Name": "aemo_duckrun", "Kind": "SemanticModel"}])
    with pytest.raises(SystemExit):
        m.main()
