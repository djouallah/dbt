"""Offline tests for the recorded sort key. No Fabric, no network, no credentials.

WHAT THIS PINS IS A SILENT GAP. The dashboard captions a sorted bar with the columns the run
ordered by, and the key is a property of the COMMIT — the model declared `['date','time','DUID']`
for a while and `['date','time']` since. A constant in the render layer was right for today's model
only, and captioned run 30955591822, a DUID sort, `by date, time`. Nothing errored; the page just
said something untrue.

So the run has to write its own key down, and every way that can quietly stop happening is here:
the model path resolving off the CWD, the regex missing a respelt config, `'auto'` being recorded as
if it named columns, and the merge landing under `layout` where the page does not read it.

    python -m pytest .github/scripts/test_sort_key.py -q
"""
import json
import os
import subprocess
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)


@pytest.fixture(autouse=True)
def _no_ambient_duckdb_env(monkeypatch):
    """Clear every DUCKDB_* var before each test.

    THE GATE MUST NOT BE A FUNCTION OF THE DISPATCH IT IS GATING. `benchmark.yml` puts
    `DUCKDB_SORTED` / `DUCKDB_SORT_BY` / the geometry into the job env, so a test that reads one
    without setting it asserts against whatever the human happened to type into the dispatch form.
    That is exactly what killed run 31073309328: a `sort_by=date,time,DUID` dispatch failed the free
    checks on a test that hardcoded `date,time`, and the run never reached a paid leg.
    """
    for k in ("DUCKDB_SORTED", "DUCKDB_SORT_BY", "DUCKDB_ROW_GROUP_SIZE", "DUCKDB_FILE_SIZE_MB"):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture(scope="module")
def stats():
    """`stats.py` with its Fabric-facing imports stubbed.

    It mints a token at MODULE level (`H = {...fabric_token()}`), so importing it for real either
    waits minutes on Azure or shells out to an `az` that is not installed. This gate runs before any
    leg spends capacity and has to stay in seconds, so `duckrun.auth` hands back a dummy.
    """
    duckrun = sys.modules.setdefault("duckrun", types.ModuleType("duckrun"))
    auth = sys.modules.setdefault("duckrun.auth", types.ModuleType("duckrun.auth"))
    auth.get_fabric_token = lambda *a, **k: "stub-token"
    duckrun.auth = auth
    sys.modules.setdefault("requests", types.ModuleType("requests"))
    os.environ.setdefault("WS_ID", "00000000-0000-0000-0000-000000000000")
    import stats as mod
    return mod


def test_every_name_stats_py_calls_is_defined(stats):
    """The one that would have saved run 31067443454.

    A rewrite of `declared_sort_key` replaced a text RANGE that `encoding_table` happened to sit
    inside, deleting it. Nothing here caught it: the function only prints to the step summary, so no
    test calls it, and `main()` is the only caller — which no offline test reaches either. The layout
    job then died on `NameError: name 'encoding_table' is not defined`, ten minutes into a paid run,
    after the build had already been paid for.

    This walks the AST and checks every bare-name call resolves to something: a def in this module,
    an import, a module-level binding, or a builtin. It costs milliseconds and catches the entire
    class — a deleted function, a typo'd call, an import dropped as "unused" that was not.
    """
    import ast
    import builtins
    src = open(os.path.join(HERE, "stats.py"), encoding="utf-8").read()
    tree = ast.parse(src)
    bound = set(dir(builtins))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            bound.update((a.asname or a.name).split(".")[0] for a in node.names)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
    called = {n.func.id for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert not (called - bound), f"stats.py calls undefined name(s): {sorted(called - bound)}"


def test_the_declared_key_comes_from_the_env_the_model_reads(stats, monkeypatch):
    """It used to regex a literal list out of `fct_summary.sql`. The model now renders `sort_by` from
    `DUCKDB_SORT_BY`, so there is no literal left to match and that regex would silently return {} —
    the same quiet gap this whole path exists to close. Reading the SAME env the model reads means
    the two cannot disagree."""
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_SORT_BY", "date,time")
    assert stats.declared_sort_key() == {"fct_summary": ["date", "time"]}
    monkeypatch.setenv("DUCKDB_SORT_BY", "date, time, DUID")     # spaces are the dispatch's problem
    assert stats.declared_sort_key() == {"fct_summary": ["date", "time", "DUID"]}
    monkeypatch.delenv("DUCKDB_SORT_BY")
    assert stats.declared_sort_key() == {"fct_summary": ["date", "time"]}, "the model's own default"


def test_an_unsorted_run_declares_no_key(stats, monkeypatch):
    """Recording one would caption an unsorted bar `by date, time`."""
    monkeypatch.setenv("DUCKDB_SORT_BY", "date,time")
    monkeypatch.delenv("DUCKDB_SORTED", raising=False)
    assert stats.declared_sort_key() == {}
    monkeypatch.setenv("DUCKDB_SORTED", "false")
    assert stats.declared_sort_key() == {}


def test_geometry_is_recorded_only_when_it_differs_from_the_default(stats, monkeypatch):
    """A default dispatch writes the parquet every earlier run wrote, so it must key to the SAME
    dashboard column. `variant()` skips null, so absence keeps the column; a value splits it."""
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "16000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "1024")
    assert stats._nondefault("DUCKDB_ROW_GROUP_SIZE", "16000000") is None
    assert stats._nondefault("DUCKDB_FILE_SIZE_MB", "1024") is None
    monkeypatch.setenv("DUCKDB_ROW_GROUP_SIZE", "4000000")
    monkeypatch.setenv("DUCKDB_FILE_SIZE_MB", "128")
    assert stats._nondefault("DUCKDB_ROW_GROUP_SIZE", "16000000") == "4000000"
    assert stats._nondefault("DUCKDB_FILE_SIZE_MB", "1024") == "128"
    # ...and neither is in force while the model declares no geometry at all.
    monkeypatch.setenv("DUCKDB_SORTED", "false")
    assert stats._nondefault("DUCKDB_ROW_GROUP_SIZE", "16000000") is None


def test_the_key_lands_at_the_top_LEVEL_dbt_branch_not_under_layout(stats, tmp_path, monkeypatch):
    """`build_doc`'s output is merged as `{"layout": doc}`, so a key placed inside it would render
    as `layout.dbt` — which `sortKeyOf` does not read. It has to be a sibling merge, and it must not
    go in `layout.config`, whose every entry the dashboard's `variant()` walks into column names."""
    import record
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setenv("DUCKDB_SORT_BY", "date,time")   # PINNED — never the dispatch's own value
    monkeypatch.setattr(stats, "build_doc", lambda *a, **k: {"stats": {}})
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    doc = json.loads(rec.read_text(encoding="utf-8"))
    assert doc["dbt"]["duckrun"]["sort_by"] == {"fct_summary": ["date", "time"]}
    assert "dbt" not in doc.get("layout", {}), "layout.dbt is invisible to the page"
    assert "sort_by" not in doc.get("layout", {}).get("config", {}).get("duckrun", {}), \
        "layout.config is walked by variant() — a commit-varying key would split the column"
    assert record  # imported for the merge under test


def _sample_parquet(tmp_path):
    """A file with one dictionary-encoded column and one that falls back to PLAIN, so the
    aggregation is exercised on both branches rather than on a uniform file."""
    import duckdb
    p = tmp_path / "p.parquet"
    duckdb.connect().execute(
        f"COPY (SELECT i::INT AS mw, (i%7)::VARCHAR AS duid FROM range(200000) t(i)) "
        f"TO '{p.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    return p


def _reader_over(stats, monkeypatch, rel):
    class R:
        def get_stats(self, table=None, detailed=False):
            return rel
    monkeypatch.setattr(stats, "reader", lambda guid: R())


def test_encodings_are_aggregated_per_column_not_per_chunk(stats, tmp_path, monkeypatch):
    """The record has to stay small: `parquet_metadata` is one row per column per row group, and
    iceberg's 1,172 row groups would be six figures of rows. One row per COLUMN is the contract."""
    import duckdb
    p = _sample_parquet(tmp_path)
    rel = duckdb.connect().sql(f"SELECT * FROM parquet_metadata('{p.as_posix()}')")
    _reader_over(stats, monkeypatch, rel)
    got = stats.encodings_for("guid", "mart.fct_summary")
    assert set(got) == {"mw", "duid"}, "one entry per column"
    assert got["duid"]["encodings"] == ["PLAIN_DICTIONARY"]
    assert got["duid"]["dict_pages"] == got["duid"]["chunks"], "every chunk wrote a dictionary"
    # The discriminating case: a high-cardinality column the writer gave up dictionary-encoding on.
    # If this ever reads PLAIN_DICTIONARY the measurement has stopped telling engines apart.
    assert got["mw"]["encodings"] == ["PLAIN"]
    assert got["mw"]["dict_pages"] == 0
    assert got["mw"]["mb"] > got["duid"]["mb"], "and it is the one that costs bytes"
    assert isinstance(got["mw"]["encodings"], list), "sets do not survive json.dump"


def test_the_profiled_table_is_schema_qualified(stats, monkeypatch):
    """A BARE name does not resolve. `get_stats()` with no argument sweeps every catalog and keys by
    table name, but `get_stats('fct_summary')` raises — a one-part name is looked up in the CURRENT
    schema, and dbt writes the mart to `mart`. Run 31008858454 hit exactly this: the layout job went
    green and the record simply had no `encodings`. The schema comes from `stats_for`, so the
    profiled table cannot drift from the one the rest of the document describes."""
    seen = []
    monkeypatch.setattr(stats, "find_guid", lambda kind, item: "guid-1")
    monkeypatch.setattr(stats, "stats_for",
                        lambda guid: {stats.MART: {"schema": "mart", "total_rows": 1}})
    monkeypatch.setattr(stats, "encodings_for",
                        lambda guid, table: seen.append(table) or {"mw": {}})
    guid, st, enc = stats.one_engine("dbt_spark", "lakehouses")
    assert seen == ["mart.fct_summary"], seen
    assert enc == {"mw": {}}


def test_a_mart_with_no_schema_is_skipped_rather_than_guessed(stats, monkeypatch):
    """No schema recorded means the aggregate read did not see the table at all. Guessing `mart`
    would send a name we have no evidence for and log a confusing resolution error."""
    monkeypatch.setattr(stats, "find_guid", lambda kind, item: "guid-1")
    monkeypatch.setattr(stats, "stats_for", lambda guid: {})
    monkeypatch.setattr(stats, "encodings_for",
                        lambda guid, table: pytest.fail("must not be called"))
    assert stats.one_engine("dbt_spark", "lakehouses")[2] == {}


def test_a_failed_or_empty_profile_is_absent_never_empty(stats, monkeypatch):
    """`{}` per column would read as "no encodings", which parquet cannot be. Absent means the
    layout job could not profile it — the same rule `landing` follows."""
    class Boom:
        def get_stats(self, table=None, detailed=False):
            raise RuntimeError("OneLake said no")
    monkeypatch.setattr(stats, "reader", lambda guid: Boom())
    assert stats.encodings_for("guid", "mart.fct_summary") == {}
    doc = stats.build_doc({}, ["duckrun"], {}, None, {"duckrun": {}})
    assert "encodings" not in doc, "nothing profiled -> no key at all"


def test_the_encodings_reach_the_document_under_their_engine(stats, tmp_path, monkeypatch):
    import duckdb
    p = _sample_parquet(tmp_path)
    rel = duckdb.connect().sql(f"SELECT * FROM parquet_metadata('{p.as_posix()}')")
    _reader_over(stats, monkeypatch, rel)
    enc = {"duckrun": stats.encodings_for("g", "mart.fct_summary"), "spark": {}}
    doc = stats.build_doc({}, ["duckrun", "spark"], {}, None, enc)
    assert doc["encodings"]["duckrun"]["mw"]["encodings"] == ["PLAIN"]
    assert "spark" not in doc["encodings"], "an engine that profiled nothing adds no column"
    json.dumps(doc, default=str)      # the record is written with json.dump


def test_an_unsorted_run_records_no_key_at_all(stats, tmp_path, monkeypatch):
    """Absence is what tells the page a run wrote unsorted parquet. A key here would caption an
    unsorted bar `by date, time`."""
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.delenv("DUCKDB_SORTED", raising=False)
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    assert "dbt" not in json.loads(rec.read_text(encoding="utf-8"))
