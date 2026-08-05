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


def test_the_declared_key_is_read_from_the_real_model(stats):
    """The one that would go stale: this asserts against the model in the tree, not a fixture."""
    assert stats.declared_sort_key() == {"fct_summary": ["date", "time"]}


def test_the_model_path_does_not_depend_on_the_cwd(stats, tmp_path, monkeypatch):
    """CI runs `python .github/scripts/stats.py` from the root today. If that ever changes, a
    CWD-relative path returns {} and the key silently stops being recorded."""
    monkeypatch.chdir(tmp_path)
    assert stats.declared_sort_key() == {"fct_summary": ["date", "time"]}


@pytest.mark.parametrize("sha,expected", [
    ("81a6c26", ["date", "time", "DUID"]),   # the manual DUID sort — run 30955591822
    ("950a92b", ["date", "time"]),
])
def test_it_reads_the_key_each_historical_commit_declared(stats, sha, expected, monkeypatch):
    """The key changed under this repo's feet twice. Parse what each commit actually said."""
    src = subprocess.run(["git", "show", f"{sha}:models/duckdb/marts/fct_summary.sql"],
                         cwd=ROOT, capture_output=True, text=True)
    if src.returncode:
        pytest.skip(f"{sha} not in this clone")
    m = stats._SORT_LITERAL.search(src.stdout)
    assert m, f"{sha} declares a literal key and the regex missed it"
    assert [c.strip().strip("'\"") for c in m.group(1).split(",") if c.strip()] == expected


def test_auto_is_left_to_the_log_scrape(stats):
    """`sort_by='auto'` names no columns. Recording the word `auto` as if it were a key is the same
    class of mistake as the constant was — `fabric_run.py` scrapes duckrun's resolved answer."""
    auto = subprocess.run(["git", "show", "a83767d:models/duckdb/marts/fct_summary.sql"],
                          cwd=ROOT, capture_output=True, text=True)
    if auto.returncode:
        pytest.skip("a83767d not in this clone")
    assert stats._SORT_LITERAL.search(auto.stdout) is None


def test_a_respelt_config_records_nothing_rather_than_something_wrong(stats, monkeypatch, tmp_path):
    """A model that stops matching must yield {} — absent reads as "not recorded" on the page, which
    is honest. A partial match that invented a key would not be."""
    model = tmp_path / "fct_summary.sql"
    model.write_text("{{ config(materialized='incremental', sort_by=SOMETHING_ELSE) }}\nselect 1",
                     encoding="utf-8")
    monkeypatch.setattr(stats, "_SORT_MODEL", model)
    assert stats.declared_sort_key() == {}
    monkeypatch.setattr(stats, "_SORT_MODEL", tmp_path / "gone.sql")
    assert stats.declared_sort_key() == {}


def test_the_key_lands_at_the_top_LEVEL_dbt_branch_not_under_layout(stats, tmp_path, monkeypatch):
    """`build_doc`'s output is merged as `{"layout": doc}`, so a key placed inside it would render
    as `layout.dbt` — which `sortKeyOf` does not read. It has to be a sibling merge, and it must not
    go in `layout.config`, whose every entry the dashboard's `variant()` walks into column names."""
    import record
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.setenv("DUCKDB_SORTED", "true")
    monkeypatch.setattr(stats, "build_doc", lambda *a, **k: {"stats": {}})
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    doc = json.loads(rec.read_text(encoding="utf-8"))
    assert doc["dbt"]["duckrun"]["sort_by"] == {"fct_summary": ["date", "time"]}
    assert "dbt" not in doc.get("layout", {}), "layout.dbt is invisible to the page"
    assert "sort_by" not in doc.get("layout", {}).get("config", {}).get("duckrun", {}), \
        "layout.config is walked by variant() — a commit-varying key would split the column"
    assert record  # imported for the merge under test


def test_an_unsorted_run_records_no_key_at_all(stats, tmp_path, monkeypatch):
    """Absence is what tells the page a run wrote unsorted parquet. A key here would caption an
    unsorted bar `by date, time`."""
    rec = tmp_path / "run.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.delenv("DUCKDB_SORTED", raising=False)
    stats.write_json({"stats": {"duckrun": {}}}, ["duckrun"])
    assert "dbt" not in json.loads(rec.read_text(encoding="utf-8"))
