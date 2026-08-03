"""Offline tests for the run record. No Fabric, no network, no credentials.

What these pin is the part that fails SILENTLY. A fragment that never lands, an item overwritten by
a later stage, or a merge order that drops the deletion timestamp all produce a record that looks
fine and attributes CU to the wrong run — or to nothing. None of that shows up as an error.

    python -m pytest .github/scripts/test_record.py -q
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import record  # noqa: E402


@pytest.fixture(autouse=True)
def _no_ambient_record(monkeypatch):
    """RUN_RECORD leaking in from the environment would make every test write to one real file."""
    monkeypatch.delenv("RUN_RECORD", raising=False)


def test_unset_run_record_is_a_no_op():
    """provision.py and stats.py must stay runnable by hand — recording is opt-in, never required."""
    assert record.merge({"items": {"A": {}}}) is None
    assert record.item("A", "output", "Lakehouse", "dbt_spark") is None


def test_item_writes_under_the_upper_cased_guid(tmp_path, monkeypatch):
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.item("abc-DEF", "output", "Lakehouse", "dbt_spark", created=True)
    doc = json.loads(p.read_text(encoding="utf-8"))
    # The metrics model returns item ids upper-cased; normalising at write time is what makes the
    # join a dict lookup instead of a case-insensitive scan.
    assert list(doc["items"]) == ["ABC-DEF"]
    assert doc["items"]["ABC-DEF"]["name"] == "dbt_spark"


def test_a_blank_guid_is_dropped_not_recorded_as_null(tmp_path, monkeypatch):
    """A null key would join to nothing and read as an item nobody can find."""
    monkeypatch.setenv("RUN_RECORD", str(tmp_path / "frag.json"))
    assert record.item(None, "compute", "Notebook", "dbt-spark-ab12") is None
    assert not (tmp_path / "frag.json").exists()


def test_deletion_merges_onto_creation_rather_than_replacing_it(tmp_path, monkeypatch):
    """The teardown fragment carries only `deleted`; the created/name/role fields must survive it.

    This is the whole reason `items` is a dict keyed by GUID and not a list — a deep merge unions
    dicts and REPLACES lists, so a list would have made the last writer win the entire entry.
    """
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.item("G1", "output", "Lakehouse", "dbt_spark", created=True, at="2026-08-02T10:00:00")
    record.item("G1", "output", "Lakehouse", "dbt_spark", deleted="2026-08-02T12:00:00")
    it = json.loads(p.read_text(encoding="utf-8"))["items"]["G1"]
    assert it == {"role": "output", "kind": "Lakehouse", "name": "dbt_spark", "created": True,
                  "at": "2026-08-02T10:00:00", "deleted": "2026-08-02T12:00:00"}


def test_fragments_sort_by_basename_not_by_path(tmp_path):
    """download-artifact nests each artifact in its OWN directory, so the full paths sort by
    artifact name and the numeric prefix's ordering would be lost."""
    for d, n in (("z-artifact", "record-00-run.json"), ("a-artifact", "record-30-layout.json")):
        (tmp_path / d).mkdir()
        (tmp_path / d / n).write_text("{}", encoding="utf-8")
    assert [os.path.basename(f) for f in record.fragments([str(tmp_path)])] == [
        "record-00-run.json", "record-30-layout.json"]


def _frag(tmp_path, name, obj):
    d = tmp_path / "fragments" / name.replace(".json", "")
    d.mkdir(parents=True)
    (d / name).write_text(json.dumps(obj), encoding="utf-8")


def test_finish_merges_every_stage_into_one_document(tmp_path):
    _frag(tmp_path, "record-00-run.json", {"schema": 1, "engine": "spark",
                                           "run": {"id": "1", "started": "T0"}})
    _frag(tmp_path, "record-10-land.json",
          {"items": {"L": {"role": "landing", "name": "dbt_landing", "created": False}}})
    _frag(tmp_path, "record-20-build-spark.json",
          {"items": {"O": {"role": "output", "name": "dbt_spark", "created": True},
                     "N": {"role": "compute", "name": "dbt-spark-ab12", "deleted": "T1"}}})
    _frag(tmp_path, "record-30-layout.json",
          {"layout": {"stats": {"spark": {"fct_summary": {"total_rows": 7}}}}})
    _frag(tmp_path, "record-40-bench-spark.json",
          {"items": {"S": {"role": "semantic_model", "name": "aemo_spark"}}})
    bench = tmp_path / "run_report.json"
    bench.write_text(json.dumps({"timings": {"aemo_spark": {"q1": {"ms_by_pass": [9, 3]}}}}),
                     encoding="utf-8")

    dest = tmp_path / "out.json"
    record.finish(str(tmp_path / "fragments"), str(bench), str(dest))
    doc = json.loads(dest.read_text(encoding="utf-8"))

    assert sorted(doc["items"]) == ["L", "N", "O", "S"]
    assert doc["engine"] == "spark"
    # Derived from the output item having been created fresh, never from a dispatch input — the
    # input states an intention, this states what happened.
    assert doc["full_load"] is True
    assert doc["layout"]["stats"]["spark"]["fct_summary"]["total_rows"] == 7
    assert doc["benchmark"]["timings"]["aemo_spark"]["q1"]["ms_by_pass"] == [9, 3]
    # started survives finished — the run block is merged, not replaced.
    assert doc["run"]["started"] == "T0" and doc["run"]["finished"]


def test_finish_without_a_benchmark_omits_the_key_rather_than_emptying_it(tmp_path):
    """A build-only dispatch has no benchmark. An empty `benchmark: {}` would read as 'ran and
    measured nothing', which is a different statement."""
    _frag(tmp_path, "record-00-run.json", {"schema": 1})
    dest = tmp_path / "out.json"
    record.finish(str(tmp_path / "fragments"), str(tmp_path / "absent.json"), str(dest))
    assert "benchmark" not in json.loads(dest.read_text(encoding="utf-8"))


def test_an_incremental_build_is_recorded_as_such(tmp_path):
    """teardown off -> the output item was FOUND, not created -> the next build is a top-up.

    The distinction matters to anyone reading a layout or a CU number afterwards: 143M rows written
    from nothing and 3M rows appended are not the same run.
    """
    _frag(tmp_path, "record-00-run.json", {"schema": 1})
    _frag(tmp_path, "record-20-build-dwh.json",
          {"items": {"O": {"role": "output", "name": "dbt_dwh", "created": False}}})
    dest = tmp_path / "out.json"
    record.finish(str(tmp_path / "fragments"), None, str(dest))
    assert json.loads(dest.read_text(encoding="utf-8"))["full_load"] is False


def test_init_reads_the_dispatch_inputs_and_skips_blanks(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_RECORD", str(tmp_path / "frag.json"))
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    monkeypatch.setenv("RUN_ENGINE", "dwh")
    monkeypatch.setenv("RUNIN_CORES", "64")
    # An input left blank is absent, not recorded as "": the record states what the run chose.
    monkeypatch.setenv("RUNIN_DOWNLOAD_LIMIT", "")
    record._init()
    doc = json.loads((tmp_path / "frag.json").read_text(encoding="utf-8"))
    assert doc["engine"] == "dwh"
    assert doc["inputs"] == {"cores": "64"}
    assert doc["run"]["id"] == "42" and doc["run"]["started"]
    # full_load is settled at finish() from what was actually created, so it is absent here.
    assert "full_load" not in doc


# ------------------------------------------------------------------ the resolved auto sort key

def _fabric_run():
    """Import fabric_run without duckrun installed — the module imports it at top level and these
    tests must stay offline. Only the log-scraping half is under test."""
    import types
    sys.modules.setdefault("duckrun", types.ModuleType("duckrun"))
    import fabric_run  # noqa: PLC0415
    return fabric_run


# The real line from run 30796667149, with the streamer prefix and ANSI codes it arrives with.
_REAL_LOG = (
    'duckdb delta_rs\tUNKNOWN STEP\t2026-08-03T08:32:57.6695754Z [remote] \x1b[0m08:31:58  '
    'Duckrun adapter: duckrun: sort_by=auto for "memory"."mart"."fct_summary__duckrun_tmp" '
    '-> date, time\n'
)


def test_the_auto_sort_key_is_scraped_from_the_leg_log():
    # The adapter does not return the key and nothing writes it to disk, so the log is the only
    # source. A run whose numbers need explaining later has nowhere else to look.
    assert _fabric_run()._sort_keys(_REAL_LOG) == {"fct_summary": ["date", "time"]}


def test_no_sort_is_recorded_as_an_empty_list_not_as_absence():
    # duckrun writes unsorted when nothing pays off. "it chose nothing" and "it was never asked"
    # are different facts and must not collapse.
    log = "duckrun: sort_by=auto for \"m\".\"mart\".\"fct_summary__x_tmp\" -> no sort (nothing pays off)"
    assert _fabric_run()._sort_keys(log) == {"fct_summary": []}


def test_the_last_resolution_wins_over_an_earlier_one():
    # The retry ladder can rebuild a model; the key that describes the parquet on disk is the one
    # the final write used.
    log = ('duckrun: sort_by=auto for "m"."mart"."fct_summary__a_tmp" -> date\n'
           'duckrun: sort_by=auto for "m"."mart"."fct_summary__a_tmp" -> date, time, DUID\n')
    assert _fabric_run()._sort_keys(log) == {"fct_summary": ["date", "time", "DUID"]}


def test_a_log_with_no_auto_resolution_records_nothing(tmp_path, monkeypatch):
    # An unsorted run must not gain an empty `dbt` branch — a key that is present-but-empty reads
    # as "measured, nothing found" rather than "not applicable".
    p = tmp_path / "f.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    _fabric_run()._record_sort_keys("nothing interesting here", "duckrun")
    assert not p.exists()


def test_the_sort_key_lands_outside_layout_config(tmp_path, monkeypatch):
    # THE point of the placement. dashboard/app.js `variant()` walks every entry of
    # layout.config[engine], so a key that changes run to run would split that engine's column and
    # its layout bar. Under `dbt` nothing reads it.
    p = tmp_path / "f.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    _fabric_run()._record_sort_keys(_REAL_LOG, "duckrun")
    doc = json.loads(p.read_text(encoding="utf-8"))
    assert doc == {"dbt": {"duckrun": {"sort_by_auto": {"fct_summary": ["date", "time"]}}}}
    assert "layout" not in doc
