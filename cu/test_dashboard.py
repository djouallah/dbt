"""Offline tests for dashboard.py — `python -m pytest cu/ -q` runs these too.

The dashboard's whole claim is that a page can be rebuilt from a committed JSON record with no
token, no network and no third-party package. That claim is only worth making if it is checked, and
every one of these runs in milliseconds against files in a tmp_path.

What they pin, all of which fail the same quiet way when wrong — a page that renders and misleads:
a record covering fewer engines must produce fewer COLUMNS rather than columns of zeros; the record
being rendered must win its own `since` floor in the generations table; an old schema must still
render; and an empty `history/` must say the contract rather than print a blank page that reads like
an idle capacity.
"""
import importlib
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def load(directory, **env):
    for k in [k for k in os.environ if k.startswith("CU_")] + ["PBI_TOKEN", "STATS_JSON"]:
        os.environ.pop(k, None)
    os.environ.update({"CU_HISTORY_DIR": str(directory), **env})
    import capacity_cu
    import dashboard
    importlib.reload(capacity_cu)
    return importlib.reload(dashboard)


def record(directory, name, *, since, written, build="b1", engines=("duckrun", "spark"),
           schema=2, analytics=True):
    rec = {
        "schema": schema, "unit": "CU (s)", "written": written, "since": since,
        "runs": {"measure": "m1", "build": build, "build_sha": "abc1234567"},
        "config": {"duckrun": {"vcores": "64"},
                   "spark": {"resource_profile": "writeHeavy",
                             "native_execution_engine": "false"}},
        "cu": {"etl": {e: {"OneLake Write": 100.0 * (i + 1)} for i, e in enumerate(engines)}},
        "tables": ["dim_duid", "fct_summary"],
        "layout_written": "2026-08-01T07:10:00",
        "layout": {e: {"dim_duid": {"schema": "mart", "total_rows": 689, "num_files": 1,
                                    "num_row_groups": 1, "avg_row_group": 689, "size_mb": 0.02,
                                    "vorder": False},
                       "fct_summary": {"schema": "mart", "total_rows": 143_980_961,
                                       "num_files": 4 + i, "num_row_groups": 79,
                                       "avg_row_group": 1_822_544, "size_mb": 998.91,
                                       "vorder": False}}
                   for i, e in enumerate(engines)},
    }
    if analytics:
        rec["cu"]["analytics"] = {e: {"XMLA Read Operation": 50.0} for e in engines}
    if schema == 1:
        rec.pop("tables"), rec.pop("layout_written")
    (directory / name).write_text(json.dumps(rec), encoding="utf-8")
    return rec


def test_a_two_engine_record_renders_two_columns(tmp_path, capsys):
    """"It can be 1 engine, 2, whatever" is the contract. A record is a closed document: an engine
    it never measured has no zero to print, so it must not get a column at all."""
    record(tmp_path, "a.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00")
    d = load(tmp_path)
    assert d.main() == 0
    out = capsys.readouterr().out
    assert "| CU (s) | duckrun | spark |" in out
    assert "iceberg" not in out and "dwh" not in out
    assert "2 engines, one landed copy" in out
    # …and the layout tables follow the same rule.
    assert "| table | rows | duckrun | spark |" in out


def test_one_engine_says_engine_not_engines(tmp_path, capsys):
    record(tmp_path, "a.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00",
           engines=("duckrun",))
    d = load(tmp_path)
    d.main()
    out = capsys.readouterr().out
    assert "1 engine, one landed copy" in out
    assert "| CU (s) | duckrun |" in out


def test_the_rendered_record_wins_its_own_floor(tmp_path, capsys):
    """Two reads of one floor, and the OLDER is the one being rendered. It must still be the "this
    record" column — a page whose generations table contradicts the page above it is worse than one
    with no generations table."""
    record(tmp_path, "a.json", since="2026-08-01T15:00:00", written="2026-08-01T07:39",
           engines=("duckrun",))
    record(tmp_path, "b.json", since="2026-08-01T15:00:00", written="2026-08-01T08:22",
           engines=("duckrun",))
    record(tmp_path, "c.json", since="2026-08-01T10:00:00", written="2026-08-01T04:26",
           engines=("duckrun",))
    d = load(tmp_path)
    recs = d.load_records()
    chosen = d.pick(recs, "a.json")
    cols = d.history_cols(recs, chosen, ["duckrun"])
    assert [c[0] for c in cols] == ["2026-08-01 10:00", "2026-08-01 15:00 · **this record**"]


def test_pick_takes_the_newest_or_a_named_run(tmp_path):
    record(tmp_path, "2026-08-01T0412Z-111.json", since="2026-08-01T10:00:00",
           written="2026-08-01T04:12")
    record(tmp_path, "2026-08-01T0822Z-222.json", since="2026-08-01T15:00:00",
           written="2026-08-01T08:22")
    d = load(tmp_path)
    recs = d.load_records()
    assert d.pick(recs, "")["_file"].endswith("222.json")        # newest by default
    assert d.pick(recs, "111")["_file"].endswith("111.json")     # by run id
    # An unmatched pick renders the newest rather than nothing: refusing to render is worse.
    assert d.pick(recs, "nope")["_file"].endswith("222.json")


def test_a_schema_1_record_still_renders(tmp_path, capsys):
    """`history/` is the copy that outlives retention. A reader that silently ignores last month's
    records is the same failure as never having written them."""
    record(tmp_path, "old.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00", schema=1)
    d = load(tmp_path)
    d.main()
    out = capsys.readouterr().out
    assert "`mart.fct_summary`" in out and "`mart.dim_duid`" in out


def test_an_empty_history_says_the_contract(tmp_path, capsys):
    """The dashboard's one failure mode, and it must not look like an idle capacity."""
    d = load(tmp_path / "nothing-here")
    assert d.main() == 0
    out = capsys.readouterr().out
    assert "No records in" in out and "spends no capacity" in out
    assert "Build, benchmark, measure" in out


def test_the_dashboard_never_reaches_the_network(tmp_path, capsys, monkeypatch):
    """The claim that makes this workflow free and unbreakable: no token, no request, not even the
    `requests` package. Rendered with it removed, so an accidental call cannot pass."""
    record(tmp_path, "a.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00")
    d = load(tmp_path)
    monkeypatch.setattr(d.cu, "requests", None)
    assert d.main() == 0
    assert "| CU (s) |" in capsys.readouterr().out


def test_a_record_with_no_analytics_is_not_a_broken_page(tmp_path, capsys):
    """A build-only dispatch never queries anything, so its analytics CU is genuinely zero. The page
    must show the ETL half and simply not draw the other chart."""
    record(tmp_path, "a.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00",
           analytics=False)
    d = load(tmp_path)
    d.main()
    out = capsys.readouterr().out
    assert "ETL \\u2014 what building" in out or "ETL — what building" in out
    assert "Analytics" not in out.split("Everything this record measured")[0].split(
        "<!--chart:")[-1]


def test_the_page_renders_from_the_dashboard_markdown(tmp_path, capsys):
    """End to end through the real HTML renderer — the artifact copy has to open off a local disk."""
    record(tmp_path, "a.json", since="2026-08-01T10:00:00", written="2026-08-01T04:00")
    d = load(tmp_path)
    d.main()
    import report_html
    page = report_html.page(capsys.readouterr().out)
    assert page.startswith("<!doctype html>") and "</html>" in page
    assert "<svg" in page and 'class="bar-caption"' in page
    for forbidden in ("<script", "src=", "@import", "<img", "<link"):
        assert forbidden not in page
