"""Offline tests for the measured row-ordering signals. No Fabric, no network, no credentials.

WHAT THIS PINS. `layout.ordering` is the only thing in the repo that can say whether a writer
PHYSICALLY REORDERED the rows — V-Order is documented as a reordering pass and the `vorder` detail
column reads a table property nobody sets, so it says `false` for spark whatever the files contain.
A measurement that quietly reports the wrong thing is worse than none: it would settle the V-Order
question with a number nobody could check, on paid capacity, months after the tables were deleted.

The failure modes here are all silent. Lexicographic comparison of VARCHAR row-group statistics
calls a perfectly sorted numeric column fully overlapping (`"9" > "10000"`). Summing
`row_group_num_rows` off a fetch that is one row per COLUMN CHUNK multiplies every file's row count
by its column count and picks the wrong sample file. A run count that trusts scan order instead of
`file_row_number` moves with DuckDB's thread count. None of those raise.

    python -m pytest .github/scripts/test_ordering.py -q
"""
import json
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


@pytest.fixture(scope="module")
def stats():
    """`stats.py` with its Fabric-facing imports stubbed — same reason as test_sort_key.py: it mints
    a token at module level, and this gate runs before any leg spends capacity."""
    duckrun = sys.modules.setdefault("duckrun", types.ModuleType("duckrun"))
    auth = sys.modules.setdefault("duckrun.auth", types.ModuleType("duckrun.auth"))
    auth.get_fabric_token = lambda *a, **k: "stub-token"
    duckrun.auth = auth
    sys.modules.setdefault("requests", types.ModuleType("requests"))
    os.environ.setdefault("WS_ID", "00000000-0000-0000-0000-000000000000")
    import stats as mod
    return mod


def _con(paths):
    """A stand-in for the duckrun connection: `get_stats(detailed=True)` is DuckDB's own
    `parquet_metadata` over local files, and `.con` is a real DuckDB for the run-length read."""
    import duckdb
    lst = ", ".join(f"'{p.as_posix()}'" for p in paths)

    class C:
        con = duckdb.connect()
        storage_options = {}

        def get_stats(self, table=None, detailed=False):
            return duckdb.connect().sql(f"SELECT * FROM parquet_metadata([{lst}])")
    return C()


def _write(tmp_path, name, select, row_group_size=10000):
    p = tmp_path / name
    import duckdb
    duckdb.connect().execute(
        f"COPY ({select}) TO '{p.as_posix()}' "
        f"(FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE {row_group_size})")
    return p


def test_a_sorted_column_partitions_the_range_and_a_shuffled_one_does_not(stats, tmp_path):
    """The headline metric. A globally sorted column gives row groups that carve the domain up —
    0% overlap — and a shuffled one gives row groups that all span it."""
    p = _write(tmp_path, "s.parquet",
               "SELECT i AS sorted_col, hash(i) % 1000000 AS shuffled_col FROM range(100000) t(i)")
    at, rows = stats.mart_chunks(_con([p]), "mart.fct_summary")
    got = stats.rg_ordering(at, rows)
    assert got["sorted_col"]["rgs"] == 10, "one entry per row group"
    assert got["sorted_col"]["rg_overlap_pct"] == 0.0, "a sorted column's ranges are disjoint"
    assert got["shuffled_col"]["rg_overlap_pct"] >= 80, got["shuffled_col"]


def test_a_sorted_low_cardinality_column_reads_zero_not_a_hundred(stats, tmp_path):
    """THE TRAP THIS METRIC ALREADY FELL INTO. A row-group boundary almost never lands on a value
    boundary, so under a PERFECT sort the last row of one row group and the first row of the next
    carry the same value — the ranges TOUCH. Counting a touch as an overlap scored every column of
    every case 100% on fct_summary-shaped data, sorted and shuffled alike: the metric saturated and
    said nothing, while looking like a finding.

    `date` here is 10 values over 100,000 rows in 10 row groups, i.e. one value straddling every
    boundary — the real mart's shape."""
    p = _write(tmp_path, "d.parquet",
               "SELECT ('2020-01-01'::DATE + (i // 10000)::INTEGER) AS date FROM range(100000) t(i)")
    got = stats.rg_ordering(*stats.mart_chunks(_con([p]), "mart.fct_summary"))
    assert got["date"]["rgs"] == 10
    assert got["date"]["rg_overlap_pct"] == 0.0, (
        "a touch at a shared boundary is what a perfect sort looks like, not an overlap")


def test_the_two_metrics_measure_different_scales(stats, tmp_path):
    """Why both exist. A SECONDARY sort key repeats through the table, so its row-group ranges all
    span the domain (100% overlap) while its values are still perfectly grouped inside each row
    group (few runs). Neither number alone tells you the table was sorted, and `rg_overlap_pct` on
    its own would call this column unordered."""
    p = _write(tmp_path, "sec.parquet",
               "SELECT (i // 10000) AS day, (i % 100) AS secondary FROM range(100000) t(i) "
               "ORDER BY day, secondary")
    con = _con([p])
    at, rows = stats.mart_chunks(con, "mart.fct_summary")
    over = stats.rg_ordering(at, rows)
    runs = stats.run_lengths(con, at, rows)["runs"]
    assert over["day"]["rg_overlap_pct"] == 0.0 and runs["day"] == 10, "the leading key: both agree"
    assert over["secondary"]["rg_overlap_pct"] == 100.0, "it repeats, so every range spans the domain"
    assert runs["secondary"] == 1000, "...and it is still perfectly grouped within each row group"


def test_row_group_statistics_compare_numerically_not_lexically(stats, tmp_path):
    """`parquet_metadata` renders min/max as VARCHAR whatever the type is, so a raw comparison is
    lexicographic — and `"9000" > "110000"`. Left unfixed this reports an ASCENDING column as
    heavily overlapping, i.e. the exact opposite of the truth, and never raises."""
    p = _write(tmp_path, "n.parquet", "SELECT (i * 11)::BIGINT AS n FROM range(100000) t(i)")
    at, rows = stats.mart_chunks(_con([p]), "mart.fct_summary")
    assert stats.rg_ordering(at, rows)["n"]["rg_overlap_pct"] == 0.0
    # And the coercion itself keeps mixed renderings sortable rather than raising mid-measurement.
    assert stats._stat("10000") > stats._stat("9")
    assert stats._stat("2020-01-01") < stats._stat("2021-01-01")
    assert (stats._stat("abc") > stats._stat("5")) or (stats._stat("abc") < stats._stat("5"))


def test_columns_without_two_comparable_row_groups_are_absent_never_zero(stats, tmp_path):
    """An all-NULL column has no statistics and a single row group has no consecutive pairs.
    Reporting 0% for either would claim perfect clustering for something never measured."""
    p = _write(tmp_path, "z.parquet",
               "SELECT i AS n, NULL::INT AS empty_col FROM range(100000) t(i)")
    got = stats.rg_ordering(*stats.mart_chunks(_con([p]), "mart.fct_summary"))
    assert "empty_col" not in got, "no stats -> no claim"
    one = _write(tmp_path, "one.parquet", "SELECT i AS n FROM range(50) t(i)", row_group_size=10000)
    assert stats.rg_ordering(*stats.mart_chunks(_con([one]), "mart.fct_summary")) == {}


def test_runs_count_adjacent_equal_values_in_physical_order(stats, tmp_path):
    """The intra-file half — the thing row-group ranges cannot see. A column of 1,000-long blocks
    is 200 runs over 200,000 rows; the same values shuffled are nearly one run per row."""
    p = _write(tmp_path, "r.parquet",
               "SELECT (i // 1000) AS blocked, hash(i) % 200 AS scattered FROM range(200000) t(i)")
    con = _con([p])
    at, rows = stats.mart_chunks(con, "mart.fct_summary")
    got = stats.run_lengths(con, at, rows)
    assert got["rows"] == 200000
    assert got["runs"]["blocked"] == 200, "one run per block of equal adjacent values"
    assert got["runs"]["scattered"] > 100000, got["runs"]
    assert got["file"] == "r.parquet", "recorded by basename, not by abfss URI"


def test_the_sample_is_the_largest_file_capped_at_the_row_budget(stats, tmp_path, monkeypatch):
    """Largest file, ties by name, and never more than ORDERING_SAMPLE_ROWS — the read has to stay
    bounded on a 143M-row mart spread over up to 80 files."""
    small = _write(tmp_path, "a_small.parquet", "SELECT i AS n FROM range(20000) t(i)")
    big = _write(tmp_path, "b_big.parquet", "SELECT i AS n FROM range(80000) t(i)")
    con = _con([small, big])
    at, rows = stats.mart_chunks(con, "mart.fct_summary")
    assert stats._file_rows(at, rows)[str(big.as_posix())] == 80000, (
        "row counts must be summed over DISTINCT row groups — the fetch is one row per column chunk")
    monkeypatch.setattr(stats, "ORDERING_SAMPLE_ROWS", 5000)
    got = stats.run_lengths(con, at, rows)
    assert got["file"] == "b_big.parquet", "the largest file, not the first"
    assert got["rows"] == 5000, "capped at the budget"
    assert got["runs"]["n"] == 5000, "distinct values -> one run each"


def test_vorder_tags_count_live_files_and_report_what_the_log_cannot_say(stats):
    """The honest V-Order check: per-file `add.tags`, last add per path wins, live set taken from
    the parquet metadata rather than replayed from the log. A live file no JSON commit describes was
    folded into a checkpoint — `unknown`, never quietly counted as untagged."""
    actions = [
        {"add": {"path": "part-1.parquet", "tags": {"VORDER": "true"}}},
        {"add": {"path": "part-2.parquet", "tags": {}}},
        {"add": {"path": "part-3.parquet", "tags": {"VORDER": "true"}}},
        {"remove": {"path": "part-3.parquet"}},
        {"add": {"path": "part-3.parquet", "tags": {}}},          # re-added untagged: last wins
        {"metaData": {"id": "x"}},
    ]
    live = ["abfss://ws@onelake.dfs.fabric.microsoft.com/g/Tables/mart/t/part-1.parquet",
            "abfss://ws@onelake.dfs.fabric.microsoft.com/g/Tables/mart/t/part-3.parquet",
            "abfss://ws@onelake.dfs.fabric.microsoft.com/g/Tables/mart/t/part-9.parquet"]
    assert stats._vorder_from_log(actions, live) == {"tagged": 1, "files": 3, "unknown": 1}
    # URL-encoded paths are how Delta writes anything with a special character.
    assert stats._vorder_from_log(
        [{"add": {"path": "dt%3D2020/part-1.parquet", "tags": {"VORDER": "TRUE"}}}],
        ["abfss://x/part-1.parquet"]) == {"tagged": 1, "files": 1, "unknown": 0}


def test_every_measurement_is_best_effort_and_absent_never_empty(stats):
    """A layout job that goes red for a measurement it added is worse than one that reports less.
    `{}` would read as "nothing is ordered"; absence reads as "not measured", which is the truth."""
    class Boom:
        storage_options = {}

        class con:
            @staticmethod
            def sql(q):
                raise RuntimeError("OneLake said no")

        def get_stats(self, table=None, detailed=False):
            raise RuntimeError("OneLake said no")
    assert stats.rg_ordering(None, []) == {}
    assert stats.run_lengths(Boom(), None, []) == {}
    assert stats.vorder_tags(Boom(), "g", "mart", "fct_summary", set()) == {}
    assert stats.ordering_for(Boom(), "g", "mart", None, []) == {}
    assert stats.ordering_for(Boom(), "g", None, {"file_name": 0}, [("f",)]) == {}


def test_the_ordering_lands_beside_stats_and_never_in_layout_config(stats, tmp_path):
    """`layout.config` is walked by the dashboard's `variant()` into column names, so a MEASURED
    value there would split an engine's column and its layout bar every time the parquet moved —
    reporting two configurations where there was one."""
    order = {"duckrun": {"table": "mart.fct_summary",
                         "sample": {"file": "part-1.parquet", "rows": 4000000},
                         "vorder_files": {"tagged": 9, "files": 9, "unknown": 0},
                         "columns": {"date": {"rg_overlap_pct": 0.0, "rgs": 9, "runs": 45}}},
             "spark": {}}
    doc = stats.build_doc({}, ["duckrun", "spark"], {}, None, None, order)
    assert doc["ordering"]["duckrun"]["columns"]["date"]["runs"] == 45
    assert "spark" not in doc["ordering"], "an engine that measured nothing adds no column"
    assert "ordering" not in doc["config"].get("duckrun", {})
    assert not any("ordering" in (c or {}) for c in doc["config"].values())
    json.dumps(doc, default=str)      # the record is written with json.dump
    assert stats.build_doc({}, ["duckrun"], {}, None, None, {"duckrun": {}}).get("ordering") is None


def test_the_step_summary_renders_without_the_parts_that_failed(stats, capsys):
    """Every piece is independently best-effort, so the renderer meets partial documents in the
    normal case — a dwh whose Delta log has no tags still has both parquet metrics."""
    order = {"duckrun": {"columns": {"date": {"rg_overlap_pct": 0.0, "rgs": 9, "runs": 45},
                                     "mw": {"rg_overlap_pct": 100.0, "rgs": 9, "inexact": True}},
                         "sample": {"file": "part-1.parquet", "rows": 4000000},
                         "vorder_files": {"tagged": 9, "files": 9, "unknown": 2}},
             "dwh": {"columns": {"date": {"rg_overlap_pct": 50.0, "rgs": 4}}}}
    stats.ordering_table(order, ["duckrun", "dwh", "spark"])
    out = capsys.readouterr().out
    assert "| duckrun | 9/9 +2? | `part-1.parquet` · 4,000,000 rows |" in out
    assert "| dwh | — | — |" in out, "a missing part is a dash, not a zero and not a crash"
    assert "spark" not in out, "an engine that measured nothing gets no column"
    assert "| `date` | 0% RG overlap · 45 runs | 50% RG overlap |" in out
    assert "100%* RG overlap" in out, "a truncated statistic is flagged for the reader"
    stats.ordering_table({}, ["duckrun"])
    assert capsys.readouterr().out == "", "nothing measured -> no section at all"
