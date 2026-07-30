"""Guards on the two semantic-model templates, checked against duckrun's OWN regexes.

Every assertion here fails at *deploy* time otherwise — after the job has already installed
ADOMD.NET and resolved the workspace — or worse, deploys something that quietly points at the wrong
item or the wrong query mode. All of it is a JSON read; no Fabric, no network.

The sharpest trap, which this caught for real while the templates were being written:
`_is_directlake_bim()` greps the model.bim's RAW BYTES for the camelCase Direct-Lake token, so a
*description string* mentioning it flips the DirectQuery template into "Direct Lake" and makes
deploy attempt a reframe the model cannot serve. Prose counts.
"""
import json
import os
import pathlib
import re

import pytest

from duckrun.workspace import _ONELAKE_REF, _SQL_DATABASE_REF, _is_directlake_bim

HERE = os.path.dirname(os.path.abspath(__file__))
DL = os.path.join(HERE, "fct_summary.SemanticModel", "model.bim")
DQ = os.path.join(HERE, "fct_summary_dq.SemanticModel", "model.bim")

# Spelled by construction so this file itself can hold the assertion without tripping it.
_DL_TOKEN = "direct" + "Lake"


def _raw(path):
    with open(path, "rb") as f:
        return f.read()


def _parts(path):
    """{table: (partition mode, schemaName, entityName)}"""
    m = json.loads(_raw(path))
    return {t["name"]: (t["partitions"][0]["mode"],
                        t["partitions"][0]["source"].get("schemaName"),
                        t["partitions"][0]["source"].get("entityName"))
            for t in m["model"]["tables"]}


def _surface(path):
    """The semantic surface the DAX suite depends on: table names, column names, the source columns
    behind them, and measure names."""
    m = json.loads(_raw(path))
    return {t["name"]: ([c["name"] for c in t["columns"]],
                        [c["sourceColumn"] for c in t["columns"]],
                        [x["name"] for x in t.get("measures", [])])
            for t in m["model"]["tables"]}


# Every shared table each engine emits, and the schema it lands in. Same set `.github/scripts/stats.py`
# reports on — the two must not disagree about what "all the tables" means.
EXPECTED = {"stg_csv_archive_log": "landing",
            "dim_calendar": "mart",
            "dim_duid": "mart",
            "fct_price": "landing",
            "fct_scada": "landing",
            "fct_price_today": "landing",
            "fct_scada_today": "landing",
            "fct_summary": "mart"}


@pytest.mark.parametrize("path", [DL, DQ])
def test_template_carries_every_shared_table(path):
    assert set(_parts(path)) == set(EXPECTED)


@pytest.mark.parametrize("path", [DL, DQ])
def test_template_table_set_matches_the_parity_dashboard(path):
    """`stats.py`'s TABLES is the definition of "every shared table each engine emits". If a model is
    added or renamed there and not here, the benchmark quietly stops covering it."""
    stats = pathlib.Path(".github/scripts/stats.py")
    if not stats.exists():           # running from outside the repo root
        pytest.skip("stats.py not reachable from cwd")
    src = stats.read_text(encoding="utf-8")
    block = re.search(r"^TABLES = \[(.*?)\]", src, re.S | re.M)
    assert block, "could not find TABLES in stats.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(path))


def test_dax_surface_is_identical_across_the_two_templates():
    """xmla_compare.py runs ONE query suite against every model. If the two templates disagree on a
    table, column, sourceColumn or measure name, the suite silently stops being comparable — or
    errors mid-benchmark, after the capacity is already spent."""
    assert _surface(DL) == _surface(DQ)


# ------------------------------------------------------------------ Direct Lake template

def test_direct_lake_template_is_recognised_as_direct_lake():
    """It must be, or deploy() skips the post-deploy reframe and the model serves stale/no data."""
    assert _is_directlake_bim(_raw(DL))


def test_direct_lake_template_carries_a_repointable_onelake_reference():
    """`deploy(lakehouse=...)` RAISES when the bim has no OneLake reference to rewrite, so without
    this every lakehouse engine fails at deploy rather than pointing somewhere wrong."""
    assert _ONELAKE_REF.search(_raw(DL).decode("utf-8"))


def test_direct_lake_template_reads_the_real_tables_in_the_real_schemas():
    """The entity/schema pair is what Direct Lake resolves against OneLake, and it is the only place
    the split between `landing` and `mart` is written down on this side. Upstream's copy had
    dim_calendar under a 'sources' schema and the fact under 'tests' — neither exists here."""
    assert _parts(DL) == {t: ("direct" + "Lake", schema, t) for t, schema in EXPECTED.items()}


# ------------------------------------------------------------------ DirectQuery template

def test_directquery_template_is_not_mistaken_for_direct_lake():
    """The trap. _is_directlake_bim() greps raw bytes, so a description string naming the mode is
    enough to flip this — and then deploy tries to reframe a model that queries live."""
    assert not _is_directlake_bim(_raw(DQ))


def test_directquery_template_mentions_the_direct_lake_token_nowhere():
    """The specific failure mode behind the test above, asserted directly so the diagnosis is
    obvious: it is a substring match over the whole file, PROSE INCLUDED."""
    assert _DL_TOKEN not in _raw(DQ).decode("utf-8")
    assert not _ONELAKE_REF.search(_raw(DQ).decode("utf-8"))


def test_directquery_template_matches_duckruns_sql_database_pattern():
    """`deploy(warehouse=...)` RAISES unless this exact literal shape is present — both the
    `*.datawarehouse.fabric.microsoft.com` server and the database name are rewritten from it."""
    m = _SQL_DATABASE_REF.search(_raw(DQ).decode("utf-8"))
    assert m, "no Sql.Database(<endpoint>, <db>) reference for duckrun to repoint"
    assert m.group("server").endswith(".datawarehouse.fabric.microsoft.com")
    assert m.group("db")


def test_directquery_partitions_are_all_directquery():
    assert {p[0] for p in _parts(DQ).values()} == {"directQuery"}


def test_directquery_partitions_navigate_to_the_right_schema_and_table():
    """DirectQuery partitions carry an M expression rather than an entity reference, so the
    schema/table pair lives in the expression TEXT and no structural assertion would catch a typo
    there — it would surface as an empty table at query time."""
    m = json.loads(_raw(DQ))
    for t in m["model"]["tables"]:
        expr = " ".join(t["partitions"][0]["source"]["expression"])
        schema = EXPECTED[t["name"]]
        assert f'Schema="{schema}"' in expr, f"{t['name']}: partition does not read {schema}"
        assert f'Item="{t["name"]}"' in expr, f"{t['name']}: partition reads a different table"
        assert "Source = Warehouse" in expr, f"{t['name']}: not wired to the Warehouse expression"


def test_directquery_expression_name_matches_what_the_partitions_reference():
    m = json.loads(_raw(DQ))
    assert [e["name"] for e in m["model"]["expressions"]] == ["Warehouse"]


# ------------------------------------------------------------------ relationships

@pytest.mark.parametrize("path", [DL, DQ])
def test_relationships_point_at_tables_and_columns_that_exist(path):
    """A relationship naming a column that was dropped from the curated set deploys fine and then
    breaks every query that crosses it."""
    m = json.loads(_raw(path))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


@pytest.mark.parametrize("path", [DL, DQ])
def test_only_fct_summary_relies_on_referential_integrity(path):
    """`relyOnReferentialIntegrity` lets the engine use an INNER join, which SILENTLY DROPS rows whose
    key is missing from the dimension. fct_summary is built with an INNER JOIN to dim_duid so its RI
    holds by construction — the RAW facts carry retired units absent from the current AEMO
    registration list, which is exactly what stats.py's `duid_probe` exists to diagnose. Asserting RI
    there would make the benchmark quietly measure fewer rows on the very tables it is comparing."""
    m = json.loads(_raw(path))
    ri = {r["name"] for r in m["model"]["relationships"]
          if r.get("relyOnReferentialIntegrity")}
    assert ri == {"fct_summary_to_dim_duid", "fct_summary_to_dim_calendar"}


# ------------------------------------------------------------------ the DAX suite resolves

@pytest.mark.parametrize("path", [DL, DQ])
def test_every_dax_reference_exists_in_the_model(path):
    """The suite is text until it reaches XMLA, so a mistyped `Table[Column]` or `[Measure]` is not
    caught by anything else until the benchmark is already running on paid capacity — and then it
    fails one query mid-flight, after the model has been deployed and warmed.

    Parses every `Table[Column]` and bare `[Measure]` out of xmla_compare.QUERIES and checks it
    against this template."""
    import xmla_compare as xc

    m = json.loads(_raw(path))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    measures = {x["name"] for t in m["model"]["tables"] for x in t.get("measures", [])}

    for _tier, name, dax in xc.QUERIES:
        # Table[Column] — the table name is the run of identifier chars before the bracket.
        for tbl, col in re.findall(r"\b(\w+)\[([^\]]+)\]", dax):
            assert tbl in cols, f"{name}: unknown table {tbl!r}"
            assert col in cols[tbl], f"{name}: {tbl} has no column {col!r}"
        # A bare [Name] is either a model measure or an EXTENSION COLUMN the query defined itself:
        # SUMMARIZECOLUMNS(..., "MWh", [Total MWh]) introduces `[MWh]`, which TOPN then orders by.
        # Every such name arrives as a double-quoted literal in the same query, so collect those.
        local = set(re.findall(r'"([^"]+)"', dax))
        for meas in re.findall(r"(?<![\w\]])\[([^\]]+)\]", dax):
            assert meas in measures or meas in local, f"{name}: unknown measure [{meas}]"
