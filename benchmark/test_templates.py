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


@pytest.mark.parametrize("path", [DL, DQ])
def test_template_is_valid_json_with_the_three_tables(path):
    assert set(_parts(path)) == {"dim_calendar", "dim_duid", "fct_summary"}


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


def test_direct_lake_template_reads_the_real_mart_tables():
    """The entity/schema pair is what Direct Lake resolves against OneLake. Upstream's copy had
    dim_calendar under a 'sources' schema and the fact under 'tests' — neither exists here."""
    assert _parts(DL) == {"dim_calendar": ("directLake", "mart", "dim_calendar"),
                          "dim_duid": ("directLake", "mart", "dim_duid"),
                          "fct_summary": ("directLake", "mart", "fct_summary")}


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


def test_directquery_partitions_navigate_to_the_mart_schema():
    """DirectQuery partitions carry an M expression rather than an entity reference, so the
    schema/table pair lives in the expression text and no other assertion would catch a typo."""
    m = json.loads(_raw(DQ))
    for t in m["model"]["tables"]:
        expr = " ".join(t["partitions"][0]["source"]["expression"])
        assert 'Schema="mart"' in expr, f"{t['name']}: partition does not read the mart schema"
        assert f'Item="{t["name"]}"' in expr, f"{t['name']}: partition reads a different table"
        assert "Source = Warehouse" in expr, f"{t['name']}: not wired to the Warehouse expression"


def test_directquery_expression_name_matches_what_the_partitions_reference():
    m = json.loads(_raw(DQ))
    assert [e["name"] for e in m["model"]["expressions"]] == ["Warehouse"]
