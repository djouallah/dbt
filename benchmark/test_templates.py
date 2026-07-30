"""Guards on the semantic-model template, checked against duckrun's OWN regexes.

Every assertion here fails at *deploy* time otherwise — after the job has already installed
ADOMD.NET and resolved the workspace — or worse, deploys something that quietly points at the wrong
item or the wrong query mode. All of it is a JSON read; no Fabric, no network.

There is ONE template now. There were two — this one plus `fct_summary_dq.SemanticModel`, a
hand-authored DirectQuery copy — because before duckrun 0.4.36 a warehouse could only be read by
DirectQuery, and the two files had to be kept in lockstep or the single DAX suite silently stopped
being comparable. `deploy(mode=)` replaced that: one authored .bim ships as either mode, so the
copy is gone and `engines.MODE` decides. The old file's sharpest trap is worth remembering if
anyone reintroduces a DirectQuery bim rather than using `mode=`: `_is_directlake_bim()` greps the
model.bim's RAW BYTES for the camelCase Direct-Lake token, so a *description string* mentioning the
mode was enough to flip it and make deploy attempt a reframe the model could not serve. Prose
counts. It caught that for real, once.
"""
import json
import os
import pathlib
import re

import pytest

from duckrun.workspace import _ONELAKE_REF, _is_directlake_bim, _normalize_mode

HERE = os.path.dirname(os.path.abspath(__file__))
DL = os.path.join(HERE, "fct_summary.SemanticModel", "model.bim")


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


def test_template_carries_every_shared_table():
    assert set(_parts(DL)) == set(EXPECTED)


def test_template_table_set_matches_the_parity_dashboard():
    """`stats.py`'s TABLES is the definition of "every shared table each engine emits". If a model is
    added or renamed there and not here, the benchmark quietly stops covering it."""
    stats = pathlib.Path(".github/scripts/stats.py")
    if not stats.exists():           # running from outside the repo root
        pytest.skip("stats.py not reachable from cwd")
    src = stats.read_text(encoding="utf-8")
    block = re.search(r"^TABLES = \[(.*?)\]", src, re.S | re.M)
    assert block, "could not find TABLES in stats.py"
    assert set(re.findall(r'"([^"]+)"', block.group(1))) == set(_parts(DL))


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


# ------------------------------------------------------------------ relationships

def test_relationships_point_at_tables_and_columns_that_exist():
    """A relationship naming a column that was dropped from the curated set deploys fine and then
    breaks every query that crosses it."""
    m = json.loads(_raw(DL))
    cols = {t["name"]: {c["name"] for c in t["columns"]} for t in m["model"]["tables"]}
    for r in m["model"]["relationships"]:
        assert r["fromColumn"] in cols[r["fromTable"]], f"{r['name']}: bad fromColumn"
        assert r["toColumn"] in cols[r["toTable"]], f"{r['name']}: bad toColumn"


def test_only_fct_summary_relies_on_referential_integrity():
    """`relyOnReferentialIntegrity` lets the engine use an INNER join, which SILENTLY DROPS rows whose
    key is missing from the dimension. fct_summary is built with an INNER JOIN to dim_duid so its RI
    holds by construction — the RAW facts carry retired units absent from the current AEMO
    registration list, which is exactly what stats.py's `duid_probe` exists to diagnose. Asserting RI
    there would make the benchmark quietly measure fewer rows on the very tables it is comparing."""
    m = json.loads(_raw(DL))
    ri = {r["name"] for r in m["model"]["relationships"]
          if r.get("relyOnReferentialIntegrity")}
    assert ri == {"fct_summary_to_dim_duid", "fct_summary_to_dim_calendar"}


# ------------------------------------------------------------------ the DAX suite resolves

def test_every_dax_reference_exists_in_the_model():
    """The suite is text until it reaches XMLA, so a mistyped `Table[Column]` or `[Measure]` is not
    caught by anything else until the benchmark is already running on paid capacity — and then it
    fails one query mid-flight, after the model has been deployed and warmed.

    Parses every `Table[Column]` and bare `[Measure]` out of xmla_compare.QUERIES and checks it
    against this template."""
    import xmla_compare as xc

    m = json.loads(_raw(DL))
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


# ------------------------------------------------------------------ storage mode (duckrun 0.4.36)

def test_every_engine_mode_is_a_spelling_duckrun_accepts():
    """`engines.DEPLOY_MODE` translates the TMSL spelling in `MODE` into the one `deploy(mode=)`
    takes. A typo here raises inside duckrun partway through a paid run, after ADOMD.NET is
    installed and the first models are already deployed."""
    import engines as E

    for engine, mode in E.MODE.items():
        assert mode in E.DEPLOY_MODE, f"{engine}: MODE {mode!r} has no DEPLOY_MODE spelling"
        assert _normalize_mode(E.DEPLOY_MODE[mode]) == mode


def test_all_four_engines_are_read_the_same_way():
    """The benchmark compares physical LAYOUTS. While every engine is Direct Lake, a timing
    difference is a layout difference; the moment one is DirectQuery its numbers are pushdown to a
    different engine and are not the same kind of number (which is why `mode` is carried on every
    verdict). dwh was the exception until duckrun 0.4.36 made `mode=` independent of item kind.

    This is a policy pin, not a law — flipping an engine back is a deliberate one-line change in
    engines.MODE, and this test is the place that makes it deliberate."""
    import engines as E

    assert set(E.MODE.values()) == {"directLake"}


def test_deploy_passes_warehouse_for_a_warehouse_and_lakehouse_otherwise():
    """WHICH item vs HOW it is read are independent now. Passing `lakehouse=` for the warehouse
    item raises in duckrun — a deploy failure partway through a run that has already spent
    capacity on the engines before it."""
    import deploy_models as D

    lake = D.deploy_kwargs({"item": "dbt_delta", "kind": "lakehouses", "mode": "directLake"})
    assert lake == {"lakehouse": "dbt_delta", "mode": "direct_lake"}

    wh = D.deploy_kwargs({"item": "dbt_dwh", "kind": "warehouses", "mode": "directLake"})
    assert wh == {"warehouse": "dbt_dwh", "mode": "direct_lake"}

    # The same item, read the other way — one template, mode decides.
    dq = D.deploy_kwargs({"item": "dbt_dwh", "kind": "warehouses", "mode": "directQuery"})
    assert dq == {"warehouse": "dbt_dwh", "mode": "direct_query"}


def test_there_is_exactly_one_template():
    """The DirectQuery copy is gone; `mode=` reproduces it from this one file. A second .bim
    reintroduces the lockstep problem that made the DAX suite silently non-comparable."""
    bims = sorted(pathlib.Path(HERE).glob("*.SemanticModel/model.bim"))
    assert [b.parent.name for b in bims] == ["fct_summary.SemanticModel"]
