"""The engine registry for the benchmark — one place that knows what the four engines are.

Deliberately mirrors `.github/scripts/stats.py`'s `ENGINES` / `WRITER`: that script is the parity
dashboard over the same four items, and the two must never disagree about which Fabric item belongs
to which engine. If an item is renamed, both change together.

`MODE` is the one thing stats.py has no opinion about and this does: the three lakehouses are read
by a **Direct Lake** semantic model (in-memory transcode from the Delta files, so the physical layout
is what shapes the timing), and the warehouse by a **DirectQuery** model (pushdown to the SQL
endpoint — a different engine, not a different layout). A DirectQuery model has no transcoded data to
evict, so it yields hot numbers only and is labelled distinctly wherever it appears.
"""
import json
import os

# (engine label, Fabric item display name, item kind) — same triple as stats.py's ENGINES.
ENGINES = [("duckrun", "dbt_delta", "lakehouses"),
           ("iceberg", "dbt_iceberg", "lakehouses"),
           ("spark", "dbt_spark", "lakehouses"),
           ("dwh", "dbt_dwh", "warehouses")]

ITEM = {e: item for e, item, _ in ENGINES}
KIND = {e: kind for e, _, kind in ENGINES}

# What actually wrote the parquet behind each engine's Delta log (stats.py's WRITER, verbatim).
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)",
          "spark": "spark", "dwh": "warehouse"}

# How the semantic model reads it. A lakehouse item is reachable by Direct Lake on OneLake; a
# warehouse item is served here as DirectQuery over the workspace SQL endpoint.
MODE = {e: ("directQuery" if kind == "warehouses" else "directLake") for e, _, kind in ENGINES}

ALL = [e for e, _, _ in ENGINES]

# Semantic models are named <PREFIX><engine>. The DAX suite is identical across them, so the model
# name is the ONLY thing that identifies which engine's table a timing came from.
PREFIX = "aemo_"


def model_name(engine):
    return f"{PREFIX}{engine}"


def engine_of(model):
    """Inverse of model_name. Tolerates being handed a bare engine label already."""
    return model[len(PREFIX):] if model.startswith(PREFIX) else model


def selected(default=None):
    """The engines this run covers, from BENCH_ENGINES (comma-separated). Order is significant:
    the FIRST one is the reference every ratio is taken against."""
    raw = (os.environ.get("BENCH_ENGINES") or "").strip()
    if not raw:
        return list(default if default is not None else ALL)
    out = []
    for part in raw.split(","):
        e = part.strip().lower()
        if not e:
            continue
        if e not in ITEM:
            raise SystemExit(f"unknown engine {e!r}; known: {', '.join(ALL)}")
        if e not in out:
            out.append(e)
    if not out:
        raise SystemExit("BENCH_ENGINES was set but named no engine")
    return out


def reference(candidates=None):
    """The engine every comparison is measured against — BENCH_REFERENCE if it is one of the
    candidates, else the first candidate. Never guesses by name length."""
    cands = list(candidates) if candidates else selected()
    want = (os.environ.get("BENCH_REFERENCE") or "").strip().lower()
    if want and want in cands:
        return want
    return cands[0] if cands else None


def items():
    """The {engine: {item, kind, guid, mode, writer}} map that resolve_env.py wrote to
    BENCH_ITEMS. Raises with a pointer rather than a KeyError, because every consumer of this needs
    resolve_env.py to have run first."""
    raw = os.environ.get("BENCH_ITEMS")
    if not raw:
        raise SystemExit("BENCH_ITEMS is not set — run benchmark/resolve_env.py first")
    return json.loads(raw)
