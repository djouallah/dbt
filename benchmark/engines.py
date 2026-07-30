"""The engine registry for the benchmark — one place that knows what the four engines are.

Deliberately mirrors `.github/scripts/stats.py`'s `ENGINES` / `WRITER`: that script is the parity
dashboard over the same four items, and the two must never disagree about which Fabric item belongs
to which engine. If an item is renamed, both change together.

The one thing stats.py has no opinion about and this does: `DEPLOY_MODE`. Every engine is read by a
**Direct Lake** semantic model — an in-memory transcode straight from the Delta files — because that
is the only reading in which the answer is about the *physical layout*. Four engines write the same
rows in four different shapes; how long each shape takes to transcode and scan is the whole question.

So the mode is a **premise, not a per-engine setting**. It is one constant, and the item's KIND
(lakehouse vs warehouse) is independent of it: duckrun 0.4.36's `deploy(mode=)` reads a warehouse's
Tables as the Delta they are, so `dwh` measures a layout like the other three rather than SQL-endpoint
pushdown to a different engine. Nothing here carries a DirectQuery alternative any more — a pushdown
timing is not a slow layout, and a report that mixed the two kinds of number invited exactly that
misreading.

The hot-only path downstream survives and is no longer about DirectQuery: `BENCH_COLD=false` skips
cold deliberately, and a dehydrate can fail when the token cannot refresh. Either way an engine
reports hot numbers only, and `render_report._totals` scopes each metric to the engines that have it.
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

# How every engine's tables are read, in the spelling duckrun's deploy(mode=) takes. Not a dict:
# comparing physical layouts requires that all four be read the same way, so this is the premise of
# the benchmark rather than a knob. Independent of the item KIND since duckrun 0.4.36 — a warehouse's
# Tables are Delta in OneLake exactly like a lakehouse's.
DEPLOY_MODE = "direct_lake"

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
    """The engines this run covers, from BENCH_ENGINES (comma-separated).

    Order decides only the ORDER THEY ARE MEASURED IN — it is the bench matrix's order, and index 0
    is simply the job that skips the idle gap. It used to also name the reference every ratio was
    taken against; there is no reference any more (see render_report's docstring), so no number in
    the report depends on how the dispatch happened to list the engines."""
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


def items():
    """The {engine: {item, kind, guid, writer}} map that resolve_env.py wrote to
    BENCH_ITEMS. Raises with a pointer rather than a KeyError, because every consumer of this needs
    resolve_env.py to have run first."""
    raw = os.environ.get("BENCH_ITEMS")
    if not raw:
        raise SystemExit("BENCH_ITEMS is not set — run benchmark/resolve_env.py first")
    return json.loads(raw)
