/**
 * Offline tests for the page. No token, no network, no Fabric, no browser — which is the property
 * being kept. `node --test cu/`
 *
 * What matters here is the JOIN. Attribution used to be substring matching on display names with a
 * `shared` bucket for anything ambiguous; it is now a dictionary lookup on the item GUID, and the class
 * comes from the role the run itself recorded. If that join is wrong the page prints a confident number
 * under the wrong engine, which is the failure this directory exists to avoid.
 *
 * These are the tests `cu/test_dashboard.py` carried, ported when the render layer moved from Python to
 * the browser. That port is the reason they exist in this file rather than being rewritten: the rules
 * they pin — that landing CU never reaches a column, that a dash is not a zero, that a variant tag
 * never contains the column separator — were each learned from a page that printed something wrong,
 * and none of them became less true for being enforced in a different language.
 *
 * The render layer produces STRINGS, so `plain()` and `rows()` turn a fragment back into something an
 * assertion can read: `<strong>` becomes `**` and `<code>` becomes a backtick, which is exactly what
 * the markdown-era assertions were written against.
 */
import { test } from "node:test";
import assert from "node:assert/strict";

import * as d from "./app.js";

// ------------------------------------------------------------------------------------ HTML → text

const UNESC = { "&amp;": "&", "&lt;": "<", "&gt;": ">", "&quot;": '"', "&#39;": "'" };

/** A fragment as readable text: emphasis and code spans come back as their markdown, everything else
 *  is dropped. An assertion should be about what the page SAYS. */
function plain(html) {
  return String(html)
    .replace(/<\/?strong>/g, "**")
    .replace(/<\/?code>/g, "`")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;|&lt;|&gt;|&quot;|&#39;/g, (m) => UNESC[m]);
}

/** Every table row on the page, as `| cell | cell |` — the shape the markdown-era assertions used. */
function rows(html) {
  return [...String(html).matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)].map(([, tr]) =>
    "| " + [...tr.matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/g)]
      .map(([, c]) => plain(c).trim()).join(" | ") + " |");
}

/**
 * The section a heading opens, up to the next heading of any level.
 *
 * Cutting on `<h4` alone is not enough and the difference is not cosmetic: the LAST block on the page
 * is followed by an `<h3>`, so a `<h4>`-only cut swallowed the sources table and a "one row per writer"
 * assertion counted five rows and called it a pass in the other direction.
 */
function block(html, heading) {
  const at = String(html).indexOf(heading);
  if (at < 0) return "";
  const rest = String(html).slice(at + heading.length);
  const end = rest.search(/<h[234][\s>]/);
  return end < 0 ? rest : rest.slice(0, end);
}

/** `[{title, subtitle, labels, values, captions}]` for each chart drawn, in page order. */
function charts(html) {
  return [...String(html).matchAll(/<figure class="chart"[^>]*>([\s\S]*?)<\/figure>/g)].map(([, f]) => ({
    title: plain((f.match(/<span class="chart-title">([\s\S]*?)<\/span>/) || [])[1] || ""),
    subtitle: plain((f.match(/<span class="chart-sub">([\s\S]*?)<\/span>/) || [])[1] || ""),
    labels: [...f.matchAll(/<text class="bar-label"[^>]*>([\s\S]*?)<\/text>/g)].map((m) => plain(m[1])),
    values: [...f.matchAll(/<text class="bar-value"[^>]*>([\s\S]*?)<\/text>/g)].map((m) => plain(m[1])),
    captions: [...f.matchAll(/<text class="bar-caption"[^>]*>([\s\S]*?)<\/text>/g)]
      .map((m) => plain(m[1])),
    svg: f,
  }));
}

// ------------------------------------------------------------------------------------- fixtures

const ago = (hours) => new Date(Date.now() - hours * 3600 * 1000).toISOString();

/** An item the teardown deleted — the normal case, and the one that is not `drifting`. */
const gone = (role, name) => ({ role, name, deleted: ago(1) });

function rec(file, engine, items, opts = {}) {
  const { config, stats, tables, landing, full_load = true, finishedHoursAgo = 48 } = opts;
  const r = {
    _file: file, schema: 1, engine, full_load,
    run: {
      id: file.split("-").pop().split(".")[0],
      started: ago(finishedHoursAgo + 1), finished: ago(finishedHoursAgo),
    },
    items,
    layout: { config: config || {}, stats: stats || {}, tables: tables || [] },
  };
  if (landing) r.layout.landing = landing;
  return r;
}

/** `{guid: {operation: CU}}`. A bare number is taken as one compute operation, for brevity. */
function ledger(items) {
  const out = {};
  for (const [g, v] of Object.entries(items)) {
    out[g] = typeof v === "object" ? v : { "Warehouse Query": v };
  }
  return {
    items: out, seconds: {},
    reads: [{ at: "2026-08-02T20:00:00+00:00" }], updated: "2026-08-02T20:00:00+00:00",
  };
}

const secs = (items) => Object.fromEntries(Object.entries(items)
  .map(([g, v]) => [g, typeof v === "object" ? v : { "Warehouse Query": v }]));

/**
 * A record that IS a whole generation: torn down, built, benchmarked.
 *
 * The DEFAULT `timings` carries no tier keys at all — only `ms_by_pass`, which is what `incomplete()`
 * checks for and nothing a tier column can read. That is deliberate: it keeps every other test
 * exercising the "no timings, no columns" path, and `timings:` is how the query-time tests opt in.
 */
function full(file, engine, opts = {}) {
  const { timings, ...rest } = opts;
  const r = rec(file, engine, {
    OUT: { role: "output", name: `dbt_${engine}`, deleted: ago(1) },
    SEM: { role: "semantic_model", name: `aemo_${engine}`, deleted: ago(1) },
    L: { role: "landing", name: "dbt_landing" },
  }, { stats: { [engine]: { fct_summary: { total_rows: 1 } } }, tables: ["fct_summary"], ...rest });
  r.benchmark = { timings: { [`aemo_${engine}`]: timings || { q: { ms_by_pass: [1] } } } };
  return r;
}

/** `{query: [cold, warm, hot]}` → the record's timing shape. A `null` cold is the real ladder-query
 *  shape: no first-pass sample at all. */
function timings(perQuery) {
  const out = {};
  for (const [q, [cold, warm, hot]] of Object.entries(perQuery)) {
    out[q] = { warm_ms: warm, hot_median_ms: hot, hot_spread_pct: 5.0 };
    if (cold !== null) out[q].cold_ms = cold;
  }
  return out;
}

/** A record whose mart layout is spelled out, so grouping has something to group on. */
function lay(engine, files, rgs, opts = {}) {
  const { vorder = false, cfg = {}, file = "x.json", ...rest } = opts;
  return full(file, engine, {
    config: { [engine]: cfg },
    stats: {
      [engine]: {
        fct_summary: {
          total_rows: 143980961, num_files: files, num_row_groups: rgs,
          avg_row_group: 1, size_mb: 1.0, vorder, schema: "mart",
        },
      },
    },
    ...rest,
  });
}

const render = (runs, led) =>
  d.renderPage(d.columnsFor(runs), runs, d.normaliseLedger(led), { now: Date.now() });

// ------------------------------------------------------------------------------------- the join

test("the role decides the class, not the Fabric item kind", () => {
  // A semantic model is only ever queried; everything else is work done to BUILD the tables. This
  // replaced classification from the metrics app's item-kind snapshot, which routinely had not
  // catalogued a minutes-old item at all.
  const r = rec("r-1.json", "spark", {
    OUT: { role: "output", name: "dbt_spark" },
    NB: { role: "compute", name: "dbt-spark-ab12" },
    SEM: { role: "semantic_model", name: "aemo_spark" },
  });
  const { cells } = d.runCu(r, d.normaliseLedger(ledger({
    OUT: { "OneLake Write via Redirect": 10.0 },
    NB: { "Jupyter Notebook Scheduled Run": 900.0 },
    SEM: { "XMLA Read Operation": 40.0 },
  })));
  assert.deepEqual(cells, {
    etl: { storage: 10.0, compute: 900.0 },
    analytics: { compute: 40.0 },
  });
  assert.equal(d.classTotal(cells, "etl"), 910.0);
  assert.equal(d.classTotal(cells, "analytics"), 40.0);
});

test("landing CU is not on the page at all", () => {
  // The page compares ENGINES. `dbt_landing` is the ingestion staging area — no run deletes it and
  // every run reads it, so its CU is one cumulative figure belonging to no engine. It is skipped
  // outright, not given a row: the same number repeated under every column read as "each of them spent
  // this". The archive's SIZE still appears — input volume is a different question from cost.
  const r = rec("r-1.json", "spark", {
    OUT: { role: "output", name: "dbt_spark" },
    LAND: { role: "landing", name: "dbt_landing" },
  });
  const { cells, unmeasured } = d.runCu(r, d.normaliseLedger(ledger({ OUT: 10.0, LAND: 507.0 })));
  assert.equal(d.classTotal(cells, "etl"), 10.0, "landing must not be added to the engine's own CU");
  assert.deepEqual(unmeasured, [], "landing is not an item whose CU could be missing");
});

test("the dbt folder costs nothing and is skipped", () => {
  const r = rec("r-1.json", "dwh", {
    F: { role: "folder", name: "dbt" },
    OUT: { role: "output", name: "dbt_dwh" },
  });
  const { cells, unmeasured } = d.runCu(r,
    d.normaliseLedger(ledger({ OUT: { "OneLake Read via Redirect": 1.0 } })));
  assert.deepEqual(cells, { etl: { storage: 1.0 } });
  assert.deepEqual(unmeasured, [], "a folder is not an item whose CU could be missing");
});

test("an item the ledger has never seen is unmeasured, not zero", () => {
  // "not measured yet" and "cost nothing" are different claims, and the sources table has to say which.
  const r = rec("r-1.json", "spark", {
    OUT: { role: "output", name: "dbt_spark" },
    SEM: { role: "semantic_model", name: "aemo_spark" },
  });
  const { cells, unmeasured } = d.runCu(r,
    d.normaliseLedger(ledger({ OUT: { "OneLake Read via Redirect": 5.0 } })));
  assert.deepEqual(cells, { etl: { storage: 5.0 } });
  assert.deepEqual(unmeasured, ["semantic_model/aemo_spark"]);
});

test("compute and storage come from the operation, not the item", () => {
  // They share an ITEM: spark bills its Livy session AND its OneLake reads against one lakehouse, a
  // warehouse bills Warehouse Query AND its OneLake writes against one warehouse. Bucketing by the
  // item's role could never separate them — measured against the live model 2026-08-02.
  const r = rec("r-1.json", "spark", { OUT: { role: "output", name: "dbt_spark" } });
  const { cells } = d.runCu(r, d.normaliseLedger(ledger({
    OUT: {
      "High Concurrency Session Livy Run": 188635.8,
      "OneLake Write via Redirect": 20267.9,
      "OneLake Read via Redirect": 5737.4,
    },
  })));
  assert.equal(cells.etl.compute, 188635.8);
  assert.equal(d.round1(cells.etl.storage), 26005.3);
});

test("every measured operation name buckets the way it should", () => {
  // The names are the real ones off the capacity, not invented.
  for (const op of ["OneLake Write via Redirect", "OneLake Iterative Read via Proxy",
    "OneLake Other Operations", "OneLake Read via Proxy"]) {
    assert.equal(d.bucket(op), "storage", op);
  }
  for (const op of ["High Concurrency Session Livy Run", "Warehouse Query", "SQL Endpoint Query",
    "Jupyter Notebook Scheduled Run", "XMLA Read Operation", "Dataset On-Demand Refresh"]) {
    assert.equal(d.bucket(op), "compute", op);
  }
});

test("the landing lakehouse's SQL endpoint is not an engine's CU", () => {
  // Fabric pairs every lakehouse with a SQL analytics endpoint — a separate billable `Warehouse` item
  // with its own GUID and the role `sql_endpoint`, not `landing`. So landing CU reached the page
  // through the one door the role check does not cover: the SAME endpoint item appears in every run
  // record and charged every engine 130.4 CU it did not spend. Caught by NAME against the record's own
  // landing items, so an engine's OWN endpoint is untouched.
  const r = rec("r-1.json", "spark", {
    L: { role: "landing", name: "dbt_landing" },
    LEP: { role: "sql_endpoint", name: "dbt_landing" },   // landing's — not this engine's
    OEP: { role: "sql_endpoint", name: "dbt_spark" },     // the engine's own — keep
    OUT: { role: "output", name: "dbt_spark" },
  });
  assert.deepEqual([...d.landingGuids(r)], ["LEP"]);
  const { cells, unmeasured } = d.runCu(r, d.normaliseLedger(ledger({
    L: { "Warehouse Query": 70.2 },
    LEP: { "SQL Endpoint Query": 130.4 },
    OEP: { "SQL Endpoint Query": 306.3 },
    OUT: { "High Concurrency Session Livy Run": 900.0 },
  })));
  assert.equal(d.classTotal(cells, "etl"), 1206.3, "900 + the engine's own endpoint, nothing else");
  assert.deepEqual(unmeasured, [], "landing's endpoint is not an item whose CU could be missing");
});

test("seconds split by role exactly like CU", () => {
  // Same GUIDs, same roles, same read — the duration rides in the same Capacity Metrics row, so the
  // join cannot disagree with the CU one.
  const r = rec("r-1.json", "spark", {
    OUT: { role: "output", name: "dbt_spark" },
    SEM: { role: "semantic_model", name: "aemo_spark" },
    L: { role: "landing", name: "dbt_landing" },
  });
  const led = ledger({
    OUT: { "High Concurrency Session Livy Run": 900.0 },
    SEM: { "XMLA Read Operation": 40.0 }, L: { "Warehouse Query": 70.2 },
  });
  led.seconds = secs({
    OUT: { "High Concurrency Session Livy Run": 30.0 },
    SEM: { "XMLA Read Operation": 4.0 }, L: { "Warehouse Query": 9.9 },
  });
  const { cells } = d.runCu(r, d.normaliseLedger(led), "seconds");
  assert.equal(d.classTotal(cells, "etl"), 30.0);
  assert.equal(d.classTotal(cells, "analytics"), 4.0, "landing is skipped here as it is for CU");
});

test("still accruing is derived from the clock, not stored", () => {
  // An hour's CU keeps growing for ~70 minutes after the fact. That is a property of the clock, not a
  // fact worth writing into a file and keeping in step.
  assert.ok(d.stillAccruing(rec("a.json", "dwh", {}, { finishedHoursAgo: 0.5 })));
  assert.ok(!d.stillAccruing(rec("a.json", "dwh", {}, { finishedHoursAgo: 48 })));
  assert.ok(!d.stillAccruing({ run: {} }), "no finished stamp, no claim");
});

// ---------------------------------------------------------------------------------- the columns

test("columns are the latest run per engine and config", () => {
  // One dispatch builds ONE engine, so rendering the newest record alone gives a comparison page with
  // a single column. And spark under readHeavyForPBI answers a different question from spark under
  // writeHeavy: one number cannot stand for both.
  const runs = [
    rec("a-1.json", "spark", {}, {
      config: { spark: { resource_profile: "writeHeavy" } }, finishedHoursAgo: 72,
    }),
    rec("b-2.json", "spark", {}, {
      config: { spark: { resource_profile: "writeHeavy" } }, finishedHoursAgo: 48,
    }),
    rec("c-3.json", "spark", {}, {
      config: { spark: { resource_profile: "readHeavyForPBI" } }, finishedHoursAgo: 24,
    }),
    rec("d-4.json", "dwh", {}, { finishedHoursAgo: 12 }),
  ];
  const cols = d.columnsFor(runs);
  // Alphabetical within an engine: `readHeavyForPBI` before `writeHeavy`. It sorted the other way
  // when the two were labelled `V-Order` and `default`, which is the order changing with the label
  // and not with anything measured — one more reason the profiles are printed verbatim.
  assert.deepEqual(cols.map((c) => c.col), ["spark·readHeavyForPBI", "spark·writeHeavy", "dwh"]);
  const byCol = Object.fromEntries(cols.map((c) => [c.col, c.rec._file]));
  assert.equal(byCol["spark·writeHeavy"], "b-2.json", "the LATER run of a config wins its column");
});

test("one config per engine gets a bare column name", () => {
  assert.deepEqual(d.columnsFor([rec("a-1.json", "dwh", {})]).map((c) => c.col), ["dwh"]);
});

test("a variant tag never contains the column separator", () => {
  // baseEngine splits on COL_SEP; a tag containing one would make the column id unparseable back to
  // its engine, and STACK lookups would silently miss.
  const tag = d.variantTag([["native_execution_engine", "true"],
    ["resource_profile", "readHeavyForPBI"], ["vcores", "64"]]);
  assert.ok(!tag.includes(d.COL_SEP));
  assert.equal(d.baseEngine(`spark${d.COL_SEP}${tag}`), "spark");
  const sorted = d.variantTag([["sorted", "true"], ["vcores", "64"]]);
  assert.ok(!sorted.includes(d.COL_SEP), sorted);
  assert.equal(d.baseEngine(`duckrun${d.COL_SEP}${sorted}`), "duckrun");
});

test("a sorted write gets its own column, and absence reads as unsorted", () => {
  // stats.py records this ONLY when on, so absence is one state — a run predating the input and an
  // unsorted run both wrote unsorted parquet. That is why there is no `unsorted` spelling and no
  // terse fallback, unlike NEE.
  assert.equal(d.variantTag([["sorted", "true"], ["vcores", "64"]]), "64c+sorted");
  assert.equal(d.variantTag([["vcores", "64"]]), "64c");
  // Two duckrun runs at one core count, one sorted: two columns, distinct headers.
  const cols = d.columnsFor([
    lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json" }),
    lay("duckrun", 4, 25, { cfg: { vcores: "64", sorted: "true" }, file: "b-2.json" }),
  ]).map((c) => c.col);
  assert.equal(new Set(cols).size, 2, cols);
  assert.ok(cols.some((c) => c.endsWith("sorted")), cols);
});

test("the layout LABEL names the sort without listing its columns", () => {
  // `duckrun sorted`, beside `spark V-Order` — which does not spell out what V-Order does either.
  // The columns live in the CAPTION now (`layoutLabel`), where the shape already sits.
  assert.equal(d.producer(lay("duckrun", 4, 25, { cfg: { sorted: "true" } })), "duckrun sorted");
  assert.equal(d.producer(lay("duckrun", 4, 27, { cfg: { vcores: "64" } })), "duckrun",
    "vcores still never reaches a caption about parquet");
});

test("a sort splits the layout bar even though the bands do not move", () => {
  // THE reason `sorted` is in layoutKey. The one measured sorted run wrote 4 files either way and
  // 27 -> 25 row groups, which fall in the SAME bands — so without the config in the key these two
  // share a bar and their cold/warm/hot means are averaged, which is the comparison the flag exists
  // to make.
  const plain = lay("duckrun", 4, 27, { cfg: { vcores: "64" } });
  const sorted = lay("duckrun", 4, 25, { cfg: { vcores: "64", sorted: "true" } });
  assert.deepEqual(d.layoutKey(plain).slice(0, 3), d.layoutKey(sorted).slice(0, 3),
    "same V-Order and same bands — the measured half cannot tell them apart");
  assert.notDeepEqual(d.layoutKey(plain), d.layoutKey(sorted));
  // This fixture records no key, so it reads `true` — sorted by something unnamed. The COLUMNS case
  // is the two-sorts test below.
  assert.equal(d.layoutKey(sorted)[3], true);
});

test("a record with no sorted key groups with an unsorted run, not alone", () => {
  // All 13 existing records predate the input. They demonstrably wrote unsorted parquet, so absence
  // here is NOT the "unmeasured" case that earns a bar of its own — that case is a missing file
  // count, which is a different thing entirely.
  const old = lay("duckrun", 4, 27, { cfg: { vcores: "64" } });          // no `sorted` key at all
  const off = lay("iceberg", 4, 27, { cfg: { vcores: "64" } });
  assert.deepEqual(d.layoutKey(old), d.layoutKey(off));
  assert.equal(d.layoutKey(old)[3], false);
});

// A sorted record's own key, in either spelling. `sort_by` is what the run DECLARED (stats.py),
// `sort_by_auto` what duckrun's picker RESOLVED (fabric_run.py's log scrape).
const sortedBy = (files, rgs, key, opts = {}) => {
  const { spelling = "sort_by", ...rest } = opts;
  const r = lay("duckrun", files, rgs, { cfg: { sorted: "true" }, ...rest });
  if (key) r.dbt = { duckrun: { [spelling]: { fct_summary: key } } };
  return r;
};

test("two sorts on different keys never share a bar, even when the bands agree", () => {
  // The real pair — `['date','time','DUID']` (run 30955591822) and `['date','time']` — sits in
  // separate bars today only because 3 and 4 files cross a band boundary. That is luck, so the key
  // carries the columns.
  const duid = sortedBy(4, 25, ["date", "time", "DUID"], { file: "a-1.json" });
  const dt = sortedBy(4, 25, ["date", "time"], { file: "b-2.json" });
  assert.equal(d.layoutKey(duid)[3], "date,time,DUID");
  assert.equal(d.layoutKey(dt)[3], "date,time");
  assert.deepEqual(d.layoutKey(duid).slice(0, 3), d.layoutKey(dt).slice(0, 3));
  assert.equal(d.layoutGroups([{ rec: duid }, { rec: dt }]).length, 2,
    "identical shape, different sort — two bars");
});

test("the sort key comes off the RECORD, in either spelling, and is never guessed", () => {
  // THE KEY IS A PROPERTY OF THE COMMIT: the model declared date,time,DUID for a while and date,time
  // since. A constant in this file was right for today's model only, and captioned run 30955591822 —
  // a DUID sort — `by date, time`. Both spellings are legitimate: `sort_by` is declared, and
  // `sort_by_auto` is the only witness for an `'auto'` run, whose declaration names no columns.
  assert.equal(d.sortKeyOf(sortedBy(4, 25, ["date", "time", "DUID"])), "date,time,DUID");
  assert.equal(d.sortKeyOf(sortedBy(4, 25, ["date", "time"], { spelling: "sort_by_auto" })),
    "date,time");
  // Sorted by SOMETHING the record does not name: `true`, which shares a bar with neither an
  // unsorted run nor any named sort — the rule `layoutKey` already applies to a missing file count.
  const unnamed = sortedBy(4, 25, null);
  assert.equal(d.sortKeyOf(unnamed), true);
  assert.equal(d.sortKeyOf(lay("duckrun", 4, 27, { cfg: { vcores: "64" } })), false);
  assert.equal(new Set([{ rec: unnamed }, { rec: sortedBy(4, 25, ["date", "time"]) },
    { rec: lay("duckrun", 4, 25, { cfg: { vcores: "64" } }) }]
    .map(({ rec }) => JSON.stringify(d.layoutKey(rec)))).size, 3);
});

test("the caption says which columns a sorted bar is ordered by, row groups only", () => {
  // Files are not printed at all: segments are what drive Direct Lake's cost, and the file BAND
  // still separates bars without being said.
  assert.equal(d.layoutLabel([{ rec: sortedBy(4, 25, ["date", "time", "DUID"]) }]),
    "by date, time, DUID · 25 RG");
  assert.equal(d.layoutLabel([{ rec: sortedBy(1, 9, ["date", "time"]) }]), "by date, time · 9 RG");
  // Sorted but unnamed adds NOTHING — the label already says `sorted`, and inventing a key here is
  // the bug this whole path exists to prevent.
  assert.equal(d.layoutLabel([{ rec: sortedBy(1, 9, null) }]), "9 RG");
  assert.equal(d.layoutLabel([{ rec: lay("duckrun", 4, 27, { cfg: { vcores: "64" } }) }]), "27 RG");
  const vo = lay("spark", 11, 11, { vorder: true, cfg: { resource_profile: "readHeavyForPBI" } });
  assert.equal(d.layoutLabel([{ rec: vo }]), "V-Order · 11 RG");
});

test("a column header calls a profile by its own name", () => {
  // The dispatch is given `readHeavyForPBI` and every doc and log line says `readHeavyForPBI`, so the
  // page does too — a reader matching this against a run's inputs should not have to translate. The
  // EFFECT is said where it is measured instead: `layoutCaption` reads `vorder` off the parquet.
  assert.equal(d.variantTag([["resource_profile", "readHeavyForPBI"]]), "readHeavyForPBI");
  // `default` survives because it is a fact about the WORKSPACE — the profile in force when a
  // dispatch asks for nothing — not a rewording of `writeHeavy`.
  assert.equal(d.variantTag([["resource_profile", "writeHeavy"]]), "writeHeavy");
  assert.equal(d.variantTag([["resource_profile", "readHeavyForSpark"]]), "readHeavyForSpark");
});

test("a flag that is off is absent from the header rather than negated", () => {
  const on = [["native_execution_engine", "true"], ["resource_profile", "writeHeavy"]];
  const off = [["native_execution_engine", "false"], ["resource_profile", "writeHeavy"]];
  assert.equal(d.variantTag(on), "writeHeavy+NEE");
  assert.equal(d.variantTag(off), "writeHeavy");
});

test("a column is named for its writer and still resolves to its engine", () => {
  // `iceberg` reads as a format beside three engines when the writer is the same DuckDB duckrun uses,
  // pointed at an Iceberg REST catalog. Naming the column is only safe because `baseEngine` reverses
  // the label — otherwise the STACK lookup and the (engine, variant) join to a record would both
  // silently miss, and a caption or a chart row would quietly go blank.
  const runs = [
    rec("a-1.json", "iceberg", {}, { config: { iceberg: { vcores: "64" } }, finishedHoursAgo: 48 }),
    rec("b-2.json", "iceberg", {}, { config: { iceberg: { vcores: "32" } }, finishedHoursAgo: 24 }),
  ];
  const names = d.columnsFor(runs).map((c) => c.col);
  assert.deepEqual(names, ["duckdb iceberg·32c", "duckdb iceberg·64c"]);
  assert.ok(names.every((c) => d.baseEngine(c) === "iceberg"));
  assert.equal(d.baseEngine("duckdb iceberg"), "iceberg");
  // An engine the map says nothing about is left exactly as it is, both ways.
  assert.deepEqual(d.columnsFor([rec("c-3.json", "dwh", {})]).map((c) => c.col), ["dwh"]);
  assert.equal(d.baseEngine("spark·V-Order"), "spark");
});

test("two configs that would share a header are spelled out instead", () => {
  // Absence-means-off is only unambiguous while every config of the engine RECORDS the flag. A record
  // predating the dispatch input has no key at all, which would collide with an explicit `false` — and
  // a page printing one column name twice is unreadable and says nothing about why.
  const runs = [
    rec("a-1.json", "spark", {}, {
      config: { spark: { resource_profile: "writeHeavy" } }, finishedHoursAgo: 48,
    }),
    rec("b-2.json", "spark", {}, {
      config: { spark: { resource_profile: "writeHeavy", native_execution_engine: "false" } },
      finishedHoursAgo: 24,
    }),
  ];
  const names = d.columnsFor(runs).map((c) => c.col);
  assert.equal(new Set(names).size, 2, names.join(","));
  assert.deepEqual(names, ["spark·writeHeavy", "spark·writeHeavy+noNEE"]);
});

// ------------------------------------------------------------------------------- whole-page shape

test("the page renders end to end with charts and a layout", () => {
  const runs = [rec("a-1.json", "duckrun", {
    OUT: { role: "output", name: "dbt_delta" },
    NB: { role: "compute", name: "dbt-duckrun-baf95ac5" },
    SEM: { role: "semantic_model", name: "aemo_duckrun" },
  }, {
    config: { duckrun: { vcores: "64" } },
    stats: {
      duckrun: {
        fct_summary: {
          total_rows: 143980961, num_files: 4, num_row_groups: 79, avg_row_group: 1822544,
          size_mb: 998.9, vorder: false, schema: "mart",
        },
      },
    },
    tables: ["fct_summary"], landing: { files: 8167, size_mb: 12345.6 },
  })];
  const out = render(runs, ledger({
    OUT: { "OneLake Write via Redirect": 1509.0 },
    NB: { "Jupyter Notebook Scheduled Run": 29571.0 },
    SEM: { "XMLA Read Operation": 2041.0 },
  }));
  const c = charts(out);
  assert.equal(c.length, 1, "analytics alone — the ETL chart is gone, its numbers are in the tables");
  const text = plain(out);
  const rr = rows(out);
  assert.ok(rr.some((r) => r.startsWith("| **etl** |")));
  assert.ok(rr.some((r) => r.startsWith("| **analytics** |")));
  // Bucket-major: the notebook's compute and the lakehouse's storage are separate rows, which is
  // where a DuckDB leg's cost actually goes.
  assert.ok(rr.some((r) => r.startsWith("| `compute` |") && r.includes("29,571.0")));
  assert.ok(rr.some((r) => r.startsWith("| `storage` |") && r.includes("1,509.0")));
  assert.ok(text.includes("fct_summary") && text.includes("1.8M"),
    "the layout block, with row-group size abbreviated");
  assert.ok(text.includes("8,167") && text.includes("12,345.60"),
    "the input archive should be on the page");
  // ANALYTICS is the only chart: it is the interactive CU that throttles, which is the point of the
  // project. It is labelled by the LAYOUT's writer and captioned by the shape, because Power BI
  // never sees the engine.
  assert.ok(c[0].title.includes("Capacity units per parquet layout"));
  assert.ok(c[0].subtitle.includes("lower is better"));
  assert.deepEqual(c[0].labels, ["duckrun"]);
  assert.deepEqual(c[0].captions, ["79 RG"], "the shape is the sub-label, row groups only");
  assert.ok(c[0].values[0].startsWith("2,041.0"));
  // The ETL total is still on the page, in the table that reports it per bucket.
  assert.ok(rr.some((r) => r.startsWith("| **etl** |") && r.includes("31,080.0")), rr.join(" / "));
});

test("a column with no operations of a kind prints a dash, not a zero", () => {
  // A dash says "nothing of that kind was billed here"; 0.0 would say "it was billed and cost
  // nothing". Real case: an iceberg lakehouse bills 40,832 CU and every operation of it is OneLake —
  // its compute is the notebook, a different item entirely.
  const runs = [
    rec("a-1.json", "duckrun", {
      NB: { role: "compute", name: "dbt-duckrun-ab12" },
      OUT: { role: "output", name: "dbt_delta" },
    }),
    rec("b-2.json", "iceberg", { OUT2: { role: "output", name: "dbt_iceberg" } }),
  ];
  const out = render(runs, ledger({
    NB: { "Jupyter Notebook Scheduled Run": 29571.0 },
    OUT: { "OneLake Write via Redirect": 1509.0 },
    OUT2: { "OneLake Iterative Read via Proxy": 40831.8 },
  }));
  const row = rows(out).find((r) => r.startsWith("| `compute` |"));
  assert.ok(row && row.includes("—"), "iceberg's lakehouse bills no compute operation at all");
});

test("the page says when a column can still rise", () => {
  const fresh = [rec("a-1.json", "dwh", { OUT: gone("output", "dbt_dwh") },
    { finishedHoursAgo: 0.5 })];
  assert.ok(plain(render(fresh, ledger({ OUT: 5.0 }))).includes("may still rise"));
  const old = [rec("a-1.json", "dwh", { OUT: gone("output", "dbt_dwh") }, { finishedHoursAgo: 48 })];
  assert.ok(!plain(render(old, ledger({ OUT: 5.0 }))).includes("may still rise"));
});

test("no records explains the contract rather than printing an empty page", () => {
  const out = plain(d.renderEmpty());
  assert.ok(out.includes("No run records") && out.includes("Benchmark"));
  assert.ok(out.includes("not that the capacity was idle"));
});

test("no records and no ledger is an empty page, not an exception", () => {
  const { html, cols } = d.compose([], null, {});
  assert.deepEqual(cols, []);
  assert.ok(plain(html).includes("No run records"));
  assert.deepEqual(d.normaliseLedger(null).items, {});
});

test("the rendered page mentions no landing CU anywhere", () => {
  // Belt and braces on the whole render path, not just the join.
  const runs = [
    rec("a-1.json", "duckrun", {
      OUT: { role: "output", name: "dbt_delta" }, L: { role: "landing", name: "dbt_landing" },
    }),
    rec("b-2.json", "spark", {
      OUT2: { role: "output", name: "dbt_spark" }, L: { role: "landing", name: "dbt_landing" },
    }),
  ];
  const out = plain(render(runs, ledger({ OUT: 1.0, OUT2: 2.0, L: 70.2 })));
  assert.ok(!out.includes("70.2") && !out.includes("dbt_landing ("));
});

test("the numbers come before the methodology", () => {
  // The charts and the table are what the page is for. A reader who already knows what a capacity unit
  // is should not have to scroll past a paragraph explaining it, and a provenance table, to reach them.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 34046.3, SEM: 1514.0 }));
  const firstChart = out.indexOf('<figure class="chart">');
  assert.ok(firstChart > 0);
  assert.ok(firstChart < out.indexOf("Capacity units (CU-seconds) are what this page leads with"));
  assert.ok(firstChart < out.indexOf("About these numbers"));
  assert.ok(firstChart < out.indexOf("Every run on this page"));
  assert.ok(firstChart < out.indexOf("[source]".replace("[", "").replace("]", "")) ||
    firstChart < out.lastIndexOf("<p>"));
  // ...and the heading still leads.
  assert.ok(out.indexOf("<h2>Capacity units") < firstChart);
});

test("EVERY table comes before the methodology, and the methodology is last", () => {
  // A reader arrives for the numbers. `About these numbers` used to sit between the layout tables and
  // the run table, pushing the last table below a screen of prose.
  const runs = [lay("spark", 11, 11, { file: "a-1.json", landing: { files: 8350, size_mb: 170491.5 } }),
    lay("dwh", 78, 78, { file: "b-2.json", landing: { files: 8350, size_mb: 170491.5 } })];
  const out = render(runs, ledger({ OUT: 34046.3, SEM: 1514.0 }));
  // `Analysis` renders here on two ties — two ETL candidates at identical CU and two layout groups
  // ditto. That depends on a tie being REPORTED rather than dropped, which is the design: two
  // indistinguishable numbers is a finding, not a missing one.
  const order = ["<h2>Capacity units", "<h3>Cost and speed by layout", "<h3>Cost by engine",
    "<h3>Table layout", "<h3>Input archive", "<h3>Every run", "<h3>Analysis",
    "<h3>About these numbers"];
  const at = order.map((h) => out.indexOf(h));
  for (const [i, v] of at.entries()) assert.ok(v > 0, `${order[i]} is missing`);
  assert.deepEqual([...at].sort((a, b) => a - b), at,
    `sections are out of order: ${order.map((h, i) => `${h}@${at[i]}`)}`);
  // The provenance line is the only thing after the methodology.
  assert.ok(at[at.length - 1] < out.lastIndexOf("history/cu.json"));
});

// ---------------------------------------------------------------------------------------- the lede

/** A record carrying a landing archive and the full eight-table inventory. */
const scaled = (file, engine, opts = {}) => {
  const { names = ["stg_csv_archive_log", "dim_calendar", "dim_duid", "fct_price", "fct_scada",
    "fct_price_today", "fct_scada_today", "fct_summary"],
    rows = [8167, 3197, 689, 4599900, 370021502, 12750, 750153, 143980961],
    landing = { files: 8350, size_mb: 170491.5 }, ...rest } = opts;
  const stats = {};
  names.forEach((t, i) => { if (rows[i] !== undefined) stats[t] = { total_rows: rows[i] }; });
  return full(file, engine, { landing, tables: names, stats: { [engine]: stats }, ...rest });
};

test("the lede states the scale of the thing, and leads the page", () => {
  // The page named its MEASURE and never its SUBJECT: four columns of CU with no statement of how
  // much data any of it describes.
  const out = render([scaled("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  const said = plain(out);
  // A lone engine NAMES itself and drops the count — "1 engine (spark)" says the same thing twice.
  assert.ok(said.includes("One dbt project on **spark**"));
  assert.ok(said.includes("**170 GB** of raw AEMO CSV (**8,350 files**)"));
  assert.ok(said.includes("built into the same **8 tables**"));
  // ONE fact. `fct_price`/`fct_scada` and their `_today` siblings are raw CSV in the `landing`
  // schema; only `fct_summary` reaches `mart`. The prefix is not the classifier.
  assert.ok(said.includes(
    "1 fact (144.0M), 2 dimensions (3.9K), 4 staging (375.4M) and a log (8.2K)"));
  assert.ok(said.includes("totalling **519,377,319 rows**"));
  // FIRST — above the section heading that used to lead, which is above the first chart.
  assert.ok(out.indexOf('<p class="lede">') >= 0);
  assert.ok(out.indexOf('<p class="lede">') < out.indexOf("<h2>Capacity units"));
});

test("the lede counts engines, not columns", () => {
  // Two configs of one engine are two columns and one engine. The subject is what was measured.
  const runs = [scaled("a-1.json", "spark", { config: { spark: { vcores: 8 } } }),
    scaled("b-2.json", "spark", { config: { spark: { vcores: 64 } } }),
    scaled("c-3.json", "dwh")];
  const said = plain(render(runs, ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(said.includes("One dbt project on **2 engines** (dwh and spark)"));
});

test("the lede counts engines, not table formats", () => {
  // `iceberg` is the same DuckDB as `duckrun` pointed at an Iceberg REST catalog — a table format,
  // not a fourth engine. Both targets still show, as what they are.
  const runs = [scaled("a-1.json", "duckrun"), scaled("b-2.json", "iceberg"),
    scaled("c-3.json", "spark"), scaled("d-4.json", "dwh")];
  const said = plain(render(runs, ledger({ OUT: 1.0, SEM: 2.0 })));
  // Named, and named by FAMILY: the pair is one `duckdb`, never `duckrun` and `iceberg` both.
  // Alphabetical, the order the side-by-side columns already use.
  assert.ok(said.includes(
    "One dbt project on **3 engines** (duckdb, dwh and spark) across **4 dbt targets**"),
    said.slice(0, 200));
  // With no shared family the clause would repeat the engine count, so it is not said at all.
  const two = plain(render([scaled("a-1.json", "spark"), scaled("b-2.json", "dwh")],
    ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(!two.includes("dbt targets"));
});

test("the lede and the Input archive table quote the SAME archive", () => {
  // Two readers of `layout.landing` picking their own record is how a page says 170 GB at the top and
  // 168 at the foot, which reads as a bug in the measurement rather than in the page.
  // Which of the two records wins is `landingBlocks`' business and is not asserted here — that they
  // AGREE is, because it is the property that survives a change to that rule.
  const runs = [scaled("a-1.json", "dwh", { landing: { files: 10, size_mb: 1000 } }),
    scaled("b-2.json", "spark", { landing: { files: 8350, size_mb: 170491.5 } })];
  const out = render(runs, ledger({ OUT: 1.0, SEM: 2.0 }));
  const foot = rows(block(out, "<h3>Input archive</h3>")).pop();
  const files = (plain(out).match(/\(\*\*([\d,]+) files\*\*\)/) || [])[1];
  assert.ok(files, "the lede must state a file count");
  assert.ok(foot.includes(`**${files}**`), `lede said ${files}, Input archive said ${foot}`);
});

test("the archive is size_mb / 1000, which is what the Input archive table prints", () => {
  // `stats.py` stores bytes/1048576, so this is really MiB and the archive is 178.8 GB decimal. The
  // page prints the figure that agrees on sight with the `170,491.5 MB` in its own table. A later
  // switch to /1024 is then a visible test change rather than a silent one.
  const said = plain(render([scaled("a-1.json", "spark")], ledger({ OUT: 1.0 })));
  assert.ok(said.includes("**170 GB**"));
  assert.ok(!said.includes("**167 GB**") && !said.includes("**179 GB**"));
});

test("an unmeasured archive is an absent clause, never 0 GB", () => {
  const said = plain(render([scaled("a-1.json", "spark", { landing: null })],
    ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(!said.includes("GB of raw AEMO CSV"), "no size may be claimed");
  assert.ok(!said.includes("0 GB"));
  // ...but what it DID measure still gets said.
  assert.ok(said.includes("built into the same **8 tables**"));
  assert.ok(said.includes("totalling **519,377,319 rows**"));
});

test("a record with no table inventory renders no table clause", () => {
  const r = scaled("a-1.json", "spark", { names: [], rows: [] });
  r.layout.stats = {};
  const said = plain(render([r], ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(!said.includes("built into the same"));
  assert.ok(!said.includes("totalling"));
  // The archive it DID measure still gets said.
  assert.ok(said.includes("**170 GB** of raw AEMO CSV"));
});

test("with nothing measured at all there is no lede, not a sentence of dashes", () => {
  const r = scaled("a-1.json", "spark", { names: [], rows: [], landing: null });
  r.layout.stats = {};
  const out = render([r], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!out.includes('<p class="lede">'));
  assert.ok(!plain(out).includes("One dbt project on"));
});

test("a PARTIAL row total is dropped, never printed as the total", () => {
  // Seven tables of eight labelled `in total` is a WRONG number, not an incomplete one, and it would
  // sit on the page looking entirely plausible.
  const said = plain(render([scaled("a-1.json", "spark", { rows: [8167, 3197, 689, 4599900] })],
    ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(!said.includes("totalling"), "a short sum must not be printed");
  assert.ok(!said.includes("4,611,953"));
  // The table COUNT is still known and still said.
  assert.ok(said.includes("built into the same **8 tables**"));
});

test("the total sums the run's table LIST, not every key of its stats block", () => {
  const r = scaled("a-1.json", "spark");
  r.layout.stats.spark.some_scratch_table = { total_rows: 999999999 };
  assert.equal(d.totalRows(r), 519377319);
  assert.ok(plain(render([r], ledger({ OUT: 1.0 }))).includes("totalling **519,377,319 rows**"));
});

test("the fct_ prefix is not the classifier — there is exactly ONE fact", () => {
  // `fct_price`, `fct_scada` and their `_today` siblings are raw AEMO CSV landed in the `landing`
  // schema; only `fct_summary` reaches `mart` and is the (date, time, DUID) grain Power BI queries.
  // Counting the prefix called four landed sources "facts" and the real one "a mart".
  const said = plain(render([scaled("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(said.includes("1 fact ("), "the mart is the fact");
  assert.ok(!said.includes("4 facts"), "the landed fct_ tables are staging, not facts");
  assert.ok(said.includes("4 staging"));
  assert.ok(said.includes("and a log"), "stg_csv_archive_log is the log");
  // The ROWS are what make the breakdown worth reading: the four landed sources carry 370M+ and
  // the one real fact 144M, which the shape alone hides. Compacted — the exact total closes the
  // same sentence, so twelve digits twice is precision nobody reads.
  assert.ok(said.includes("1 fact (144.0M)"), said);
  assert.ok(said.includes("4 staging (375.4M)"), said);
  assert.ok(said.includes("2 dimensions (3.9K)"), said);
  assert.ok(said.includes("a log (8.2K)"), said);
});

test("one unmeasured table withholds every row count, not just its own", () => {
  // Same rule as totalRows dropping a partial sum: a category quietly short of a table sits beside
  // the others looking complete. The SHAPE still goes out — it is measured by name, not by stats.
  const said = plain(render([scaled("a-1.json", "spark",
    { rows: [8167, 3197, 689, 4599900, 370021502, 12750, 750153, undefined] })],
    ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(said.includes("1 fact, 2 dimensions, 4 staging and a log"), said.slice(0, 240));
  assert.ok(!said.includes("(375.4M)"), "no category may print while another cannot");
});

test("a breakdown that would not add up is dropped, and the count goes out alone", () => {
  // A decomposition quietly short of the count beside it contradicts it.
  const said = plain(render([scaled("a-1.json", "spark",
    { names: ["fct_summary", "dim_duid", "mystery_table"], rows: [1, 2, 3] })],
    ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(said.includes("built into the same **3 tables**"));
  assert.ok(!said.includes("1 fact"), "no breakdown when it does not account for every table");
  assert.ok(said.includes("totalling **6 rows**"));
});

test("the page says which of its measures is the comparable one", () => {
  // A capacity unit already prices in how much compute an engine was given — that is the whole reason
  // CU leads. The two time measures do NOT have that property.
  const out = plain(render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(out.includes("The CU columns are directly comparable"));
  assert.ok(out.includes("reason to lead with cost"));
  assert.ok(out.includes("sample of a shared capacity"), "the ms caveat has to be stated");
});

test("the table says where the compute/storage split comes from", () => {
  // Compute and storage share an item, so a reader who assumes the rows are per-item will misread
  // every column.
  const out = plain(render([full("a-1.json", "spark")], ledger({ OUT: 34046.3, SEM: 1514.0 })));
  assert.ok(out.includes("comes from the OPERATION"));
  assert.ok(out.includes("share an ITEM"));
  assert.ok(out.includes("Every `OneLake …` operation is storage"));
});

test("a class with one item per engine is not decomposed", () => {
  // analytics is always exactly one semantic model per engine, so bucket rows there would repeat the
  // subtotal and add a row of em dashes for every other engine. etl splits because a DuckDB leg really
  // is a notebook plus a lakehouse.
  //
  // The lakehouse bills a OneLake operation on purpose: the Python original gave it a compute one, so
  // `etl` held a single bucket and did not decompose at all — and the assertion passed anyway, on the
  // words `compute` and `storage` in the note underneath. Rows, not prose.
  const runs = [
    rec("a-1.json", "duckrun", {
      NB: gone("compute", "dbt-duckrun-ab12"), OUT: gone("output", "dbt_delta"),
      SEM: gone("semantic_model", "aemo_duckrun"),
    }),
    rec("b-2.json", "spark", {
      OUT2: gone("output", "dbt_spark"), SEM2: gone("semantic_model", "aemo_spark"),
    }),
  ];
  const out = render(runs, ledger({
    NB: 26403.5, OUT: { "OneLake Write via Redirect": 2463.9 }, SEM: 2157.8,
    OUT2: 34046.3, SEM2: 1514.0,
  }));
  const rr = rows(out);
  const analytics = rr.find((r) => r.startsWith("| **analytics** |"));
  assert.ok(analytics.includes("2,157.8") && analytics.includes("1,514.0"));
  assert.ok(!plain(out).includes("semantic_model"), "no per-item analytics rows");
  // etl still decomposes: duckrun is genuinely a notebook plus a lakehouse.
  assert.ok(rr.some((r) => r.startsWith("| `compute` |")));
  assert.ok(rr.some((r) => r.startsWith("| `storage` |")));
});

// ------------------------------------------------------------------------------------- validity

test("a whole generation is accepted", () => {
  assert.equal(d.incomplete(full("a-1.json", "spark")), null);
});

test("a run that was not torn down is caveated, not rejected", () => {
  // Its items are still alive and Fabric keeps billing them, so its total creeps upward — but the
  // creep is small, and a column that disappears costs more than one carrying a caveat.
  const r = full("a-1.json", "duckrun");
  delete r.items.OUT.deleted;
  assert.equal(d.incomplete(r), null, "it still renders");
  assert.deepEqual(d.drifting(r), ["output/dbt_duckrun"], "and it is named as still billing");
});

test("a torn-down run is not drifting", () => {
  assert.deepEqual(d.drifting(full("a-1.json", "spark")), []);
});

test("the sources table says which column is still billing", () => {
  const good = full("a-1.json", "spark");
  const bad = full("b-2.json", "duckrun");
  delete bad.items.OUT.deleted;
  const out = plain(render([good, bad], ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(out.includes("**still billing** — 1 item(s) never deleted"));
  assert.ok(out.includes("predates that teardown and still owns `output/dbt_duckrun`"));
  assert.ok(out.includes("upper bound on that run rather than a measurement of it"));
});

test("a run with no benchmark is rejected", () => {
  // An empty analytics column reads as "querying this engine was free" rather than "nobody measured
  // it". Run 30743411308 is exactly this — the bench job was skipped by a needs bug.
  const r = full("a-1.json", "spark");
  r.benchmark = {};
  assert.match(d.incomplete(r), /query half did not run/);
});

test("a run with no layout is rejected", () => {
  const r = full("a-1.json", "spark");
  r.layout.stats = {};
  assert.match(d.incomplete(r), /build half did not report/);
});

test("incomplete records are skipped by the loader and named", () => {
  // Skipped, never silently dropped: a page that quietly ignores a record is indistinguishable from
  // one that never had it.
  const good = full("a-1.json", "spark"), bad = full("b-2.json", "dwh");
  bad.benchmark = {};
  const { runs, skipped } = d.selectRuns([good, bad]);
  assert.deepEqual(runs.map((r) => r._file), ["a-1.json"]);
  assert.equal(skipped.length, 1);
  assert.match(skipped[0], /^b-2\.json: /);
});

// ------------------------------------------------------------------------------- the input archive

test("the input archive is one table, not a column per engine", () => {
  // dbt_landing holds ONE copy of the CSVs and every engine reads the same bytes.
  const landing = {
    files: 8338, size_mb: 170491.40,
    folders: {
      "csv_raw/daily": { files: 3042, size_mb: 170004.56 },
      "csv_raw/price_today": { files: 2550, size_mb: 381.24 },
    },
  };
  const runs = [full("a-1.json", "duckrun", { landing }), full("b-2.json", "spark", { landing })];
  const out = render(runs, ledger({ OUT: 1.0, SEM: 2.0 }));
  const block = plain(out.split("Input archive")[1].split("<h3")[0]);
  assert.ok(block.includes("folder") && block.includes("size MB"));
  assert.ok(!block.includes("duckrun") && !block.includes("spark"), "no engine column");
  assert.ok(block.includes("csv_raw/daily") && block.includes("170,004.56"));
  assert.ok(block.includes("**8,338**") && block.includes("**170,491.40**"));
  assert.equal(block.split("170,491.40").length - 1, 1, "the total is stated once, not per engine");
});

test("a changed archive between runs is stated, not averaged", () => {
  const runs = [
    full("a-1.json", "duckrun", { landing: { files: 8000, size_mb: 150000.0, folders: {} } }),
    full("b-2.json", "spark", { landing: { files: 8338, size_mb: 170491.4, folders: {} } }),
  ];
  const out = plain(render(runs, ledger({ OUT: 1.0, SEM: 2.0 })));
  assert.ok(out.includes("did not all read the same archive") && out.includes("150,000.0"));
});

// ------------------------------------------------------------- the run table's autofilter
//
// A stub DOM, because the alternative is shipping the only interactive code on the page with nothing
// checking it. It implements exactly what `wireTables` touches and nothing else — if that function
// starts reaching for something new, this stub is where it will say so.

class El {
  constructor(tag) {
    this.tagName = String(tag).toUpperCase();
    this.children = []; this.dataset = {}; this.style = {}; this.attrs = {};
    this.listeners = {}; this.classes = new Set(); this._text = "";
    this.classList = {
      toggle: (c, on) => (on ? this.classes.add(c) : this.classes.delete(c)),
      contains: (c) => this.classes.has(c),
    };
  }
  get className() { return [...this.classes].join(" "); }
  set className(v) { this.classes = new Set(String(v).split(/\s+/).filter(Boolean)); }
  get textContent() { return this.children.length ? this.children.map((c) => c.textContent).join("") : this._text; }
  set textContent(v) { this._text = String(v); this.children = []; }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { this.children = this.children.filter((x) => x !== c); this.children.push(c); return c; }
  insertBefore(c, ref) {
    const at = ref ? this.children.indexOf(ref) : this.children.length;
    this.children.splice(at < 0 ? this.children.length : at, 0, c);
    return c;
  }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  addEventListener(k, fn) { (this.listeners[k] = this.listeners[k] || []).push(fn); }
  fire(k, ev = {}) { for (const fn of this.listeners[k] || []) fn(ev); }
  find(pred, out = []) {
    for (const c of this.children) { if (pred(c)) out.push(c); c.find && c.find(pred, out); }
    return out;
  }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  querySelectorAll(sel) {
    return sel.startsWith(".")
      ? this.find((c) => c.classes.has(sel.slice(1)))
      : this.find((c) => c.tagName === sel.toUpperCase());
  }
}

/** A `.filtered` box holding one table, plus the document that built it. */
function stubTable(head, body, data = {}) {
  const doc = { createElement: (t) => new El(t) };
  const box = new El("div");
  box.className = "filtered";
  Object.assign(box.dataset, data);
  const tbl = new El("table");
  const th = head.map((h) => { const e = new El("th"); e.textContent = h; return e; });
  tbl.tHead = { rows: [{ cells: th }] };
  const rows = body.map((cells) => {
    const tr = new El("tr");
    tr.cells = cells.map((c) => { const e = new El("td"); e.textContent = c; return e; });
    return tr;
  });
  tbl.tBodies = [Object.assign(new El("tbody"), {
    rows, appendChild(r) { const i = this.rows.indexOf(r); if (i >= 0) this.rows.splice(i, 1); this.rows.push(r); return r; },
  })];
  box.appendChild(tbl);
  const root = new El("div");
  root.appendChild(box);
  return { root, box, tbl, doc, th, rows };
}

const visible = (rows) => rows.filter((r) => r.style.display !== "none")
  .map((r) => r.cells[0].textContent);

test("a number sorts as a number, and what is not one sorts last", () => {
  // `26,583.6` against `9,986.3`: text order puts the smaller first on its leading digit.
  assert.equal(d.cellNumber("26,583.6"), 26583.6);
  assert.ok(Number.isNaN(d.cellNumber("—")));
  assert.ok(Number.isNaN(d.cellNumber("2026-08-03 11:32 (full)")));
  assert.ok(d.compareCells("9,986.3", "26,583.6") < 0);
  assert.ok(d.compareCells("100", "—") < 0, "a dash is not measured, so it never wins a ranking");
  assert.ok(d.compareCells("—", "100") > 0, "...in either direction");
  assert.ok(d.compareCells("duckrun", "spark") < 0);
});

test("the free text is a substring and a menu is exact, ANDed", () => {
  const cells = ["duckrun·64c", "30809945203", "2026-08-03 11:32 (full)", "26,591.0", "settled"];
  assert.ok(d.matchesFilter(cells, "DUCK"), "case-insensitive substring");
  assert.ok(d.matchesFilter(cells, "3080"), "and it reaches the run id");
  assert.ok(!d.matchesFilter(cells, "spark"));
  assert.ok(d.matchesFilter(cells, "", { 4: "settled" }));
  assert.ok(!d.matchesFilter(cells, "", { 4: "may still rise" }), "a menu is EXACT, not substring");
  assert.ok(!d.matchesFilter(cells, "spark", { 4: "settled" }), "both, never either");
  assert.ok(d.matchesFilter(cells, "", { 4: "" }), "an unset menu constrains nothing");
});

test("the filter bar is built from the rows that are already there", () => {
  // The dropdown's options ARE the column's distinct values, read off the DOM — so the list cannot
  // describe a column it no longer matches, and the render layer stays a pure string function.
  const { root, box, doc, rows } = stubTable(
    ["column", "run", "etl CU", "state"],
    [["duckrun·64c", "301", "26,990.9", "settled"],
      ["duckrun·64c", "302", "22,623.6", "settled"],
      ["spark·V-Order", "303", "34,048.3", "may still rise"]],
    { find: "filter runs", menus: "0,3" });
  assert.equal(d.wireTables(root, doc), 1);
  const bar = box.querySelector(".filterbar");
  const menus = bar.querySelectorAll("select");
  assert.equal(menus.length, 2, "one per declared column, and no more");
  assert.deepEqual(menus[0].children.map((o) => o.textContent),
    ["all column", "duckrun·64c", "spark·V-Order"], "distinct, with an all-clear first");
  assert.deepEqual(menus[1].children.map((o) => o.textContent),
    ["all state", "may still rise", "settled"]);
  assert.equal(bar.querySelector(".fcount").textContent, "3 rows");

  // Free text narrows...
  const find = bar.querySelector("input");
  find.value = "spark";
  find.fire("input");
  assert.deepEqual(visible(rows), ["spark·V-Order"]);
  assert.equal(bar.querySelector(".fcount").textContent, "1 of 3 rows");
  // ...and a hidden row is HIDDEN, never removed: ctrl-F and the offline copy still see every run.
  assert.equal(rows.length, 3);
  find.value = "";
  find.fire("input");
  assert.deepEqual(visible(rows), ["duckrun·64c", "duckrun·64c", "spark·V-Order"]);
  // A menu is ANDed with the text.
  menus[0].value = "duckrun·64c";
  menus[0].fire("change");
  assert.equal(bar.querySelector(".fcount").textContent, "2 of 3 rows");
});

test("a header click sorts, and clicking it again reverses", () => {
  const { root, doc, th, tbl } = stubTable(
    ["column", "etl CU"],
    [["duckrun", "26,990.9"], ["spark", "9,986.3"], ["dwh", "38,225.3"]],
    { menus: "" });
  d.wireTables(root, doc);
  const order = () => tbl.tBodies[0].rows.map((r) => r.cells[0].textContent);
  th[1].fire("click");
  assert.deepEqual(order(), ["spark", "duckrun", "dwh"], "cheapest first, numerically");
  th[1].fire("click");
  assert.deepEqual(order(), ["dwh", "duckrun", "spark"], "and the same header reverses it");
  th[0].fire("click");
  assert.deepEqual(order(), ["duckrun", "dwh", "spark"], "a new column starts ascending");
  assert.ok(th[0].classList.contains("asc") && !th[1].classList.contains("desc"),
    "the caret marks one column, and only the current one");
});

test("a sort-only table gets clickable headers and none of the bar", () => {
  // `table(…, {sort: true})` — the Cost-and-speed table wants reordering, not a search box and a
  // row count over seven rows.
  const { root, box, doc, th, tbl } = stubTable(
    ["layout", "CU"],
    [["duckrun", "1,810.1"], ["spark V-Order", "1,381.0"]], {});
  box.className = "sortable";
  assert.equal(d.wireTables(root, doc), 1, "a .sortable box counts as wired");
  assert.equal(box.querySelector(".filterbar"), null, "no bar, no menus, no count");
  th[1].fire("click");
  assert.deepEqual(tbl.tBodies[0].rows.map((r) => r.cells[0].textContent),
    ["spark V-Order", "duckrun"], "and the headers sort, cheapest first");
  th[1].fire("click");
  assert.deepEqual(tbl.tBodies[0].rows.map((r) => r.cells[0].textContent),
    ["duckrun", "spark V-Order"], "and reverse");
});

test("the run table is the only filterable one, and renders whole without JS", () => {
  // Progressive enhancement: the markup carries every row and no controls at all. A reader with
  // scripts off, and every test here, sees the table as it always was.
  const { html } = d.compose([full("a-1.json", "spark")], ledger({ OUT: 12.5, SEM: 3.25 }), {});
  const runs = block(html, "Every run on this page");
  assert.ok(runs.includes('class="filtered"'), "the run table is marked for the autofilter");
  assert.ok(runs.includes('data-menus="0,7"'), "menus on `column` and `state`");
  assert.ok(!runs.includes("<select") && !runs.includes("<input"),
    "and it emits no controls — `wireTables` builds them from the rows");
  assert.equal((html.match(/class="filtered"/g) || []).length, 1, "no other table gets one");
  assert.ok(rows(runs).length >= 2, "header and at least one run, filter or no filter");
});

test("every run carries its own RG count, and a run without one carries a dash", () => {
  // The shape the row's analytics numbers were measured against, per run — the chart caption can
  // only say the bar's range. A dash when the run recorded no layout: unmeasured is not zero.
  const measured = lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json" });
  const bare = full("b-2.json", "spark",                        // stats carry no num_row_groups
    { stats: { spark: { fct_summary: { total_rows: 143980961 } } } });
  const { html } = d.compose([measured, bare], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  const body = rows(block(html, "Every run on this page")).slice(1);
  const cell = (r) => r.split("|").map((c) => c.trim())[4];     // column, run, built, RG
  assert.ok(rows(block(html, "Every run on this page"))[0].includes("| RG |"));
  assert.equal(cell(body.find((r) => r.includes("duckrun"))), "27");
  assert.equal(cell(body.find((r) => r.includes("spark"))), "—");
});

// ------------------------------------------------------------------------------------- the charts

test("the bar is the MEDIAN across runs, and the whisker is the full range", () => {
  // One dispatch is one sample of a SHARED capacity, so a single number is a reading rather than a
  // result — and a BAD sample is not a property of the layout. Real case: run 30966983384 read
  // 2,629.3 against 1,331.5/1,577.1/1,586.7 for byte-identical parquet, because its XMLA read billed
  // 49s against ~33s and its refresh took 28.4s against ~8s. A mean lets that one run lift the bar;
  // the median does not. The values below are that shape — mean 2,000, median 1,500 — so this test
  // fails if anyone puts the mean back. The whisker still shows the outlier: the median is what the
  // bar claims, the range is what the reader checks it against.
  const runs = [full("a-1.json", "spark", { finishedHoursAgo: 72 }),
    full("b-2.json", "spark", { finishedHoursAgo: 48 }),
    full("c-3.json", "spark", { finishedHoursAgo: 24 })];
  runs.forEach((r, i) => {
    r.items = { [`S${i}`]: gone("semantic_model", "aemo_spark"), [`O${i}`]: gone("output", "dbt_spark") };
  });
  const led = ledger({
    S0: { "XMLA Read Operation": 1000.0 }, O0: { "Warehouse Query": 1.0 },
    S1: { "XMLA Read Operation": 3500.0 }, O1: { "Warehouse Query": 1.0 },
    S2: { "XMLA Read Operation": 1500.0 }, O2: { "Warehouse Query": 1.0 },
  });
  const out = render(runs, led);
  const c = charts(out)[0];
  assert.deepEqual(c.labels, ["spark"], "the analytics bar is NAMED for its writer");
  assert.equal(c.values[0], "1,500.0", "the median, NOT the 2,000.0 mean");
  assert.ok(c.svg.includes("range 1,000.0–3,500.0"), "the outlier is still drawn, not averaged away");
  assert.ok(!c.subtitle.includes("mean of"), "no run-count chatter in the subtitle — the whisker shows it");
  // ...and the two tables under it quote that same 1,500: one measurement shown three times.
  assert.ok(rows(block(out, "Cost and speed by layout")).some((r) => r.includes("| 1,500 |")),
    rows(block(out, "Cost and speed by layout")).join(" / "));
});

test("an even number of runs takes the middle two, and one run is itself", () => {
  // The honest limit, pinned so nobody reads the median as a noise fix: at n=1 and n=2 it IS the
  // mean, and four of nine bars on the real page are that thin. It dampens an outlier once there
  // are three samples; only more dispatches make one trustworthy.
  const four = [1000, 1500, 1600, 4000];   // middle two -> 1,550, mean would be 2,025
  const mk = (vals) => {
    const runs = vals.map((_, i) => full(`${"abcd"[i]}-${i}.json`, "spark",
      { finishedHoursAgo: 96 - i * 24 }));
    runs.forEach((r, i) => {
      r.items = { [`S${i}`]: gone("semantic_model", "aemo_spark"),
        [`O${i}`]: gone("output", "dbt_spark") };
    });
    return charts(render(runs, ledger(Object.fromEntries(vals.flatMap((v, i) =>
      [[`S${i}`, { "XMLA Read Operation": v }], [`O${i}`, { "Warehouse Query": 1.0 }]])))))[0];
  };
  assert.equal(mk(four).values[0], "1,550.0");
  assert.equal(mk([1000, 3000]).values[0], "2,000.0", "n=2: the median is the mean");
  assert.equal(mk([2500]).values[0], "2,500.0", "n=1: the reading itself");
});

test("the chart sorts by the bar value", () => {
  const runs = [full("a-1.json", "spark"), full("b-2.json", "dwh")];
  runs[0].items = { S0: gone("semantic_model", "aemo_spark"), O0: gone("output", "dbt_spark") };
  runs[1].items = { S1: gone("semantic_model", "aemo_dwh"), O1: gone("output", "dbt_dwh") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 9.0 }, O0: { "Warehouse Query": 1.0 },
    S1: { "XMLA Read Operation": 3.0 }, O1: { "Warehouse Query": 1.0 },
  }));
  assert.deepEqual(charts(out)[0].labels, ["dwh", "spark"], "cheapest mean first");
});

test("the svg draws a whisker only when there is a range", () => {
  // A single run is a point, and drawing a zero-width whisker on it would suggest a spread that was
  // never measured.
  const wide = d.chartSvg("t", "s", [["spark", 1500.0, 1000.0, 2000.0, "cap"]]);
  assert.equal((wide.match(/class="whisker"/g) || []).length, 1);
  assert.equal((wide.match(/whisker-cap/g) || []).length, 2);
  assert.ok(wide.includes("range 1,000.0–2,000.0"), "the exact spread is in the tooltip");
  const flat = d.chartSvg("t", "s", [["dwh", 1853.5, 1853.5, 1853.5, "cap"]]);
  assert.ok(!flat.includes('class="whisker"'));
});

test("the svg still takes the older three-field row", () => {
  // `[label, value, caption]` — so a chart spec from an artifact rendered months ago still draws.
  const svg = d.chartSvg("t", "s", [["spark", 42.0, "cap"]]);
  assert.ok(svg.includes("42.0") && !svg.includes('class="whisker"'));
});

test("a chart with nothing but zeros is not drawn", () => {
  assert.equal(d.chartSvg("t", "s", [["spark", 0, 0, 0, ""]]), "");
  assert.equal(d.chartSvg("t", "s", []), "");
});

// ------------------------------------------------------- one bar per LAYOUT, not per engine

test("the same parquet is one bar however many engines wrote it", () => {
  // Power BI never sees the engine — it opens parquet through Direct Lake and transcodes row groups.
  // duckrun at 64 cores and at 32 wrote 4 files and 27 row groups either way, so two bars 50% apart
  // was not a comparison: it was one layout measured twice, presented as two results.
  const runs = [
    lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 27, { cfg: { vcores: "32" }, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 2000.0 }, O1: 1.0,
  }));
  const c = charts(out);
  assert.deepEqual(c[0].labels, ["duckrun"], "one layout, one bar");
  assert.equal(c[0].values[0], "1,500.0");
  assert.deepEqual(c[0].captions, ["27 RG"], "the shape it grouped on sits underneath");
  assert.equal(c.length, 1, "and it is the only chart");
  // ...while the ETL side keeps BOTH columns, because there the writer and the compute it was given
  // are the entire subject. That asymmetry is the change and it must not be tidied away — it just
  // reads across a chart and a table now rather than across two charts.
  assert.deepEqual(rows(block(out, "Cost by engine"))[0],
    "| CU (s) | duckrun·32c | duckrun·64c |");
});

test("two runs of ONE column that wrote different parquet are two bars", () => {
  // The bug this pins, on the real records: `duckrun·64c+sorted` wrote 3 files / 26 RG under an
  // explicit sort key and 4 files / 25 under the one `sort_by='auto'` resolved to. Grouping the
  // COLUMNS and pouring every run of each into its bar put them together at their mean — 2,041.8, a
  // number neither run measured — captioned with only the newer one's shape. The layout is measured
  // per RUN, so it has to be grouped per run.
  const cfg = { vcores: "64", sorted: "true" };
  const runs = [
    lay("duckrun", 3, 26, { cfg, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 25, { cfg, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs.forEach((r) => { r.dbt = { duckrun: { sort_by: { fct_summary: ["date", "time"] } } }; });
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 2400.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 1600.0 }, O1: 1.0,
  }));
  const c = charts(out)[0];
  assert.deepEqual(c.labels, ["duckrun sorted", "duckrun sorted"], "one label, two layouts");
  assert.deepEqual(c.values, ["1,600.0", "2,400.0"], "each run's OWN CU, never their mean");
  // The caption is the whole reason two bars with one label read: the label answers who wrote it.
  assert.deepEqual(c.captions, ["by date, time · 25 RG", "by date, time · 26 RG"]);
  // ...and the mart block says the same thing, because its rows ARE these groups.
  const body = rows(block(out, "the mart the queries land on")).slice(1);
  assert.equal(body.length, 2, "one mart row per bar, not one per writer");
  // Fewest files first, and the block carries LAYOUT ONLY — the CU that used to sit here is in the
  // charts and in `Cost by engine`, on the run that measured it.
  assert.ok(body[0].startsWith("| duckrun sorted | 3 | 26 |"), body[0]);
  assert.ok(body[1].startsWith("| duckrun sorted | 4 | 25 |"), body[1]);
  assert.ok(!body[0].includes("1,600"), "no CU column on the layout block");
  // The ETL half is unmoved and it is now a TABLE rather than a bar: both runs are samples of ONE
  // column, so `Cost by engine` reports the column once.
  assert.equal(charts(out).length, 1, "analytics only");
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.startsWith("| **etl** |")));
});

test("a column whose runs recorded no layout stays ONE bar", () => {
  // The "two unmeasured layouts are not one layout" rule is about two different COLUMNS. Splitting one
  // column's own runs would print the same label three times with no caption able to say why.
  const runs = ["a-1.json", "b-2.json", "c-3.json"].map((f, i) =>
    full(f, "spark", { finishedHoursAgo: 72 - i * 24 }));
  runs.forEach((r, i) => {
    r.items = { [`S${i}`]: gone("semantic_model", "aemo_spark"), [`O${i}`]: gone("output", "dbt_spark") };
  });
  const c = charts(render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, S1: { "XMLA Read Operation": 2000.0 },
    S2: { "XMLA Read Operation": 1500.0 },
  })))[0];
  assert.deepEqual(c.labels, ["spark"]);
  assert.equal(c.values[0], "1,500.0", "the mean of all three, as before");
});

test("an engine is named for who writes when the target name misleads", () => {
  assert.equal(d.producer(lay("iceberg", 357, 1172)), "duckdb iceberg");
  assert.equal(d.producer(lay("duckrun", 4, 27)), "duckrun", "only where the name misleads");
});

test("V-Order never merges with anything", () => {
  // The sharpest experiment on the page: the same file band with V-Order on and off.
  const a = lay("spark", 11, 11, { vorder: true, cfg: { resource_profile: "readHeavyForPBI" } });
  const b = lay("spark", 14, 14, { vorder: false, cfg: { resource_profile: "writeHeavy" } });
  assert.notDeepEqual(d.layoutKey(a), d.layoutKey(b));
  assert.equal(d.layoutKey(a)[1], d.layoutKey(b)[1], "same file band, on purpose");
});

test("a band absorbs drift but not a real difference", () => {
  // 78 files and 80 are the same writer with the same settings and one more incremental run.
  assert.equal(d.layoutBand(78), d.layoutBand(80));
  assert.equal(d.layoutBand(10), d.layoutBand(11));
  assert.equal(d.layoutBand(11), d.layoutBand(14));
  assert.notEqual(d.layoutBand(27), d.layoutBand(1172));
  assert.notEqual(d.layoutBand(1172), d.layoutBand(4));
  assert.equal(d.layoutBand(0), -1);
  assert.equal(d.layoutBand(null), -1);
});

test("an unmeasured layout is never grouped with another one", () => {
  // Two records carrying no file count are not two identical layouts, they are two unmeasured ones.
  const a = full("a-1.json", "spark"), b = full("b-2.json", "dwh");   // stats carry total_rows only
  assert.equal(d.layoutKey(a), null);
  assert.equal(d.layoutKey(b), null);
  assert.equal(d.layoutGroups(d.columnsFor([a, b])).length, 2);
});

test("the producer name drops what never reached the parquet", () => {
  assert.equal(d.producer(lay("spark", 11, 11, {
    cfg: { resource_profile: "readHeavyForPBI", native_execution_engine: "true" },
  })), "spark readHeavyForPBI");
  assert.equal(d.producer(lay("spark", 14, 14, {
    cfg: { resource_profile: "writeHeavy", native_execution_engine: "false" },
  })), "spark writeHeavy");
  assert.equal(d.producer(lay("duckrun", 4, 27, { cfg: { vcores: "64" } })), "duckrun");
  // An unmapped profile keeps its own name — `readHeavyForSpark` reads like it enables V-Order and
  // sets no vorder at all.
  assert.equal(d.producer(lay("spark", 4, 4, { cfg: { resource_profile: "readHeavyForSpark" } })),
    "spark readHeavyForSpark");
});

test("a group of genuinely different writers names both", () => {
  const members = [
    { col: "duckrun·64c", rec: lay("duckrun", 4, 27, { cfg: { vcores: "64" } }) },
    { col: "duckrun·32c", rec: lay("duckrun", 4, 27, { cfg: { vcores: "32" } }) },
    { col: "spark·writeHeavy", rec: lay("spark", 4, 27, { cfg: { resource_profile: "writeHeavy" } }) },
  ];
  assert.equal(d.producers(members), "duckrun, spark writeHeavy", "deduplicated, and both kept");
});

test("the layout table is one row per writer and agrees with the chart", () => {
  // The table groups by the DECLARED producer and the chart by the MEASURED parquet — two directions
  // onto the same rows. And both quote the same CU.
  const runs = [
    lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 27, { cfg: { vcores: "32" }, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 2000.0 }, O1: 1.0,
  }));
  const body = rows(block(out, "the mart the queries land on"));
  assert.equal(body.length, 2, "a header and ONE row — duckrun, not duckrun twice");
  assert.ok(body[1].startsWith("| duckrun | 4 | 27 |"), body[1]);
  assert.equal(charts(out)[0].values[0].split(" ")[0], "1,500.0", "the chart still carries the CU");
  assert.ok(!body[1].includes("1,500"), "but the layout block does not");
  assert.ok(!body[0].includes("| writer |"), "the row label IS the writer now");
});

test("the row count lives in the heading until the engines disagree", () => {
  const same = [lay("duckrun", 4, 27, { file: "a-1.json" }), lay("dwh", 78, 78, { file: "b-2.json" })];
  let out = render(same, ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(plain(out).includes("143,980,961 rows on every engine"));
  assert.ok(!rows(out).some((r) => r.includes("| rows |")));
  const drifted = [lay("duckrun", 4, 27, { file: "a-1.json" }),
    lay("dwh", 78, 78, { file: "b-2.json" })];
  drifted[1].layout.stats.dwh.fct_summary.total_rows = 143980960;
  out = render(drifted, ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(plain(out).includes("row counts DISAGREE"));
  assert.ok(rows(out).some((r) => r.includes("| rows |")), "and the numbers come back");
});

// ---------------------------------------------------------------- query time, in the mart block

test("a tier is summed over the queries every column has", () => {
  // A total over different queries is not a comparison. A query one engine never ran is dropped from
  // EVERY column's total, not counted for the engines that have it.
  const runs = [
    full("a-1.json", "duckrun", { timings: timings({ a: [10, 5, 4], b: [100, 50, 40] }) }),
    full("b-2.json", "dwh", { timings: timings({ a: [20, 6, 5] }) }),
  ];
  const perCol = { duckrun: d.benchTimings(runs[0]), dwh: d.benchTimings(runs[1]) };
  const { totals, n } = d.benchTotals(perCol, "cold_ms");
  assert.equal(n, 1, "`b` is duckrun's alone and must not inflate its total");
  assert.deepEqual(totals, { duckrun: 10.0, dwh: 20.0 });
});

test("the three tiers are columns of the PER-RUN table, not of the layout block", () => {
  const t = timings({ a: [10, 5, 4], b: [20, 6, 5] });
  const runs = [
    full("a-1.json", "duckrun", {
      timings: t, stats: { duckrun: { fct_summary: { total_rows: 1, num_files: 4 } } },
    }),
    full("b-2.json", "dwh", {
      timings: t, stats: { dwh: { fct_summary: { total_rows: 1, num_files: 78 } } },
    }),
  ];
  const out = render(runs, ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!plain(out).includes("Query time"), "no section of its own");
  // The LAYOUT block is physical layout only — no CU, no tiers.
  const rr = rows(block(out, "the mart the queries land on"));
  assert.ok(rr[0].startsWith("| layout | files | row groups |"), rr[0]);
  assert.ok(!rr[0].includes("cold ms") && !rr[0].includes("| CU |"), rr[0]);
  // Exactly two tables carry them, and neither is a layout block: the cost-and-speed table, one row
  // per layout, and the run table, one row per dispatch.
  const heads = rows(out).filter((r) => r.includes("cold ms"));
  assert.equal(heads.length, 2, `two headers carry the tiers: ${heads}`);
  assert.ok(heads.some((h) => h.startsWith("| layout | CU | cold ms | warm ms | hot ms |")), heads[0]);
  assert.ok(heads.some((h) =>
    h.includes("| etl CU | analytics CU | cold ms | warm ms | hot ms | items |")), heads[1]);
  assert.ok(rows(out).some((r) => r.includes("| 30 | 11 | 9 |")), "the run's own tiers");
});

test("no layout block carries the tiers, mart included", () => {
  // They are a property of the RUN, not of any table's parquet.
  const runs = [full("a-1.json", "duckrun", {
    timings: timings({ a: [10, 5, 4] }),
    stats: {
      duckrun: { fct_summary: { total_rows: 1 }, fct_scada: { total_rows: 9, schema: "landing" } },
    },
    tables: ["fct_summary", "fct_scada"],
  })];
  const out = render(runs, ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!block(out, "the mart the queries land on").includes("cold ms"));
  assert.ok(!block(out, "landing.fct_scada").includes("cold ms"));
  assert.ok(plain(out).includes("cold ms"), "but the run table still has them");
});

test("cold covers fewer queries than hot and the note says so", () => {
  // The selectivity-ladder queries have NO cold sample — the top DUID is resolved after pass 1.
  const t = timings({ probe: [10, 5, 4], sel_1duid: [null, 7, 6] });
  const runs = [full("a-1.json", "duckrun", { timings: t }), full("b-2.json", "dwh", { timings: t })];
  assert.ok(plain(render(runs, ledger({ OUT: 1.0, SEM: 2.0 })))
    .includes("cold over 1, warm over 2, hot over 2"));
});

test("a record with no tier timings adds no columns", () => {
  // Absent columns say "not measured"; zeros would say "instant".
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!plain(out).includes("cold ms"));
  assert.ok(rows(out).some((r) => r.startsWith("| layout | files |")),
    "the block itself still renders");
});

// -------------------------------------------------------------------------------------- the rate

test("the rate is a row of the engine table, not a section", () => {
  const runs = [lay("spark", 11, 11, { file: "a-1.json" })];
  const led = ledger({
    OUT: { "High Concurrency Session Livy Run": 900.0 }, SEM: { "XMLA Read Operation": 40.0 },
  });
  led.seconds = secs({
    OUT: { "High Concurrency Session Livy Run": 30.0 }, SEM: { "XMLA Read Operation": 4.0 },
  });
  const out = render(runs, led);
  assert.ok(!plain(out).includes("### Time"), "no section of its own");
  const rr = rows(out);
  assert.ok(rr.some((r) => r === "| **etl** | **900.0** |"));
  assert.ok(rr.some((r) => r === "| `compute CU per second` | 30.0 |"), "under its class");
  assert.equal(charts(out).length, 1, "and it brought no bar with it");
});

test("etl carries a duration row and analytics deliberately does not", () => {
  // "How long did the build take" is worth answering, and it rides the same Capacity Metrics row as
  // the CU so it costs no extra query. `analytics` gets none: the query half already reports latency
  // as cold/warm/hot milliseconds beside the layout, and those are time a user actually waited — a
  // second, differently-defined duration next to them would invite the two to be compared.
  const runs = [full("a-1.json", "spark")];
  const led = ledger({
    OUT: { "High Concurrency Session Livy Run": 900.0 }, SEM: { "XMLA Read Operation": 40.0 },
  });
  led.seconds = secs({
    OUT: { "High Concurrency Session Livy Run": 645.79 }, SEM: { "XMLA Read Operation": 25.93 },
  });
  const rr = rows(render(runs, led));
  const secondsRows = rr.filter((r) => r.includes("compute seconds"));
  assert.equal(secondsRows.length, 1, "exactly one, and it is etl's");
  assert.ok(secondsRows[0].includes("| 646 |"), secondsRows[0]);
  // The caveat rides ON the label. A note four rows below is not attached to anything.
  assert.ok(secondsRows[0].includes("billed, not wall clock"), secondsRows[0]);
  // ...and it reconciles: compute CU / compute seconds is the rate printed underneath.
  const rate = rr.filter((r) => r.startsWith("| `compute CU per second`"));
  assert.equal(rate.length, 2, "one per class — the rate is not etl-only");
  assert.ok(rate[0].includes(`| ${(900.0 / 645.79).toFixed(1)} |`), rate[0]);
});

test("the duration row uses compute seconds, never total", () => {
  // A storage operation bills real CU over a duration of essentially nothing — 383.25 CU in 0.049 s,
  // measured — so its seconds track OneLake traffic rather than how long anything ran. Including them
  // would also break the reconciliation with the rate underneath.
  const runs = [full("a-1.json", "duckrun")];
  runs[0].items = {
    NB: gone("compute", "dbt-duckrun-ab12"), OUT: gone("output", "dbt_delta"),
    SEM: gone("semantic_model", "aemo_duckrun"),
  };
  const led = ledger({
    NB: { "Jupyter Notebook Scheduled Run": 20665.6 },
    OUT: { "OneLake Write via Redirect": 384.1 },
    SEM: { "XMLA Read Operation": 1287.2 },
  });
  led.seconds = secs({
    NB: { "Jupyter Notebook Scheduled Run": 645.79 },
    OUT: { "OneLake Write via Redirect": 0.031 },
    SEM: { "XMLA Read Operation": 25.93 },
  });
  const row = rows(render(runs, led)).find((r) => r.includes("compute seconds"));
  assert.ok(row.includes("| 646 |"), `645.79 compute, not 645.82 with storage: ${row}`);
});

test("a ledger with no seconds renders no duration row either", () => {
  // Absent says "not measured"; a 0 would say the build was instant.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!rows(out).some((r) => r.includes("compute seconds")));
});

test("a column the ledger has not read is a dash in the duration row, not a zero", () => {
  const runs = [lay("duckrun", 4, 27, { file: "a-1.json" }), lay("dwh", 78, 78, { file: "b-2.json" })];
  runs[0].items = { O0: gone("output", "dbt_delta"), S0: gone("semantic_model", "aemo") };
  runs[1].items = { O1: gone("output", "dbt_dwh"), S1: gone("semantic_model", "aemo_dwh") };
  const led = ledger({ O0: { "Jupyter Notebook Scheduled Run": 900.0 } });   // nothing for dwh
  led.seconds = secs({ O0: { "Jupyter Notebook Scheduled Run": 30.0 } });
  const row = rows(render(runs, led)).find((r) => r.includes("compute seconds"));
  assert.ok(row.endsWith("| 30 | — |"), row);
});

test("a class the ledger has not read yet is a dash, not a zero", () => {
  // `**0.0**` on a subtotal says the engine did that work for FREE, which is the one reading this
  // whole page is built to prevent. Live case: a record landed from CI mid-render and printed 0.0
  // down an entire column.
  const runs = [lay("duckrun", 4, 27, { file: "a-1.json" }), lay("dwh", 78, 78, { file: "b-2.json" })];
  runs[0].items = { O0: gone("output", "dbt_delta"), S0: gone("semantic_model", "aemo") };
  runs[1].items = { O1: gone("output", "dbt_dwh"), S1: gone("semantic_model", "aemo_dwh") };
  const led = ledger({
    O0: { "Jupyter Notebook Scheduled Run": 900.0 }, S0: { "XMLA Read Operation": 40.0 },
  });                                                            // nothing for dwh at all
  led.seconds = secs({
    O0: { "Jupyter Notebook Scheduled Run": 30.0 }, S0: { "XMLA Read Operation": 4.0 },
  });
  const rr = rows(render(runs, led));
  assert.ok(rr.some((r) => r === "| **etl** | **900.0** | — |"), "measured, then not-yet-measured");
  assert.ok(rr.some((r) => r === "| `compute CU per second` | 30.0 | — |"));
  assert.ok(!rr.some((r) => r.includes("| 0.0 |") || r.includes("**0.0**")), "no cell reads as free");
});

test("a ledger with no seconds renders no rate row", () => {
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!rows(out).some((r) => r.startsWith("| `compute CU per second` |")), "no ROW");
  assert.equal(charts(out).length, 1, "no seconds, no second chart");
});

test("the rate is compute over compute, never total over total", () => {
  // A storage operation bills real CU over a duration of essentially nothing — 383.25 CU in 0.049 s,
  // measured — so putting it in the ratio does not dilute the rate, it detonates it. Live symptom: the
  // same DuckDB in the same 64-vCore notebook read 36.1 for iceberg and 31.2 for duckrun.
  const runs = [full("a-1.json", "duckrun")];
  runs[0].items = {
    NB: gone("compute", "dbt-duckrun-ab12"), OUT: gone("output", "dbt_delta"),
    SEM: gone("semantic_model", "aemo_duckrun"),
  };
  const led = ledger({
    NB: { "Jupyter Notebook Scheduled Run": 20665.6 },
    OUT: { "OneLake Write via Redirect": 384.1 },
    SEM: { "XMLA Read Operation": 1287.2 },
  });
  led.seconds = secs({
    NB: { "Jupyter Notebook Scheduled Run": 645.79 },
    OUT: { "OneLake Write via Redirect": 0.031 },
    SEM: { "XMLA Read Operation": 25.93 },
  });
  const rr = rows(render(runs, led));
  assert.ok(rr.some((r) => r === "| `compute CU per second` | 32.0 |"), "the node's own draw");
  // And the compute CU row still stands beside it — it is the rate alone that must exclude storage.
  assert.ok(rr.some((r) => r === "| `compute` | 20,665.6 |"));
});

test("the rate scales with the cores the column was given", () => {
  // It is `cores` ÷ 2 for a single-node Python notebook — 32 at 64 vCores, 16 at 32 — NOT the constant
  // 32 it is tempting to read it as. The invariant is that two legs at the SAME cores agree.
  const big = full("a-1.json", "duckrun", { config: { duckrun: { vcores: "64" } } });
  const small = full("b-2.json", "duckrun", { config: { duckrun: { vcores: "32" } } });
  big.items = { NB: gone("compute", "dbt-duckrun-big") };
  small.items = { NB2: gone("compute", "dbt-duckrun-small") };
  const led = ledger({
    NB: { "Jupyter Notebook Scheduled Run": 3200.0 },
    NB2: { "Jupyter Notebook Scheduled Run": 1600.0 },
  });
  led.seconds = secs({
    NB: { "Jupyter Notebook Scheduled Run": 100.0 },
    NB2: { "Jupyter Notebook Scheduled Run": 100.0 },
  });
  assert.deepEqual(d.columnsFor([big, small]).map((c) => c.col), ["duckrun·32c", "duckrun·64c"],
    "never one blended column");
  const out = render([big, small], led);
  const rate = rows(out).find((r) => r.startsWith("| `compute CU per second`"));
  assert.equal(rate, "| `compute CU per second` | 16.0 | 32.0 |", "cores ÷ 2, per column");
  // The size reaches the reader through the column TAG, which is what keeps two core counts from
  // blending into one column. With the ETL chart gone the tables are where it shows.
  assert.ok(rate.includes("16.0") && rate.includes("32.0"), rate);
  assert.ok(!charts(out).some((c) => c.title.includes("ETL")), "and no ETL bar remains");
});

test("the rate is computed per class", () => {
  const runs = [full("a-1.json", "spark")];
  const led = ledger({
    OUT: { "High Concurrency Session Livy Run": 900.0 }, SEM: { "XMLA Read Operation": 40.0 },
  });
  led.seconds = secs({
    OUT: { "High Concurrency Session Livy Run": 30.0 }, SEM: { "XMLA Read Operation": 4.0 },
  });
  const out = render(runs, led);
  const rr = rows(out);
  assert.ok(rr.some((r) => r === "| **etl** | **900.0** |"));
  assert.ok(rr.some((r) => r === "| `compute CU per second` | 30.0 |"), "900 CU over 30 s");
  assert.ok(rr.some((r) => r === "| **analytics** | **40.0** |"));
  assert.ok(rr.some((r) => r === "| `compute CU per second` | 10.0 |"), "40 CU over 4 s");
  assert.equal(charts(out).length, 1, "the one CU chart and no second");
});

// ------------------------------------------------------------------------ live loading, new here

// ------------------------------------------------------------------- one source generation

/** A whole-generation record whose mart row count is spelled out. */
function gen(file, engine, rows, opts = {}) {
  const r = lay(engine, 4, 27, { file, ...opts });
  if (rows === null) delete r.layout.stats[engine].fct_summary.total_rows;
  else r.layout.stats[engine].fct_summary.total_rows = rows;
  return r;
}

test("the newest run defines the source, and disagreeing runs are dropped", () => {
  // The columns are different dispatches days apart and nothing made them comparable. If the archive
  // changes, an engine that has not been rebuilt keeps its column and its numbers sit beside engines
  // built from different data — in the table and inside both charts' means.
  const runs = [
    gen("a-1.json", "duckrun", 143980960, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 48 }),
    gen("c-3.json", "dwh", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { runs: kept, dropped, reference } = d.sameGeneration(runs);
  assert.equal(reference, 143980961, "the LATEST run sets it");
  assert.deepEqual(kept.map((r) => r._file), ["b-2.json", "c-3.json"]);
  assert.deepEqual(dropped.map((x) => [x.engine, x.rows]), [["duckrun", 143980960]]);
});

test("newest wins, never the most common value", () => {
  // Right after a genuine source change the OLD count is still the majority — which is precisely the
  // case this filter exists to handle. A mode would keep the stale generation and drop the new run.
  const runs = [
    gen("a-1.json", "duckrun", 100, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 100, { finishedHoursAgo: 48 }),
    gen("c-3.json", "dwh", 200, { finishedHoursAgo: 24 }),
  ];
  const { kept, reference } = (({ runs: kept, reference }) => ({ kept, reference }))(
    d.sameGeneration(runs));
  assert.equal(reference, 200);
  assert.deepEqual(kept.map((r) => r._file), ["c-3.json"], "the two-strong majority is the one dropped");
});

test("a run that recorded no row count is kept, not dropped", () => {
  // Unmeasured is a different claim from different — the same distinction `layoutKey` makes by
  // keying `null` to a bar of its own.
  const runs = [
    gen("a-1.json", "duckrun", null, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { runs: kept, dropped } = d.sameGeneration(runs);
  assert.deepEqual(kept.map((r) => r._file), ["a-1.json", "b-2.json"]);
  assert.deepEqual(dropped, []);
});

test("with no reference anywhere, nothing is filtered", () => {
  // A record set where nobody recorded total_rows must render WHOLE rather than vanish.
  const runs = [gen("a-1.json", "duckrun", null), gen("b-2.json", "spark", null)];
  const { runs: kept, dropped, reference } = d.sameGeneration(runs);
  assert.equal(reference, null);
  assert.equal(kept.length, 2);
  assert.deepEqual(dropped, []);
});

test("the filter runs BEFORE columnsFor, so a stale engine loses its column entirely", () => {
  // Order is load-bearing: columnsFor takes the latest run per (engine, config), so filtering
  // afterwards would let a stale-generation run hold a column of its own.
  const runs = [
    gen("a-1.json", "duckrun", 999, { finishedHoursAgo: 72 }),   // duckrun's ONLY run, stale
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { cols, dropped } = d.compose(runs, ledger({ OUT: 1.0, SEM: 2.0 }), {});
  assert.deepEqual(cols.map((c) => c.col), ["spark"], "duckrun is gone, not merely re-ranked");
  assert.equal(dropped.length, 1);
});

test("a chart mean never blends two generations", () => {
  // spreadFor walks the whole runs array, so filtering the array is what stops a stale run from
  // pulling the mean. Two spark runs, one stale: the bar must be the survivor's number alone.
  const runs = [
    gen("a-1.json", "spark", 999, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_spark"), O0: gone("output", "dbt_spark") };
  runs[1].items = { S1: gone("semantic_model", "aemo_spark"), O1: gone("output", "dbt_spark") };
  const { html } = d.compose(runs, ledger({
    S0: { "XMLA Read Operation": 5000.0 }, O0: { "Warehouse Query": 1.0 },
    S1: { "XMLA Read Operation": 1000.0 }, O1: { "Warehouse Query": 1.0 },
  }), {});
  const c = charts(html)[0];
  assert.equal(c.values[0], "1,000.0", "no range, no mean of 5000 and 1000 — one sample survives");
});

test("the excluded runs are NAMED on the page, with their counts", () => {
  // The loudness test, and the reason this is not a silent drop. Filtering to one generation made
  // the mart's `row counts DISAGREE` heading unreachable — that shout has to be paid back here.
  const runs = [
    gen("a-1.json", "duckrun", 143980960, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 48 }),
    gen("c-3.json", "dwh", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { html } = d.compose(runs, ledger({ OUT: 1.0, SEM: 2.0 }), {});
  const text = plain(html);
  assert.ok(text.includes("**1 run(s) excluded**"), "a heading, not a footnote");
  assert.ok(text.includes("143,980,960"), "the excluded run's own count");
  assert.ok(text.includes("143,980,961"), "and the current one");
  const row = rows(html).find((r) => r.includes("143,980,960"));
  assert.ok(row.includes("duckrun"), `the engine is named: ${row}`);
  assert.ok(row.includes("-1"), `and the delta against current: ${row}`);
});

test("excluding nearly everything says the newest run is the likely anomaly", () => {
  // Newest-wins cannot tell "the source changed" from "the newest run is broken". When almost
  // everything is dropped, the page has to say which reading is more likely.
  const runs = [
    gen("a-1.json", "duckrun", 143980961, { finishedHoursAgo: 96 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 72 }),
    gen("c-3.json", "dwh", 143980961, { finishedHoursAgo: 48 }),
    gen("d-4.json", "iceberg", 7, { finishedHoursAgo: 24 }),          // the newest, and wrong
  ];
  const { cols, html } = d.compose(runs, ledger({ OUT: 1.0, SEM: 2.0 }), {});
  assert.deepEqual(cols.map((c) => c.col), ["duckdb iceberg"]);
  const text = plain(html);
  assert.ok(text.includes("3 of 4 runs were excluded"), text.slice(0, 200));
  assert.ok(text.includes("NEWEST run is the anomaly"));
});

test("a pinned record bypasses the generation filter", () => {
  // `?record=` means "reproduce this page as it was", including from an older source.
  const runs = [
    gen("a-1.json", "duckrun", 143980960, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { cols, dropped } = d.compose(runs, ledger({ OUT: 1.0, SEM: 2.0 }), { record: "a-1" });
  assert.deepEqual(cols.map((c) => c.col), ["duckrun"], "the stale run renders when asked for");
  assert.deepEqual(dropped, []);
});

test("martRows reads the mart's count and says null when absent", () => {
  assert.equal(d.martRows(gen("a.json", "spark", 143980961)), 143980961);
  assert.equal(d.martRows(gen("a.json", "spark", null)), null);
  assert.equal(d.martRows({}), null);
});

test("the loader reads raw for files and the contents API for the listing", async () => {
  // raw.githubusercontent serves the repo's own files with CORS and a CDN; it has no directory index,
  // which is the only reason the contents API is touched at all. A `legacy/` DIRECTORY entry must not
  // become a fetch — those records predate the item GUIDs and cannot be joined to a ledger.
  const seen = [];
  const fake = async (url) => {
    seen.push(url);
    if (url.includes("api.github.com")) {
      return {
        ok: true, json: async () => [
          { type: "file", name: "b-2.json" },
          { type: "file", name: "a-1.json" },
          { type: "dir", name: "legacy" },
          { type: "file", name: "notes.md" },
        ],
      };
    }
    if (url.endsWith("cu.json")) return { ok: true, json: async () => ledger({ OUT: 1.0 }) };
    return { ok: true, json: async () => full(url.split("/").pop(), "spark") };
  };
  const { records, names, ledger: led } = await d.loadRemote({ fetch: fake, repo: "o/r", ref: "main" });
  assert.deepEqual(names, ["a-1.json", "b-2.json"], "sorted, files only");
  assert.equal(records.length, 2);
  assert.ok(records.every((r) => r._file), "each record remembers the file it came from");
  assert.ok(led.items.OUT);
  assert.ok(seen.some((u) => u.startsWith("https://api.github.com/repos/o/r/contents/history/runs")));
  assert.ok(seen.some((u) =>
    u === "https://raw.githubusercontent.com/o/r/main/history/runs/a-1.json"));
  assert.ok(!seen.some((u) => u.includes("legacy") || u.includes("notes.md")));
});

test("one unreadable record does not cost the whole page", async () => {
  const fake = async (url) => {
    if (url.includes("api.github.com")) {
      return { ok: true, json: async () => [{ type: "file", name: "a-1.json" },
        { type: "file", name: "b-2.json" }] };
    }
    if (url.endsWith("cu.json")) return { ok: true, json: async () => ledger({ OUT: 1.0 }) };
    if (url.endsWith("b-2.json")) return { ok: false, status: 404, statusText: "Not Found" };
    return { ok: true, json: async () => full("a-1.json", "spark") };
  };
  const { records } = await d.loadRemote({ fetch: fake, repo: "o/r", ref: "main" });
  assert.equal(records.length, 1);
});

test("a failed listing rejects rather than rendering an empty page", async () => {
  // An empty page and a rate-limited API look identical to a reader, and only one of them means
  // "nothing has been measured". The boot handler says which.
  const fake = async () => ({ ok: false, status: 403, statusText: "rate limit exceeded" });
  await assert.rejects(() => d.loadRemote({ fetch: fake, repo: "o/r", ref: "main" }), /403/);
});

test("the dispatch inputs are query params now", () => {
  // `?record=30776174056` is a link to one run's page. It used to be a workflow dispatch.
  assert.deepEqual(d.optsFromSearch("?record=30776174056&ref=topic&table=fct_scada"), {
    repo: d.DEFAULTS.repo, ref: "topic", table: "fct_scada", record: "30776174056",
  });
  assert.deepEqual(d.optsFromSearch(""), { ...d.DEFAULTS });
});

test("compose renders one run alone when a record is pinned", () => {
  const runs = [full("a-1.json", "spark"), full("b-2.json", "dwh")];
  const led = ledger({ OUT: 1.0, SEM: 2.0 });
  assert.deepEqual(d.compose(runs, led, { record: "b-2" }).cols.map((c) => c.col), ["dwh"]);
  // A pin that matches nothing renders the newest rather than an empty page.
  assert.deepEqual(d.compose(runs, led, { record: "nope" }).cols.map((c) => c.col), ["dwh"]);
  assert.deepEqual(d.compose(runs, led, {}).cols.map((c) => c.col).sort(), ["dwh", "spark"]);
});

test("the offline copy links back to the live page it was frozen from", () => {
  assert.equal(d.pagesUrl("djouallah/fabric-dbt-benchmark"),
    "https://djouallah.github.io/fabric-dbt-benchmark/");
});

/** The smallest thing `boot()` will accept: three elements it can look up and write into. */
function fakeDoc(snapshot) {
  const el = () => ({ innerHTML: "", textContent: "" });
  const nodes = { app: el(), status: el(), snapshot: { ...el(), textContent: snapshot || "" } };
  return { getElementById: (id) => nodes[id] || null, nodes };
}

test("boot prefers an inlined snapshot over the network", async () => {
  // This is what makes the offline artifact copy work, and it has to be the SAME render path — the
  // whole reason there is one implementation now is that a frozen copy and a live page cannot be
  // allowed to disagree about what the numbers are.
  const snap = JSON.stringify({
    built: "2026-08-03 11:00 UTC",
    records: [full("a-1.json", "spark")],
    ledger: ledger({ OUT: 900.0, SEM: 40.0 }),
  });
  const doc = fakeDoc(snap);
  // No fetch is stubbed: reaching the network at all would throw and fail this test.
  await d.boot(doc, { search: "" });
  assert.ok(plain(doc.nodes.app.innerHTML).includes("Capacity units"));
  assert.ok(rows(doc.nodes.app.innerHTML).some((r) => r.startsWith("| **etl** |")));
  assert.ok(plain(doc.nodes.status.innerHTML).includes("Offline copy"));
  assert.ok(plain(doc.nodes.status.innerHTML).includes("2026-08-03 11:00 UTC"));
});

test("a page that cannot read its data says so instead of reading as empty", async () => {
  // The API's 60/hour anonymous rate limit, a renamed branch and a private fork all land here, and
  // an empty page would claim the far more alarming thing: that nothing has ever been measured.
  const doc = fakeDoc("");
  globalThis.fetch = async () => ({ ok: false, status: 403, statusText: "rate limit exceeded" });
  try {
    await d.boot(doc, { search: "" });
  } finally {
    delete globalThis.fetch;
  }
  const text = plain(doc.nodes.app.innerHTML);
  assert.ok(text.includes("Could not read the data"));
  assert.ok(text.includes("403"), "the reason has to be on the page, not only in the console");
  assert.ok(!text.includes("No run records"), "never the empty-repo message");
});

test("the two surviving tags are exact tokens, so they cannot carry an attribute", () => {
  // `<br>` and `<sub>` are un-escaped after the fact, which is a deliberate hole and has to stay a
  // token-shaped one: no attribute position, so nothing can ride in on it.
  assert.equal(d.inline("a<br>b"), "a<br>b");
  assert.equal(d.inline("x <sub>note</sub>"), "x <sub>note</sub>");
  assert.equal(d.inline('<sub onload="x()">'), '&lt;sub onload="x()"&gt;', "no attributes");
  assert.equal(d.inline("<subtle>"), "&lt;subtle&gt;", "prefix match must not open a tag");
  assert.equal(d.inline("<script>alert(1)</script>"),
    "&lt;script&gt;alert(1)&lt;/script&gt;");
});

// ------------------------------------------------------------------------------- presentation

test("the layout blocks sit behind a tab strip when more than one table renders", () => {
  const runs = [full("a-1.json", "duckrun", {
    stats: {
      duckrun: { fct_summary: { total_rows: 1 }, fct_scada: { total_rows: 9, schema: "landing" } },
    },
    tables: ["fct_summary", "fct_scada"],
  })];
  const out = render(runs, ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(out.includes('class="tabs"'));
  assert.equal((out.match(/name="layout-tab"/g) || []).length, 2, "one radio per table");
  assert.ok(out.includes('id="lt-0" checked'), "the mart tab starts selected");
  // Every panel stays in the DOM — hidden by CSS, never dropped — so ctrl-F, print, the offline
  // snapshot and every other test here still see every table.
  assert.ok(plain(out).includes("landing.fct_scada"));
  assert.ok(plain(out).includes("9 rows on every engine"));
});

test("ONE chart, full width, and it is the analytics one", () => {
  // The page's own thesis applied to itself: background CU is smoothed over 24h and nobody waits for
  // it, query CU is interactive and is what throttles. A second bar chart ranking the build gave the
  // half that does not hurt equal visual weight.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!out.includes('<div class="charts">'), "no side-by-side wrapper to share a row with");
  assert.equal((out.match(/<figure class="chart"/g) || []).length, 1, "one figure");
  assert.ok(!out.includes("data-kind"), "one measure, one hue, nothing to mark");
  assert.ok(out.includes("Capacity units per parquet layout"));
  assert.ok(!out.includes("ETL — what building them cost"), "the ETL chart is gone");
  // Gone from the CHART, not from the page.
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.startsWith("| **etl** |")));
});

// ------------------------------------------------------------------- cost and speed by layout

/**
 * `n` layouts, each with its own CU and cold/warm/hot.
 *
 * File and row-group counts are a POWER OF TWO APART on purpose: `layoutKey` bands them, so
 * `4, 5, 6` files would be one group and one row rather than three.
 */
const fitRuns = (spec) => spec.map(([engine, cold, warm, hot], i) =>
  lay(engine, 4 << (i * 2), 20 << (i * 2), {
    file: `f-${i}.json`, cfg: { vcores: String(10 + i) },
    timings: timings({ q1: [cold, warm, hot] }),
  }));

test("cost and speed is one table, cheapest first, with a title and nothing else", () => {
  const out = render(fitRuns([
    ["spark", 20000, 4000, 3000], ["duckrun", 40000, 5000, 4000],
    ["dwh", 80000, 3000, 5000],
  ]), ledger({ OUT: 1.0, SEM: 2.0 }));
  const at = out.indexOf("<h3>Cost and speed by layout</h3>");
  assert.ok(at > 0, "the table is on the page");
  assert.ok(out.indexOf('<div class="charts">') < at, "after the two bar charts");
  assert.ok(at < out.indexOf("<h3>Cost by engine</h3>"), "before the cost table");
  const head = rows(out).find((r) => r.startsWith("| layout | CU |"));
  assert.ok(head, "layout, CU, then the tiers");
  assert.ok(head.includes("| cold ms | warm ms | hot ms |"), head);
  // A TITLE AND NOTHING ELSE — no verdict, no correlation, no reading of the numbers.
  const said = plain(out);
  assert.ok(!said.includes("Does paying more buy speed"));
  assert.ok(!said.includes("tracks CU") && !said.includes("no relation"));
  assert.ok(!said.includes("Cold is the tier the layout moves"));
});

test("the cost-and-speed rows are cheapest first", () => {
  const out = render(fitRuns([
    ["spark", 80000, 4000, 3000], ["duckrun", 40000, 5000, 4000],
    ["dwh", 20000, 3000, 5000],
  ]), ledger({ OUT: 1.0, SEM: 2.0 }));
  const body = rows(out).filter((r) => /^\| (spark|duckrun|dwh)[^|]*\| [\d,]+ \| [\d,]+ \|/.test(r));
  const cu = body.map((r) => Number(r.split("|")[2].trim().replace(/,/g, "")));
  assert.deepEqual([...cu].sort((a, b) => a - b), cu, `cheapest first: ${cu}`);
});

test("a layout with no CU read yet is absent, not printed as free", () => {
  assert.equal(d.renderFit([], {}, ["cold"]), "");
  const groups = [["k", [{ qid: "0", cu: 0, rec: lay("spark", 4, 20) }]]];
  assert.equal(d.renderFit(groups, {}, ["cold"]), "", "cu 0 means unmeasured, not free");
});

test("a tier nothing recorded is not a column", () => {
  const groups = [
    ["a", [{ qid: "0", cu: 100, rec: lay("spark", 4, 20) }]],
    ["b", [{ qid: "1", cu: 200, rec: lay("dwh", 16, 80) }]],
  ];
  const times = { 0: { cold: 10, warm: 5 }, 1: { cold: 20, warm: 6 } };
  const html = d.renderFit(groups, times, ["cold", "warm", "hot"]);
  const head = rows(html)[0];
  assert.ok(head.includes("| cold ms | warm ms |"), head);
  assert.ok(!head.includes("hot ms"), "a tier with no samples adds no column");
  assert.ok(html.includes('class="sortable"'),
    "the reader can reorder it by any column — sort-only, no filter bar");
});

test("the cost-and-speed table and the mart block are one measurement, not two", () => {
  // `martPoints` is the single source for both, so a row here and a row there cannot disagree.
  const runs = fitRuns([["spark", 20000, 4000, 3000], ["duckrun", 40000, 5000, 4000],
    ["dwh", 80000, 3000, 5000]]);
  const cols = d.columnsFor(runs);
  const entries = runs.map((rec, i) => ({ col: cols[0].col, rec, qid: String(i), cu: 0 }));
  const { times } = d.queryTime(entries.map(({ qid, rec }) => ({ col: qid, rec })));
  const pts = d.martPoints(d.layoutGroups(entries), times);
  assert.ok(pts.length >= 1);
  for (const p of pts) assert.ok(p.name && p.rec, "every point carries its label and its record");
});

test("a single table renders without a tab strip", () => {
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!out.includes('class="tabs"'));
  assert.ok(plain(out).includes("the mart the queries land on"), "the block itself still renders");
});

test("the adapters are named and linked once, under the charts", () => {
  // The bars stopped captioning the adapter; this note is where a reader finds out what
  // dbt-duckrun, dbt-duckdb, dbt-fabricspark and the samdebruyn fork actually are.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  for (const [engine, url] of Object.entries(d.ADAPTER_URLS)) {
    assert.equal(out.split(`href="${url}"`).length - 1, 1, `${engine} linked exactly once`);
  }
  const text = plain(out);
  assert.ok(text.includes("dbt-fabric-samdebruyn") && text.includes("dbt-fabricspark"));
  assert.ok(out.indexOf('<div class="charts">') < out.indexOf("The adapters:"), "under the charts");
  assert.ok(out.indexOf("The adapters:") < out.indexOf("Cost by engine"), "not buried below");
  // ONE PER LINE — joined with `·`, the separator between two entries looked like the em dash inside
  // one, so four `name — what it is` pairs read as one wrapped run of text.
  const note_ = out.slice(out.indexOf("The adapters:"), out.indexOf("</p>", out.indexOf("The adapters:")));
  assert.equal(note_.split("<br>").length - 1, Object.keys(d.ADAPTER_URLS).length,
    `one break for the label and one between each pair: ${note_}`);
  assert.ok(!note_.includes(" · "), "and no inline separator left over");
});

test("methodology folds, but the exclusion notice never does", () => {
  const runs = [
    gen("a-1.json", "duckrun", 143980960, { finishedHoursAgo: 72 }),
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  const { html } = d.compose(runs, ledger({ OUT: 1.0, SEM: 2.0 }), {});
  // The long notes fold behind <details>, with their full text still in the DOM.
  assert.ok(html.includes("<details"));
  assert.ok(plain(html).includes("Every `OneLake …` operation is storage"));
  // The excluded-runs block is the loud one and stays fully visible.
  const excl = block(html, "run(s) excluded");
  assert.ok(excl.includes("143,980,960"), "the dropped run's count is in the open");
  assert.ok(!excl.includes("<details"), "loud by design — never folded");
});

test("a run links to its committed record, never to CI", () => {
  // Actions runs expire — logs at 90 days, the run page eventually with them — while the record in
  // history/runs/ is the permanent copy of everything this page renders. Sources table, excluded
  // table and the skipped note all point there.
  const runs = [
    gen("a-1.json", "duckrun", 143980960, { finishedHoursAgo: 72 }),   // dropped: old generation
    gen("b-2.json", "spark", 143980961, { finishedHoursAgo: 24 }),
  ];
  const bad = full("c-3.json", "dwh");
  bad.benchmark = {};                                                  // skipped: incomplete
  const { html } = d.compose([...runs, bad], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  for (const f of ["a-1.json", "b-2.json", "c-3.json"]) {
    assert.ok(html.includes(`href="${d.recordUrl(d.DEFAULTS.repo, f)}"`), `${f} links to history/`);
  }
  assert.ok(!html.includes("/actions/runs/"), "no CI link anywhere on the page");
});

test("a skipped record is named on the page, with its reason", () => {
  // It used to be only a count in the live status line — which the offline copy does not even have.
  // A page that quietly ignores a record is indistinguishable from a page that never had it.
  const good = full("a-1.json", "spark");
  const bad = full("b-2.json", "dwh");
  bad.benchmark = {};
  const { html } = d.compose([good, bad], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  const text = plain(html);
  assert.ok(text.includes("1 record(s) skipped as incomplete"));
  assert.ok(text.includes("`b-2.json` — no benchmark timings — the query half did not run"),
    "the file and the reason, not only a count");
  assert.ok(text.includes("(+1 skipped)"), "and the footer counts it");
  const at = html.indexOf("skipped as incomplete");
  const before = html.slice(0, at);
  assert.ok(before.lastIndexOf("<details") <= before.lastIndexOf("</details>"),
    "visible, never folded — same rule as the generation exclusions");
});

test("a still-billing drifter is a visible note, not a folded one", () => {
  // The one state that never resolves by waiting must not sit behind a click.
  const good = full("a-1.json", "spark");
  const bad = full("b-2.json", "duckrun");
  delete bad.items.OUT.deleted;
  const out = render([good, bad], ledger({ OUT: 1.0, SEM: 2.0 }));
  const at = out.indexOf("predates that teardown");
  assert.ok(at > 0);
  const before = out.slice(0, at);
  assert.ok(before.lastIndexOf("<details") <= before.lastIndexOf("</details>"),
    "the drifter warning is not inside a <details>");
});

test("each run carries its own etl and analytics CU", () => {
  // The two halves used to sit a table away from the run that produced them. On the row that names
  // the dispatch, the build mode and whether the number has settled, they are qualified by the four
  // facts that qualify a CU figure.
  const r = full("a-1.json", "spark");
  const out = d.renderSources([{ col: "spark", engine: "spark", rec: r }], null,
    d.normaliseLedger(ledger({ OUT: 12.5, SEM: 3.25 })), "o/r");
  const head = rows(out)[0];
  assert.ok(head.includes("etl CU") && head.includes("analytics CU"), head);
  assert.ok(!/\|\s*CU\s*\|/.test(head),
    "the settle column is `state` — one header called CU beside two holding CU is doing two jobs");
  const row = rows(out).find((x) => x.startsWith("| spark |"));
  assert.ok(row.includes("| 12.5 |"), `etl total on the row: ${row}`);
  assert.ok(row.includes("| 3.3 |"), `analytics total on the row: ${row}`);
});

test("a class the ledger has not read is a dash on the run row, never 0.0", () => {
  // Same rule as every other CU cell: `0.0` there says the engine did that work for free, which is
  // the one reading this page is built to prevent.
  const r = full("a-1.json", "spark");
  const out = d.renderSources([{ col: "spark", engine: "spark", rec: r }], null,
    d.normaliseLedger(ledger({ OUT: 12.5 })), "o/r");       // no SEM => analytics unmeasured
  const row = rows(out).find((x) => x.startsWith("| spark |"));
  assert.ok(row.includes("—"), `unread analytics is a dash: ${row}`);
  assert.ok(!row.includes("| 0.0 |"), row);
});

test("every run a chart drew from has a row of its own", () => {
  // A bar with no row behind it is what this table exists to prevent. The charts average an engine's
  // whole history, so a superseded run still moves one — and while this listed column holders only,
  // that run's CU appeared nowhere else on the page: `duckrun sorted` read 2,454.1 and no row said so.
  const cfg = { vcores: "64", sorted: "true" };
  const runs = [
    lay("duckrun", 3, 26, { cfg, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 25, { cfg, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const { html } = d.compose(runs, ledger({
    S0: { "XMLA Read Operation": 2400.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 1600.0 }, O1: 1.0,
  }), {});
  const body = rows(block(html, "Every run on this page")).slice(1);
  assert.equal(body.length, 2, "both runs, not just the one holding the column");
  assert.ok(body[0].startsWith("| duckrun |"), body[0]);
  assert.ok(body[0].includes("| 1,600.0 |"), body[0]);
  // ...and the superseded one is a row like any other: the RUN is the key, and which one is newest is
  // already what the sort order and the `built` column say.
  assert.ok(body[1].startsWith("| duckrun |"), body[1]);
  assert.ok(body[1].includes("| 2,400.0 |"), `the number the older bar reads: ${body[1]}`);
  assert.ok(charts(html)[0].values.includes("2,400.0"), "which is on a bar");
});

test("the run rows and Cost by engine quote the same numbers", () => {
  // Both read the column's latest run through the same GUID join, so a reader comparing the two
  // tables must not find two figures for one measurement. The CHARTS may differ — they average every
  // run of a column — which is what the note under the run table says.
  const { html } = d.compose([full("a-1.json", "spark")], ledger({ OUT: 12.5, SEM: 3.25 }), {});
  const engine = rows(block(html, "Cost by engine")).find((x) => x.startsWith("| **etl**"));
  const run = rows(block(html, "Every run on this page"))
    .find((x) => x.startsWith("| spark |"));
  assert.ok(engine.includes("12.5") && run.includes("| 12.5 |"), `${engine} / ${run}`);
});

test("an item name cannot inject markup", () => {
  // The page escapes before it interprets markdown, so a `<` in a Fabric display name is text.
  const r = rec("a-1.json", "spark", {
    OUT: { role: "output", name: "<img src=x onerror=alert(1)>" },
  });
  const out = d.renderSources([{ col: "spark", engine: "spark", rec: r }], null,
    d.normaliseLedger(ledger({ OUT: 1.0 })), "o/r");
  assert.ok(!out.includes("<img"));
  assert.ok(out.includes("&lt;img"));
});

// ---------------------------------------------------------------------------------- the analysis

/**
 * One run with its OWN item GUIDs, so the ledger can hand two runs of one column different CU.
 *
 * `full()`/`lay()` hardcode `OUT`/`SEM`, which is right everywhere else and fatal here: two runs
 * sharing a GUID read identical CU and the measured floor comes out at 0%.
 */
const own = (r, tag) => {
  r.items = { [`O${tag}`]: gone("output", `dbt_${r.engine}`),
    [`S${tag}`]: gone("semantic_model", `aemo_${r.engine}`) };
  return r;
};

/**
 * The whole section, `<h4>`s and tables included — `block()` cuts at the next heading of any level,
 * which here is the section's own first sub-block.
 */
const analysis = (html) => {
  const at = String(html).indexOf("<h3>Analysis");
  return at < 0 ? "" : String(html).slice(at, String(html).indexOf("<h3>About these numbers"));
};

/** Two runs of ONE column, each with its own GUIDs — a repeat for the floor to measure. */
const twice = (engine, opts = {}) => [
  own(lay(engine, 4, 4, { file: `${engine}-1.json`, ...opts }), `${engine}1`),
  own(lay(engine, 4, 4, { file: `${engine}-2.json`, ...opts }), `${engine}2`),
];

/** …and a second column, because one column is not a ranking and renders nothing at all. */
const rival = () => own(lay("dwh", 78, 78, { file: "dwh-1.json" }), "dwh");
const repeated = () => [...twice("duckrun"), rival()];
const REPEAT = ledger({ Oduckrun1: 100, Oduckrun2: 120, Odwh: 300 });

test("the noise floor is MEASURED from the repeats, not assumed", () => {
  // Two runs of one column at 100 and 120 CU: the spread is 20/110 = 18.2%, and the page prints that
  // rather than carrying a constant somebody chose.
  const text = plain(analysis(render(repeated(), REPEAT)));
  assert.ok(text.includes("etl CU 18.2%"), text.slice(0, 400));
  assert.ok(text.includes("1 column(s) here have been run more than once"), text.slice(0, 400));
});

test("spread is a mean, a range and a RELATIVE width", () => {
  assert.deepEqual(d.spread([100, 120]), { n: 2, mean: 110, min: 100, max: 120, rel: 20 / 110 });
  assert.equal(d.spread([]), null, "no readings is not a spread of zero");
  assert.equal(d.spread([0, 0]), null, "a run that measured nothing is dropped, not averaged in");
  assert.equal(d.spread([5]).rel, 0, "one reading has no width");
});

test("a margin inside the floor is `within spread`, outside it is not", () => {
  const floor = { n: 1, rel: 0.2, lo: 0.2, hi: 0.2 };
  assert.equal(d.verdictOf(0.1, floor), "within spread");
  assert.equal(d.verdictOf(0.5, floor), "beyond spread");
  assert.equal(d.verdictOf(0, floor), "tie");
  assert.equal(d.verdictOf(0.5, null), "no repeat", "no floor means no verdict, not a pass");
});

test("the range check only applies when BOTH sides repeat", () => {
  const floor = { n: 1, rel: 0.1, lo: 0.1, hi: 0.1 };
  const a = { n: 2, mean: 10, min: 9, max: 11, rel: 0.2 };
  const far = { n: 2, mean: 30, min: 29, max: 31, rel: 0.07 };
  const near = { n: 2, mean: 30, min: 10, max: 50, rel: 1.3 };
  assert.equal(d.verdictOf(2.0, floor, a, far), "beyond spread, ranges disjoint");
  assert.equal(d.verdictOf(2.0, floor, a, near), "beyond spread, ranges overlap");
  // One reading is not a range, and asserting separation from a single point is the error this whole
  // section exists to avoid.
  assert.equal(d.verdictOf(2.0, floor, { n: 1 }, far), "beyond spread");
});

test("with nothing measured twice, every verdict is `no repeat` and the page says so", () => {
  const runs = [lay("spark", 11, 11, { file: "a-1.json" }), lay("dwh", 78, 78, { file: "b-2.json" })];
  const text = plain(analysis(render(runs, ledger({ OUT: 30.0, SEM: 5.0 }))));
  assert.ok(text.includes("Nothing on this page has been measured twice"), text.slice(0, 400));
  assert.ok(!text.includes("The yardstick is measured"));
});

test("the section states its scope, and the counts are DERIVED", () => {
  // One dataset, one query suite, one capacity — the caveat that qualifies every number under it.
  const text = plain(analysis(render(repeated(), REPEAT)));
  assert.ok(text.includes("One dataset, one query suite, one capacity"), text.slice(0, 300));
  assert.ok(text.includes("3 run(s) across 2 configuration(s) of 2 engine(s)"), text.slice(0, 300));
  // The row count is a fact about the DATA, so it is derived from the records when the caller passes
  // no generation reference — `render()` does not, and the sentence must still be complete.
  assert.ok(text.includes("143,980,961 rows"), text.slice(0, 300));
});

test("the scope caveat is NOT folded", () => {
  // Repo rule: explanation folds, anything qualifying a number does not.
  const sec = analysis(render(repeated(), REPEAT));
  const at = sec.indexOf("One dataset");
  assert.ok(at > 0, "the caveat renders");
  assert.ok(!sec.slice(0, at).includes("<details"), "nothing has opened a fold before it");
  assert.ok(sec.slice(0, at).includes('<p class="note">'), "it is a note");
});

test("`variantPairs` takes one-key differences and rejects everything else", () => {
  const col = (name, cfg) => ({ col: name, engine: "duckrun", rec: lay("duckrun", 4, 4, { cfg }) });
  assert.deepEqual(d.variantPairs([col("a", { vcores: "8" }), col("b", { vcores: "64" })])
    .map((p) => [p.key, p.from, p.to]), [["vcores", "8", "64"]]);
  // Two keys apart is not a controlled comparison and must not be presented as one.
  assert.deepEqual(d.variantPairs([col("a", { vcores: "8", sorted: "true" }),
    col("b", { vcores: "64", sorted: "false" })]), []);
  // Nor across engines: a pair is one engine's own two configurations.
  assert.deepEqual(d.variantPairs([col("a", { vcores: "8" }),
    { col: "b", engine: "spark", rec: lay("spark", 4, 4, { cfg: { vcores: "64" } }) }]), []);
});

test("ABSENCE IS A VALUE — an off flag is not recorded, and still pairs", () => {
  // This is what makes `sorted`, NEE and V-Order findable without any of the three being named in
  // the code. A missing key reads `off`, not "no comparison".
  const col = (name, cfg) => ({ col: name, engine: "duckrun", rec: lay("duckrun", 4, 4, { cfg }) });
  assert.deepEqual(d.variantPairs([col("a", { vcores: "64" }),
    col("b", { vcores: "64", sorted: "true" })]).map((p) => [p.key, p.from, p.to, p.a, p.b]),
  [["sorted", "off", "true", "a", "b"]]);
});

test("the lower value leads, so a delta reads as what turning it UP did", () => {
  const col = (name, cfg) => ({ col: name, engine: "duckrun", rec: lay("duckrun", 4, 4, { cfg }) });
  // Declared high-first; `compareCells` puts it back NUMERICALLY, so it is 8 → 64 and never sorted
  // as text on the first digit.
  const p = d.variantPairs([col("hi", { vcores: "64" }), col("lo", { vcores: "8" })])[0];
  assert.deepEqual([p.from, p.to, p.a, p.b], ["8", "64", "lo", "hi"]);
});

test("the knob table pairs columns, bolds what clears the floor, and says whose layout differs", () => {
  const runs = [...twice("duckrun", { cfg: { vcores: "8" } }),
    own(lay("duckrun", 4, 4, { file: "duckrun-3.json", cfg: { vcores: "64" } }), "big")];
  const out = render(runs, ledger({ Oduckrun1: 100, Oduckrun2: 120, Obig: 400 }));
  const row = rows(block(out, "One knob at a time")).find((r) => r.includes("vcores"));
  assert.ok(row.includes("8 → 64"), row);
  assert.ok(row.includes("2 vs 1"), `both sides' run counts: ${row}`);
  // 110 → 400 is +263.6%, far outside the 18.2% floor the repeat measured.
  assert.ok(row.includes("**+263.6**"), row);
  // Same files, same row groups: Power BI cannot tell the two apart, so any query-side delta between
  // them is two readings of one bar.
  assert.ok(row.includes("| same |"), row);
});

test("Part A quotes the CHARTS' numbers, not a second derivation", () => {
  // A page printing 1,916 in a bar and 1,960 in the row under it is asking which one it meant.
  const runs = [own(lay("duckrun", 4, 4, { file: "d-1.json" }), "d"),
    own(lay("spark", 11, 11, { file: "s-1.json", vorder: true }), "s")];
  const out = render(runs, ledger({ Od: 100, Sd: 40, Os: 300, Ss: 90 }));
  const find = (what) => rows(block(out, "Where the rankings hold")).find((r) => r.includes(what));
  assert.ok(find("cheapest to build").includes("| 100.0 |"), find("cheapest to build"));
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.includes("| **100.0** |")),
    "which is what `Cost by engine` reports for that column");
  assert.ok(find("cheapest to query").includes("| 40.0 |"), find("cheapest to query"));
  assert.ok(charts(out)[0].values.includes("40.0"), "which is the cheapest analytics bar");
});

test("nothing to compare renders NOTHING, not an empty heading", () => {
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 12.5, SEM: 3.25 }));
  assert.ok(!out.includes("<h3>Analysis"), "one column is not a ranking and has no pair");
});

test("a tier nothing recorded produces no finding row", () => {
  // `full()`'s default timings carry `ms_by_pass` and no tier keys at all.
  const runs = [own(lay("duckrun", 4, 4, { file: "d-1.json" }), "d"),
    own(lay("spark", 11, 11, { file: "s-1.json", vorder: true }), "s")];
  const text = plain(analysis(render(runs, ledger({ Od: 100, Sd: 40, Os: 300, Ss: 90 }))));
  assert.ok(text.includes("cheapest to build"));
  assert.ok(!text.includes("fastest cold"), "no timings, no tier ranking");
});

test("the page says why there is no p-value", () => {
  const text = plain(analysis(render(repeated(), REPEAT)));
  assert.ok(text.includes("No p-value is offered"), "stated where the verdicts are");
  assert.ok(text.includes("not independent draws"), "and the reason is given");
});

test("the analysis section introduces no new CSS", () => {
  // Every class it emits has to already exist in `index.html`, which this file cannot read — so the
  // guard is that the set stays the one the rest of the page already uses.
  const sec = analysis(render(repeated(), REPEAT));
  for (const m of sec.matchAll(/class="([^"]+)"/g)) {
    assert.ok(["note", "sortable", "scroll", "left", "right", "sub"].includes(m[1]), m[1]);
  }
});
