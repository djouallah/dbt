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

/**
 * `[{writer, runs, cu}]` from *Cost and speed by parquet layout* — one entry per layout group, in
 * page order (cheapest first).
 *
 * THIS IS WHERE THE GROUPING SURFACES NOW. The analytics bar chart used to be the readable form of
 * `layoutGroups` + `groupMid`, and the tests below reached for its bar labels and values; the chart
 * is gone and the grouping is not, so they read the table that always carried the same numbers.
 */
function layoutTable(html) {
  return rows(block(html, "Cost and speed by parquet layout"))
    .map((r) => r.split("|").map((c) => c.trim()))
    .filter((c) => c.length > 7 && c[1] && c[1] !== "parquet writer")
    // Column order is: writer, ordering, dictionary, rg size, MB, runs, cores, etl CU, analytics CU.
    .map((c) => ({ writer: c[1], ordering: c[2], rgSize: c[4], runs: c[6],
      cores: c[7], etl: c[8], cu: c[9] }));
}

/** `[{title, subtitle, labels, values, captions}]` for each chart drawn, in page order. */
function charts(html) {
  return [...String(html).matchAll(/<figure class="chart"[^>]*>([\s\S]*?)<\/figure>/g)].map(([, f]) => ({
    title: plain((f.match(/<span class="chart-title">([\s\S]*?)<\/span>/) || [])[1] || ""),
    subtitle: plain((f.match(/<span class="chart-sub">([\s\S]*?)<\/span>/) || [])[1] || ""),
    // `labels` was the bar chart's row names and has no source left; kept as an empty array so a
    // stale assertion fails loudly on a length rather than on `undefined.length`.
    labels: [],
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
  const { config, stats, tables, landing, ordering,
    full_load = true, finishedHoursAgo = 48 } = opts;
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
  // Absent unless a test asks for it, matching stats.py: an empty `ordering` would read as "nothing
  // was measured about the row order", which is a claim rather than the silence of an older record.
  if (ordering) r.layout.ordering = ordering;
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
  const { vorder = false, cfg = {}, file = "x.json", mb = 1.0, ...rest } = opts;
  // `avg_row_group` defaults to what stats.py would actually record — `total_rows / num_row_groups`,
  // because every engine writes the identical row count. It was a hardcoded `1`, which is not a
  // number any run can produce. Pass `avg: null` to omit the field and exercise the derived path.
  const avg = "avg" in opts ? opts.avg : 143980961 / rgs;
  const summary = {
    total_rows: 143980961, num_files: files, num_row_groups: rgs,
    size_mb: mb, vorder, schema: "mart",
  };
  if (avg != null) summary.avg_row_group = avg;
  return full(file, engine, {
    config: { [engine]: cfg },
    stats: { [engine]: { fct_summary: summary } },
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

test("the writer name carries no ordering — that is a column of its own", () => {
  // `sorted` left LAYOUT_CONFIG: `keyCells` prints the resolved column list, so appending the flag to
  // the writer said the same thing twice and less precisely. It still SPLITS bars — `layoutKey`
  // carries the sort key independently of what a writer is CALLED.
  assert.equal(d.producer(lay("duckrun", 4, 25, { cfg: { sorted: "true" } })), "delta_rs");
  assert.equal(d.producer(lay("duckrun", 4, 27, { cfg: { vcores: "64" } })), "delta_rs",
    "vcores still never reaches a caption about parquet");
});

test("a sort splits the layout bar even though the bands do not move", () => {
  // THE reason `sorted` is in layoutKey. The one measured sorted run wrote 4 files either way and
  // 27 -> 25 row groups, which fall in the SAME bands — so without the config in the key these two
  // share a bar and their cold/warm/hot means are averaged, which is the comparison the flag exists
  // to make.
  const plain = lay("duckrun", 4, 27, { cfg: { vcores: "64" } });
  const sorted = lay("duckrun", 4, 25, { cfg: { vcores: "64", sorted: "true" } });
  assert.deepEqual(d.layoutKey(plain).slice(0, 2), d.layoutKey(sorted).slice(0, 2),
    "same V-Order and same band — the measured half cannot tell them apart");
  assert.notDeepEqual(d.layoutKey(plain), d.layoutKey(sorted));
  // This fixture records no key, so it reads `true` — sorted by something unnamed. The COLUMNS case
  // is the two-sorts test below.
  assert.equal(d.layoutKey(sorted)[2], true);
});

test("a record with no sorted key groups with an unsorted run, not alone", () => {
  // All 13 existing records predate the input. They demonstrably wrote unsorted parquet, so absence
  // here is NOT the "unmeasured" case that earns a bar of its own — that case is a missing file
  // count, which is a different thing entirely.
  // ONE engine on both sides: the engine is in the key now, so two engines never share a bar and
  // this test would pass for the wrong reason if it kept comparing duckrun against iceberg.
  // Both sides carry NO `sorted` key, because that is the only spelling an unsorted run has —
  // `stats.py` records the flag when it is on and not otherwise. (`sorted: "false"` would not be
  // that case: it is a truthy STRING, so `sortKeyOf` reads it as sorted-but-unnamed.)
  const old = lay("duckrun", 4, 27, { cfg: { vcores: "64" } });          // no `sorted` key at all
  const off = lay("duckrun", 4, 25, { cfg: { vcores: "64" }, file: "y.json" });
  assert.deepEqual(d.layoutKey(old), d.layoutKey(off));
  assert.equal(d.layoutKey(old)[2], false);
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
  // The real pair — `['date','time','DUID']` (run 30955591822) and `['date','time']` — writes the
  // same row-group band, so the COLUMNS are now the only thing keeping them apart. This used to be
  // covered accidentally by 3 vs 4 files crossing a band boundary; that element is gone.
  const duid = sortedBy(4, 25, ["date", "time", "DUID"], { file: "a-1.json" });
  const dt = sortedBy(4, 25, ["date", "time"], { file: "b-2.json" });
  assert.equal(d.layoutKey(duid)[2], "date,time,DUID");
  assert.equal(d.layoutKey(dt)[2], "date,time");
  assert.deepEqual(d.layoutKey(duid).slice(0, 2), d.layoutKey(dt).slice(0, 2));
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

test("a warehouse's V-Order comes off `vorder_enabled`, because the property cannot see it", () => {
  // THE BUG THIS FIXES, and it ran for every dwh record ever measured. Both of stats.py's V-Order
  // signals are Spark-shaped — `vorder` is the `TBLPROPERTIES` key `delta.parquet.vorder.enabled` and
  // `vorder_files` was the Spark writer's per-file `add.tags.VORDER`. Fabric's WAREHOUSE writer sets
  // neither and V-Orders by default (irreversible once off, and nothing in this repo turns it off),
  // so the page printed `·` and banded dwh's bars as un-V-Ordered against V-Ordered parquet.
  const dwh = lay("dwh", 77, 77, { ordering: { dwh: { vorder_enabled: true } } });
  assert.equal(d.vorderOf(dwh), true, "the authoritative flag wins over a `vorder: false` property");
  assert.equal(d.layoutLabel([{ rec: dwh }]), "V-Order · 77 RG");
  assert.equal(d.keyCells([{ rec: dwh }]).ordering, "V-Order");
  assert.equal(d.layoutKey(dwh)[0], true, "and it is what bands the bar");

  // A REAL `false` MUST BEAT A `true` PROPERTY — someone ran the irreversible ALTER, and that is a
  // measurement, not an absence. Hence `typeof === "boolean"` rather than a truthiness test.
  const off = lay("dwh", 77, 77, { vorder: true, ordering: { dwh: { vorder_enabled: false } } });
  assert.equal(d.vorderOf(off), false);
  assert.equal(d.layoutKey(off)[0], false);

  // No key at all is a LAKEHOUSE engine, where the property and the tag are the right instruments —
  // so the fallback has to stay, and must not throw on a record with no ordering block.
  assert.equal(d.vorderOf(lay("spark", 11, 11, { vorder: true })), true);
  assert.equal(d.vorderOf(lay("duckrun", 1, 9)), false);
  assert.equal(d.vorderOf(undefined), false);

  // Two dwh runs, one V-Ordered and one not, must not share a bar — the whole point of the key.
  assert.notDeepEqual(d.layoutKey(dwh), d.layoutKey(off));
  assert.equal(d.layoutKey(dwh)[1], d.layoutKey(off)[1], "same RG band, on purpose");
});

test("a non-default write geometry gets its own column, and the tag says so", () => {
  // stats.py records these ONLY when they differ from the default, so a run carrying one has
  // genuinely written different parquet and `variant()` has already split its column. If the tag
  // stayed silent that column would land under a header identical to the default one's — two
  // identical headers, which is unreadable and says nothing about why.
  assert.equal(d.variantTag([["sorted", "true"], ["vcores", "64"]]), "64c+sorted");
  assert.equal(d.variantTag([["row_group_size", "4000000"], ["sorted", "true"], ["vcores", "64"]]),
    "64c+sorted+4.0Mrg");
  assert.equal(d.variantTag([["file_size_mb", "128"], ["sorted", "true"], ["vcores", "64"]]),
    "64c+sorted+128MB");
  // Two sorted duckrun runs at one core count, one at a smaller row group: two distinct columns.
  const cols = d.columnsFor([
    lay("duckrun", 1, 9, { cfg: { vcores: "64", sorted: "true" }, file: "a-1.json" }),
    lay("duckrun", 4, 36, { cfg: { vcores: "64", sorted: "true", row_group_size: "4000000" },
      file: "b-2.json" }),
  ]).map((c) => c.col);
  assert.equal(new Set(cols).size, 2, cols);
  assert.ok(cols.some((c) => c.endsWith("4.0Mrg")), cols);
  // ...and the tag still never carries the column separator.
  assert.ok(!d.variantTag([["row_group_size", "4000000"], ["file_size_mb", "128"]])
    .includes(d.COL_SEP));
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
    config: { duckrun: { vcores: "8" } },
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
  // ANALYTICS CU is per LAYOUT — named for the writer, keyed on the measured parquet, because Power
  // BI never sees the engine. One run is one layout, and it is a TABLE ROW: the two bar charts that
  // used to draw these same figures are gone, since a bar length is a worse way to read a number
  // printed one block away.
  const t = layoutTable(out);
  assert.deepEqual(t.map((r) => r.writer), ["delta_rs"]);
  assert.equal(t[0].cu, "2,041");
  assert.ok(text.includes("Cost and speed by parquet layout"));
  assert.ok(!/Capacity units per (parquet layout|engine build)/.test(text), "no bar charts");
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
  // The chart and the tables are what the page is for. A reader who already knows what a capacity
  // unit is should not have to scroll past a paragraph explaining it, and a provenance table, to
  // reach them.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 34046.3, SEM: 1514.0 }));
  // THE FIRST TABLE, not the first chart. A single run is a single layout, and the line chart needs
  // two points to show a relationship, so on this fixture there is no figure at all — anchoring on
  // one compared -1 against every index below and passed regardless of the order.
  const firstChart = out.indexOf("<table");
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
  const order = ["<h3>Cost and speed by parquet layout", "<h3>Cost by engine",
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
  // FIRST, and now the only thing above the first table — the `Capacity units` heading that used
  // to sit between them is gone: the shell's `<h1>` names the page and the `as of` stamp restated
  // a date the `built` column carries per run.
  assert.ok(out.indexOf('<p class="lede">') >= 0);
  assert.ok(!out.includes("<h2>Capacity units"), "no section heading, and no as-of stamp");
  assert.ok(out.indexOf('<p class="lede">')
    < out.indexOf("<h3>Cost and speed by parquet layout"));
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
  assert.equal(box.querySelector(".ffind"), null, "no search box");
  assert.equal(box.querySelector(".fpick"), null, "no menus");
  assert.equal(box.querySelector(".fcount"), null, "no row count");
  assert.ok(box.querySelector(".copybtn"), "but a copy button — every table is worth pasting");
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
  assert.ok(runs.includes('data-menus="0,8"'), "menus on `column` and `state`");
  assert.ok(!runs.includes("<select") && !runs.includes("<input"),
    "and it emits no controls — `wireTables` builds them from the rows");
  assert.equal((html.match(/class="filtered"/g) || []).length, 1, "no other table gets one");
  assert.ok(rows(runs).length >= 2, "header and at least one run, filter or no filter");
});

test("every run carries its own row-group count, and a run without one carries a dash", () => {
  // The shape the row's analytics numbers were measured against, per run — the chart caption can
  // only say the bar's range. A dash when the run recorded no layout: unmeasured is not zero.
  const measured = lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json" });
  const bare = full("b-2.json", "spark",                        // stats carry no num_row_groups
    { stats: { spark: { fct_summary: { total_rows: 143980961 } } } });
  const { html } = d.compose([measured, bare], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  const body = rows(block(html, "Every run on this page")).slice(1);
  const cell = (r) => r.split("|").map((c) => c.trim())[4];     // column, run, built, row groups
  assert.ok(rows(block(html, "Every run on this page"))[0].includes("| row groups |"),
    "spelled out — the same quantity is `row groups` in every other table on the page");
  assert.equal(cell(body.find((r) => r.includes("duckrun"))), "27");
  assert.equal(cell(body.find((r) => r.includes("spark"))), "—");
});

test("every run carries its own mart SIZE beside the row groups", () => {
  // The row-group count alone does not say what was written: `duckrun·64c+sorted` runs share a column and a 24-RG
  // count while ranging 543-813 MB on the sort key alone, so a reader comparing two rows of one
  // column sees identical numbers for parquet that differs by half. Same dash rule as RG —
  // unmeasured is not zero, and a table reading `0` would say the run wrote nothing.
  const measured = lay("duckrun", 1, 24, { mb: 543.03, cfg: { vcores: "64" }, file: "a-1.json" });
  const bare = full("b-2.json", "spark",
    { stats: { spark: { fct_summary: { total_rows: 143980961, num_row_groups: 9 } } } });
  const { html } = d.compose([measured, bare], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  const head = rows(block(html, "Every run on this page"))[0];
  assert.ok(head.includes("| row groups | MB |"), `size sits beside the shape: ${head}`);
  const body = rows(block(html, "Every run on this page")).slice(1);
  const cell = (r) => r.split("|").map((c) => c.trim())[5];   // column, run, built, row groups, MB
  assert.equal(cell(body.find((r) => r.includes("duckrun"))), "543", "whole megabytes");
  assert.equal(cell(body.find((r) => r.includes("spark"))), "—", "no size recorded -> dash");
});

// ------------------------------------------------------------------------------------- the charts

test("a layout's CU is the MEDIAN across its runs", () => {
  // One dispatch is one sample of a SHARED capacity, so a single number is a reading rather than a
  // result — and a BAD sample is not a property of the layout. Real case: run 30966983384 read
  // 2,629.3 against 1,331.5/1,577.1/1,586.7 for byte-identical parquet, because its XMLA read billed
  // 49s against ~33s and its refresh took 28.4s against ~8s. A mean lets that one run lift the
  // figure; the median does not. The values below are that shape — mean 2,000, median 1,500 — so
  // this test fails if anyone puts the mean back. The outlier is still reachable: `Every run`
  // carries the dispatch, and the line chart's hover carries the sample size.
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
  const t = layoutTable(out);
  assert.deepEqual(t.map((r) => r.writer), ["spark"], "one row, NAMED for its writer");
  assert.equal(t[0].cu, "1,500", "the median, NOT the 2,000.0 mean");
  assert.equal(t[0].runs, "3", "and the sample size behind it is printed");
});

test("an even number of runs takes the middle two, and one run is itself", () => {
  // The honest limit, pinned so nobody reads the median as a noise fix: at n=1 and n=2 it IS the
  // mean, and four of nine rows on the real page are that thin. It dampens an outlier once there
  // are three samples; only more dispatches make one trustworthy.
  const four = [1000, 1500, 1600, 4000];   // middle two -> 1,550, mean would be 2,025
  const mk = (vals) => {
    const runs = vals.map((_, i) => full(`${"abcd"[i]}-${i}.json`, "spark",
      { finishedHoursAgo: 96 - i * 24 }));
    runs.forEach((r, i) => {
      r.items = { [`S${i}`]: gone("semantic_model", "aemo_spark"),
        [`O${i}`]: gone("output", "dbt_spark") };
    });
    return layoutTable(render(runs, ledger(Object.fromEntries(vals.flatMap((v, i) =>
      [[`S${i}`, { "XMLA Read Operation": v }], [`O${i}`, { "Warehouse Query": 1.0 }]])))))[0];
  };
  assert.equal(mk(four).cu, "1,550");
  assert.equal(mk([1000, 3000]).cu, "2,000", "n=2: the median is the mean");
  assert.equal(mk([2500]).cu, "2,500", "n=1: the reading itself");
});

test("the layout table sorts by CU", () => {
  const runs = [full("a-1.json", "spark"), full("b-2.json", "dwh")];
  runs[0].items = { S0: gone("semantic_model", "aemo_spark"), O0: gone("output", "dbt_spark") };
  runs[1].items = { S1: gone("semantic_model", "aemo_dwh"), O1: gone("output", "dbt_dwh") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 9.0 }, O0: { "Warehouse Query": 1.0 },
    S1: { "XMLA Read Operation": 3.0 }, O1: { "Warehouse Query": 1.0 },
  }));
  assert.deepEqual(layoutTable(out).map((r) => r.writer), ["dwh", "spark"], "cheapest first");
});

// ------------------------------------------------------- one ROW per LAYOUT, not per engine

test("the same parquet is one row however many engines wrote it", () => {
  // Power BI never sees the engine — it opens parquet through Direct Lake and transcodes row groups.
  // duckrun at 64 cores and at 32 wrote 4 files and 27 row groups either way, so two entries 50%
  // apart was not a comparison: it was one layout measured twice, presented as two results.
  const runs = [
    lay("duckrun", 4, 27, { cfg: { vcores: "8" }, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 2000.0 }, O1: 1.0,
  }));
  const t = layoutTable(out);
  assert.deepEqual(t.map((r) => r.writer), ["delta_rs"], "one layout, one row");
  assert.equal(t[0].cu, "1,500");
  assert.equal(t[0].runs, "2", "both runs behind it");
  // ...while `Cost by engine` keeps BOTH columns, because there the writer and the compute it was
  // given are the entire subject. ONE layout row, TWO engine columns, from the same two runs — that
  // asymmetry is why the two are keyed differently.
  assert.deepEqual(rows(block(out, "Cost by engine"))[0],
    "| CU (s) | duckrun·64c | duckrun·8c |",
    "both configs keep a column — string sort, so 64c precedes 8c");
});

test("two runs of ONE column that wrote different parquet are two rows", () => {
  // The bug this pins, on the real records: `duckrun·64c+sorted` wrote two different shapes under
  // one column. Grouping the COLUMNS and pouring every run of each into one entry put them together
  // at their mean — 2,041.8, a number neither run measured — described by only the newer's shape.
  // The layout is measured per RUN, so it has to be grouped per run. (The original pair differed by
  // file count too; that element has left the key, so the fixture separates on row groups.)
  const cfg = { vcores: "8", sorted: "true" };
  const runs = [
    lay("duckrun", 3, 9, { cfg, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 25, { cfg, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs.forEach((r) => { r.dbt = { duckrun: { sort_by: { fct_summary: ["date", "time"] } } }; });
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 2400.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 1600.0 }, O1: 1.0,
  }));
  const t = layoutTable(out);
  assert.deepEqual(t.map((r) => r.writer), ["delta_rs", "delta_rs"], "one writer, two layouts");
  assert.deepEqual(t.map((r) => r.cu), ["1,600", "2,400"], "each run's OWN CU, never their mean");
  // The `ordering`/`row group size` cells are the whole reason two rows with one writer read: the
  // writer answers who wrote it, the key answers what.
  assert.deepEqual(t.map((r) => r.rgSize), ["5.8M", "16.0M"], "and the shapes tell them apart");
  // ...and the mart block says the same thing, because its rows ARE these groups.
  const body = rows(block(out, "the mart the queries land on")).slice(1);
  assert.equal(body.length, 2, "one mart row per bar, not one per writer");
  // Fewest files first, and the block carries LAYOUT ONLY — the CU that used to sit here is in
  // *Cost and speed by parquet layout* and in `Cost by engine`, on the run that measured it.
  assert.ok(body[0].startsWith("| delta_rs | 3 | 9 |"), body[0]);
  assert.ok(body[1].startsWith("| delta_rs | 4 | 25 |"), body[1]);
  assert.ok(!body[0].includes("1,600"), "no CU column on the layout block");
  // `Cost by engine` groups per COLUMN, and both runs are samples of one — so two layout rows and
  // ONE engine column.
  assert.equal(rows(block(out, "Cost by engine"))[0].split("|").length - 2, 2,
    "one measure column and one engine column");
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.startsWith("| **etl** |")));
});

test("a column whose runs recorded no layout stays ONE row", () => {
  // The "two unmeasured layouts are not one layout" rule is about two different COLUMNS. Splitting one
  // column's own runs would print the same label three times with no caption able to say why.
  const runs = ["a-1.json", "b-2.json", "c-3.json"].map((f, i) =>
    full(f, "spark", { finishedHoursAgo: 72 - i * 24 }));
  runs.forEach((r, i) => {
    r.items = { [`S${i}`]: gone("semantic_model", "aemo_spark"), [`O${i}`]: gone("output", "dbt_spark") };
  });
  const t = layoutTable(render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, S1: { "XMLA Read Operation": 2000.0 },
    S2: { "XMLA Read Operation": 1500.0 },
  })));
  assert.deepEqual(t.map((r) => r.writer), ["spark"]);
  assert.equal(t[0].cu, "1,500", "the median of all three, as before");
});

test("an engine is named for who WRITES, not for the dbt target that asked", () => {
  // The column is headed `parquet writer`. `duckrun` is an adapter; delta-rs writes its files, which
  // is why they carry parquet's v2 dictionary spelling and spark's carry v1.
  assert.equal(d.producer(lay("iceberg", 357, 1172)), "duckdb iceberg");
  assert.equal(d.producer(lay("duckrun", 4, 27)), "delta_rs");
  // WRITER_LABEL is producer-only: the COLUMN id keeps the target name, because `baseEngine`
  // reverses it to reach STACK and the (engine, variant) join.
  assert.ok(d.columnsFor([lay("duckrun", 4, 27, { cfg: { vcores: "64" } })])[0].col
    .startsWith("duckrun"), "the column is still the target");
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
  assert.equal(d.producer(lay("duckrun", 4, 27, { cfg: { vcores: "64" } })), "delta_rs");
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
  assert.equal(d.producers(members), "delta_rs, spark writeHeavy", "deduplicated, and both kept");
});

test("the mart layout block is one row per writer, and carries no CU", () => {
  // The mart block groups by the DECLARED producer; *Cost and speed by parquet layout* groups by the
  // MEASURED parquet — two directions onto the same rows, and only the latter carries a cost.
  const runs = [
    lay("duckrun", 4, 27, { cfg: { vcores: "8" }, file: "a-1.json", finishedHoursAgo: 72 }),
    lay("duckrun", 4, 27, { cfg: { vcores: "8" }, file: "b-2.json", finishedHoursAgo: 48 }),
  ];
  runs[0].items = { S0: gone("semantic_model", "aemo_duckrun"), O0: gone("output", "dbt_delta") };
  runs[1].items = { S1: gone("semantic_model", "aemo_duckrun"), O1: gone("output", "dbt_delta") };
  const out = render(runs, ledger({
    S0: { "XMLA Read Operation": 1000.0 }, O0: 1.0,
    S1: { "XMLA Read Operation": 2000.0 }, O1: 1.0,
  }));
  const body = rows(block(out, "the mart the queries land on"));
  assert.equal(body.length, 2, "a header and ONE row — one writer, not one per run");
  assert.ok(body[1].startsWith("| delta_rs | 4 | 27 |"), body[1]);
  assert.equal(layoutTable(out)[0].cu, "1,500", "the cost table still carries the CU");
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
  assert.ok(!rr[0].includes("cold ms") && !/\| (analytics |etl )?CU/.test(rr[0]), rr[0]);
  // Exactly two tables carry them, and neither is a layout block: the cost-and-speed table, one row
  // per layout, and the run table, one row per dispatch.
  const heads = rows(out).filter((r) => r.includes("cold ms"));
  assert.equal(heads.length, 2, `two headers carry the tiers: ${heads}`);
  assert.ok(heads.some((h) =>
    /^\| parquet writer \| ordering \| dictionary \| row group size \| MB \| runs \| cores \| etl CU \| analytics CU \| cold ms \(\d+ q\)/
      .test(h)), heads[0]);
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
  assert.ok(!charts(out).some((c) => /per second/.test(c.title)),
    "the rate row adds no chart of its own");
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
  assert.ok(!charts(out).some((c) => /per second/.test(c.title)),
    "seconds drive the rate ROW, not a chart");
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
  assert.ok(!charts(out).some((c) => /per second/.test(c.title)),
    "the rate adds no chart of its own");
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

test("a group's median never blends two generations", () => {
  // spreadFor walks the whole runs array, so filtering the array is what stops a stale run from
  // pulling the middle. Two spark runs, one stale: the figure must be the survivor's alone.
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
  const t = layoutTable(html);
  assert.equal(t.length, 1, "one generation, one row");
  assert.equal(t[0].cu, "1,000", "no mean of 5,000 and 1,000 — one sample survives");
  assert.equal(t[0].runs, "1");
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

test("the listing comes from a committed index, and the contents API is only the fallback", async () => {
  // The API is 60 requests/hour/IP unauthenticated and answers 403 after that — on a shared or
  // corporate egress IP a reader hits it long before they have loaded the page 60 times, and gets a
  // page with no data. `history/runs/index.json` is served by raw, which has no such limit.
  const seen = [];
  const fake = async (url) => {
    seen.push(url);
    if (url.endsWith("runs/index.json")) {
      return { ok: true, json: async () => ["b-2.json", "a-1.json", "index.json", "legacy/x.json"] };
    }
    if (url.includes("api.github.com")) throw new Error("the API must not be touched");
    if (url.endsWith("cu.json")) return { ok: true, json: async () => ledger({ OUT: 1.0 }) };
    return { ok: true, json: async () => full(url.split("/").pop(), "spark") };
  };
  const { names } = await d.loadRemote({ fetch: fake, repo: "o/r", ref: "main" });
  assert.deepEqual(names, ["a-1.json", "b-2.json"], "sorted; itself and legacy/ filtered out");
  assert.ok(!seen.some((u) => u.includes("api.github.com")), "no contents API call at all");

  // ...and a fork or branch with no index still renders, through the API.
  const noIndex = async (url) => {
    if (url.endsWith("runs/index.json")) return { ok: false, status: 404, statusText: "Not Found" };
    if (url.includes("api.github.com")) {
      return { ok: true, json: async () => [{ type: "file", name: "a-1.json" }] };
    }
    if (url.endsWith("cu.json")) return { ok: true, json: async () => ledger({ OUT: 1.0 }) };
    return { ok: true, json: async () => full("a-1.json", "spark") };
  };
  assert.deepEqual((await d.loadRemote({ fetch: noIndex, repo: "o/r", ref: "main" })).names,
    ["a-1.json"]);
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

/** The shape `stats.py`'s `encodings_for` writes: one entry per column, per engine. */
const enc = (dict) => ({
  date: { encodings: ["PLAIN_DICTIONARY", "RLE"], type: "INT32", dict_pages: 9, chunks: 9, mb: 1.2 },
  mw: { encodings: dict ? ["PLAIN_DICTIONARY"] : ["PLAIN"], type: "DOUBLE",
    dict_pages: dict ? 9 : 0, chunks: 9, mb: dict ? 310.5 : 640.2 },
  price: { encodings: ["PLAIN_DICTIONARY", "PLAIN"], type: "DOUBLE",
    dict_pages: 4, chunks: 9, mb: 214.9 },
});

test("column encoding renders per layout, and flags a column with no dictionary", () => {
  // The question `Table layout` cannot answer: shape does not explain the CU, and what Direct Lake
  // pays for on a cold pass is transcoding, which depends on what the columns are ENCODED as.
  const runs = [lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json" }),
    lay("spark", 10, 10, { vorder: true, cfg: { resource_profile: "readHeavyForPBI" },
      file: "b-2.json" })];
  // `layout.encodings`, where stats.py actually merges it — a sibling of `layout.stats`, not a
  // top-level key. Reading the wrong one renders nothing and looks exactly like "not measured".
  runs[0].layout.encodings = { duckrun: enc(false) };
  runs[1].layout.encodings = { spark: enc(true) };
  runs.forEach((r, i) => {
    r.items = { [`S${i}`]: gone("semantic_model", `aemo_${r.engine}`),
      [`O${i}`]: gone("output", `dbt_${r.engine}`) };
  });
  const { html } = d.compose(runs, ledger({ S0: { "XMLA Read Operation": 10 }, O0: 1,
    S1: { "XMLA Read Operation": 20 }, O1: 1 }), {});
  const body = rows(block(html, "Column encoding")).slice(1);
  assert.equal(body.length, 3, "one row per column");
  const mw = body.find((r) => r.startsWith("| `mw`"));
  assert.ok(mw.includes("⚠️ no dict"), `the PLAIN column is flagged: ${mw}`);
  assert.ok(mw.includes("640.2 MB") && mw.includes("310.5 MB"), mw);
  // A column that started dictionary-encoded and gave up partway still LISTS PLAIN_DICTIONARY, so
  // the list alone would read as "dictionary" for a column that is mostly not.
  assert.ok(body.find((r) => r.startsWith("| `price`")).includes("dict in 4/9"), body);
});

test("no record carrying encodings renders NO encoding table", () => {
  // Every record written before stats.py learned to profile the mart. An empty table would read as
  // "these engines have no encodings", which is not a state parquet can be in.
  const { html } = d.compose([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }), {});
  assert.ok(!html.includes("Column encoding"), "absent, not empty");
  assert.equal(d.renderEncodings([]), "");
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

test("the two CU bar charts are GONE, and their numbers are not", () => {
  // They were `Capacity units per parquet layout` and `… per engine build`, analytics above ETL.
  // Removing them is not a judgement on the build half — that is still where the sharpest
  // operational result lives (duckrun costs 1.8x at 64 cores for the same wall time). It is that
  // both drew a figure the page already PRINTS one block away: the analytics bar was the `CU`
  // column of *Cost and speed by parquet layout*, the ETL bar the `etl` row of *Cost by engine*.
  // So this test is really the no-loss check — if either number ever stops being printed, restoring
  // a chart is a different argument from the one that removed these.
  const out = render([full("a-1.json", "spark")], ledger({ OUT: 1.0, SEM: 2.0 }));
  assert.ok(!/Capacity units per parquet layout|Capacity units per engine build/.test(plain(out)));
  assert.equal((out.match(/class="bar"/g) || []).length, 0, "no bar marks anywhere");
  assert.ok(!out.includes('<div class="charts">'), "and no wrapper left behind");
  assert.equal(layoutTable(out)[0].cu, "2", "analytics CU is a table cell");
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.startsWith("| **etl** |")),
    "and the build CU is a table row");
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
    file: `f-${i}.json`,
    timings: timings({ q1: [cold, warm, hot] }),
  }));

test("etl CU is computed at ONE core count, even while the column is hidden", () => {
  // BUILD COST TRACKS THE MACHINE, and `layoutKey` does not carry `vcores` — it is about the
  // parquet, and duckrun writes the same files at every core count. So a layout group really does
  // hold runs from several machines, and a median over all of them describes none of them: measured
  // on the real records one duckrun layout reads 9,986 CU at 8 vCores and 22,547 blended.
  //
  // Driven through `martPoints` rather than the rendered table, because the COLUMN is off (see
  // `SHOW_ETL` / TODO.md) and the logic is not. That is the point of hiding rather than deleting:
  // this stays green while the column waits, so switching it back on is one constant.
  const at = (cores, file, cu, etl) => ({
    col: "duckrun", qid: file, cu, etl,
    rec: lay("duckrun", 4, 27, { cfg: { vcores: cores }, file }),
  });
  const mid = (entries) => d.martPoints(d.layoutGroups(entries), {})[0];

  const both = mid([at("8", "a-1.json", 1500, 9986), at("64", "b-2.json", 1500, 22547)]);
  assert.equal(both.n, 2, "one layout — both runs wrote the same parquet");
  assert.equal(both.etl, 9986, "the 8-core reading alone, never a blend with the 64-core one");
  assert.equal(both.cu, 1500, "while analytics CU spans BOTH — that one belongs to the parquet");

  // A LAYOUT NOBODY BUILT AT THIS SIZE IS ZERO here and a dash when printed — never a blend.
  assert.equal(mid([at("64", "c-3.json", 1500, 22547), at("32", "d-4.json", 1500, 13083)]).etl, 0);

  // SPARK AND DWH RECORD NO `vcores` AT ALL — FABRIC_CORES sizes the DuckDB notebook and neither
  // reads it. Filtering on the value alone would delete two of the four engines from the column.
  assert.equal(d.vcoresOf(lay("spark", 11, 11)), undefined);
  assert.equal(d.vcoresOf(lay("dwh", 78, 78)), undefined);
  assert.equal(d.vcoresOf(lay("duckrun", 4, 27, { cfg: { vcores: "8" } })), "8");
  const spark = mid([{ col: "spark", qid: "s-1.json", cu: 1500, etl: 33444,
    rec: lay("spark", 11, 11, { file: "s-1.json" }) }]);
  assert.equal(spark.etl, 33444, "an engine with no core count keeps its build CU");
});

test("the cores column reports duckrun's vCores and dashes everyone else", () => {
  // The header cannot state one core count for a table whose engines do not share the concept, so
  // the truth goes per row — and only where the number means something.
  const mk = (engine, cfg, file) => ({
    col: engine, qid: file, cu: 1500, etl: 9000,
    rec: lay(engine, 4, 27, cfg ? { cfg, file } : { file }),
  });
  const cores = (entries) => d.martPoints(d.layoutGroups(entries), {})[0].cores;
  // A NUMBER FOR duckrun ALONE — the only engine whose compute this repo both sizes and varies, and
  // the only reason `ETL_VCORES` pins the column to one size.
  assert.equal(cores([mk("duckrun", { vcores: "8" }, "a-1.json")]), "8");
  // A DASH for everyone else, including iceberg, which records a core count but is not what the
  // pinning exists for. spark's compute is the workspace Livy pool and dwh's is the warehouse.
  assert.equal(cores([mk("iceberg", { vcores: "8" }, "b-2.json")]), "—");
  assert.equal(cores([mk("spark", null, "c-3.json")]), "—");
  assert.equal(cores([mk("dwh", null, "d-4.json")]), "—");
  // And the header carries no core count, because it could only ever be right for some rows.
  const out = render(fitRuns([["duckrun", 20000, 4000, 3000]]), ledger({ OUT: 1.0, SEM: 2.0 }));
  const head = rows(block(out, "Cost and speed by parquet layout"))[0];
  assert.ok(/\| etl CU \|/.test(head), `no parenthetical on the header: ${head}`);
  assert.ok(!/vCores/.test(head), head);
});

test("a layout never built at ETL_VCORES leaves the section, and is NAMED as excluded", () => {
  // The other way round from hiding the column: every row that IS here is complete, and a cost
  // column that is mostly dashes never gets the chance to read as "the build was free".
  const at = (cores, file, rgs) => {
    const r = lay("duckrun", 4, rgs, { cfg: { vcores: cores }, file,
      timings: timings({ q1: [20000, 4000, 3000] }) });
    r.items = { [`O${file}`]: gone("output", "dbt_delta"),
      [`S${file}`]: gone("semantic_model", "aemo_duckrun") };
    return r;
  };
  const led = {
    "Oa-1.json": { "Jupyter Notebook Scheduled Run": 9986.0 },
    "Sa-1.json": { "XMLA Read Operation": 1500.0 },
    "Ob-2.json": { "Jupyter Notebook Scheduled Run": 22547.0 },
    "Sb-2.json": { "XMLA Read Operation": 2500.0 },
  };
  // Two DIFFERENT layouts (row-group bands a power of two apart): one built at 8, one only at 64.
  const out = render([at("8", "a-1.json", 9), at("64", "b-2.json", 80)], ledger(led));
  const t = layoutTable(out);
  assert.equal(t.length, 1, "the 64-core-only layout is not a row");
  assert.equal(t[0].cu, "1,500", "the surviving row is the one built at 8");
  const head = rows(block(out, "Cost and speed by parquet layout"))[0];
  assert.ok(/\| cores \| etl CU \| analytics CU \|/.test(head), `the column is SHOWN now: ${head}`);
  assert.equal(t[0].cores, "8", "and the row states the compute it was measured on");
  // NAMED, never silent — the same discipline the generation filter follows. A page quietly showing
  // a subset would read as "these are the layouts", which is the one thing it must not say.
  const text = plain(out);
  assert.ok(/1 layout not shown/.test(text), `the exclusion is stated: ${text.slice(0, 400)}`);
  assert.ok(/never built at 8 vCores/.test(text), text.slice(0, 400));

  // NOTHING EXCLUDED MEANS NOTHING SAID.
  const all8 = render([at("8", "a-1.json", 9), at("8", "b-2.json", 80)], ledger(led));
  assert.ok(!/layouts? not shown/.test(plain(all8)), "no caveat where nothing was cut");
  assert.equal(layoutTable(all8).length, 2);

  // ON MEMBERSHIP, NOT ON THE VALUE: a layout built at 8 whose CU the ledger has not read keeps its
  // row and dashes that one cell. "Measured, not yet costed" is not "never built at this size".
  const unread = render([at("8", "a-1.json", 9)],
    ledger({ "Sa-1.json": { "XMLA Read Operation": 1500.0 } }));
  const kept = rows(block(unread, "Cost and speed by parquet layout")).slice(1);
  assert.equal(kept.length, 1, "still a row");
  assert.equal(kept[0].split("|")[8].trim(), "—", `etl unread is a dash: ${kept[0]}`);
});

test("cost and speed is one table, cheapest first, with a title and nothing else", () => {
  const out = render(fitRuns([
    ["spark", 20000, 4000, 3000], ["duckrun", 40000, 5000, 4000],
    ["dwh", 80000, 3000, 5000],
  ]), ledger({ OUT: 1.0, SEM: 2.0 }));
  const at = out.indexOf("<h3>Cost and speed by parquet layout</h3>");
  assert.ok(at > 0, "the table is on the page");
  // FIRST, above the charts. It carries what a bar does — the same median from the same
  // `martPoints` — plus the grouping key, the sample size and the tiers, as numbers rather than bar
  // lengths, so it is what a reader wanting one thing from this page should meet first.
  // The selector is UNTERMINATED — the only figure left is `class="chart wide"`, and a selector
  // closing the quote matches nothing. This line read `<div class="charts">` for as long as it
  // existed, which is in no version of the page, so it compared -1 against a positive index and
  // passed whatever the order was; then it read `class="chart">`, which was right until the bar
  // charts went. Twice now, the same silent-pass shape.
  assert.ok(at < out.indexOf('<figure class="chart'), "above its chart");
  assert.ok(at < out.indexOf("<h3>Cost by engine</h3>"), "and above the cost table");
  // The GROUPING KEY is printed between the label and the numbers — six rows reading
  // `duckrun sorted` with nothing to tell them apart is a table hiding what it grouped on.
  // V-Order and the sort key share ONE cell: same kind of fact (a write-time row arrangement), and
  // as two columns each was a dash on every row the other was not.
  // `MB` sits beside `RG` and is NOT part of `layoutKey` — printed so a reader can see where the key
  // is coarser than the parquet, since a group merged on RG band can still hold two file sizes.
  const head = rows(out).find((r) =>
    r.startsWith("| parquet writer | ordering | dictionary | row group size | MB | runs "
    + "| cores | etl CU | analytics CU |"));
  assert.ok(head, "layout, the key, the size, the sample size, CU, then the tiers");
  // The count rides in the HEADER: each tier cell is a SUM over the suite, and the bare `cold ms`
  // read exactly like one query's time.
  assert.ok(/\| cold ms \(\d+ q\) \| warm ms \(\d+ q\) \| hot ms \(\d+ q\) \|/.test(head), head);
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

test("every run a summary drew from has a row of its own", () => {
  // A summarised figure with no row behind it is what this table exists to prevent. A layout group's
  // median spans its whole history, so a superseded run still moves one — and while this listed
  // column holders only, that run's CU appeared nowhere else: `duckrun sorted` read 2,454.1 and no
  // row said so.
  const cfg = { vcores: "8", sorted: "true" };   // 8 so the layout rows survive the etl filter
  const runs = [
    lay("duckrun", 3, 9, { cfg, file: "a-1.json", finishedHoursAgo: 72 }),
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
  assert.ok(body[0].startsWith("| duckrun |"), body[0]);   // the COLUMN id, not the writer
  assert.ok(body[0].includes("| 1,600.0 |"), body[0]);
  // ...and the superseded one is a row like any other: the RUN is the key, and which one is newest is
  // already what the sort order and the `built` column say.
  assert.ok(body[1].startsWith("| duckrun |"), body[1]);
  assert.ok(body[1].includes("| 2,400.0 |"), `the older run's own number: ${body[1]}`);
  assert.ok(layoutTable(html).some((r) => r.cu === "2,400"), "which is a layout row of its own");
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
// Both halves, because the section needs a RANKING to exist at all and only analytics is ranked now
// — `cheapest to build` is gone, so an etl-only ledger renders no Analysis section whatever its
// repeats say. The etl figures stay 100/120 so the measured floor is still 20/110 = 18.2%.
const REPEAT = ledger({ Oduckrun1: 100, Oduckrun2: 120, Odwh: 300,
  Sduckrun1: 40, Sduckrun2: 44, Sdwh: 90 });

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

test("Part A quotes the LAYOUT TABLE's numbers, not a second derivation", () => {
  // A page printing 1,916 in one place and 1,960 in the row under it is asking which one it meant.
  const runs = [own(lay("duckrun", 4, 4, { file: "d-1.json" }), "d"),
    own(lay("spark", 11, 11, { file: "s-1.json", vorder: true }), "s")];
  const out = render(runs, ledger({ Od: 100, Sd: 40, Os: 300, Ss: 90 }));
  const find = (what) => rows(block(out, "Where the rankings hold")).find((r) => r.includes(what));
  assert.ok(find("cheapest to query").includes("| 40.0 |"), find("cheapest to query"));
  assert.ok(layoutTable(out)[0].cu === "40", "which is the cheapest layout row");
  // Build CU is still reported, just not RANKED — it belongs to the engine and the compute it was
  // given, not to the parquet, so it has no place in a table whose every other row ranks layouts.
  assert.ok(rows(block(out, "Cost by engine")).some((r) => r.includes("| **100.0** |")),
    "`Cost by engine` is where build CU lives");
  assert.ok(!plain(analysis(out)).includes("cheapest to build"), "and it is not a finding");
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
  assert.ok(text.includes("cheapest to query"), "the analytics ranking still stands");
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

test("V-Order and the sort key share the ordering cell", () => {
  // Two columns meant each was a dash on every row the other was not. They are the same kind of
  // fact — a write-time arrangement of the rows — which is why `layoutKey` carries them together.
  const vo = { rec: lay("spark", 11, 11, { vorder: true }) };
  const sorted = { rec: lay("duckrun", 1, 24, { cfg: { sorted: "true" } }) };
  sorted.rec.dbt = { duckrun: { sort_by: { fct_summary: ["date", "time"] } } };
  const both = { rec: lay("duckrun", 1, 9, { vorder: true, cfg: { sorted: "true" } }) };
  both.rec.dbt = { duckrun: { sort_by: { fct_summary: ["date", "DUID"] } } };
  assert.equal(d.keyCells([vo]).ordering, "V-Order");
  assert.equal(d.keyCells([sorted]).ordering, "date, time");
  assert.equal(d.keyCells([both]).ordering, "V-Order · date, DUID", "a row may hold both");
  // Sorted by something the record does not name: say so, never invent a key.
  const unnamed = { rec: lay("duckrun", 1, 9, { cfg: { sorted: "true" } }) };
  assert.equal(d.keyCells([unnamed]).ordering, "sorted");
  assert.equal(d.keyCells([{ rec: lay("dwh", 78, 78) }]).ordering, "—", "neither is a dash");
});

test("the dictionary check reads PLAIN differently per parquet version", () => {
  // `dict_pages == chunks` on every column of every real run, so it discriminates nothing. What does
  // is PLAIN beside a dictionary encoding — and PLAIN means opposite things in v1 and v2.
  const enc = (cols) => ({ rec: { engine: "x", layout: { encodings: { x: cols } } } });
  // v2 (arrow-rs / duckrun): data pages are RLE_DICTIONARY and the DICTIONARY PAGE is PLAIN, so
  // PLAIN is always present. The naive rule would condemn every duckrun column on the page.
  assert.equal(d.dictCell([enc({ mw: { encodings: ["PLAIN", "RLE", "RLE_DICTIONARY"] } })]), "yes");
  // v1 (parquet-mr / spark): PLAIN_DICTIONARY covers both page kinds, so a separate PLAIN can only
  // be data pages that abandoned the dictionary. This is `writeHeavy`'s mw, worth ~200 MB.
  assert.equal(d.dictCell([enc({
    DUID: { encodings: ["PLAIN_DICTIONARY", "RLE"] },
    mw: { encodings: ["PLAIN", "PLAIN_DICTIONARY", "RLE"] },
    price: { encodings: ["PLAIN", "PLAIN_DICTIONARY", "RLE"] },
  })]), "no (mw, price)", "names the columns that fell back, sorted");
  // No dictionary at all is not "yes".
  assert.equal(d.dictCell([enc({ mw: { encodings: ["PLAIN"] } })]), "no (mw)");
  // Never measured is a dash, not a "no" — most groups mix runs profiled before and after
  // `encodings_for` existed, so the newest member carrying encodings is the one that answers.
  assert.equal(d.dictCell([{ rec: { engine: "x", layout: {} } }]), "—");
  assert.equal(d.dictCell([{ rec: { engine: "x", layout: {} } },
    enc({ mw: { encodings: ["PLAIN_DICTIONARY", "RLE"] } })]), "yes");
});

// ------------------------------------------------------------------ copying a table to a spreadsheet

test("copy emits the header and every visible row, tab separated", () => {
  // TSV, not markdown or CSV: the destination is a spreadsheet, and unlike CSV it needs no quoting
  // rules for the commas already in `date, time, price` and `1,053`.
  const { root, doc, tbl } = stubTable(
    ["parquet writer", "ordering", "CU"],
    [["delta_rs", "date, time", "1,569"], ["dwh", "—", "1,960"]], {});
  const heads = tbl.tHead.rows[0].cells;
  const rows = tbl.tBodies[0].rows;
  assert.equal(d.tableTsv(heads, rows),
    "parquet writer\tordering\tCU\ndelta_rs\tdate, time\t1,569\ndwh\t—\t1,960");
  assert.ok(root && doc);
});

test("copy takes what you SEE — current sort order, filtered rows dropped", () => {
  // Reading the DOM after `wireSort` rather than the underlying model is the point: a second path
  // to the same numbers is how a copy button starts disagreeing with the table above it.
  const { root, doc, th, tbl } = stubTable(
    ["column", "etl CU"],
    [["duckrun", "26,990.9"], ["spark", "9,986.3"], ["dwh", "38,225.3"]], { menus: "" });
  d.wireTables(root, doc);
  th[1].fire("click");                                    // cheapest first
  const heads = tbl.tHead.rows[0].cells;
  assert.equal(d.tableTsv(heads, tbl.tBodies[0].rows),
    "column\tetl CU\nspark\t9,986.3\nduckrun\t26,990.9\ndwh\t38,225.3",
    "the sort the reader applied is the order they get");
  tbl.tBodies[0].rows.find((r) => r.cells[0].textContent === "duckrun").style.display = "none";
  assert.ok(!d.tableTsv(heads, tbl.tBodies[0].rows).includes("duckrun"),
    "a filtered-out row is hidden from the copy too, not silently included");
});

test("a clipboard that refuses is reported, never silently swallowed", async () => {
  assert.equal(await d.writeClipboard("x", {}), false, "no clipboard API at all");
  assert.equal(await d.writeClipboard("x", { clipboard: {} }), false, "no writeText");
  assert.equal(
    await d.writeClipboard("x", { clipboard: { writeText: () => Promise.reject(new Error("nope")) } }),
    false, "a rejected write is false, not an unhandled rejection");
  let got = null;
  assert.equal(
    await d.writeClipboard("hello", { clipboard: { writeText: (t) => { got = t; return Promise.resolve(); } } }),
    true);
  assert.equal(got, "hello");
});

test("the copy button says what happened", async () => {
  const { root, doc, box } = stubTable(
    ["column", "CU"], [["duckrun", "1,810.1"]], {});
  box.className = "sortable";
  d.wireTables(root, doc);
  const btn = box.querySelector(".copybtn");
  assert.equal(btn.textContent, "copy");
  // No `navigator` in the test runner, so `writeClipboard` finds no API and reports the failure —
  // which is exactly the state a reader on an insecure origin or a locked-down policy would see.
  await btn.listeners.click[0]({});
  assert.equal(btn.textContent, "select and copy",
    "a button that silently does nothing is worse than no button");
});

// ---------------------------------------------- the encoding table only ever prints a column it can name

/** A column-mapped footer: the physical names a Fabric Warehouse actually writes. */
const guidEnc = () => ({
  "col-198f7fa3-51d0-4557-905a-c6408fec0454": {
    encodings: ["PLAIN", "RLE", "RLE_DICTIONARY"], type: "INT64", dict_pages: 77, chunks: 77, mb: 0.01 },
  "col-89683a34-759f-4df8-a82f-f52e60fb35e0": {
    encodings: ["PLAIN", "RLE", "RLE_DICTIONARY"], type: "INT64", dict_pages: 77, chunks: 77, mb: 492.46 },
});

const encRuns = (a, b) => {
  const runs = [lay("duckrun", 4, 27, { cfg: { vcores: "64" }, file: "a-1.json" }),
    lay("dwh", 78, 78, { file: "b-2.json" })];
  runs[0].layout.encodings = { duckrun: a };
  runs[1].layout.encodings = { dwh: b };
  runs.forEach((r, i) => {
    r.items = { [`S${i}`]: gone("semantic_model", `aemo_${r.engine}`),
      [`O${i}`]: gone("output", `dbt_${r.engine}`) };
  });
  return d.compose(runs, ledger({ S0: { "XMLA Read Operation": 10 }, O0: 1,
    S1: { "XMLA Read Operation": 20 }, O1: 1 }), {}).html;
};

test("a physical column name is never printed as a row", () => {
  // Fabric Warehouse writes Delta with COLUMN MAPPING on, so its parquet footer carries
  // `col-89683a34-759f-…` and not `mw`. Keying rows on Object.keys() put six of those down the first
  // column, in rows of their own, pushing the real names into a second block — twelve rows for six
  // columns, every cell in each half a dash. A render layer has no business showing an identifier.
  const html = encRuns(enc(false), guidEnc());
  assert.ok(!html.includes("col-89683a34"), "no GUID reaches the page");
  assert.ok(!html.includes("col-198f7fa3"));
  const body = rows(block(html, "Column encoding")).slice(1);
  assert.deepEqual(body.map((r) => r.split("|")[1].trim()), ["`date`", "`mw`", "`price`"],
    "only the named columns, in MART_COLUMNS order — never alphabetical");
});

test("a layout whose columns are ALL unnameable is named in a caveat, not dropped in silence", () => {
  // An unmeasured column and an unnameable one look identical as a column of dashes, and only one of
  // them is a tooling problem the reader can act on.
  const html = encRuns(enc(false), guidEnc());
  const said = plain(html);
  assert.ok(said.includes("column names this page cannot resolve"), said.slice(0, 400));
  assert.ok(said.includes("0.4.47"), "and it says which duckrun resolves them");
});

test("every column being unnameable renders the caveat and NO empty table", () => {
  const html = encRuns(guidEnc(), guidEnc());
  assert.ok(plain(html).includes("column names this page cannot resolve"));
  assert.ok(!/Column encoding[\s\S]{0,400}<tbody>/.test(html),
    "a header row with no body reads as 'these engines have no encodings'");
});

test("MART_COLUMNS is the model's own select list, in its own order", () => {
  assert.deepEqual(d.MART_COLUMNS, ["date", "time", "DUID", "mw", "price", "cutoff"]);
});

test("MART_COLUMNS covers every column stats.py has actually recorded", async () => {
  // THE DRIFT GUARD, and the cost of hardcoding the list: a new model column would silently not
  // appear in the encoding table. Checked against real recorded data rather than by parsing SQL —
  // `history/` holds what `stats.py` read out of the footers, and the duckdb engines write no column
  // mapping, so their names ARE the logical ones. Skips when nothing has been profiled yet.
  const fs = await import("node:fs");
  let seen = new Set(), any = false;
  const dir = "history/runs";
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir).filter((n) => n.endsWith(".json") && n !== "index.json")) {
    const rec = JSON.parse(fs.readFileSync(`${dir}/${f}`, "utf8"));
    const e = ((rec.layout || {}).encodings || {})[rec.engine];
    if (!e || !["duckrun", "iceberg", "spark"].includes(rec.engine)) continue;
    any = true;
    for (const c of Object.keys(e)) seen.add(c);
  }
  if (!any) return;
  const missing = [...seen].filter((c) => !d.MART_COLUMNS.includes(c));
  assert.deepEqual(missing, [],
    `stats.py recorded column(s) MART_COLUMNS does not list, so the page silently hides them: ${missing}`);
});

test("row group size is rows per group in millions, ranged when the group differs", () => {
  // The same fact as the row-group count — every engine writes the identical 143,980,961 rows, so
  // `avg_row_group` IS `total_rows / num_row_groups` — said the way it can be acted on: `16.0M` is a
  // segment size to compare against VertiPaq's own, where `9` is a number you must divide first.
  const one = d.keyCells([{ rec: lay("duckrun", 1, 9, { avg: 15997884.6, file: "a-1.json" }) }]);
  assert.equal(one.rgSize, "16.0M");
  const many = d.keyCells([{ rec: lay("spark", 12, 9, { avg: 16000000, file: "a-1.json" }) },
    { rec: lay("spark", 12, 11, { avg: 13089178, file: "b-2.json" }) }]);
  assert.equal(many.rgSize, "13.1–16.0M", "a band of counts is a band of sizes");
  // Small groups must not round to 0.0M and vanish — iceberg writes 1,172 of them.
  assert.equal(d.keyCells([{ rec: lay("iceberg", 366, 1172, { avg: 122850.6, file: "c-3.json" }) }])
    .rgSize, "0.1M");
  // Derived when `avg_row_group` was never recorded, so older records still fill the column.
  const derived = d.keyCells([{ rec: lay("duckrun", 4, 24, { file: "d-4.json", avg: null }) }]);
  assert.equal(derived.rgSize, "6.0M", "total_rows / num_row_groups when the field is absent");
  assert.equal(d.keyCells([{ rec: full("e-5.json", "spark") }]).rgSize, "—", "unmeasured is a dash");
});

// ------------------------------------------------------------------- CU against query time (scatter)

const pt = (label, x, y, n = 1) => ({ label, x, y, n, sub: "16.0M" });

test("the scatter brackets the data on round ticks, not on a snapped bound", () => {
  // THE BUG THIS PINS, found by rendering it and measuring where the dots landed: snapping the
  // BOUND to 1/2/2.5/5/10x a power of ten rounds a 5,237 maximum up to 10,000, and the whole cloud
  // then lives in the left third with two thirds of the panel empty. The STEP is what snaps.
  const svg = d.scatterSvg("t", "s", [pt("a", 2773, 1514), pt("b", 5237, 8641)]);
  const xs = [...svg.matchAll(/<circle class="dot c\d" cx="([\d.]+)"/g)].map((m) => +m[1]);
  const span = Math.max(...xs) - Math.min(...xs);
  // Measured against the figure's OWN viewBox, so enlarging the chart cannot quietly weaken this.
  const vb = +/viewBox="0 0 (\d+)/.exec(svg)[1];
  assert.ok(span > vb * 0.4, `the cloud fills the plot, not a corner: ${span.toFixed(0)} of ${vb}`);
  assert.ok(!/NaN|Infinity/.test(svg), "no degenerate geometry");
});

test("a dot is labelled iff the caller gave it an id", () => {
  // The CALLER decides, because only it knows which names are unique on the plot: `dwh` and the
  // three spark profiles are one dot each and get labels, `delta_rs` is seven dots and gets its
  // identity from the legend's colour instead — labelling it would print one word seven times.
  const svg = d.scatterSvg("t", "s", [
    { label: "dwh", id: "dwh", x: 5000, y: 1000, c: 2e6, hue: 2 },
    { label: "delta_rs", id: "", x: 4000, y: 2000, c: 6e6, hue: 1 },
    { label: "delta_rs", id: "", x: 3000, y: 2500, c: 16e6, hue: 1 }],
  "cold ms", "row group size");
  assert.equal([...svg.matchAll(/<title>/g)].length, 3, "one title per dot, always");
  const named = [...svg.matchAll(/<text class="bar-caption" [^>]*>([^<]+)</g)].map((m2) => m2[1])
    .filter((t) => !["cold ms", "CU"].includes(t));
  assert.deepEqual(named, ["dwh"], "only the point with an id, and never twice");
  // The legend is what names the rest, so identity never rests on colour alone.
  const key = [...svg.matchAll(/<text class="bar-caption key"[^>]*>([^<]+)</g)].map((m2) => m2[1]);
  assert.ok(key.includes("delta_rs") && key.includes("dwh"), `legend names every writer: ${key}`);
});

test("a label that would overrun the plot flips to the left of its dot", () => {
  // The cheapest-CU point is often also the slowest, i.e. hard against the right edge.
  const svg = d.scatterSvg("t", "s", [pt("a-very-long-writer-name-indeed", 9999, 1000),
    pt("b", 1000, 2000)]);
  assert.ok(svg.includes('text-anchor="end"'), "flipped rather than allowed to overflow");
  for (const m of svg.matchAll(/<text class="bar-caption" x="([\d.]+)"[^>]*>([^<]+)</g)) {
    if (m[2] === "hot ms" || m[2] === "CU") continue;
    assert.ok(+m[1] <= +/viewBox="0 0 (\d+)/.exec(svg)[1],
      `label starts inside the viewBox: ${m[2]} at ${m[1]}`);
  }
});

test("fewer than two points is no chart at all", () => {
  assert.equal(d.scatterSvg("t", "s", []), "");
  assert.equal(d.scatterSvg("t", "s", [pt("a", 1, 1)]), "", "one dot shows no relationship");
  assert.equal(d.scatterSvg("t", "s", [pt("a", 0, 5), pt("b", 3, 0)]), "",
    "a missing measure is dropped, not plotted at zero");
});



test("iceberg is off the scatter, and the subtitle says so", () => {
  // A ~4x outlier on BOTH axes set the scale for everyone else: 12 of 78 dot pairs overlapped with
  // it in, 1 of 66 without. Dropped by a NAMED constant, never a computed "more than Nx the median"
  // rule — an automatic filter silently changes which point it removes as records land, and a
  // dropped run on this page is always a named run.
  const p = (engine, name, cold, cu) => ({
    name, cu, n: 1, ms: { cold }, members: [{ rec: lay(engine, 4, 24, { file: `${name}.json` }) }],
  });
  const svg = d.scatterFit([p("duckrun", "delta_rs", 25000, 1800),
    p("spark", "spark writeHeavy", 45000, 3700),
    p("iceberg", "duckdb iceberg", 100394, 8641)]);
  assert.equal([...svg.matchAll(/<circle class="dot c\d"/g)].length, 2, "two dots, not three");
  assert.equal([...svg.matchAll(/<title>([^<]+)/g)].filter((m) => /iceberg/.test(m[1])).length, 0);
  const sub = /<span class="chart-sub">([^<]+)</.exec(svg)[1];
  assert.ok(sub.includes("iceberg left out"), `the exclusion is stated: ${sub}`);
  assert.ok(sub.includes("its cold pass 2x the slowest of these"), sub);
});

test("with nothing to exclude the subtitle claims no exclusion", () => {
  const p = (engine, name, cold, cu) => ({
    name, cu, n: 1, ms: { cold }, members: [{ rec: lay(engine, 4, 24, { file: `${name}.json` }) }],
  });
  const svg = d.scatterFit([p("duckrun", "delta_rs", 25000, 1800),
    p("spark", "spark writeHeavy", 45000, 3700)]);
  assert.ok(!/left out/.test(svg), "no caveat where nothing was cut");
});

test("every point is named and no two names collide", () => {
  // The chart labelled three dots and left nine anonymous, and the label was the WRITER — so even
  // naming them all would have printed `delta_rs` seven times and separated nothing. The label is
  // the table's row identity, and placement is greedy over two rings of candidate offsets.
  const cluster = Array.from({ length: 9 }, (_, i) => ({
    label: "delta_rs", id: `date, time · ${i}.0M`, x: 25000 + i * 300, y: 1800 + i * 40, c: i * 1e6,
  }));
  const svg = d.scatterSvg("t", "s", [...cluster,
    { label: "dwh", id: "dwh", x: 33000, y: 2600, c: 1.8e6 },
    { label: "spark writeHeavy", id: "spark writeHeavy", x: 45000, y: 3700, c: 10e6 }],
  "cold ms", "row group size");
  const CH = 5.15, LH = 13;
  const labs = [...svg.matchAll(/<text class="bar-caption" x="([\d.]+)" y="([\d.]+)"([^>]*)>([^<]+)</g)]
    .map((m) => ({ x: +m[1], y: +m[2], a: /end/.test(m[3]) ? "end" : /middle/.test(m[3]) ? "mid" : "s", t: m[4] }))
    .filter((l) => !["cold ms", "CU", "row group size"].includes(l.t) && !/^[\d,.]+M?$/.test(l.t));
  assert.equal(labs.length, 11, "one label per dot, none dropped");
  const box = (l) => {
    const w = l.t.length * CH;
    const x0 = l.a === "end" ? l.x - w : l.a === "mid" ? l.x - w / 2 : l.x;
    return { x0, x1: x0 + w, y0: l.y - LH * 0.72, y1: l.y + LH * 0.28 };
  };
  for (let i = 0; i < labs.length; i++) {
    for (let j = i + 1; j < labs.length; j++) {
      const A = box(labs[i]), B = box(labs[j]);
      assert.ok(!(A.x0 < B.x1 && A.x1 > B.x0 && A.y0 < B.y1 && A.y1 > B.y0),
        `"${labs[i].t}" collides with "${labs[j].t}"`);
    }
  }
  const vb = /viewBox="0 0 (\d+) (\d+)"/.exec(svg);
  for (const l of labs) {
    const b = box(l);
    assert.ok(b.x0 >= 0 && b.x1 <= +vb[1] && b.y0 >= 0 && b.y1 <= +vb[2],
      `"${l.t}" stays inside the viewBox`);
  }
});

test("a layout is one LINE on one shared time axis, warm end to cold end", () => {
  // ONE AXIS, NEVER TWO. Both tiers are milliseconds, so a second scale would make the length —
  // which is the entire reading — not a quantity. This replaced a second chart plotting cold
  // against warm: same three numbers, one panel, and the transcode cost readable without
  // subtracting anything.
  const p = (engine, name, cold, warm) => ({
    name, n: 1, cu: 2000, ms: { cold, warm, hot: warm - 300 },
    members: [{ rec: lay(engine, 4, 24, { file: `${name}.json` }) }],
  });
  const svg = d.scatterFit([p("duckrun", "delta_rs", 25000, 4500), p("dwh", "dwh", 33767, 4330),
    p("iceberg", "duckdb iceberg", 100394, 3646)]);
  const seg = [...svg.matchAll(
    /<line class="pair c\d" x1="([\d.]+)" y1="([\d.]+)" x2="([\d.]+)" y2="([\d.]+)"/g)];
  assert.equal(seg.length, 2, "one line per layout, and iceberg is off it");
  // NO END MARKERS. The length is the reading; two circles on it were a second grammar to learn
  // before it could be read, which is what got this chart simplified.
  assert.equal([...svg.matchAll(/<circle class="dot c\d"/g)].length, 0, "and no dots at all");
  for (const [, x1, y1, x2, y2] of seg) {
    assert.equal(y1, y2, "the line sits at ONE y — its length is a time, not a slope");
    assert.ok(+x1 < +x2, "warm left, cold right: the second visit is always the cheap one");
    // The shared domain reaches the warm end: scaling to `p.x` alone would run every line off the
    // left of the plot, which is the failure that makes a span not a span.
    assert.ok(+x1 > 62, `the warm end is inside the plot, not on the axis: ${x1}`);
  }
  assert.ok(/its cold pass 2x the slowest of these/.test(svg), "the exclusion is still stated");
  const axes = [...svg.matchAll(
    /<text class="bar-caption"[^>]*>((?:cold|warm|hot) ms|query time \(ms\)|CUs?)</g)]
    .map((m2) => m2[1]).sort();
  assert.deepEqual(axes, ["CUs", "query time (ms)"], `one time axis, not two: ${axes}`);
  // NO SIZE KEY: with the dots gone there is nothing to size. Row group size is NOT lost — it stays
  // a column of the table above and a line of every hover, which is why this looks at the legend
  // text alone rather than at the whole document.
  const key = [...svg.matchAll(/<text class="bar-caption key"[^>]*>([^<]*)</g)].map((m) => m[1]);
  assert.ok(!key.some((t) => /row group size/.test(t)), `no key for an unencoded channel: ${key}`);
  assert.ok(key.includes("delta_rs") && key.includes("dwh"), `but the writer key stays: ${key}`);
  assert.ok(/row group size: /.test(svg), "and the size is still on every hover");
});

test("both axes are LOG, so an equal RATIO is an equal distance", () => {
  // WHAT THE CHANGE MEANS, and it is not cosmetic: on a log x a line's LENGTH stops being a
  // difference and becomes a ratio — "the cold pass is 6x the warm one", which is a property of the
  // layout, where "18,000 ms slower" mostly tracks how big the query happened to be. Pinned by
  // geometry rather than by reading a flag: three points a decade apart must be evenly spaced.
  const svg = d.scatterSvg("t", "s", [
    { label: "a", x: 1000, y: 1000 }, { label: "b", x: 10000, y: 10000 },
    { label: "c", x: 100000, y: 100000 }], "query time (ms)");
  const at = [...svg.matchAll(/<circle class="dot c\d" cx="([\d.]+)" cy="([\d.]+)"/g)]
    .map((m) => ({ x: +m[1], y: +m[2] })).sort((p, q) => p.x - q.x);
  assert.equal(at.length, 3);
  const dx = [at[1].x - at[0].x, at[2].x - at[1].x];
  assert.ok(Math.abs(dx[0] - dx[1]) < 0.5, `equal decades, equal gaps on x: ${dx}`);
  const dy = [at[0].y - at[1].y, at[1].y - at[2].y];
  assert.ok(Math.abs(dy[0] - dy[1]) < 0.5, `and on y: ${dy}`);
  // AND THE BOUND IS NOT SNAPPED OUT TO WHOLE DECADES. `fct_summary`'s CU spans half a decade
  // (1,332-3,769); a 1,000-10,000 axis would put every layout on the page in the bottom half.
  const narrow = d.scatterSvg("t", "s", [{ label: "a", x: 1332, y: 1332 },
    { label: "b", x: 3769, y: 3769 }], "query time (ms)");
  const ys = [...narrow.matchAll(/<circle class="dot c\d" cx="[\d.]+" cy="([\d.]+)"/g)].map((m) => +m[1]);
  assert.ok(Math.abs(ys[0] - ys[1]) > 380, `the pair fills the plot, not a corner: ${ys}`);
  // A narrow log range still gets gridlines — the coarse 1/2/5 mantissas yield one tick over half a
  // decade, and an axis with no numbers on it reads as a rendering failure, not as a narrow range.
  assert.ok([...narrow.matchAll(/<line class="axis"/g)].length >= 4, "and keeps its gridlines");
});

test("a writer that names several layouts is labelled with the layout, at the cold end", () => {
  // `delta_rs` is most of the lines, so its NAME separates nothing — it is in the legend, and what
  // tells its lines apart is the sort key and the row group count. Both ends are free for such a
  // line (no unique name means no label at the cold end), and the COLD end wins because that is
  // where the eye already is: the cold ends are what the chart is ranked by and what spreads out.
  // A writer whose name IS unique keeps the name, in the same place.
  const p = (rec, name, cold, warm, cu) => ({
    name, cu, n: 1, ms: { cold, warm }, members: [{ rec }],
  });
  const svg = d.scatterFit([
    p(sortedBy(1, 9, ["date", "time"], { file: "a.json" }), "delta_rs", 25000, 4500, 1500),
    p(sortedBy(1, 24, ["date", "time", "price"], { file: "b.json" }), "delta_rs", 27000, 4200, 1700),
    p(lay("dwh", 78, 90, { file: "c.json" }), "dwh", 33767, 1500, 2000),
  ]);
  const labs = [...svg.matchAll(/<text class="bar-caption"([^>]*)>([^<]+)</g)]
    .map((m) => ({ end: /text-anchor="end"/.test(m[1]), x: +/x="([\d.]+)"/.exec(m[1])[1], t: m[2] }))
    .filter((l) => !["query time (ms)", "CUs"].includes(l.t));
  const texts = labs.map((l) => l.t);
  assert.ok(texts.includes("date, time · 9 RG") && texts.includes("date, time, price · 24 RG"),
    `the layout, not the writer: ${texts}`);
  assert.ok(texts.includes("dwh"), `and a unique writer keeps its name: ${texts}`);
  // THE SAME STRING THE ANALYTICS BAR IS CAPTIONED WITH, minus its leading `by ` — a line and its
  // bar must not describe the same parquet two different ways, and three characters each is what
  // keeps thirteen of these off each other in the crowded left half.
  assert.equal(d.layoutLabel([{ rec: sortedBy(1, 9, ["date", "time"], { file: "a.json" }) }]),
    "by date, time · 9 RG");
  // Anchored `start` and past its own line's cold end — printed rightward into the label gutter,
  // never leftward back across the line it names.
  const colds = [...svg.matchAll(/<line class="pair c\d" x1="[\d.]+" y1="[\d.]+" x2="([\d.]+)"/g)]
    .map((m) => +m[1]);
  for (const l of labs.filter((x) => /RG$/.test(x.t))) {
    assert.ok(!l.end, `"${l.t}" reads rightward`);
    assert.ok(colds.some((c) => l.x > c && l.x - c < 40),
      `"${l.t}" at ${l.x} sits just past a cold end; colds ${colds}`);
  }
  assert.equal(texts.filter((t) => t === "delta_rs").length, 0, "and the bare writer name is gone");

  // THE GUTTER IS ONLY RESERVED WHEN SOMETHING GOES IN IT. Widening the x axis unconditionally
  // squeezes the gaps a label has to find, and on a dense cluster of plain dots that made eleven
  // names that used to fit start colliding. A gutter with nothing in it is pure loss.
  const rightmost = (id2) => Math.max(...[...d.scatterSvg("t", "s",
    [{ label: "a", x: 1000, y: 10, id2 }, { label: "b", x: 10000, y: 20 }], "query time (ms)")
    .matchAll(/<circle class="dot c\d" cx="([\d.]+)"/g)].map((m) => +m[1]));
  assert.ok(rightmost("") > 870, `no right-hand labels, no gutter: ${rightmost("")}`);
  assert.ok(rightmost("9 RG") < rightmost("") - 60,
    `one label opens the gutter and everything shifts left: ${rightmost("9 RG")}`);

  // WHEREVER IT LANDS, IT LANDS INSIDE THE PLOT. A name is never dropped, and the forcing fallback
  // flips side rather than running off the axis — the bounds-free version pushed one 25 units past
  // the y axis and across an unrelated line.
  const cramped = d.scatterFit([
    p(sortedBy(1, 9, ["date", "time", "price"], { file: "a.json" }), "delta_rs", 25000, 4500, 1500),
    p(sortedBy(1, 24, ["date", "time", "price"], { file: "b.json" }), "delta_rs", 27000, 4600, 1700),
  ]);
  const far = [...cramped.matchAll(/<text class="bar-caption" x="([\d.]+)"([^>]*)>([^<]+)</g)]
    .map((m) => ({ x: +m[1], end: /text-anchor="end"/.test(m[2]), t: m[3] }))
    .filter((l) => /RG$/.test(l.t));
  assert.equal(far.length, 2, "both are still labelled — a name is never dropped");
  for (const l of far) {
    const w = l.t.length * 5.15;
    const x0 = l.end ? l.x - w : l.x;
    assert.ok(x0 >= 62 && x0 + w <= 904, `"${l.t}" stays inside the plot: ${x0}–${x0 + w}`);
  }
});

test("a layout with no warm pass is still plotted, as a dot", () => {
  const svg = d.scatterSvg("t", "s", [
    { label: "a", x: 25000, x2: 4500, y: 1800, hue: 1 },
    { label: "b", x: 33000, y: 2600, hue: 2 }], "query time (ms)");
  assert.equal([...svg.matchAll(/<line class="pair c\d"/g)].length, 1, "one has a span");
  assert.equal([...svg.matchAll(/<circle class="dot c\d"/g)].length, 1, "the other is a dot");
  // ONE SHAPE PER POINT, never both — and unmeasured is an absent thing, never a zero. A line run
  // back to x=0 would sit on the axis and read as "this layout answered instantly".
  assert.ok(!/<line class="pair[^>]*x1="62\./.test(svg));
  assert.equal(d.scatterSvg("t", "s", [{ label: "a", x: 0, x2: 4500, y: 5 },
    { label: "b", x: 3, y: 2 }]), "", "and a zero x still drops the whole row, second end or not");
});

test("a label lands on no line", () => {
  // A line reaches much further across the plot than a dot did, so it is the occluder that matters
  // now. If this fails, reorder CANDIDATES or add a ring — never drop the line from `hits`, which
  // is the fix that hides the problem.
  const pts = Array.from({ length: 11 }, (_, i) => ({
    label: `w${i}`, id: `w${i}`, x: 25000 + i * 900, x2: 3500 + i * 250,
    y: 1400 + i * 220, hue: (i % 5) + 1,
  }));
  const svg = d.scatterSvg("t", "s", pts, "query time (ms)");
  const CH = 5.15, LH = 13;
  const labs = [...svg.matchAll(/<text class="bar-caption" x="([\d.]+)" y="([\d.]+)"([^>]*)>([^<]+)</g)]
    .map((m) => ({ x: +m[1], y: +m[2], a: /end/.test(m[3]) ? "end" : /middle/.test(m[3]) ? "mid" : "s", t: m[4] }))
    .filter((l) => /^w\d+$/.test(l.t));
  assert.equal(labs.length, 11, "one label per line, none dropped");
  const box = (l) => {
    const w = l.t.length * CH;
    const x0 = l.a === "end" ? l.x - w : l.a === "mid" ? l.x - w / 2 : l.x;
    return { x0, x1: x0 + w, y0: l.y - LH * 0.72, y1: l.y + LH * 0.28 };
  };
  const segs = [...svg.matchAll(/<line class="pair c\d" x1="([\d.]+)" y1="([\d.]+)" x2="([\d.]+)"/g)]
    .map((m) => ({ x0: +m[1], x1: +m[3], y: +m[2] }));
  assert.equal(segs.length, 11);
  for (const l of labs) {
    const b = box(l);
    for (const s of segs) {
      assert.ok(!(b.x0 < s.x1 && b.x1 > s.x0 && b.y0 < s.y + 3 && b.y1 > s.y - 3),
        `"${l.t}" is printed across a line`);
    }
  }
  for (let i = 0; i < labs.length; i++) {
    for (let j = i + 1; j < labs.length; j++) {
      const A = box(labs[i]), B = box(labs[j]);
      assert.ok(!(A.x0 < B.x1 && A.x1 > B.x0 && A.y0 < B.y1 && A.y1 > B.y0),
        `"${labs[i].t}" collides with "${labs[j].t}"`);
    }
  }
});

test("slot 6 is the neutral Other, not a sixth hue", () => {
  // Five is the ceiling the validator allows on BOTH surfaces: every candidate sixth collapsed
  // against slot 1 (protan 3.1) or, once light enough for the dark surface, against slot 4.
  assert.equal(d.WRITER_HUE["duckdb iceberg"], 6);
  assert.deepEqual(Object.values(d.WRITER_HUE).sort((a, b) => a - b), [1, 2, 3, 4, 5, 6],
    "one slot per writer, fixed and never cycled");
});

// ----------------------------------------------------------------- saving a chart as an image
//
// The browser half of this — `getComputedStyle`, `<img>`, canvas — cannot run here, and the parts
// that CAN are the parts that were wrong: an unescaped font stack made the exported document
// unparseable, and an unparseable SVG does not raise, it just silently fails to load and the export
// falls back to writing SVG. So the string layer is pinned hard and the DOM layer only has to be
// reached.

test("a chart's file is named after the chart, so two never overwrite each other", () => {
  assert.equal(d.chartFilename("CU against query time"), "cu-against-query-time.png");
  assert.equal(d.chartFilename("Capacity units per engine build"),
    "capacity-units-per-engine-build.png");
  assert.equal(d.chartFilename("Capacity units per engine build", "svg"),
    "capacity-units-per-engine-build.svg");
  assert.equal(d.chartFilename("  —  "), "chart.png", "a title of punctuation still names a file");
  assert.equal(d.chartFilename(""), "chart.png");
});

test("the caption wraps on words and never inside one", () => {
  assert.deepEqual(d.wrapText("one dot per layout", 12), ["one dot per", "layout"]);
  assert.deepEqual(d.wrapText("", 20), []);
  assert.deepEqual(d.wrapText("supercalifragilistic", 8), ["supercalifragilistic"],
    "a word longer than the line is not broken");
});

test("a transparent page background is not carried into the image", () => {
  // `getComputedStyle(body).backgroundColor` reads `rgba(0, 0, 0, 0)` whenever the page paints its
  // background elsewhere, and a transparent PNG renders BLACK in a dark chat window — the one place
  // these get pasted.
  assert.equal(d.opaque("rgba(0, 0, 0, 0)"), "");
  assert.equal(d.opaque("transparent"), "");
  assert.equal(d.opaque(""), "");
  assert.equal(d.opaque("rgb(255, 255, 255)"), "rgb(255, 255, 255)");
  assert.equal(d.opaque("rgba(20, 20, 24, 1)"), "rgba(20, 20, 24, 1)");
});

test("a quoted font stack does not blow the exported document apart", () => {
  // THE BUG THIS EXISTS FOR: `getComputedStyle` reports the family list WITH its quotes, so writing
  // it raw produced a font-family attribute closed by its own value — not well-formed, so the
  // `<img>` fired `error`, the canvas stayed blank and every save silently degraded to SVG. Found
  // by rendering the file, not by reading the code.
  const svg = d.wrapSvg("<circle/>", { title: "T", font: '"Segoe UI", system-ui' });
  assert.ok(!/font-family=""/.test(svg), "the attribute is not closed by its own value");
  assert.ok(svg.includes("&quot;Segoe UI&quot;"));
  assert.ok(!/=""[A-Za-z]/.test(svg), "and nothing else opens a bare attribute either");
});

test("the exported document is a caption above the plot, and the plot is not rescaled", () => {
  const svg = d.wrapSvg('<circle cx="1" cy="2"/>', {
    width: 920, plotHeight: 610, title: "CU against cold query time",
    subtitle: "one dot per layout", note: "queried over 1 fact (144.0M)",
    bg: "#fff", fg: "#111", dim: "#666", font: "sans-serif",
  });
  const dims = /width="(\d+)" height="(\d+)"/.exec(svg);
  // 16 units of bleed EACH SIDE, and the plot is offset by exactly that — the page grants the chart
  // `overflow: visible`, a standalone file grants it nothing, and the last x tick is anchored
  // `middle` on the axis end, so half of `50,000` fell outside and was clipped away.
  const g = /<g transform="translate\(16 (\d+)\)">/.exec(svg);
  assert.equal(Number(dims[1]), 920 + 32, "the plot's own width plus the bleed, never rescaled");
  assert.ok(g, "the plot is pushed down and across rather than redrawn");
  assert.equal(Number(dims[2]), Number(g[1]) + 610 + 9, "height is caption + plot, exactly");
  // All three caption lines reach the file — this is the whole reason the export is not just the
  // `<svg>`: the title, the subtitle and the model-shape note are HTML siblings of it.
  for (const t of ["CU against cold query time", "one dot per layout", "queried over 1 fact"]) {
    assert.ok(svg.includes(t), `the image carries "${t}"`);
  }
  assert.ok(/<rect x="0" y="0"[^>]*fill="#fff"/.test(svg), "on an opaque background");
  assert.ok(svg.startsWith('<svg xmlns="http://www.w3.org/2000/svg"'), "namespaced, so it renders");
});

test("paint is inlined, defaults are not, and type properties stay on text", () => {
  // A node's computed style has EVERY property, so copying the type ones onto four hundred circles
  // tripled the file for nothing — and the file travels as a `data:` URL.
  const node = (tag) => ({
    tagName: tag, attrs: {}, querySelectorAll: () => [],
    setAttribute(k, v) { this.attrs[k] = v; }, removeAttribute(k) { delete this.attrs[k]; },
  });
  const style = {
    fill: "rgb(0, 114, 178)", stroke: "none", opacity: "1",
    "font-family": "sans-serif", "text-anchor": "end",
  };
  const styleOf = () => ({ getPropertyValue: (p) => style[p] || "" });
  const circle = node("circle"), text = node("text");
  circle.attrs.class = "dot c1"; text.attrs.class = "bar-caption";
  d.inlinePaint(node("circle"), circle, styleOf);
  d.inlinePaint(node("text"), text, styleOf);
  assert.ok(circle.attrs.style.includes("fill:rgb(0, 114, 178)"));
  assert.ok(!circle.attrs.style.includes("font-family"), "a circle has no type");
  assert.ok(!circle.attrs.style.includes("stroke:none"), "and a default is not written back");
  assert.ok(!circle.attrs.style.includes("opacity:1"));
  assert.ok(text.attrs.style.includes("font-family:sans-serif")
    && text.attrs.style.includes("text-anchor:end"), "text keeps both");
  assert.ok(!("class" in circle.attrs) && !("class" in text.attrs),
    "the class goes: it names rules the exported file will not have");
  // A STROKED MARK KEEPS ITS WIDTH. `stroke-width` is skipped when nothing pushed a `stroke:`
  // before it — right for the four hundred `<text>` nodes that guard was written for, and it would
  // silently flatten every line on the scatter to a hairline if `SVG_PAINT`'s order ever moved.
  // Nothing else in the suite exercises it in the positive direction.
  const line = node("line");
  const paint = { stroke: "rgb(0, 114, 178)", "stroke-width": "3px" };
  d.inlinePaint(node("line"), line, () => ({ getPropertyValue: (p) => paint[p] || "" }));
  for (const want of ["stroke:rgb(0, 114, 178)", "stroke-width:3px"]) {
    assert.ok(line.attrs.style.includes(want), `${want} in ${line.attrs.style}`);
  }
});

test("every chart gets a save button, and nothing that is not a chart does", () => {
  const doc = { createElement: (t) => new El(t) };
  const root = new El("div");
  const chart = (withSvg) => {
    const fig = new El("figure");
    fig.className = "chart";
    const cap = new El("figcaption");
    const title = new El("span");
    title.className = "chart-title";
    title.textContent = "CU against query time";
    cap.appendChild(title);
    fig.appendChild(cap);
    if (withSvg) fig.appendChild(new El("svg"));
    return fig;
  };
  root.appendChild(chart(true));
  root.appendChild(chart(true));
  root.appendChild(chart(false));            // a caption with no plot is not a chart
  const win = { getComputedStyle: () => ({ getPropertyValue: () => "" }), document: doc };
  assert.equal(d.wireCharts(root, doc, win), 2);
  const btns = root.querySelectorAll(".savebtn");
  assert.equal(btns.length, 2);
  assert.equal(btns[0].textContent, "save PNG");
  assert.equal(btns[0].attrs["aria-label"], "save this chart as a PNG image");
  assert.equal(d.wireCharts(null, doc, win), 0, "nothing to wire is not an error");
});

test("the scatter says what was queried, and derives it from the record", () => {
  // A hardcoded 144M is right until the archive grows and then goes stale SILENTLY — the exact
  // failure this repo is built against. It comes off the plotted runs' own stats.
  const rec = lay("duckrun", 4, 24, {
    file: "a.json",
    tables: ["fct_summary", "dim_duid", "dim_calendar", "fct_scada", "stg_csv_archive_log"],
  });
  const stats = rec.layout.stats.duckrun;
  stats.dim_duid = { total_rows: 689, schema: "mart" };
  stats.dim_calendar = { total_rows: 3197, schema: "mart" };
  stats.fct_scada = { total_rows: 370021502, schema: "landing" };
  stats.stg_csv_archive_log = { total_rows: 8167, schema: "landing" };
  const pts = [
    { name: "delta_rs", n: 1, cu: 1500, ms: { cold: 25000, warm: 4500 }, members: [{ rec }] },
    {
      name: "dwh", n: 1, cu: 2000, ms: { cold: 33767, warm: 4330 },
      members: [{ rec: lay("dwh", 78, 90, { file: "b.json" }) }],
    },
  ];
  const note = /<span class="chart-note">([^<]*)<\/span>/.exec(d.scatterFit(pts));
  assert.ok(note, "the chart carries it — it is the one that leaves the page");
  assert.equal(note[1], "queried over 1 fact (144.0M) and 2 dimensions (3.9K)");
  // THE MART, AND NOTHING ELSE. The model carries the landing tables and the suite queries them,
  // but this chart groups and captions on fct_summary alone — every layout on the plot reads the
  // identical landing parquet, so naming it described a difference that is not there.
  assert.ok(!/staging|log/.test(note[1]), note[1]);
  assert.equal(/<span class="chart-note">/.test(d.scatterSvg("T", "S",
    [{ x: 1, y: 1, label: "a" }, { x: 2, y: 2, label: "b" }])), false,
  "and no empty note element when there is nothing to say");
});

test("a dot's hover is its whole table row, sort key included", () => {
  // THE SORT KEY IS THE POINT. The plot encodes four things and the table above prints eight, and
  // `ordering` is the one that separates two dots of the same colour sitting a thousand CU apart —
  // it is on no label (only unique writers get one) and in no legend.
  const rec = sortedBy(1, 9, ["date", "time", "price"], { file: "a.json", mb: 574.6 });
  const pts = [
    { name: "delta_rs", n: 3, cu: 1461.7, ms: { cold: 20845, warm: 4016, hot: 3315 },
      members: [{ rec }] },
    { name: "dwh", n: 1, cu: 2000, ms: { cold: 33767, warm: 4330, hot: 4100 },
      members: [{ rec: lay("dwh", 78, 90, { file: "b.json" }) }] },
  ];
  const tip = /<title>([\s\S]*?)<\/title>/.exec(d.scatterFit(pts))[1].split("\n");
  assert.equal(tip[0], "delta_rs", "the writer leads, as it does in the table");
  assert.ok(tip.includes("ordering: date, time, price"), tip.join(" | "));
  assert.ok(tip.includes("row groups: 9") && tip.includes("row group size: 16.0M"),
    tip.join(" | "));
  assert.ok(tip.includes("size: 575 MB") && tip.includes("CU: 1,462"), tip.join(" | "));
  // EVERY TIER, including `hot`, which is on neither end of the line and so appears nowhere else on
  // the chart — as does the row group size, now that nothing is sized by it.
  assert.ok(tip.includes("cold: 20,845 ms") && tip.includes("warm: 4,016 ms")
    && tip.includes("hot: 3,315 ms"), tip.join(" | "));
  assert.ok(tip.includes("3 runs"), "and the sample size behind the median");
  // Nothing measured is nothing SAID — a dash is a column that has to line up with its neighbours,
  // and nothing in a tooltip lines up with anything. This record filed no encodings, so it simply
  // has no `dictionary` line rather than one reading a dash.
  assert.ok(!tip.some((l) => l.includes("—")), tip.join(" | "));
  assert.ok(!tip.some((l) => l.startsWith("dictionary")), tip.join(" | "));
  // ONE HOVER PER LAYOUT, on the line itself, which is the mark a reader is pointing at.
  assert.equal((d.scatterFit(pts).match(/<title>/g) || []).length, pts.length);
});
