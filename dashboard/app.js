/**
 * The page. Reads the run records and the CU ledger, joins them on the ITEM GUID, renders HTML.
 *
 * **This runs in the browser, against `history/` on `main`, at VIEW time.** That is the whole point
 * of it: `Benchmark` commits a run record, `Capacity units` commits the ledger, and the published
 * page picks both up on the next load. Publishing is what happens when the VISUALISATION changes —
 * `Dashboard` fires on a push to `dashboard/**` and does nothing else. It used to mean "when a number
 * changed", because measuring and deploying were two jobs of one workflow, which made every new
 * measurement cost a Pages deploy.
 *
 * **It must fetch from `raw.githubusercontent.com`, not from the Pages origin.** Serving `history/`
 * out of `site/` would put the data back inside the published artifact and make every commit a
 * republish again. Raw serves the repo's own files with `Access-Control-Allow-Origin: *` and a ~5
 * minute CDN TTL, which is what makes a page hosted on `djouallah.github.io` able to read them at all.
 * The repo is public, so nothing here is a new disclosure — the item GUIDs have always been committed.
 *
 * **Two JSON documents, joined on one key.** `history/runs/<ts>-<run id>.json` is written by the
 * `Benchmark` workflow and names every Fabric item GUID that run created, with its role, plus the
 * layout, the input archive and the raw query timings. `history/cu.json` is the cumulative ledger
 * `measure.py` builds, `{item GUID: {operation: CU}}`. Nothing else passes between them.
 *
 * That join replaces the whole apparatus the old page needed. Attribution used to be substring
 * matching on item DISPLAY NAMES, with a `shared` column for everything ambiguous, a lagging `'Items'`
 * snapshot for kinds, and heuristics — idle-hour gaps, repeated model names — to guess where one run
 * ended and the next began. Now every item bar the landing lakehouse is created and destroyed inside
 * one run, so a GUID belongs to exactly one run and the class comes from the role WE recorded. There
 * is no `shared`, no `engine_of`, no sessionize.
 *
 * Properties worth keeping:
 *
 * - **ONE implementation.** This module both draws the live page and, with a snapshot inlined, the
 *   offline artifact copy — `build.mjs` produces both from this file. There is deliberately no second
 *   renderer: `dashboard.py` and `report_html.py` were deleted rather than kept alongside, because two
 *   implementations of this join is exactly the drift the rest of the repo is built to avoid.
 * - **No build step for data, and no third-party package anywhere.** Plain ES modules and `fetch`.
 *   DuckDB-WASM was considered and rejected: ~30 MB of wasm to query 300 KB of JSON that already
 *   arrives in the shape the page wants.
 * - **It renders what the records CONTAIN.** One engine, two, a dispatch that skipped the benchmark
 *   and so has no analytics CU: the columns come from the records, never from a configured list. An
 *   engine nothing ever measured has no zero to print.
 * - **The page is composed from EVERY record** — each engine's latest run, once per config. One
 *   dispatch builds one engine, so rendering the newest record alone would give a comparison page with
 *   one column. `?record=` pins one run when reproducing an old page.
 *
 * The render layer produces STRINGS, never DOM nodes, and touches no global at import time. That is
 * what lets `app.test.mjs` run the whole page under `node --test` with no browser and no jsdom.
 */

// ------------------------------------------------------------------------------------ what to read

export const DEFAULTS = {
  repo: "djouallah/fabric-dbt-benchmark",
  ref: "main",
  // Which table the layout grouping and the mart block are ABOUT.
  table: "fct_summary",
  // Render ONE run alone. A substring of the filename, so a run id or a date both work.
  record: "",
};

export const SERVER = "https://github.com";

// Engine order wherever one is needed. Not a filter — an engine outside this list still renders, it
// just sorts to the end.
export const ENGINES = ["duckrun", "iceberg", "spark", "dwh"];

// What each engine IS. One thing renders from this now: the adapters note under the charts, which
// is the page's only pointer to what actually did the writing since the ETL captions stopped
// restating the adapter (`spark·writeHeavy` under `dbt-fabricspark` was one fact twice) and the layout
// table's `writer` column became the row label. The entries match stats.py's WRITER map exactly,
// which is what `ENGINE_LABEL` is derived from.
export const STACK = {
  landing: ["download_aemo.py", "the shared AEMO archive every leg reads", "—"],
  duckrun: ["dbt-duckrun", "DuckDB → delta-rs", "delta-rs"],
  iceberg: ["dbt-duckdb", "DuckDB → Iceberg REST catalog", "duckdb (iceberg)"],
  spark: ["dbt-fabricspark", "Fabric Spark (Livy) → Delta", "spark"],
  dwh: ["dbt-fabric-samdebruyn", "Fabric Warehouse (T-SQL)", "warehouse"],
};

// Where each adapter lives, keyed like STACK. duckrun's adapter ships inside the duckrun package;
// dwh is Sam Debruyn's fork of dbt-fabric (the PyPI package the build installs), not Microsoft's —
// the URL is the fork's, verified against the package's own PyPI metadata.
export const ADAPTER_URLS = {
  duckrun: "https://github.com/djouallah/duckrun",
  iceberg: "https://github.com/duckdb/dbt-duckdb",
  spark: "https://github.com/microsoft/dbt-fabricspark",
  dwh: "https://github.com/sdebruyn/dbt-fabric",
};

// A column is an engine (`spark`) or an engine under one CONFIG (`spark·readHeavyForPBI+NEE`), which is what
// puts the same engine's two resource profiles side by side. A tag joins its own parts with `+`, never
// with this, so the split back to the engine is unambiguous.
export const COL_SEP = "·";

// An engine named by WHO WRITES, where the TARGET name misleads. `iceberg` reads as a format beside
// three engines, when the writer is the same DuckDB that duckrun uses — pointed at an Iceberg REST
// catalog instead of delta-rs. On a page whose subject is what got written, that distinction is the
// entire reason the pair exists, and calling it `iceberg` hides it. Matches `STACK`'s writer column.
// It names the COLUMN as well as the layout row, so the page calls it one thing throughout;
// `baseEngine` reverses it, which is why every lookup downstream still resolves to `iceberg`.
export const ENGINE_LABEL = { iceberg: "duckdb iceberg" };
const ENGINE_OF_LABEL = Object.fromEntries(
  Object.entries(ENGINE_LABEL).map(([k, v]) => [v, k]));

// The COMPUTE behind a target, where two targets share one. `iceberg` is the same DuckDB as
// `duckrun` pointed at an Iceberg REST catalog instead of delta-rs — a table format, not a fourth
// engine — so the lede folds the pair into one engine and calls the columns what they are: targets.
export const ENGINE_FAMILY = { duckrun: "duckdb", iceberg: "duckdb" };

// Role -> which half of the page an item's CU belongs to. Everything that is not a semantic model is
// work done to BUILD the tables; a semantic model is only ever queried. This replaces classification
// by Fabric item kind, read out of a snapshot that had usually not catalogued a minutes-old item.
export const ANALYTICS_ROLES = new Set(["semantic_model"]);

// OPERATION -> bucket. `OneLake …` is storage; everything else is compute. Measured against the live
// model 2026-08-02, and it is the only split that works, because compute and storage share an ITEM:
//
//   dbt_spark  [Lakehouse]  High Concurrency Session Livy Run  188,636   <- compute
//                           OneLake Write via Redirect          20,268   <- storage
//   dbt_dwh    [Warehouse]  Warehouse Query                    129,177   <- compute
//                           OneLake Write via Redirect           1,640   <- storage
//
// Bucketing by the item's ROLE was wrong for exactly that reason and this replaces it. Checked
// against every operation name on the capacity: the `OneLake` prefix separates them cleanly.
export const STORAGE_PREFIX = "OneLake";

// Skipped entirely — not a column, not a row, not a footnote. This page compares ENGINES. The landing
// lakehouse is the ingestion staging area that no run deletes and every run reads, so its CU is one
// cumulative figure belonging to no engine; a workspace `folder` never accrues a capacity unit at all.
// The archive's SIZE is still reported (renderInput) — that is the input volume, which is a different
// question from what ingesting it cost.
export const NON_ENGINE_ROLES = new Set(["landing", "folder"]);

// Roles the teardown must have deleted. If one is still alive, that run's items are STILL ACCRUING and
// its numbers are not a measurement of that run — they are a measurement of everything since.
export const DELETABLE_ROLES = new Set(["output", "dwh_src", "compute", "semantic_model"]);

// THE RESOURCE PROFILE IS PRINTED BY ITS OWN NAME. There was a `PROFILE_LABEL` map that renamed the
// two in use by their EFFECT on the parquet — `readHeavyForPBI` → `V-Order`, `writeHeavy` → `default`
// — and it is gone, in both directions. `readHeavyForPBI` and `writeHeavy` are the strings the
// dispatch input takes, the strings `profiles.yml` sets and the strings Microsoft's own reference
// publishes, so a reader matching this page against a run's inputs had to translate, and the page and
// the record disagreed about the name of the same setting. `default` was the worse half: it named the
// workspace's CHOICE rather than the profile, so it would silently become a lie the day the workspace
// default changed, and it hid which profile a bare dispatch actually got.
// The effect is still said — where it is MEASURED rather than declared. `layoutLabel` reads
// `vorder` off the parquet, so a bar reads `spark readHeavyForPBI` over `V-Order ·
// 10–11 RG`: the label names the knob that was turned, the caption states what came out. That split
// also survives a profile whose name misleads, which is not hypothetical — `readHeavyForSpark` reads
// like it enables V-Order and sets no vorder at all.

// How a LAYOUT_CONFIG entry is NAMED on a layout caption, keyed `<key>=<value>` rather than by the
// value alone — a bare `true` does not say which knob it is, and would label every boolean config in
// LAYOUT_CONFIG the same way the moment a second one joins. It is the ONLY relabelling left: the
// resource profile is now printed verbatim (see above), and this exists because `sorted=true` has no
// name of its own to print.
// The LABEL still does not spell the sort key out — the caption does (`sortKeyOf`), which is where
// the shape of what was written already lives.
export const CONFIG_LABEL = { "sorted=true": "sorted" };

// The dispatch config that is SHOWN to change what gets written. `vcores` and
// `native_execution_engine` are excluded, and that is measured rather than assumed: duckrun at 64 and
// at 32 cores wrote 4 files and 27 row groups either way, and spark under `readHeavyForPBI` wrote the
// same layout with NEE on and off. Neither reaches the parquet, so neither belongs on a caption about
// parquet — `duckrun·64c, duckrun·32c` names one layout twice and puts a knob in front of the reader
// that demonstrably had nothing to do with it. `resource_profile` stays because it plainly does:
// `readHeavyForPBI` writes V-Order at ~10 files, `writeHeavy` writes neither. `sorted` stays for
// the same reason and is the more direct case — it is nothing BUT a physical ordering of the rows,
// so it reaches the parquet by definition. Note it does so without moving the numbers this page
// measures: the one sorted run wrote 4 files either way, changing size (985 -> 777 MB) and row
// groups (27 -> 25) but not the BANDS those fall in. That is exactly why it has to be carried as
// config — see `layoutKey`.
export const LAYOUT_CONFIG = ["resource_profile", "sorted"];

// Pass POSITION, which is what cold/warm/hot mean here — the first visit to a freshly deployed
// semantic model, the second, then the median of the rest. NOT the record's own `tier` field, which is
// the query CATEGORY (`probe`/`composite`/`raw`/`hot_only`) and names four different things.
export const TIERS = [["cold", "cold_ms"], ["warm", "warm_ms"], ["hot", "hot_median_ms"]];

// ------------------------------------------------------------------------------------- primitives

export function bucket(op) {
  return String(op).startsWith(STORAGE_PREFIX) ? "storage" : "compute";
}

const items_ = (rec) => (rec && rec.items) || {};
const role_ = (it) => (it && it.role) || "";

/**
 * Every GUID in this record that is really the LANDING lakehouse, including its SQL endpoint.
 *
 * `NON_ENGINE_ROLES` filters on the role, and the landing lakehouse's paired SQL analytics endpoint
 * does not carry it: Fabric makes that endpoint a separate billable `Warehouse` item with its own
 * GUID, and `provision.py` records it under the role `sql_endpoint`. So landing CU reached the page
 * through the one door the role check does not cover — the SAME item, `A8CF6202-…`, in every run
 * record, charging every engine 130.4 CU it did not spend.
 *
 * It is caught by NAME, matched against the record's own `landing` items, so nothing is hardcoded and
 * an engine's OWN endpoint — which is genuinely that engine's work — is untouched.
 *
 * Worth knowing what it distorted, because it is not only a total. That endpoint bills 130.4 CU over
 * 83.2 s, a rate of 1.6, against a 64-vCore notebook's 32.0. Blending the two dragged `compute CU per
 * second` to 28.5 for duckrun and 26.4 for iceberg — the same DuckDB in the same notebook, reading
 * differently — and the size of the gap tracked nothing but how much the rest of the class weighed.
 */
export function landingGuids(rec) {
  const names = new Set(Object.values(items_(rec))
    .filter((it) => role_(it) === "landing" && it.name).map((it) => it.name));
  return new Set(Object.entries(items_(rec))
    .filter(([, it]) => role_(it) === "sql_endpoint" && names.has(it.name))
    .map(([g]) => g));
}

/**
 * `spark·readHeavyForPBI+NEE` → `spark`; `spark` → `spark`; `duckdb iceberg·64c` → `iceberg`.
 *
 * The label reversal is what lets a column be NAMED for its writer while every lookup keyed on the
 * engine — `STACK`, the (engine, variant) join to a record — still finds it.
 */
export function baseEngine(col) {
  const head = String(col).split(COL_SEP)[0].trim();
  return ENGINE_OF_LABEL[head] || head;
}

/**
 * Where a run's identifier links to: its COMMITTED record in `history/runs/`, never the Actions
 * run. CI runs expire — logs at 90 days, the run page eventually with them — while the record is
 * the permanent copy of everything this page renders from it, so a link that outlives the page's
 * own data source is the only honest one. `runUrl` (the `/actions/runs/` form) was deleted for
 * exactly that reason; do not bring it back.
 */
export function recordUrl(repo, file, ref = DEFAULTS.ref) {
  return `${SERVER}/${repo}/blob/${encodeURIComponent(ref)}/history/runs/${file}`;
}

/** `owner/name` → the project-pages URL the live copy is published at. Derived rather than
 *  hardcoded, so a fork's offline artifact links to the fork's own page. */
export function pagesUrl(repo) {
  const [owner, name] = String(repo).split("/");
  return `https://${owner}.github.io/${name}/`;
}

// ---------------------------------------------------------------------------- loading and validity

/**
 * Why this run cannot go on the page, or `null` if it can.
 *
 * The page compares generations, so a run has to be a WHOLE generation: built, benchmarked, and torn
 * down. A partial one is not a smaller answer, it is a misleading one —
 *
 * - **no benchmark** means an empty analytics column, which reads as "querying this engine was free"
 *   rather than "nobody measured it". Run 30743411308 is exactly that: the `bench` job was skipped by
 *   a `needs` bug and only the ETL half exists.
 * - **no layout** means the build half never reported.
 *
 * A run that was never TORN DOWN is not rejected — see `drifting()`. Its numbers do keep creeping, but
 * the creep is small and a missing column costs more than a caveated one; the page says so instead of
 * hiding the run.
 *
 * Non-compliant records are skipped and NAMED, never silently dropped — and `measure.py` still reads
 * them, because their items really did cost capacity and the ledger is the ledger.
 */
export function incomplete(rec) {
  if (!rec || !rec.engine) return "no engine recorded";
  const run = rec.run || {};
  if (!(run.started && run.finished)) return "no start/finish stamp";
  if (!Object.values(items_(rec)).some((it) => role_(it) === "output")) return "no output item";
  const stats = ((rec.layout || {}).stats || {})[rec.engine];
  if (!stats || !Object.keys(stats).length) {
    return "no layout recorded — the build half did not report";
  }
  const timings = ((rec.benchmark || {}).timings) || {};
  if (!Object.keys(timings).length) {
    return "no benchmark timings — the query half did not run";
  }
  return null;
}

/**
 * Items this run created and never deleted — so its CU has no upper bound.
 *
 * A run whose teardown ran has a FINAL cost: every item is gone, nothing can be charged to it again.
 * One whose teardown did not (run 30733912205 predates the job) leaves its lakehouse and semantic
 * model alive, and Fabric keeps billing them — background OneLake reads against an idle lakehouse, a
 * Direct Lake model that gets refreshed. Its number is therefore "that run, plus whatever those items
 * have done since", and it grows every time the ledger is topped up.
 *
 * Reported rather than rejected. The drift is small in practice and a column that disappears is worse
 * than one carrying a caveat — but the caveat has to be there, because "settled" and "still climbing"
 * are different claims and only one of them is comparable to a torn-down run.
 */
export function drifting(rec) {
  return Object.entries(items_(rec))
    .filter(([, it]) => DELETABLE_ROLES.has(role_(it)) && !it.deleted)
    .map(([g, it]) => `${it.role}/${it.name || g}`)
    .sort();
}

/**
 * Every readable record that is a whole generation, oldest first, plus what was skipped and why.
 * Skipped records are NAMED — a page that quietly ignores one is indistinguishable from a page that
 * never had it.
 */
export function selectRuns(records) {
  const runs = [], skipped = [];
  for (const rec of records || []) {
    if (!rec) continue;
    const why = incomplete(rec);
    if (why) { skipped.push(`${rec._file || "?"}: ${why}`); continue; }
    runs.push(rec);
  }
  runs.sort((a, b) => {
    const ka = ((a.run || {}).started || "") + "\u0000" + (a._file || "");
    const kb = ((b.run || {}).started || "") + "\u0000" + (b._file || "");
    return ka < kb ? -1 : ka > kb ? 1 : 0;
  });
  return { runs, skipped };
}

export function normaliseLedger(doc) {
  const d = doc && typeof doc === "object" ? doc : {};
  return {
    items: d.items || {},
    // Absent on every ledger written before `measure.py` read duration, and absent again on any read
    // where the model had no duration column. Empty is the honest state for both, and the rate row
    // renders NOTHING rather than a table of zeros.
    seconds: d.seconds || {},
    reads: d.reads || [],
    updated: d.updated || "",
  };
}

/**
 * `{operation: CU}` — or `{operation: seconds}` — for one Fabric item. `null` when the ledger has
 * never seen it.
 *
 * `null` and `{}` are different claims — "not measured yet" against "cost nothing" — and the sources
 * table has to be able to say which.
 */
export function itemCu(ledger, guid, key = "items") {
  const v = (ledger[key] || {})[guid];
  if (v === undefined || v === null) return null;
  // An older ledger stored one NUMBER per item, before the operation was needed to split compute from
  // storage. It cannot be bucketed, so it is reported as unsplit rather than guessed into the wrong
  // half; `measure.py` drops such entries on its next read and they come back in full.
  return typeof v === "object" ? { ...v } : { "(operation not recorded)": Number(v) };
}

// -------------------------------------------------------------------------------------- the join

/**
 * `{cells, unmeasured}` for one run. `key="seconds"` gives the same breakdown in billed SECONDS, off
 * the ledger's sibling dict — same GUIDs, same roles, same compute/storage split, because it is the
 * same read.
 *
 * THE join, and it is a dictionary lookup: every GUID the run recorded, looked up in the ledger, filed
 * under the class its ROLE implies. No allocation and no heuristic, because the teardown means a GUID
 * belongs to exactly one run.
 *
 * **`landing` and `folder` are skipped entirely, not reported apart.** The page compares ENGINES.
 * `dbt_landing` is the ingestion staging area — no run deletes it, every run reads it, so its CU is
 * one cumulative figure that belongs to no engine and answers no question this page asks. It was
 * briefly given a row of its own; the same number repeated under every column read as "each of them
 * spent this", which is the opposite of what it meant. The archive's SIZE is still reported (see
 * `renderInput`) — that is the input volume, not the cost of ingesting it.
 */
export function runCu(rec, ledger, key = "items") {
  const cells = {}, unmeasured = [];
  const skip = landingGuids(rec);
  for (const [guid, item] of Object.entries(items_(rec))) {
    const role = role_(item) || "?";
    if (NON_ENGINE_ROLES.has(role) || skip.has(guid)) continue;
    const value = itemCu(ledger, guid, key);
    if (value === null) { unmeasured.push(`${role}/${item.name || guid}`); continue; }
    const cls = ANALYTICS_ROLES.has(role) ? "analytics" : "etl";
    for (const [op, cu] of Object.entries(value)) {
      const label = bucket(op);
      cells[cls] = cells[cls] || {};
      cells[cls][label] = (cells[cls][label] || 0) + Number(cu);
    }
  }
  return { cells, unmeasured };
}

export function classTotal(cells, cls) {
  return Object.values((cells || {})[cls] || {}).reduce((a, b) => a + b, 0);
}

/**
 * True when this run finished recently enough that its CU can still rise.
 *
 * DERIVED, never stored. An hour's CU keeps growing for ~70 minutes after the fact, so a number read
 * minutes after a run is a lower bound — but that is a property of the clock, not a fact worth writing
 * into a file and keeping in step.
 */
export function stillAccruing(rec, hours = 2.0, now = null) {
  const stamp = ((rec || {}).run || {}).finished;
  if (!stamp) return false;
  const t = Date.parse(String(stamp));
  if (Number.isNaN(t)) return false;
  return ((now === null ? Date.now() : now) - t) / 1000 < hours * 3600;
}

/**
 * The config signature this run ran under, as sorted `[key, value]` pairs. `[]` when it recorded none
 * — which is dwh always, since Fabric Warehouse exposes no per-run knob.
 */
export function variant(rec) {
  const cfg = ((rec || {}).layout || {}).config || {};
  const c = cfg[(rec || {}).engine] || {};
  return Object.entries(c)
    .filter(([, v]) => v !== null && v !== undefined)
    .map(([k, v]) => [k, String(v)])
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
}

export const variantKey = (sig) => JSON.stringify(sig);

/**
 * The short label separating one config from another in a column header. Compact on purpose: it sits
 * in a table head — the column is repeated across every table and both charts — and the full reading
 * is in the layout section and the chart captions.
 *
 * What keeps it short is that a flag which is OFF is simply absent, so `spark·readHeavyForPBI+NEE`
 * contrasts with `spark·readHeavyForPBI` rather than with `spark·readHeavyForPBI+noNEE`.
 * Absence-means-off is only unambiguous while every column of that engine RECORDS the flag —
 * `columnsFor` checks that and falls back to `terse=false` for the whole engine if two configs would
 * collide.
 *
 * The RESOURCE PROFILE is printed verbatim. It was shortened to its effect — `V-Order` / `default` —
 * and that is reverted: it is the string the dispatch takes and `profiles.yml` sets, so the header
 * now matches the record it came from.
 */
export function variantTag(sig, terse = true) {
  const d = Object.fromEntries(sig);
  const bits = [];
  if (d.vcores) bits.push(`${d.vcores}c`);
  if (d.resource_profile) bits.push(String(d.resource_profile));
  const nee = d.native_execution_engine;
  if (nee !== undefined) {
    if (String(nee).toLowerCase() === "true") bits.push("NEE");
    else if (!terse) bits.push("noNEE");
  }
  // No `terse` branch and no `unsorted` spelling, unlike NEE above: `stats.py` records this ONLY
  // when it is on, because off and never-offered produce the same parquet. So absence here is one
  // state rather than two that merely look alike, and there is nothing for the terse fallback to
  // disambiguate.
  if (d.sorted) bits.push("sorted");
  // `+`, never COL_SEP — baseEngine splits on that, and a tag containing one would make
  // `spark·readHeavyForPBI+NEE` unparseable back to `spark`.
  return bits.join("+") || "unrecorded";
}

// -------------------------------------------------------------- what a layout IS, and whose it is
//
// Power BI never sees the engine. It opens parquet through Direct Lake and transcodes row groups, so
// what a query costs is a property of the LAYOUT and the writer that produced it is metadata. That is
// why the analytics chart groups by what was written while the ETL chart — where the writer and the
// compute it was given are the entire subject — does not.

/**
 * `13,089,178` → `13.1M`. Row-group sizes span four orders of magnitude across these engines — 123K
 * against 13.1M — and that ratio is the finding; twelve digits of it is not.
 */
export function compact(n) {
  const v = Number(n || 0);
  if (!Number.isFinite(v)) return "—";
  for (const [cut, suffix] of [[1e9, "B"], [1e6, "M"], [1e3, "K"]]) {
    if (Math.abs(v) >= cut) return fmt(v / cut, 1) + suffix;
  }
  return fmt(v, 0);
}

/**
 * The power-of-two band a count falls in. `-1` for missing or zero.
 *
 * Banded, not exact. Exact equality splits dwh's own two runs from each other — 78 files and 80, same
 * writer, same settings, incremental drift — and splits duckrun on 1.1 MB of size. The accepted cost
 * is the boundary: 15 row groups and 17 land in different bands despite being close. That edge is
 * visible in the mart block, and no tolerance rule avoids it without chaining groups together through
 * their neighbours.
 */
export function layoutBand(n) {
  const v = Number(n || 0);
  if (!Number.isFinite(v) || v < 1) return -1;
  return Math.floor(Math.log2(v));
}

const martStats = (rec, table) =>
  ((((rec || {}).layout || {}).stats || {})[(rec || {}).engine] || {})[table] || {};

/** The mart's row count for one run, or `null` when the run did not record one. */
export function martRows(rec, table = DEFAULTS.table) {
  const v = martStats(rec, table).total_rows;
  return v === undefined || v === null ? null : Math.trunc(Number(v));
}

/**
 * ONE SOURCE GENERATION — the newest run defines it, and every run that disagrees is dropped.
 *
 * The page's columns are different dispatches, days apart, and NOTHING made them comparable. If the
 * AEMO archive changes, an engine that has not been rebuilt since keeps its column, and its numbers
 * sit beside engines built from different data — in the same table, and inside both charts' means.
 * The reference is the mart's `total_rows` from the LATEST record, because the source may
 * legitimately change and when it does the newest run is right: everything built before it is a
 * different experiment, not a slower one.
 *
 * **Newest wins, NOT the most common value**, and that is the whole point rather than a shortcut.
 * Right after a genuine source change the old count is still the majority, which is precisely the
 * case this exists to handle — a mode would keep the stale generation and drop the new run.
 *
 * The failure mode that buys: **if the newest run is itself the anomaly, it excludes all the good
 * history.** Inherent to newest-wins — a bad run and a real source change look identical from here —
 * and survivable only because it is LOUD (`renderSources` names every excluded run and its count, so
 * "10 of 11 excluded" is unmistakable) and because the next good run reverses it.
 *
 * Two things it deliberately does not do. A run with NO recorded count is **kept**: unmeasured is a
 * different claim from different, the same distinction `layoutKey` makes by keying `null` to a bar of
 * its own. And with no reference anywhere it filters **nothing** — a record set where nobody recorded
 * `total_rows` must render whole rather than vanish.
 */
export function sameGeneration(runs, table = DEFAULTS.table) {
  let reference = null;
  // `runs` arrives oldest-first from `selectRuns`, so the last one carrying a count is the newest.
  for (let i = runs.length - 1; i >= 0 && reference === null; i--) {
    reference = martRows(runs[i], table);
  }
  if (reference === null) return { runs, dropped: [], reference: null };
  const kept = [], dropped = [];
  for (const rec of runs) {
    const rows = martRows(rec, table);
    if (rows === null || rows === reference) kept.push(rec);
    else dropped.push({ file: rec._file || "?", engine: rec.engine || "?", rows,
      run: (rec.run || {}).id || null });
  }
  return { runs: kept, dropped, reference };
}

/**
 * What Power BI can actually tell apart: `[V-Order, files band, row-groups band, sorted]` for the
 * mart.
 *
 * `avg RG rows` is `total_rows ÷ row groups` and every engine writes the same 143,980,961 rows, so it
 * carries nothing the row-group count does not. `size MB` is excluded deliberately — see `layoutBand`.
 *
 * `null` when neither metric was recorded, which keeps that column a bar of its OWN rather than filing
 * it into a group it was never measured into. That distinction is the whole point: two records
 * carrying no file count are not two identical layouts, they are two unmeasured ones, and merging them
 * would claim Power BI cannot tell apart two things nobody looked at.
 *
 * THE LAST ELEMENT IS DECLARED, NOT MEASURED, and it is the one exception to that rule on this page.
 * A sort is the same class of thing as V-Order — a write-time reordering that changes what Power BI
 * transcodes — and V-Order is already element `[0]`, so it belongs in the same key. The difference is
 * where each comes from: `vorder` is read off the Delta table property by `stats.py`, while nothing in
 * `stats.py` measures sort ORDER, so the config the run recorded is the only witness that the parquet
 * differs. Leaving it out is not neutral — a sorted and an unsorted duckrun run wrote 4 files and the
 * same bands either way, so they would share one bar and have their cold/warm/hot means averaged
 * together, which is precisely the comparison the flag exists to make. It carries the resolved
 * COLUMN LIST, not a boolean, so two sorts on different keys can never share a bar either — the
 * `['date','time','DUID']` and `['date','time']` runs land in separate bars today only because their
 * file bands happen to differ, and that is luck, not a rule.
 *
 * A measured version is possible later and would be better: per-file `date` min/max from the Delta log
 * says whether files cover disjoint date ranges, which is what a date sort actually produces.
 */
export function layoutKey(rec, table = DEFAULTS.table) {
  const d = martStats(rec, table);
  if (d.num_files === undefined && d.num_row_groups === undefined) return null;
  if (d.num_files === null && d.num_row_groups === null) return null;
  return [Boolean(d.vorder), layoutBand(d.num_files), layoutBand(d.num_row_groups),
    sortKeyOf(rec, table)];
}

// The declared sort key of the one sorted model in the project: `models/duckdb/marts/fct_summary.sql`
// sets `sort_by=['date','time']` behind DUCKDB_SORTED, and the run record carries only the boolean —
// so this constant is the fallback witness for WHICH columns a sorted run ordered by. Change the
// model's key and this must move with it.
export const DECLARED_SORT_KEY = ["date", "time"];

/**
 * The sort key one run wrote under, as a `"date,time"` string, or `false` when it wrote unsorted.
 *
 * `false` — not `null` — for a record with no `sorted` config at all: every run before the input
 * existed demonstrably wrote unsorted parquet, so absence here is not "unmeasured" (unlike the file
 * counts in `layoutKey`) and must key identically to an explicit unsorted run.
 *
 * A `sort_by='auto'`-era record carries the picker's own answer under
 * `dbt.<engine>.sort_by_auto.<table>`, and that measured value wins; otherwise the declared constant
 * stands in, because the record's boolean is all the recent runs wrote down.
 */
export function sortKeyOf(rec, table = DEFAULTS.table) {
  const engine = (rec || {}).engine || "?";
  const cfg = (((rec || {}).layout || {}).config || {})[engine] || {};
  if (!cfg.sorted) return false;
  const auto = ((((rec || {}).dbt || {})[engine] || {}).sort_by_auto || {})[table];
  return (Array.isArray(auto) && auto.length ? auto : DECLARED_SORT_KEY).join(",");
}

/**
 * `[[key, [entry]]]` — the entries that wrote parquet Power BI cannot distinguish.
 *
 * **ONE ENTRY PER RUN, not per column, and that is the whole point.** A column is `(engine, config)`
 * and a run is one dispatch, so two runs of ONE column can write different parquet — which is not
 * hypothetical: `duckrun·64c+sorted` wrote 3 files / 26 row groups under an explicit
 * `sort_by=['date','time','DUID']` and 4 files / 25 under the `sort_by='auto'` the picker resolved to
 * `['date','time']`. Grouping the COLUMNS and then averaging every run of each is what put those two
 * in one bar, valued at their mean (2,041.8) and captioned with only the newer one's shape. The layout
 * is measured PER RUN, so it has to be grouped per run; two runs that wrote different shapes are two
 * bars sharing a label, which the caption then explains.
 *
 * Entries are passed through untouched, so a caller can hang the run's CU and its query timings on
 * them and read them back off the members.
 *
 * Insertion-ordered, so the caller's order survives into the grouping; the chart re-sorts by value
 * anyway.
 *
 * An entry with no `layoutKey` falls back to its COLUMN, which is as much as is known about it, and to
 * a singleton when it has no column either. Two unmeasured records are still never merged with each
 * other — that rule is about two different columns, and it holds — but a column's own runs are not
 * split into a bar each just because none of them recorded a file count, which would print the same
 * label several times with no caption able to say why.
 */
export function layoutGroups(entries, table = DEFAULTS.table) {
  const out = [], seen = new Map();
  for (const entry of entries) {
    const key = layoutKey(entry.rec, table);
    const id = key !== null ? JSON.stringify(key)
      : entry.col === undefined || entry.col === null ? null : `col:${entry.col}`;
    const at = id === null ? undefined : seen.get(id);
    if (at === undefined) {
      if (id !== null) seen.set(id, out.length);
      out.push([key, [entry]]);
    } else {
      out[at][1].push(entry);
    }
  }
  return out;
}

/**
 * The bar label: the layout itself, short enough for the chart's 224px label gutter.
 *
 * `V-Order · 11 RG`, `19–27 RG`, `by date, time · 9 RG`. Row groups only — segments are what drive
 * Direct Lake's transcode and scan cost, and the file count was a second number saying less; the
 * file BAND still separates bars (`layoutKey`), it just is not printed. A metric that differs across
 * the group's members prints as a range, which is what a band means in practice. A sorted bar names
 * its sort columns, because "sorted" alone does not say what Power BI is reading in order.
 */
export function layoutLabel(members, table = DEFAULTS.table) {
  const stats = members.map(({ rec }) => martStats(rec, table));
  const rng = (field) => {
    const vals = [...new Set(stats
      .filter((s) => s[field] !== undefined && s[field] !== null)
      .map((s) => Math.trunc(Number(s[field]))))].sort((a, b) => a - b);
    if (!vals.length) return null;
    return vals.length === 1 ? fmt(vals[0], 0)
      : `${fmt(vals[0], 0)}–${fmt(vals[vals.length - 1], 0)}`;
  };
  const bits = [];
  if (stats.some((s) => s.vorder)) bits.push("V-Order");
  // One value per bar when grouping came through `layoutKey` (the sort key is IN the key); the
  // dedup only matters for the unmeasured-column fallback, where members are grouped by column.
  const sorts = [...new Set(members.map(({ rec }) => sortKeyOf(rec, table)).filter(Boolean))];
  if (sorts.length) bits.push(`by ${sorts.join(" / ").split(",").join(", ")}`);
  for (const [field, unit] of [["num_row_groups", "RG"]]) {
    const v = rng(field);
    if (v) bits.push(`${v} ${unit}`);
  }
  // Nothing measured, so there is no layout to name and the writer is all there is to say. Falls back
  // rather than printing "not recorded" on several bars at once, which would look like one repeated
  // group when it is several unmeasured ones.
  return bits.join(" · ") || producers(members);
}

/**
 * Who wrote it, named by the config that reached the parquet and nothing else.
 *
 * `duckrun`, `spark readHeavyForPBI`, `spark writeHeavy`. No core count and no NEE flag — see
 * `LAYOUT_CONFIG` — because neither reaches the parquet: `spark·readHeavyForPBI+NEE` is three facts
 * and only the profile is one of them. The profile itself is printed VERBATIM; what it did to the
 * parquet is the caption's job, and `layoutCaption` measures that rather than inferring it.
 *
 * This is what the analytics chart and the layout blocks carry instead of the column id. `variantTag`
 * is untouched and keeps naming columns everywhere the ENGINE is the subject — the ETL chart, the CU
 * table, the sources table.
 */
export function producer(rec) {
  const engine = (rec || {}).engine || "?";
  const c = (((rec || {}).layout || {}).config || {})[engine] || {};
  const bits = [ENGINE_LABEL[engine] || engine];
  for (const k of LAYOUT_CONFIG) {
    if (!c[k]) continue;
    const v = String(c[k]);
    bits.push(CONFIG_LABEL[`${k}=${v}`] || v);
  }
  return bits.join(" ");
}

/**
 * The group's writers, DEDUPLICATED — two members reducing to the same name appear once.
 *
 * So duckrun at two core counts reads `duckrun`, and a group holding genuinely different writers keeps
 * both (`duckrun, spark writeHeavy`), which is the case worth reading: two engines that produced parquet
 * Power BI cannot tell apart.
 */
export function producers(members) {
  const out = [];
  for (const { rec } of members) {
    const name = producer(rec);
    if (!out.includes(name)) out.push(name);
  }
  return out.join(", ");
}

/**
 * `[{col, engine, rec}]` — each engine's LATEST run, once per configuration.
 *
 * This is what the page is for. One dispatch builds ONE engine, so rendering the newest record alone
 * gives a comparison page with a single column. The key is (engine, config) rather than engine,
 * because spark under `readHeavyForPBI` answers a different question from spark under `writeHeavy` and
 * one number cannot stand for both; and an engine nobody has rebuilt keeps showing its last real
 * measurement instead of vanishing.
 *
 * The cost is that columns are different dispatches, days apart — which `renderSources` states per
 * column rather than smoothing over.
 */
export function columnsFor(runs) {
  const latest = new Map();                 // oldest first, so later runs win their key
  for (const rec of runs) {
    if (!rec.engine) continue;
    latest.set(JSON.stringify([rec.engine, variant(rec)]), rec);
  }
  const sigs = new Map();
  for (const rec of latest.values()) {
    const list = sigs.get(rec.engine) || [];
    list.push(variant(rec));
    sigs.set(rec.engine, list);
  }
  // `variantTag` drops a flag that is OFF, which is only unambiguous while every config of that engine
  // records it. Where two configs would collapse to one header, spell the whole engine out rather than
  // print the same column name twice — a duplicate header is unreadable and silent.
  const terse = new Map();
  for (const [e, ss] of sigs) {
    terse.set(e, new Set(ss.map((s) => variantTag(s))).size === ss.length);
  }
  const cols = [];
  for (const rec of latest.values()) {
    const e = rec.engine;
    const tag = variantTag(variant(rec), terse.get(e) !== false);
    // The column is NAMED for its writer (`duckdb iceberg`) and keyed on its engine; `baseEngine`
    // reverses the label, so `STACK` and the (engine, variant) join both still resolve.
    const name = ENGINE_LABEL[e] || e;
    const col = sigs.get(e).length < 2 ? name : `${name}${COL_SEP}${tag}`;
    cols.push({ col, engine: e, rec });
  }
  const order = new Map(ENGINES.map((e, i) => [e, i]));
  cols.sort((a, b) => {
    const oa = order.has(a.engine) ? order.get(a.engine) : order.size;
    const ob = order.has(b.engine) ? order.get(b.engine) : order.size;
    if (oa !== ob) return oa - ob;
    if (a.engine !== b.engine) return a.engine < b.engine ? -1 : 1;
    return a.col < b.col ? -1 : a.col > b.col ? 1 : 0;
  });
  return cols;
}

// ------------------------------------------------------------------------------- render primitives

export function esc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/**
 * `**bold**`, `` `code` ``, `[text](url)`, `<br>`, `<sub>`, and nothing else. Escaped first, so a
 * stray `<` in an item name cannot inject markup.
 *
 * The two tags that survive are matched as EXACT tokens with no attribute position, so a display
 * name containing a literal `<sub>` becomes a harmless empty tag rather than an injection point.
 *
 * `<sub>` is repurposed: it marks a dim annotation, not a subscript, and the stylesheet aligns it to
 * the baseline for that reason. It is how a caveat rides ALONGSIDE the number it qualifies —
 * `compute seconds` needs "billed, not wall clock" attached to it, and a note four rows below is not
 * attached to anything.
 *
 * Links are restricted to `http(s)://` — the page only ever emits GitHub URLs, and a scheme allowlist
 * is what keeps that true even if an item NAME ever reaches this function looking like markdown. A
 * non-matching link is left as literal text rather than dropped.
 */
export function inline(text) {
  let out = esc(text);
  out = out.replace(/&lt;br&gt;/g, "<br>");
  out = out.replace(/&lt;(\/?)sub&gt;/g, "<$1sub>");
  out = out.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g,
    (_m, label, url) => `<a href="${url.replace(/"/g, "&quot;")}">${label}</a>`);
  out = out.replace(/\*\*([\s\S]+?)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/`([^`]+?)`/g, "<code>$1</code>");
  return out;
}

/** `1234.5, 1` → `1,234.5`. Numbers are formatted in exactly one place, so the page cannot disagree
 *  with itself about how many decimals a quantity carries. */
export function fmt(v, dp = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("en-US", { minimumFractionDigits: dp, maximumFractionDigits: dp });
}

export const round1 = (v) => Math.round(Number(v) * 10) / 10;

const DASH = "—";

/**
 * One table. `align` is per column, `"left"` or `"right"`; a row whose first cell starts with `**` is
 * a subtotal and gets ruled off rather than only emboldened. Wrapped in a scroller, because a wide
 * table must scroll inside itself and never make the page scroll sideways.
 *
 * `filter` — `{find, menus}` — marks the table as one a reader can search and sort, the way a
 * spreadsheet's autofilter does. It emits NO controls: `wireTables` builds the bar in the browser from
 * the rows already in the DOM, so the markup carries the data once and a distinct-value list cannot
 * drift from the column it describes. With scripts off, or in a test reading the HTML, the whole table
 * is simply there — the same progressive-enhancement rule the CSS-only tab strip follows for the same
 * reason.
 */
export function table(head, align, rows, filter = null) {
  const th = head.map((c, i) => `<th class="${align[i] || "left"}"${filter ? ` data-col="${i}"` : ""}` +
    `>${inline(c)}</th>`).join("");
  const body = rows.map((r) => {
    const cls = r.length && String(r[0]).startsWith("**") ? ' class="sub"' : "";
    const td = r.map((c, i) => `<td class="${align[i] || "left"}">${inline(c)}</td>`).join("");
    return `<tr${cls}>${td}</tr>`;
  }).join("\n");
  const out = `<div class="scroll"><table>\n<thead><tr>${th}</tr></thead>\n<tbody>\n${body}\n` +
    "</tbody></table></div>";
  if (!filter) return out;
  // `{sort: true}` alone: clickable headers with none of the bar. A short ranking table wants to be
  // reordered, but a search box and a row count over seven rows is furniture pretending to be a tool.
  if (filter.sort) return `<div class="sortable">${out}</div>`;
  return `<div class="filtered" data-find="${esc(filter.find || "filter")}" ` +
    `data-menus="${esc((filter.menus || []).join(","))}">${out}</div>`;
}

/** A table cell as the filter sees it: its text, whitespace collapsed. */
export const cellText = (td) => String((td && td.textContent) || "").replace(/\s+/g, " ").trim();

/**
 * `"26,583.6"` → `26583.6`, `"—"` → `NaN`. Thousands separators are the page's own formatting, so a
 * sort that did not strip them would order 9,986 above 26,583 on the first digit.
 */
export function cellNumber(text) {
  const s = String(text == null ? "" : text).replace(/,/g, "").trim();
  return s === "" || !/^[-+]?[0-9]*\.?[0-9]+$/.test(s) ? NaN : Number(s);
}

/**
 * Sort order for two cells: numeric when BOTH parse, otherwise text.
 *
 * A cell that is not a number — a dash, an unread class — sorts to the END in either direction, which
 * is the same rule the charts use for a zero: "not measured" must never take the top of a ranking.
 */
export function compareCells(a, b) {
  const x = cellNumber(a), y = cellNumber(b);
  const nx = Number.isFinite(x), ny = Number.isFinite(y);
  if (nx && ny) return x === y ? 0 : x < y ? -1 : 1;
  if (nx !== ny) return nx ? -1 : 1;
  return String(a).localeCompare(String(b), "en");
}

/**
 * Does a row survive the filter bar? `q` is matched against every cell; `picks` is `{index: value}`
 * from the dropdowns and each entry must match its column EXACTLY.
 *
 * Substring on the free text, exact on a menu, and the two are ANDed — a reader who typed `duckrun`
 * and then chose a state means both, which is what a spreadsheet does.
 */
export function matchesFilter(cells, q, picks = {}) {
  const needle = String(q || "").trim().toLowerCase();
  if (needle && !cells.some((c) => String(c).toLowerCase().includes(needle))) return false;
  for (const [i, want] of Object.entries(picks)) {
    if (want && cells[Number(i)] !== want) return false;
  }
  return true;
}

export const note = (text) => `<p class="note">${inline(text)}</p>`;
/** `cls` is optional and only the lede uses it — everything else on this page is unclassed prose. */
export const para = (text, cls = "") => `<p${cls ? ` class="${cls}"` : ""}>${inline(text)}</p>`;

/**
 * A methodology note folded behind one line. The full text stays in the DOM — every sentence the
 * tests pin still renders, and ctrl-F still finds it — but the page reads numbers-first and the
 * reasoning opens on demand. Anything that must stay LOUD (the excluded-runs table, a still-billing
 * drifter) is never folded; those go through `note`/`table` as before.
 */
export const fold = (summary, ...texts) =>
  `<details class="note"><summary>${inline(summary)}</summary>` +
  texts.map((t) => `<p class="note">${inline(t)}</p>`).join("") + "</details>";

// ------------------------------------------------------------------------------------------ charts
//
// Horizontal bars, one series, drawn as inline SVG.
//
// Bars because the job is magnitude across a handful of named categories; HORIZONTAL because the
// categories are words. ONE series per chart, so there is no legend and no categorical palette to
// validate — the engine names are on the axis, and colouring each bar differently would encode nothing
// the label does not already say. Colour by RANK would be worse: it repaints when the numbers move.
//
// Geometry follows the mark spec: bar ≤ 24px thick, square at the baseline, 4px rounded data-end, and
// a gap between bands wider than the 2px surface-gap minimum. No gridlines — every bar carries its
// value at the tip, so ticks would be a second copy of the same number.
//
// The value label wears text ink, never the bar colour, and each bar carries a <title> so hovering
// gives a native tooltip.
//
// A row may carry a CAPTION — the adapter and the compute the engine ran on — drawn as a second,
// dimmer line under the name. It is the difference between "iceberg cost 2.3x duckrun" and "the same
// DuckDB writing to an Iceberg catalog instead of through delta-rs, at the same notebook size, cost
// 2.3x". The label gutter widens and the band grows only when a caption is actually present, so a
// plain chart keeps the geometry it had.
const BAR_H = 18, BAND = 30, PAD_T = 26, LABEL_W = 96, VALUE_W = 74, WIDTH = 660;
const SUB_BAND = 36, SUB_LABEL_W = 224;

function barPath(w, h, r = 4) {
  if (w <= r) return `M0,0 h${w.toFixed(1)} v${h} h-${w.toFixed(1)} Z`;
  return `M0,0 H${(w - r).toFixed(1)} A${r},${r} 0 0 1 ${w.toFixed(1)},${r} V${h - r} ` +
    `A${r},${r} 0 0 1 ${(w - r).toFixed(1)},${h} H0 Z`;
}

/**
 * A bar at the MEAN, with a whisker spanning min..max across that engine's runs.
 *
 * One run is one sample and Fabric's capacity is shared, so a single number is a reading rather than a
 * result. The bar is the average because that is what a ranking should be built on; the whisker is
 * there so a reader can see when two averages are closer together than either engine's own spread —
 * which is the case where the ranking means nothing. With one run the whisker collapses to the bar and
 * says so honestly.
 *
 * Rows arrive as `[label, mean, min, max, caption]`, and are sorted CHEAPEST FIRST because "lower is
 * better" makes the ranking the finding. A ZERO sorts to the BOTTOM: zero means "this engine did no
 * such work", and at the top under that caption it would read as the winner — the one value whose rank
 * would lie. A chart with nothing but zeros is not drawn at all.
 */
export function chartSvg(title, subtitle, rowsIn) {
  const rows = [...(rowsIn || [])]
    .map((r) => {
      // `[label, mean, min, max, caption]`, tolerating the older `[label, value, caption]` — so a
      // chart spec carried in an artifact rendered months ago still draws, with the whisker collapsed
      // onto the bar because there was never a range in it.
      const avg = Number(r[1]) || 0;
      const ranged = r.length >= 4 && r[2] !== null && r[2] !== undefined &&
        r[3] !== null && r[3] !== undefined;
      return {
        label: String(r[0]), avg,
        lo: ranged ? Number(r[2]) || 0 : avg,
        hi: ranged ? Number(r[3]) || 0 : avg,
        sub: ranged ? (r[4] ? String(r[4]) : "") : (r[2] ? String(r[2]) : ""),
      };
    })
    .sort((a, b) => (a.avg === 0) - (b.avg === 0) || a.avg - b.avg);
  if (!rows.length || !rows.some((r) => r.avg)) return "";
  for (const r of rows) {
    r.ranged = r.hi - r.lo > 0.05;
    // The MEAN alone at the tip. The range used to ride beside it in parentheses, which doubled the
    // ink for a fact the whisker already draws — the exact numbers stay in the tooltip.
    r.value = fmt(r.avg, 1);
  }
  const subs = rows.some((r) => r.sub);
  const band = subs ? SUB_BAND : BAND;
  // Both gutters are sized to what is actually printed, so the text ends inside the viewBox and the
  // bars get whatever is left. They used to be fixed and rely on `overflow:visible` spilling into
  // empty page — which stopped existing the moment a second chart sat to the right.
  const need = Math.max(...rows.map((r) =>
    Math.max(r.label.length * 7.2, (r.sub || "").length * 5.4)));
  const labelW = Math.max(LABEL_W, Math.min(SUB_LABEL_W + 12, Math.ceil(need) + 14));
  const top = Math.max(...rows.map((r) => r.hi)) || 1;
  const valueW = Math.max(VALUE_W,
    Math.ceil(Math.max(...rows.map((r) => r.value.length)) * 6.8) + 14);
  const plot = WIDTH - labelW - valueW;
  const height = PAD_T + rows.length * band + 6;
  const out = [
    '<figure class="chart">' +
    `<figcaption><span class="chart-title">${esc(title)}</span>` +
    `<span class="chart-sub">${esc(subtitle)}</span></figcaption>`,
    `<svg viewBox="0 0 ${WIDTH} ${height}" width="100%" height="${height}" role="img" ` +
    `aria-label="${esc(title)}">`,
  ];
  rows.forEach((r, i) => {
    const y = PAD_T + i * band;
    const w = plot * (r.avg / top), wlo = plot * (r.lo / top), whi = plot * (r.hi / top);
    // With a caption the name sits on the bar's upper half and the caption under it, so the pair reads
    // as one block against the bar rather than as two columns.
    const ly = subs ? BAR_H / 2 : BAR_H / 2 + 4;
    const mid = BAR_H / 2;
    // Drawn OVER the bar, not beside it: the range belongs to the same quantity the bar measures, and
    // a separate mark would read as a second series.
    const spread = whi - wlo > 0.5
      ? `<line class="whisker" x1="${wlo.toFixed(1)}" y1="${mid}" x2="${whi.toFixed(1)}" y2="${mid}"/>` +
        `<line class="whisker-cap" x1="${wlo.toFixed(1)}" y1="${mid - 5}" x2="${wlo.toFixed(1)}" y2="${mid + 5}"/>` +
        `<line class="whisker-cap" x1="${whi.toFixed(1)}" y1="${mid - 5}" x2="${whi.toFixed(1)}" y2="${mid + 5}"/>`
      : "";
    out.push(
      `<g transform="translate(0,${y})">` +
      `<title>${esc(r.label)}${r.sub ? ` (${esc(r.sub)})` : ""}: mean ${fmt(r.avg, 1)} CU` +
      `${r.ranged ? `, range ${fmt(r.lo, 1)}–${fmt(r.hi, 1)}` : ""}</title>` +
      `<text class="bar-label" x="${labelW - 10}" y="${ly.toFixed(0)}" text-anchor="end">` +
      `${esc(r.label)}</text>` +
      (r.sub ? `<text class="bar-caption" x="${labelW - 10}" y="${(ly + 13).toFixed(0)}" ` +
        `text-anchor="end">${esc(r.sub)}</text>` : "") +
      `<g transform="translate(${labelW},0)"><path class="bar" d="${barPath(w, BAR_H)}"/>${spread}</g>` +
      `<text class="bar-value" x="${(labelW + Math.max(w, whi) + 8).toFixed(1)}" ` +
      `y="${(BAR_H / 2 + 4).toFixed(0)}">${r.value}</text></g>`);
  });
  out.push(`<line class="axis" x1="${labelW}" y1="${PAD_T - 6}" x2="${labelW}" ` +
    `y2="${PAD_T + rows.length * band - band + BAR_H + 4}"/>`);
  out.push("</svg></figure>");
  return out.join("\n");
}

// ------------------------------------------------------------------------------------- the page


/**
 * `{column: [CU, …]}` — every run's total for `cls`, not just the latest. `key="seconds"` reads the
 * ledger's duration dict instead.
 *
 * One run is one sample of a SHARED capacity, so a single number is a reading rather than a result.
 * Collecting every run of a column is what lets the chart show a mean and a range, and the range is
 * the honest part: when two engines' averages sit closer together than either one's own spread, the
 * ranking between them means nothing and the reader can see it.
 */
export function spreadFor(runs, ledger, cls, keyOf, key = "items") {
  const out = {};
  for (const rec of runs) {
    const col = keyOf(rec);
    if (col === undefined || col === null) continue;
    const value = classTotal(runCu(rec, ledger, key).cells, cls);
    if (value) (out[col] = out[col] || []).push(value);
  }
  return out;
}

const meanOf = (vals) => vals.reduce((a, b) => a + b, 0) / vals.length;

// A group's numbers are its RUNS' mean — the same runs the bar above averaged, so the two cannot
// disagree. One dispatch is a sample of a shared capacity; a group with several runs has simply been
// measured that many times, and a run that wrote a different shape is in a different group. A run
// that measured NOTHING is dropped rather than averaged in as a zero, which would drag the mean
// toward "free" for a run that was never read.
const groupMean = (vals) => {
  const v = (vals || []).filter((x) => x);
  return v.length ? meanOf(v) : 0;
};

/**
 * `[label, mean, min, max, caption]` per LAYOUT — the analytics chart's rows.
 *
 * One bar per thing Power BI can distinguish, not per engine, because Power BI never sees the engine:
 * it opens parquet through Direct Lake and transcodes row groups. Two producers that wrote the same
 * shape are one bar, and every run of either of them is a sample of it — which is what turns a 50% gap
 * between duckrun at two core counts from a comparison into what it actually is, one layout measured
 * twice.
 *
 * **The bar is NAMED for its writer and captioned with the shape** — `spark readHeavyForPBI` over
 * `V-Order · 10–11 files · 10–11 RG`. The grouping is still the layout, which is the whole point; but a
 * reader scanning bars wants to know which thing they are looking at, and a file count is a poor name
 * even when it is the real subject. The shape sits underneath, where it explains why two writers would
 * ever share a bar.
 *
 * The mean is over the group's OWN runs — `layoutGroups` keys per run, so a run that wrote a different
 * shape is a sample of a different bar rather than of this one. That is what makes two bars with the
 * same label possible (`duckrun sorted` twice, at `3 files · 26 RG` and `4 files · 25 RG`), and the
 * caption is what tells them apart: the label answers who wrote it, the caption answers what.
 */
/**
 * One entry per layout group: `{name, rec, cu, ms: {cold, warm, hot}}`.
 *
 * The single source for both things that describe the mart — the rows of its layout block and the
 * dots of the fit chart. They are the same measurement shown twice, so deriving them separately is
 * how a page ends up printing 1,916 in a table and plotting 1,960 above it.
 *
 * `rec` is the NEWEST member's record: entries arrive oldest-first, and within a group the physical
 * stats agree to the band anyway — 78 files and 80 is one bar — so this only picks which of the two
 * prints. The CU and the tier times are the group's MEAN across its runs, which is what the chart's
 * bars already quote.
 */
export function martPoints(groups, times) {
  return (groups || []).map(([, ms]) => {
    const rec = ms[ms.length - 1].rec;
    return {
      name: producers(ms), rec, cu: groupMean(ms.map((m) => m.cu)),
      ms: Object.fromEntries(TIERS.map(([lbl]) =>
        [lbl, groupMean(ms.map((m) => ((times || {})[m.qid] || {})[lbl]))])),
    };
  });
}

/**
 * What each layout cost to query and how long it took — one row per layout, cheapest first.
 *
 * These four numbers used to be columns of the mart's layout block, which made one table answer two
 * questions: what the parquet looks like, and what querying it cost. *Table layout* is now physical
 * layout alone and this is the cost-and-time half, standing on its own between the charts and the
 * cost table.
 *
 * Same `martPoints` as the bars and the layout rows, so all three quote the same mean.
 */
export function renderFit(groups, times, tiers) {
  const pts = martPoints(groups, times).filter((p) => p.cu > 0);
  if (!pts.length) return "";
  const cols = (tiers || []).filter((l) => pts.some((p) => p.ms[l]));
  pts.sort((a, b) => a.cu - b.cu);
  return ["<h3>Cost and speed by layout</h3>",
    table(["layout", "CU", ...cols.map((l) => `${l} ms`)],
      ["left", "right", ...cols.map(() => "right")],
      pts.map((p) => [p.name, fmt(p.cu, 0), ...cols.map((l) => (p.ms[l] ? fmt(p.ms[l], 0) : DASH))]),
      { sort: true }),
  ].join("\n");
}

export function groupRows(groups, table = DEFAULTS.table) {
  const out = [];
  for (const [, members] of groups) {
    const vals = members.map((m) => m.cu).filter((v) => v);
    const label = producers(members);
    let caption = layoutLabel(members, table);
    if (caption === label) caption = "";       // nothing was measured, so it would read twice
    if (!vals.length) { out.push([label, 0, 0, 0, caption]); continue; }
    out.push([label, round1(meanOf(vals)), round1(Math.min(...vals)), round1(Math.max(...vals)),
      caption]);
  }
  return out;
}


/**
 * Engines across, BUCKETS down, grouped by class — the shape the whole repo reads in.
 *
 * ENGINE-MAJOR, and that orientation is what makes the width work: item-major would need a column per
 * Fabric item and every run creates different ones. Turned ninety degrees those are rows.
 *
 * **No total column and no grand-total row.** Both would sum ACROSS engines, which is the one sum on
 * this page that answers nothing — the engines are alternatives to each other. The class subtotals
 * stay: they sum DOWN a column, which is "what this engine spent building".
 *
 * **The rate is a ROW HERE, not a section of its own.** It comes from the same Capacity Metrics row as
 * the CU above it — same GUIDs, same roles, same compute/storage split, read from the ledger's
 * `seconds` dict — so a separate table restated the whole join to add two numbers per class, and put
 * "what it cost" and "how long it took" on two tables the reader had to hold in their head at once.
 */
export function engineTable(perCol, cols, secsCol) {
  const names = cols.map((c) => c.col);
  const labels = {};
  for (const cls of ["etl", "analytics"]) {
    const seen = new Map();
    for (const col of names) {
      for (const [label, value] of Object.entries((perCol[col] || {})[cls] || {})) {
        seen.set(label, (seen.get(label) || 0) + value);
      }
    }
    // Decompose a class ONLY when it decomposes something: some column has to hold more than one
    // bucket in it. `analytics` is always exactly one semantic model per engine, so its rows would
    // repeat the subtotal and add a row of em dashes for every other engine — three rows carrying one
    // row's information. `etl` splits because a DuckDB leg really is a notebook plus a lakehouse.
    const deepest = Math.max(0, ...names.map((c) => Object.keys((perCol[c] || {})[cls] || {}).length));
    labels[cls] = deepest > 1 ? [...seen.keys()].sort((a, b) => seen.get(b) - seen.get(a)) : [];
  }
  const rows = [];
  for (const cls of ["etl", "analytics"]) {
    if (!names.some((c) => (perCol[c] || {})[cls])) continue;
    // An em dash when the ledger has nothing for this column yet — a run committed minutes ago whose
    // CU has not been read. `**0.0**` there says the engine did this work for free, which is the one
    // reading the whole page is built to prevent, and it is the same distinction the bucket rows below
    // already make.
    rows.push([`**${cls}**`, ...names.map((c) => ((perCol[c] || {})[cls]
      ? `**${fmt(classTotal(perCol[c], cls), 1)}**` : DASH))]);
    for (const label of labels[cls]) {
      // An em dash, not 0.0: this engine never billed an operation of that kind, which is a different
      // statement from one that cost nothing.
      rows.push([`\`${label}\``, ...names.map((col) => {
        const v = ((perCol[col] || {})[cls] || {})[label];
        return v === undefined ? DASH : fmt(v, 1);
      })]);
    }
    if (!(secsCol && names.some((c) => (secsCol[c] || {})[cls]))) continue;
    // HOW LONG THE BUILD TOOK — **`etl` only, and one row.** The seconds were dropped from this table
    // once, on the grounds that they are billed OPERATION seconds which SUM across concurrent
    // operations (spark's five Livy REPLs total more than the clock they ran on) and so needed more
    // hedging than they were worth. That objection is real and has not gone away; what changed is the
    // judgement that "how long did the build take" is a question worth answering anyway, with the
    // caveat carried in the row's own label rather than in a note four rows below it.
    //
    // `analytics` deliberately does NOT get one: the query half already reports latency properly, as
    // cold/warm/hot milliseconds per pass position in the mart block, and those are wall clock a user
    // actually waited. A second, differently-defined duration beside them would invite the two to be
    // compared.
    //
    // COMPUTE seconds, not total, for the same reason the rate below is compute over compute: a
    // storage operation bills real CU against a duration of essentially nothing — 383.25 CU in
    // 0.049 s, measured — so storage durations are noise that tracks OneLake traffic rather than
    // anything about how long the engine ran. It also makes the three rows RECONCILE: `compute` CU
    // divided by `compute seconds` is exactly the rate printed underneath, so a reader can check the
    // column against itself.
    if (cls === "etl") {
      rows.push(["`compute seconds` <sub>billed, not wall clock</sub>", ...names.map((c) => {
        const secs = ((secsCol[c] || {})[cls] || {}).compute;
        // A dash, never 0 — the ledger not having read this column yet is not a build that took no
        // time. Same rule as every other cell here.
        return secs ? fmt(secs, 0) : DASH;
      })]);
    }
    // THE RATE. Unaffected by the concurrency that makes the row above hard to read across engines —
    // it is in the numerator and the denominator alike, so it cancels. A high rate is a WIDE engine,
    // not a slow one.
    // COMPUTE over COMPUTE. A storage operation bills real CU against a duration of essentially
    // nothing — 383.25 CU in 0.049 s, measured — so including it does not dilute the rate, it detonates
    // it, by an amount that tracks how much OneLake traffic the engine made rather than anything about
    // the engine. That is what made two runs of the same DuckDB on the same notebook read 31.2 and 36.1.
    rows.push(["`compute CU per second`", ...names.map((c) => {
      const secs = ((secsCol[c] || {})[cls] || {}).compute;
      const cu = ((perCol[c] || {})[cls] || {}).compute;
      return !secs || !cu ? DASH : fmt(cu / secs, 1);
    })]);
  }
  return table(["CU (s)", ...names], ["left", ...names.map(() => "right")], rows) + "\n" + fold(
    "how to read this table",
    "`etl` against `analytics` comes from each item's recorded ROLE — a semantic model is only " +
    "ever queried, everything else is work done to build the tables. `compute` against `storage` " +
    "comes from the OPERATION, which is the only thing that can separate them: they share an ITEM. " +
    "Spark bills its Livy session and its OneLake reads against the same lakehouse; a warehouse bills " +
    "`Warehouse Query` and its OneLake writes against the same warehouse. Every `OneLake …` " +
    "operation is storage; everything else — Livy runs, warehouse queries, notebook runs, " +
    "SQL-endpoint queries — is compute. A dash means no operation of that kind was billed there " +
    "at all — or, on a class subtotal, that the ledger has not read that column yet; never that " +
    "the work was free.<br>**`compute seconds`** is how long the build BILLED for, on the `etl` half " +
    "only, read from `Duration (s)` in the same Capacity Metrics row as the CU above it — so it " +
    "costs no extra query. **Read it as billed time, not as a stopwatch.** It is the sum of every " +
    "compute operation's duration, and those run CONCURRENTLY: a duckrun leg is one long notebook " +
    "run so its seconds land close to the clock, while spark opens five Livy REPLs under one session " +
    "whose durations sum to more than the wall time anyone waited. Compare it freely between two runs " +
    "of the SAME engine; compare it across engines only knowing that. Storage is left out because a " +
    "storage operation bills real CU over a duration of essentially nothing (383.25 CU in 0.049 s), " +
    "so its seconds are noise that tracks OneLake traffic rather than how long anything ran. " +
    "`analytics` gets no such row on purpose: the query half reports latency properly, as the " +
    "`cold`/`warm`/`hot` milliseconds beside the layout that produced them, and those are time a user " +
    "actually waited.<br>**`compute CU per second`** divides the two rows above it, so the column " +
    "reconciles against itself. It is the average capacity the node drew while it ran, and it is the " +
    "sturdiest number here — the concurrency that makes the seconds awkward is in the numerator and " +
    "the denominator alike, so it cancels. A high rate is a WIDE engine, not a slow one. It is " +
    "COMPUTE against COMPUTE, and that is not a refinement: a total-over-total rate drifts upward " +
    "with however much OneLake traffic an engine happened to make. It SCALES with the compute the " +
    "column was given — a single-node Python notebook draws `vCores ÷ 2`, 32 at 64 vCores and 16 " +
    "at 32 — so compare it across columns only at equal size.");
}

/**
 * EVERY run the page drew from — which dispatch, what it cost, and whether that cost can still rise.
 *
 * The one thing a composed page owes the reader that a single-run page did not: the columns are
 * different dispatches, so a column can be days older than the one beside it. The other half is that a
 * run measured minutes ago is a LOWER BOUND — an hour's CU keeps growing for ~70 minutes after the
 * fact — so the reader is told to dispatch again rather than left to wonder.
 *
 * **THE RUN IS THE KEY — one row per dispatch, and nothing is marked or ranked against a column.**
 * This listed one run per column, so the runs that hold no column were invisible while still moving
 * every bar: a `duckrun sorted` bar read 2,454.1 and no row on the page said so. A number on a chart
 * with no row behind it is exactly what this table exists to prevent. The rows that also hold a column
 * carry no annotation saying so — which run is newest is already in the `built` column, and a marker
 * on top of it was inventing a second grammar for a fact the sort order states.
 *
 * It carries the two class totals as well, which is why `ledger` is a parameter rather than a leftover.
 * Everywhere else the two halves are read a table apart from the run that produced them; here they sit
 * on the row that names the dispatch, its build mode and whether the number has settled — the four
 * facts that qualify a CU figure, in one place.
 */
export function renderSources(cols, entries, ledger, repo, now = null, gen = {}) {
  // A HEADING OF ITS OWN. Every other section on the page has one; this table opened with a bare
  // note, so it read as a continuation of whatever sat above it — and with the methodology moved to
  // the foot, what sits above it is now the input archive.
  const out = ["<h3>Every run</h3>",
    note("**Every run on this page**, newest dispatch first. The RUN is the key — one row " +
      "per dispatch, with its own totals:")];
  // The query tiers belong HERE rather than beside the layout: they were measured by one dispatch
  // against one deployed semantic model, so the run is their natural key. On the layout block they
  // had to be a group's MEAN, which is a number no single run recorded.
  const times = (gen.times || {});
  const tiers = TIERS.map(([l]) => l).filter((l) => l in (gen.counts || {}));
  // NEWEST DISPATCH FIRST. Everywhere else on the page the order is the engine order, which is what
  // makes columns comparable across two renders; here the point of the table is precisely that the
  // rows are NOT contemporaneous, so it sorts on the thing it is reporting.
  const sorted = [...(entries && entries.length ? entries : cols)].sort((a, b) => {
    const sa = ((a.rec.run || {}).started || ""), sb = ((b.rec.run || {}).started || "");
    return sa < sb ? 1 : sa > sb ? -1 : 0;
  });
  const rows = [];
  for (const { col, rec, qid } of sorted) {
    const rid = (rec.run || {}).id;
    // The run id is the label; the target is the committed record, which outlives the CI run.
    const link = rec._file ? `[${rid || rec._file}](${recordUrl(repo, rec._file, gen.ref)})`
      : rid ? String(rid) : DASH;
    const skip = landingGuids(rec);
    const items = Object.entries(items_(rec))
      .filter(([g, it]) => !NON_ENGINE_ROLES.has(role_(it)) && !skip.has(g));
    const started = String((rec.run || {}).started || "?").slice(0, 16).replace("T", " ");
    // THIS run's own two halves and its own unmeasured items, not the column's. Same join, same GUIDs,
    // same roles as `engineTable` — `runCu` is called again rather than threaded in, which costs one
    // dictionary walk per row and is the only way a row that does not hold its column can report
    // itself. A DASH where the ledger holds nothing for that class yet, never `0.0`: the whole page is
    // built to stop a not-yet-measured run reading as work done for free.
    const { cells: cu, unmeasured: missing } = runCu(rec, ledger);
    const live = drifting(rec);
    let state;
    if (live.length) {
      // Loudest of the three, because it is the only one that never resolves: the other two are "wait
      // and read again", this one is "the number has no upper bound until someone deletes these".
      state = `**still billing** — ${live.length} item(s) never deleted`;
    } else if (missing.length) {
      state = `${items.length - missing.length}/${items.length} items measured`;
    } else if (stillAccruing(rec, 2.0, now)) {
      state = "may still rise";
    } else {
      state = "settled";
    }
    const load = rec.full_load ? "full" : "incremental";
    // This run's OWN tiers. A dash where it recorded none — a run that was built but not benchmarked
    // is skipped entirely, but a run can still be missing one tier (`runs < 3` yields no hot at all).
    const ms = (times[qid] || {});
    // The mart row groups THIS run wrote — the shape the analytics numbers on the same row were
    // measured against, per run rather than as the bar's range. A dash when the run recorded no
    // layout, same as everywhere else: unmeasured is not zero.
    const rg = martStats(rec, gen.table).num_row_groups;
    rows.push([col, link, `${started} (${load})`,
      rg === undefined || rg === null ? DASH : fmt(Math.trunc(Number(rg)), 0),
      cu.etl ? fmt(classTotal(cu, "etl"), 1) : DASH,
      cu.analytics ? fmt(classTotal(cu, "analytics"), 1) : DASH,
      ...tiers.map((l) => (ms[l] ? fmt(ms[l], 0) : DASH)),
      String(items.length), state]);
  }
  // `state` was headed `CU` until this table grew CU numbers of its own — one column headed `CU`
  // holding the word "settled" beside two holding capacity units is a header doing two jobs.
  // THE ONE FILTERABLE TABLE ON THE PAGE, and the only one that wants to be: it is the only one with
  // a row per RUN rather than a row per measured thing, so it is the only one that grows without bound
  // and the only one a reader arrives at looking for a particular dispatch. Menus on `column` and
  // `state` — the two cells that repeat; `run`, `built` and the CU columns are unique per row, so a
  // dropdown of them would just be the table again.
  out.push(table(["column", "run", "built", "RG", "etl CU", "analytics CU",
    ...tiers.map((l) => `${l} ms`), "items", "state"],
  ["left", "left", "left", "right", "right", "right", ...tiers.map(() => "right"), "right", "left"],
  rows,
  { find: "filter runs — engine, run id, date…", menus: [0, 7 + tiers.length] }));
  out.push(note("**`etl CU` and `analytics CU` are that RUN's own totals** — the same GUID join as " +
    "*Cost by engine*, which quotes each column's newest run. The CHARTS quote neither: each bar is " +
    "the mean over the runs listed here that fed it. The two group differently, so one run can sit in " +
    "a bar with different company on each — ETL by column, analytics by the parquet the run " +
    "measured."));
  if (tiers.length) {
    const counted = Object.entries(gen.counts || {}).map(([l, n]) => `${l} over ${n}`).join(", ");
    out.push(note("**`cold`, `warm` and `hot` are the DAX suite summed per PASS POSITION** — the " +
      "first visit to a freshly deployed semantic model, the second, then the median of the rest. " +
      "They are on the RUN because that is what measured them: one dispatch, against one model it " +
      "had just deployed. Beside the layout they had to be a group's mean, which is a number no " +
      `single run recorded. Each is summed over the queries EVERY run carries at that tier ` +
      `(${counted}); cold covers fewer, because the selectivity-ladder queries have no first-pass ` +
      "sample at all — the top DUID is resolved after pass 1. **Cold is the tier layout can move**: " +
      "it is the one that transcodes columns out of parquet, while warm and hot converge on what " +
      "the model already holds in memory — which is what the third chart above plots."));
  }
  const drifters = cols.map(({ col, rec }) => [col, drifting(rec)]).filter(([, v]) => v.length);
  // The drifter warning stays a VISIBLE note — it is the one state that never resolves by waiting,
  // so it must not sit behind a click. Only the general how-numbers-settle prose is folded.
  if (drifters.length) {
    out.push(note(drifters.map(([c, v]) => `**${c}** predates that teardown and still owns ` +
      v.map((x) => `\`${x}\``).join(", ") +
      " — Fabric keeps billing them, so its total creeps upward and is an upper bound on that " +
      "run rather than a measurement of it. Delete them and it settles.").join(" ")));
  }
  out.push(fold("how a number settles",
    "An hour's CU keeps growing for up to ~70 minutes after the work happened, so a run " +
    "measured just now is a lower bound. It settles itself: the **Capacity units** workflow re-reads " +
    "the whole window daily and keeps the larger of the two figures, so reloading this page tomorrow " +
    "shows the final number and nothing has to be reconciled. Every item a run creates is deleted " +
    "when it finishes, which is what makes a Fabric item GUID belong to exactly one run and the " +
    "attribution exact."));

  // THE EXCLUSION HAS TO BE LOUD. Filtering to one source generation replaced a shout with a
  // silence: the mart's `row counts DISAGREE` heading — the loudest signal this page had — can no
  // longer fire, because every surviving column agrees by construction. Naming each dropped run and
  // its count is what pays that back, and it is strictly sharper than the heading was: "duckrun
  // wrote 143,980,960 against the current 143,980,961" beats "row counts DISAGREE".
  const dropped = gen.dropped || [];
  if (dropped.length) {
    const total = dropped.length + cols.length;
    out.push(`<h4>${inline(`**${dropped.length} run(s) excluded** — built from a different source`)}` +
      "</h4>");
    out.push(table(["run", "engine", `${gen.table || DEFAULTS.table} rows`, "against current"],
      ["left", "left", "right", "right"],
      dropped.map((d) => [
        d.file && d.file !== "?" ? `[${d.run || d.file}](${recordUrl(repo, d.file, gen.ref)})`
          : d.run ? String(d.run) : `\`${d.file}\``,
        d.engine,
        d.rows === null ? DASH : fmt(d.rows, 0),
        d.rows === null || gen.reference == null ? DASH
          : (d.rows > gen.reference ? "+" : "") + fmt(d.rows - gen.reference, 0),
      ])));
    out.push(note("**The newest run defines the current source**, and a run whose mart row count " +
      "disagrees with it was built from different data — a different experiment, not a slower one, " +
      "so it is dropped rather than ranked beside the others. It is excluded from the tables, from " +
      "both charts, and from the means and ranges those charts draw. The current count is " +
      `**${gen.reference == null ? "—" : fmt(gen.reference, 0)}**. ` +
      "A run that recorded no count at all is KEPT, because unmeasured is a different claim from " +
      "different." +
      // The one reading that would be wrong, stated where it can be seen rather than only in the
      // docs. Newest-wins cannot distinguish "the source changed" from "the newest run is broken",
      // and this is the shape that tells you which.
      (dropped.length > cols.length
        ? ` **Note that ${dropped.length} of ${total} runs were excluded** — when nearly everything ` +
          "is dropped, the more likely reading is that the NEWEST run is the anomaly rather than " +
          "that every earlier one is. Check it before trusting this page; the next good run reverses " +
          "the exclusion on its own."
        : "")));
  }
  return out.join("\n");
}

/**
 * How much data went IN — ONE archive, not one per engine.
 *
 * `dbt_landing` holds a single copy of the AEMO CSVs and every engine reads the same bytes, so a
 * column per engine repeated one number across the page and invited the reading that each engine had
 * its own input. It is broken down by FOLDER instead, which is a real decomposition and comes free in
 * the record.
 *
 * Taken from the most recent run that listed it. If an older column read a different archive — a
 * dispatch with `skip_download` off extends it — that is stated rather than averaged away, because the
 * two runs then did genuinely different amounts of work.
 */
/**
 * Every column's landing block, oldest first — the page's one statement of what went IN.
 *
 * Split out because the LEDE quotes the same archive the *Input archive* table does. Two readers of
 * `layout.landing` picking their own record is exactly how a page ends up saying 170 GB at the top
 * and 168 GB at the bottom, which reads as a bug in the measurement rather than in the page.
 */
export function landingBlocks(cols) {
  return cols
    .map(({ col, rec }) => [col, ((rec.layout || {}).landing) || {}])
    .filter(([, d]) => Object.keys(d).length);
}

export function renderInput(cols) {
  const have = landingBlocks(cols);
  if (!have.length) return "";
  const latest = have[have.length - 1][1];
  const folders = latest.folders || {};
  const rows = Object.entries(folders)
    .sort((a, b) => (b[1].size_mb || 0) - (a[1].size_mb || 0))
    .map(([name, f]) => [`\`${name}\``, fmt(f.files || 0, 0), fmt(f.size_mb || 0, 2)]);
  rows.push([`**total**`, `**${fmt(latest.files || 0, 0)}**`, `**${fmt(latest.size_mb || 0, 2)}**`]);
  const differ = [...new Set(have.map(([, d]) => round1(d.size_mb || 0)))].sort((a, b) => a - b);
  return [
    "<h3>Input archive</h3>",
    table(["folder", "files", "size MB"], ["left", "right", "right"], rows),
    // The changed-archive warning stays VISIBLE — it qualifies every comparison above it — while
    // the description of what the table is folds away.
    differ.length > 1
      ? note(`The runs on this page did not all read the same archive: sizes ranged ` +
          `${fmt(differ[0], 1)}–${fmt(differ[differ.length - 1], 1)} MB, so they did different ` +
          `amounts of work.`)
      : "",
    fold("what this table is",
      "The landed AEMO archive `stats.py` listed in `dbt_landing/Files` — **one copy, read by " +
      "every engine**, so this is not per column. Every other number on this page is about what came " +
      "OUT; this is what went in, and it is what makes a duration or a CU total mean anything. It " +
      "moves only when a dispatch runs with `skip_download` off."),
  ].filter(Boolean).join("\n");
}

/**
 * Every shared table's physical layout, one block each, the mart first, ONE ROW PER PRODUCER.
 *
 * The mart leads because it is the table the benchmark's queries land on, and it is the only block
 * carrying the CU column AND the three query-time columns — both are one number per producer, not per
 * table, so printing them in every block would read as one measurement per table. That block's rows
 * are ordered by that CU, cheapest first; the rest keep the engine order.
 *
 * **A row is a `producer()`, not a column.** `spark readHeavyForPBI` and `spark writeHeavy`, not
 * `spark·readHeavyForPBI+NEE` — the profile named by what it does to the parquet, and the core count
 * and NEE flag dropped because neither reaches it. duckrun's two core counts and spark's two NEE
 * settings each collapse to one row, and they had written identical layouts, so the rows they replaced
 * were the same row printed twice.
 *
 * **The MART block takes its rows from the chart's own groups, and the other blocks stay per
 * producer.** That is what keeps the two agreeing when a producer wrote more than one layout: the mart
 * block is the only one carrying CU and query time, so it is the only one where a row that averaged two
 * different shapes would print a number belonging to neither — which is exactly what `duckrun sorted`
 * did, quoting the mean of a 3-file run and a 4-file one on a row showing 4 files. Per group, that
 * producer has two mart rows and the `files`/`row groups` columns say which is which. The other blocks
 * are physical layout alone and describe a table the mart's shape says nothing about, so splitting them
 * the same way would print the same row twice for a difference that is not in it.
 *
 * **`cold`/`warm`/`hot` are here rather than in a section of their own, and that placement is the
 * point.** They were briefly a table further down the page, which put the layout and the speed it
 * produced on two different tables — and the only question worth asking of these numbers is whether
 * one explains the other. On one row, `files`, `row groups`, `rows per RG` and `V-Order` sit beside the
 * milliseconds they produced, and a reader can see for themselves whether a smaller file count bought
 * a faster first visit. Cold especially: it is the tier that transcodes columns out of parquet, so it
 * is the one layout can move at all.
 */
export function renderLayouts(cols, groups, times, counts, martTable = DEFAULTS.table) {
  const stats = {};
  for (const { col, rec } of cols) {
    stats[col] = ((rec.layout || {}).stats || {})[rec.engine] || {};
  }
  // ONE ROW PER PRODUCER, not per column, for every block but the mart. `producer()` has already
  // dropped the config that never reached the parquet, so duckrun's core counts and spark's two NEE
  // settings each collapse to one name — and they wrote identical layouts, so the rows they replaced
  // were the same row twice.
  const order = [], members = new Map();
  for (const { col, rec } of cols) {
    const name = producer(rec);
    if (!members.has(name)) { members.set(name, []); order.push(name); }
    members.get(name).push({ col, rec });
  }

  const tables = [], schema = {};
  for (const { col, rec } of cols) {
    for (const t of ((rec.layout || {}).tables || [])) if (!tables.includes(t)) tables.push(t);
    for (const [t, d] of Object.entries(stats[col] || {})) {
      if (!(t in schema)) schema[t] = (d || {}).schema;
      if (!tables.includes(t)) tables.push(t);
    }
  }
  if (!tables.length) return "";
  const mart = tables.includes(martTable) ? martTable : tables[0];
  const ordered = [mart, ...tables.filter((t) => t !== mart)];
  const metrics = [["num_files", "files", 0], ["num_row_groups", "row groups", 0],
    // One decimal, not zero: the mart reads fine either way but `stg_csv_archive_log` is 0.37 MB, and
    // rounding that to `0` says the table is empty.
    ["avg_row_group", "rows per RG", -1], ["size_mb", "size MB", 1]];

  const out = ["<h3>Table layout</h3>"];
  const blocks = [];
  for (const t of ordered) {
    const byLayout = t === mart;
    // The mart's rows are still the CHART's groups, so a writer that produced two different shapes
    // gets a row each — `duckrun sorted` wrote 3 files/26 RG and 4/25, which is two layouts and not
    // one. Every other block is one row per producer, read off that producer's first column: those
    // describe a table the mart's shape says nothing about, so splitting them the same way would
    // print one row twice for a difference that is not in it.
    let present = byLayout
      ? martPoints(groups, times)
        .map((p) => ({ ...p, d: (((p.rec.layout || {}).stats || {})[p.rec.engine] || {})[t] }))
        .filter(({ d }) => d)
      : order.map((n) => ({ name: n, d: (stats[members.get(n)[0].col] || {})[t], ms: {} }))
        .filter(({ d }) => d);
    if (!present.length) continue;
    if (byLayout) {
      // FEWEST FILES FIRST. It sorted cheapest-CU-first while the CU column was here; ordering by a
      // column that is no longer printed is a ranking a reader cannot check. Files is the layout
      // fact this block leads with, so it is the one to sort on.
      present = present.sort((a, b) =>
        (Number(a.d.num_files) || 0) - (Number(b.d.num_files) || 0) ||
        (Number(a.d.num_row_groups) || 0) - (Number(b.d.num_row_groups) || 0));
    }
    // The ROW COUNT goes in the heading, not in a column. It is identical on every row — that is the
    // parity statement the whole project rests on — and a 143,980,961 repeated down the table is a wide
    // column carrying one fact. When the engines DISAGREE it becomes a column again and the heading
    // says so, because that disagreement is the loudest signal this page has.
    const seenCounts = [...new Set(present.filter(({ d }) => d.total_rows)
      .map(({ d }) => Math.trunc(Number(d.total_rows))))].sort((a, b) => a - b);
    const agree = seenCounts.length === 1;
    const rowsNote = agree ? ` — ${fmt(seenCounts[0], 0)} rows on every engine`
      : seenCounts.length ? " — **row counts DISAGREE**" : "";
    const head = t === mart
      ? `\`${t}\` — the mart the queries land on${rowsNote}`
      : `\`${schema[t] ? schema[t] + "." : ""}${t}\`${rowsNote}`;
    const colsHere = (agree ? [] : [["total_rows", "rows", 0]]).concat(metrics);
    // PHYSICAL LAYOUT AND NOTHING ELSE. The mart block carried the analytics CU and the three query
    // tiers, so one table was answering two questions — what the parquet looks like, and what it cost
    // and took to query it. Those belong to the run that measured them: the CU is in the charts and in
    // *Cost by engine*, and the tiers moved to the per-run table, where each row is one dispatch
    // rather than one layout. What is left here is what `stats.py` read off the Delta log.
    const header = ["layout", ...colsHere.map(([, h]) => h), "V-Order"];
    const align = ["left", ...colsHere.map(() => "right"), "left"];
    const body = present.map(({ name, d }) => [
      name,
      ...colsHere.map(([k, , dp]) => (d[k] === undefined || d[k] === null ? DASH
        : dp < 0 ? compact(d[k]) : fmt(d[k], dp))),
      d.vorder ? "**yes**" : "·",
    ]);
    blocks.push({ name: t,
      html: `<h4>${inline(head)}</h4>\n` + table(header, align, body, { sort: true }) });
  }
  // ONE BLOCK VISIBLE AT A TIME. Eight stacked tables buried the mart under seven it explains; a
  // tab per table keeps them all one click away without the scroll. CSS-only — radio inputs, no
  // JS — so the offline snapshot and a script-blocked browser behave identically, and every panel
  // stays in the DOM (the tests and ctrl-F read all of them; print shows all). The stylesheet's
  // nth-of-type pairing is enumerated to 12 panels, so past that this falls back to stacking
  // rather than rendering tabs whose panels could never show.
  if (blocks.length > 1 && blocks.length <= 12) {
    const inputs = blocks.map((_, i) =>
      `<input type="radio" name="layout-tab" id="lt-${i}"${i === 0 ? " checked" : ""}>`).join("");
    const labels = blocks.map((b, i) => `<label for="lt-${i}">${esc(b.name)}</label>`).join("");
    out.push(`<div class="tabs">${inputs}<nav class="tab-nav">${labels}</nav>\n` +
      blocks.map((b) => `<section>\n${b.html}\n</section>`).join("\n") + "</div>");
  } else {
    for (const b of blocks) out.push(b.html);
  }
  out.push(fold("how these layouts were read",
    "Every shared table the project writes, in pipeline order, as `stats.py` read the " +
    "Delta log in that run's **layout** job. Sizes are what the tables held at that moment, and " +
    "nothing here re-read a Delta log. **This block is PHYSICAL LAYOUT ONLY** — the analytics CU and " +
    "the `cold`/`warm`/`hot` milliseconds used to sit beside the mart, which made one table answer " +
    "both what the parquet looks like and what querying it cost. The cost is in the charts and in " +
    "*Cost by engine*; the times are in the per-run table below, where a row is one dispatch rather " +
    "than one layout. **A row is a WRITER, not a dispatch:** the core count and the NEE flag are left " +
    "off because two runs each showed they never reach the parquet — duckrun wrote 4 files and 27 row " +
    "groups at 64 cores and at 32, and spark wrote the same layout with NEE on and off — so " +
    "everything but the resource profile and the sort collapses to one row. The mart is the " +
    "exception: it splits by the shape a run actually WROTE, so a writer that produced two different " +
    "layouts gets a row each. Row counts sit in the heading because they are identical by design; if " +
    "they ever stop being, the heading says so and they come back as a column."));
  return out.join("\n");
}

// ------------------------------------------------------------------------------------- query time

/**
 * `{query: {metric: ms}}` for one run. One record measured ONE engine, so there is one semantic model
 * in it; a record holding two would merge, last wins.
 */
export function benchTimings(rec) {
  const out = {};
  for (const queries of Object.values(((rec || {}).benchmark || {}).timings || {})) {
    for (const [q, t] of Object.entries(queries || {})) {
      if (t && typeof t === "object") out[q] = t;
    }
  }
  return out;
}

/**
 * `{totals, n}` over the query set EVERY column carries at this metric.
 *
 * The common set, not each column's own, because a total over different queries is not a comparison —
 * and it genuinely differs by metric here, not just by engine: the selectivity-ladder queries
 * `sel_1duid`/`sel_1duid_1mo` have no `cold_ms` at all, since the top DUID is only resolved after pass
 * 1. Cold is therefore summed over two fewer queries than warm and hot, which is why the count is
 * returned and printed rather than left to be inferred from a total that looks small.
 */
export function benchTotals(perCol, metric) {
  const entries = Object.entries(perCol);
  if (!entries.length) return { totals: {}, n: 0 };
  const sets = entries.map(([, timings]) => new Set(
    Object.entries(timings || {}).filter(([, t]) => t[metric] !== undefined && t[metric] !== null)
      .map(([q]) => q)));
  let common = [...sets[0]];
  for (const s of sets.slice(1)) common = common.filter((q) => s.has(q));
  if (!common.length) return { totals: {}, n: 0 };
  const totals = {};
  for (const [col, timings] of entries) {
    totals[col] = round1(common.reduce((a, q) => a + Number(timings[q][metric]), 0));
  }
  return { totals, n: common.length };
}

/**
 * `{times: {column: {tier: ms}}, counts: {tier: n queries}}` — the whole DAX suite, per pass position.
 *
 * Feeds three columns of the mart block and nothing else. There is no query-time section: a table of
 * its own put the layout and the speed it produced side by side on the PAGE but not on the same ROW,
 * and the only question worth asking of these numbers is whether one explains the other.
 */
export function queryTime(cols) {
  const perCol = {};
  for (const { col, rec } of cols) {
    const t = benchTimings(rec);
    if (Object.keys(t).length) perCol[col] = t;
  }
  if (!Object.keys(perCol).length) return { times: {}, counts: {} };
  const times = {}, counts = {};
  for (const [label, metric] of TIERS) {
    const { totals, n } = benchTotals(perCol, metric);
    if (!n) continue;
    counts[label] = n;
    for (const [col, ms] of Object.entries(totals)) {
      times[col] = times[col] || {};
      times[col][label] = ms;
    }
  }
  return { times, counts };
}

// ---------------------------------------------------------------------------------- the analysis
//
// The page RANKS — cheapest bar first, `Cost and speed by layout` sorted by CU — and never says
// whether a ranking is a result or a coin toss. A reader sees `spark readHeavyForPBI` at 1,381 CU
// above `duckrun` at 1,794 with no way to learn that the gap is the size of either one's own
// run-to-run wobble. This section attaches a margin, a sample size and a verdict to the findings the
// charts already imply.
//
// EVERY CLAIM IS COMPUTED, none is written down. The page reads `history/` in the reader's browser
// on every load, so prose naming a winner goes stale the moment a run lands. The winners, the knobs
// compared and the yardstick itself are all derived from the same joined data the charts are drawn
// from — the rule `layoutLabel` and the lede already follow.

export const MEASURES = ["etl", "analytics", ...TIERS.map(([l]) => l)];

/** The value a config key has when it is ABSENT. A pair differing only by a key one side never
 *  recorded is a real comparison — it is how `sorted` and NEE become findable — and this is what
 *  lets it print as one. */
const OFF = "off";

const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);

const median = (vals) => {
  const a = [...vals].sort((x, y) => x - y);
  if (!a.length) return 0;
  const mid = a.length / 2;
  return a.length % 2 ? a[Math.floor(mid)] : (a[mid - 1] + a[mid]) / 2;
};

/**
 * `{n, mean, min, max, rel}` over one cell's readings, or `null` when it has none.
 *
 * `rel` is `(max - min) / mean` — the RELATIVE spread, which is the only form comparable between a
 * capacity unit and a millisecond. Zeros are dropped rather than averaged in, the same rule
 * `groupMean` follows: a run that measured nothing is not a run that measured zero.
 */
export function spread(vals) {
  const v = (vals || []).filter((x) => x);
  if (!v.length) return null;
  const mean = v.reduce((a, b) => a + b, 0) / v.length;
  const min = Math.min(...v), max = Math.max(...v);
  return { n: v.length, mean, min, max, rel: mean ? (max - min) / mean : 0 };
}

/**
 * `{column: {measure: [reading per run]}}` across every measure this page carries.
 *
 * The ETL half comes through `spreadFor` rather than being re-derived, so the floor is measured from
 * the very samples the ETL chart's whisker draws. The analytics and tier halves come off `entries`,
 * which is the only place a run's own CU and its own timings are keyed together.
 */
export function columnSamples(runs, ledger, keyOf, entries, times) {
  const out = {};
  const at = (col) => (out[col] = out[col] || Object.fromEntries(MEASURES.map((m) => [m, []])));
  for (const [col, vals] of Object.entries(spreadFor(runs, ledger, "etl", keyOf))) {
    at(col).etl.push(...vals);
  }
  for (const e of entries || []) {
    if (e.col === undefined || e.col === null) continue;
    const b = at(e.col);
    if (e.cu) b.analytics.push(e.cu);
    const t = (times || {})[e.qid] || {};
    for (const [label] of TIERS) if (t[label]) b[label].push(t[label]);
  }
  return out;
}

/**
 * `{measure: {n, rel, lo, hi} | null}` — THE NOISE FLOOR, measured rather than assumed.
 *
 * A repeated CELL is one column run more than once: same engine, same config, nothing changed but the
 * hour it ran in. Its relative spread is therefore what this page's numbers do when the answer should
 * be identical, and that is the only yardstick these sample sizes can support. The floor is the MEDIAN
 * across such columns; `lo`/`hi` ride along because one 0.6% repeat beside one 17.8% repeat is itself
 * a fact about the measure.
 *
 * **COLUMN-LEVEL, NEVER GROUP-LEVEL.** A layout group mixes configurations — the `duckrun` bar holds
 * six runs at five different core counts — so its internal spread is partly a real config effect and
 * would inflate the floor into hiding the very differences this section is looking for.
 *
 * `null` for a measure nothing has repeated at, which is a state the section STATES rather than one it
 * papers over with a zero.
 */
export function noiseFloor(samples, measures = MEASURES) {
  const out = {};
  for (const m of measures) {
    const rels = [];
    for (const bucket of Object.values(samples || {})) {
      const s = spread((bucket || {})[m]);
      if (s && s.n > 1) rels.push(s.rel);
    }
    out[m] = rels.length
      ? { n: rels.length, rel: median(rels), lo: Math.min(...rels), hi: Math.max(...rels) }
      : null;
  }
  return out;
}

/**
 * The verdict on one margin: `tie` · `within spread` · `beyond spread` · `no repeat`.
 *
 * **NO P-VALUE, AND THAT IS THE HONEST ANSWER RATHER THAN A MISSING FEATURE.** Four cells on this page
 * have been measured more than once, at n of 2 or 3; the runs share one capacity and one 24-hour
 * smoothing window, so they are not independent draws. A t-test on two degrees of freedom would put a
 * decimal point on a claim the design cannot carry. What the data does support is the comparison
 * above: is this gap bigger than the gap the same thing shows against itself.
 *
 * The range check is appended only when BOTH sides repeat — with one reading there is no range, and
 * asserting separation from a single point is the error this whole section exists to avoid.
 */
export function verdictOf(rel, floor, a = null, b = null) {
  if (!rel) return "tie";
  if (!floor) return "no repeat";
  if (Math.abs(rel) <= floor.rel) return "within spread";
  if (!(a && b && a.n > 1 && b.n > 1)) return "beyond spread";
  return a.max < b.min || b.max < a.min
    ? "beyond spread, ranges disjoint" : "beyond spread, ranges overlap";
}

/**
 * `[{label, unit, winner, value, runnerUp, margin, a, b, verdict}]` — what the page ranks, and whether
 * the ranking holds.
 *
 * **NOTHING IS DERIVED A SECOND TIME.** The analytics and tier means come from `martPoints` — the
 * same object the chart's bars and `Cost and speed by layout` quote — so this table cannot print
 * 1,916 under a bar showing 1,960. The ETL means come through `spreadFor`, which is what `Cost by
 * engine` reads for the same columns.
 *
 * ETL ranks COLUMNS and everything else ranks LAYOUT GROUPS, matching the chart exactly: Power BI
 * never sees the engine, so what a query cost belongs to what was written.
 */
export function findings(cols, samples, groups, times, floors) {
  const rows = [];
  const rank = (label, unit, cands, floor) => {
    const ok = cands.filter((c) => c.s && c.value > 0);
    // One candidate is not a ranking. Nothing to be runner-up to means nothing to report.
    if (ok.length < 2) return;
    ok.sort((x, y) => x.value - y.value);
    const [a, b] = ok;
    const margin = a.value ? (b.value - a.value) / a.value : 0;
    rows.push({ label, unit, winner: a.name, value: a.value, runnerUp: b.name, margin,
      a: a.s, b: b.s, verdict: verdictOf(margin, floor, a.s, b.s) });
  };
  rank("cheapest to build", "CU", (cols || []).map(({ col }) => {
    const s = spread(((samples || {})[col] || {}).etl);
    return { name: col, s, value: s ? s.mean : 0 };
  }), (floors || {}).etl);

  // `martPoints` and `groups` are both in group order — the former `.map`s over the latter — so index
  // is a safe join and the printed mean stays the one the bar drew.
  const pts = martPoints(groups || [], times);
  const members = (groups || []).map(([, ms]) => ms);
  rank("cheapest to query", "CU", pts.map((p, i) => ({
    name: p.name, s: spread(members[i].map((m) => m.cu)), value: p.cu,
  })), (floors || {}).analytics);
  for (const [label] of TIERS) {
    rank(`fastest ${label}`, "ms", pts.map((p, i) => ({
      name: p.name,
      s: spread(members[i].map((m) => ((times || {})[m.qid] || {})[label])),
      value: p.ms[label],
    })), (floors || {})[label]);
  }
  return rows;
}

/**
 * `[{engine, key, from, to, a, b}]` — every pair of ONE engine's columns differing in exactly one
 * `variant()` key.
 *
 * The only place on this page where one variable moves and the rest are held fixed. V-Order, NEE,
 * `sorted` and core scaling all fall out of this rule and **not one of them is named in the code**,
 * which is what keeps a fifth knob working without an edit here.
 *
 * **ABSENCE IS A VALUE.** `{vcores:64}` against `{vcores:64, sorted:true}` is a pair reading
 * `off → true` — a flag that is off is simply not recorded (see `variantTag`), so treating a missing
 * key as "no comparison" would hide every on/off knob the project has.
 *
 * LOWER VALUE LEADS, ordered by `compareCells` — the page's own numeric-when-both-parse rule — so
 * `8 → 16 → 32 → 64` orders numerically and `off → true` as text. One rule, no per-key table, and a
 * delta always reads as "what turning it up did".
 */
export function variantPairs(cols) {
  const byEngine = new Map();
  for (const c of cols || []) {
    const list = byEngine.get(c.engine) || [];
    list.push({ col: c.col, cfg: Object.fromEntries(variant(c.rec)) });
    byEngine.set(c.engine, list);
  }
  const out = [];
  for (const [engine, list] of byEngine) {
    for (let i = 0; i < list.length; i++) {
      for (let j = i + 1; j < list.length; j++) {
        let A = list[i], B = list[j];
        const keys = [...new Set([...Object.keys(A.cfg), ...Object.keys(B.cfg)])];
        const diff = keys.filter((k) =>
          hasOwn(A.cfg, k) !== hasOwn(B.cfg, k) || A.cfg[k] !== B.cfg[k]);
        if (diff.length !== 1) continue;
        const key = diff[0];
        let from = hasOwn(A.cfg, key) ? A.cfg[key] : OFF;
        let to = hasOwn(B.cfg, key) ? B.cfg[key] : OFF;
        if (compareCells(from, to) > 0) { [A, B] = [B, A]; [from, to] = [to, from]; }
        out.push({ engine, key, from, to, a: A.col, b: B.col });
      }
    }
  }
  const order = new Map(ENGINES.map((e, i) => [e, i]));
  const at = (e) => (order.has(e) ? order.get(e) : order.size);
  out.sort((x, y) => at(x.engine) - at(y.engine) || (x.engine < y.engine ? -1 : x.engine > y.engine
    ? 1 : 0) || x.key.localeCompare(y.key, "en") || compareCells(x.from, y.from)
    || compareCells(x.to, y.to));
  return out;
}

/** `+26.0` / `-4.7`, no `%` sign — `cellNumber` rejects one, and the sortable header would then text
 *  sort `+138.4` below `+26.0`. The unit is in the column head. */
const pct = (v) => `${v >= 0 ? "+" : ""}${fmt(v * 100, 1)}`;

/**
 * `<h3>Analysis</h3>` and its two tables, or `""` when there is nothing to compare.
 *
 * Positional for what every renderer takes, a trailing bag for the rest — the shape `renderSources`
 * already uses.
 */
export function renderAnalysis(cols, entries, groups, times, ctx = {}) {
  const { runs = [], ledger = { items: {} }, keyOf = () => undefined, table: martTable, counts = {},
    reference = null } = ctx;
  const samples = columnSamples(runs, ledger, keyOf, entries, times);
  const floors = noiseFloor(samples);
  const rows = findings(cols, samples, groups, times, floors);
  const pairs = variantPairs(cols);

  // Which columns wrote which layouts — the single most interpretive fact in the knob table. Two
  // columns sharing a bar means their analytics and tier deltas are two readings of ONE layout rather
  // than a comparison of two.
  const gs = new Map();
  (groups || []).forEach(([, ms], i) => {
    for (const m of ms) {
      if (m.col === undefined || m.col === null) continue;
      if (!gs.has(m.col)) gs.set(m.col, new Set());
      gs.get(m.col).add(i);
    }
  });
  const layoutRel = (a, b) => {
    const A = gs.get(a), B = gs.get(b);
    if (!A || !B) return DASH;
    if (![...A].some((i) => B.has(i))) return "differs";
    return A.size === 1 && B.size === 1 ? "same" : "mixed";
  };

  const delta = (col, other, m) => {
    const x = spread(((samples[col] || {})[m])), y = spread(((samples[other] || {})[m]));
    if (!x || !y || !x.mean) return null;
    return (y.mean - x.mean) / x.mean;
  };
  const shown = MEASURES.filter((m) => pairs.some((p) => delta(p.a, p.b, m) !== null));
  const pairRows = pairs.map((p) => [
    `\`${p.key}\` ${p.from} → ${p.to}`, `${p.a} → ${p.b}`, layoutRel(p.a, p.b),
    `${(spread((samples[p.a] || {}).etl) || { n: 0 }).n} vs ` +
      `${(spread((samples[p.b] || {}).etl) || { n: 0 }).n}`,
    ...shown.map((m) => {
      const d = delta(p.a, p.b, m);
      if (d === null) return DASH;
      const floor = floors[m];
      // Bold means "clears the floor". Never the first cell — `table()` reads a leading `**` as a
      // subtotal row and would rule the whole row off.
      return floor && Math.abs(d) > floor.rel ? `**${pct(d)}**` : pct(d);
    }),
  ]);

  if (!rows.length && !pairRows.length) return "";

  const out = ["<h3>Analysis</h3>"];

  // THE SCOPE CAVEAT, VISIBLE AND FIRST. Repo convention folds explanation and never folds anything
  // that qualifies a number; this qualifies every number below it. Derived, so it cannot drift from
  // what it describes — only the standing "one workload" clause is fixed prose.
  const dates = (runs || []).map((r) => String((r.run || {}).started || "").slice(0, 10))
    .filter(Boolean).sort();
  const engines = new Set((cols || []).map(({ col }) => baseEngine(col))).size;
  const queries = Math.max(0, ...Object.values(counts || {}));
  const scope = [`\`${martTable || DEFAULTS.table}\``];
  // The generation's reference count when the caller has one, else the newest run that recorded one.
  // The size of the thing measured is a fact about the data, not about who called this.
  let rowCount = reference;
  for (let i = runs.length - 1; i >= 0 && (rowCount === null || rowCount === undefined); i--) {
    rowCount = martRows(runs[i], martTable || DEFAULTS.table);
  }
  if (rowCount) scope.push(`${fmt(rowCount, 0)} rows`);
  if (queries) scope.push(`${queries} DAX queries`);
  scope.push(`${runs.length} run(s) across ${cols.length} configuration(s) of ${engines} engine(s)`);
  if (dates.length) {
    scope.push(dates[0] === dates[dates.length - 1] ? `on ${dates[0]}`
      : `between ${dates[0]} and ${dates[dates.length - 1]}`);
  }
  out.push(note("**One dataset, one query suite, one capacity.** Everything below describes this " +
    `project and nothing wider — ${scope.join(", ")}, on a single Fabric capacity. A different data ` +
    "shape, cardinality or query mix could reorder every row here, and none of it has been tried on " +
    "a second workload. Read these as findings about this benchmark, not about the engines."));

  const floorBits = MEASURES.filter((m) => floors[m])
    .map((m) => `${m === "etl" || m === "analytics" ? `${m} CU` : m} ${fmt(floors[m].rel * 100, 1)}%`);
  const repeats = Math.max(0, ...MEASURES.map((m) => (floors[m] ? floors[m].n : 0)));
  out.push(note(floorBits.length
    ? `**The yardstick is measured, not assumed.** ${repeats} column(s) here have been run more than ` +
      "once with nothing changed, and the median spread between their own repeats is " +
      `${floorBits.join(" · ")}. A margin inside that reads \`within spread\`. ` +
      "**No p-value is offered** — see below for why."
    : "**Nothing on this page has been measured twice**, so there is no floor to judge a margin " +
      "against and every verdict reads `no repeat`. The margins below are real; whether they would " +
      "survive a second run of the same configuration is not yet known."));

  if (rows.length) {
    out.push("<h4>Where the rankings hold</h4>");
    out.push(table(["finding", "winner", "value", "runner-up", "margin %", "runs", "verdict"],
      ["left", "left", "right", "left", "right", "left", "left"],
      rows.map((r) => [r.label, r.winner, fmt(r.value, r.unit === "ms" ? 0 : 1), r.runnerUp,
        fmt(r.margin * 100, 1), `${r.a.n} vs ${r.b.n}`, r.verdict]),
      { sort: true }));
  }
  if (pairRows.length) {
    out.push("<h4>One knob at a time</h4>");
    out.push(table(["change", "columns", "layout", "runs",
      ...shown.map((m) => (m === "etl" || m === "analytics" ? `${m} CU Δ%` : `${m} Δ%`))],
      ["left", "left", "left", "left", ...shown.map(() => "right")],
      pairRows, { sort: true }));
  }

  out.push(fold("why there is no p-value, and what the floor is instead",
    "**The sample cannot carry a significance test.** Only a handful of configurations here have " +
    "been run more than once, at two or three runs each, and those runs share one capacity and one " +
    "24-hour smoothing window — so they are not independent draws. A t-test on two degrees of " +
    "freedom would put a decimal point on a claim the design does not support.",
    "**What the repeats DO support is a floor.** Running the same engine at the same configuration " +
    "again changes nothing but the hour, so the gap between those two readings is what this page's " +
    "numbers do when the answer should be identical. Any margin smaller than that is reported as " +
    "`within spread`, which does not mean the ranking is wrong — it means this page cannot tell it " +
    "apart from the wobble, and the next run of either side could reverse it. " +
    "Where both sides of a comparison have been run twice, whether their min–max ranges overlap is " +
    "stated as well — a gap between two means whose ranges still overlap is weaker than one whose " +
    "ranges are disjoint.",
    "**The floor is measured per COLUMN, never per layout bar.** A bar groups every run that wrote " +
    "the same parquet shape, which on this page means several core counts at once, so its internal " +
    "spread already contains a real effect and would hide the differences this section looks for.",
    "**In the knob table a bold delta is one that clears the floor for its measure.** `layout` says " +
    "whether the two columns wrote parquet Power BI can tell apart: `same` means their analytics " +
    "and query-time deltas are two readings of one bar rather than a comparison, so a difference " +
    "there is noise by construction."));
  return out.join("\n");
}

// -------------------------------------------------------------------------------------- the lede

/** The shared table list a run recorded, or the tables it filed stats for. */
const tableNames = (rec) => {
  const layout = (rec || {}).layout || {};
  if (Array.isArray(layout.tables) && layout.tables.length) return layout.tables;
  return Object.keys((layout.stats || {})[(rec || {}).engine] || {});
};

/**
 * `1 fact (144.0M), 2 dimensions (4.0K), 4 staging (375.4M) and a log (3.2K)` — the table count
 * decomposed, each part carrying its own rows.
 *
 * **THE `fct_` PREFIX IS NOT THE CLASSIFIER, and reading it as one got this wrong.** Four of the
 * five `fct_*` tables — `fct_price`, `fct_scada` and their `_today` siblings — are raw AEMO CSV
 * landed in the `landing` schema. Only `fct_summary` reaches `mart`, and it is the one actual fact
 * table: the `(date, time, DUID)` grain Power BI queries. So the split is the MART TABLE against
 * everything else, not the prefix, and the record's own `schema` field says so.
 *
 * **THE ROWS ARE WHAT MAKE THE BREAKDOWN WORTH READING.** "1 fact, 2 dimensions, 4 staging and a
 * log" describes the SHAPE and hides the scale, which on this project is the interesting half: the
 * four staging tables carry 375M rows of raw CSV and the one fact carries 144M, while the two
 * dimensions are four thousand. A reader seeing only the total — 519,377,319 — cannot tell whether
 * it is one huge table or eight middling ones.
 *
 * Compacted (`144.0M`, not `143,980,961`): the sentence already ends with the exact total, so a
 * second set of twelve-digit numbers inside it is precision nobody reads, in the place it is
 * hardest to read.
 *
 * **Counts are all-or-nothing, the same rule `totalRows` follows.** One table missing its
 * `total_rows` drops the numbers from EVERY part rather than printing a category short — a
 * breakdown quietly missing a table sits beside a total that includes it and contradicts it. The
 * shape still goes out; only the numbers are withheld.
 *
 * Returns `""` when the parts do not add up to the whole list, for the same reason.
 */
const tableShape = (names, martTable, stats = null) => {
  const of = (pred) => names.filter(pred);
  const groups = [
    ["1 fact", of((t) => t === martTable)],
    [null, of((t) => t.startsWith("dim_"))],
    [null, of((t) => t.startsWith("fct_") && t !== martTable)],
    [null, of((t) => t.startsWith("stg_"))],
  ];
  const [fact, dims, stg, logs] = groups.map(([, ts]) => ts.length);
  if (fact + dims + stg + logs !== names.length) return "";

  // One unmeasured table withholds every number, never just its own category's.
  const rowsOf = (ts) => {
    if (!stats) return null;
    let total = 0;
    for (const t of ts) {
      const v = Number((stats[t] || {}).total_rows);
      if (!Number.isFinite(v)) return null;
      total += v;
    }
    return total;
  };
  const counts = groups.map(([, ts]) => rowsOf(ts));
  const measured = counts.every((c) => c !== null);
  const label = (text, i) => (measured ? `${text} (${compact(counts[i])})` : text);

  const parts = [];
  if (fact) parts.push(label("1 fact", 0));
  if (dims) parts.push(label(`${dims} dimension${dims !== 1 ? "s" : ""}`, 1));
  if (stg) parts.push(label(`${stg} staging`, 2));
  if (logs) parts.push(label(logs === 1 ? "a log" : `${logs} logs`, 3));
  if (parts.length < 2) return "";
  return `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
};

/**
 * Every shared table's rows added up, for ONE run — or `null` if any of them is missing.
 *
 * **A partial sum is dropped, never printed.** Seven tables of eight, labelled "in total", is a
 * WRONG number rather than an incomplete one, and it would sit on the page looking entirely
 * plausible. Same rule as the ledger's dash-instead-of-zero: absent says "not measured", a number
 * says something that is not true.
 *
 * Summed over the run's own table LIST rather than over every key of its stats block, so a key
 * `stats.py` adds outside that list cannot silently inflate it — and the tables it does add up are
 * exactly the ones the layout tab strip prints, so a reader can check this against the page.
 */
export function totalRows(rec, names = tableNames(rec)) {
  const stats = (((rec || {}).layout || {}).stats || {})[(rec || {}).engine] || {};
  if (!names.length) return null;
  let total = 0;
  for (const t of names) {
    const v = Number((stats[t] || {}).total_rows);
    if (!Number.isFinite(v)) return null;
    total += v;
  }
  return Math.trunc(total);
}

/**
 * ONE SENTENCE SAYING WHAT THIS IS — how much goes in, what comes out, on how many engines.
 *
 * The page led with `Capacity units` and went straight into the charts, so it named its MEASURE and
 * never its SUBJECT: a reader arriving on a link saw four columns of CU with no statement of the
 * scale any of it describes. The `<h1>` in the shell says what the project is; this says how big.
 *
 * **Every number is DERIVED from the records the page already loaded.** A hardcoded `170 GB` goes
 * stale the first dispatch that runs with `skip_download` off, and goes stale SILENTLY — the exact
 * failure this repo is built against. It also reads the landing block through `landingBlocks`, the
 * same call the `Input archive` table makes, so the top and the foot of the page cannot quote
 * different archives.
 *
 * **An absent input is an absent clause, never a zero** — the rule the `compute seconds` row and the
 * `cold`/`warm`/`hot` columns already follow. With nothing measurable at all it renders nothing
 * rather than a sentence made of dashes.
 *
 * On the unit: `stats.py` stores `bytes / 1048576`, so `size_mb` is really MiB and the archive is
 * 178.8 GB decimal. This prints `size_mb / 1000` because that is the figure which agrees on sight
 * with the `170,491.5 MB` in the `Input archive` table on this same page; raw bytes never reach the
 * record, so there is no exact byte figure to print instead.
 */
export function pageLede(cols, opts = {}) {
  const martTable = opts.table || DEFAULTS.table;
  const targets = new Set(cols.map(({ col }) => baseEngine(col)));
  // Alphabetical, the same order the side-by-side columns use — the only order that is neutral
  // between peers and stable enough to read two runs against each other.
  const families = [...new Set([...targets].map((e) => ENGINE_FAMILY[e] || e))].sort();
  const n = families.length;
  if (!n) return "";

  const land = (landingBlocks(cols).pop() || ["", {}])[1];
  const gb = Number(land.size_mb) / 1000;
  const files = Number(land.files);
  const input = Number.isFinite(gb) && gb > 0
    ? `**${fmt(gb, 0)} GB** of raw AEMO CSV` +
      (Number.isFinite(files) && files > 0 ? ` (**${fmt(files, 0)} files**)` : "")
    : "";

  const withTables = cols.filter(({ rec }) => tableNames(rec).length);
  const rec = ((withTables[withTables.length - 1] || cols[cols.length - 1] || {}).rec) || {};
  const names = tableNames(rec);
  const shape = tableShape(names, martTable,
    ((rec.layout || {}).stats || {})[rec.engine] || null);
  const tables = names.length
    ? `the same **${fmt(names.length, 0)} table${names.length !== 1 ? "s" : ""}**` +
      (shape ? ` — ${shape} —` : "")
    : "";
  const rows = totalRows(rec, names);

  const made = [input, tables && `built into ${tables}`].filter(Boolean).join(" ");
  if (!made && rows === null) return "";
  // NAMED, not just counted. "3 engines" states the scale and withholds the subject — a reader
  // arriving on a link had to scroll to a chart's column headers to learn WHICH three, and the
  // answer is the whole point of the project. One engine names itself and drops the count, which
  // would otherwise read "1 engine (spark)".
  //
  // Parenthesised rather than set off with dashes, because the clause that may follow is itself a
  // dashless insert: "**3 engines** — duckdb, dwh and spark across **4 dbt targets**" reads as the
  // list swallowing the targets, and a closing dash only works when that clause is present.
  const named = n === 1
    ? `**${families[0]}**`
    : `**${n} engines** (${families.slice(0, -1).join(", ")} and ${families[n - 1]})`;
  let sentence = `One dbt project on ${named}`;
  // Only said when it differs: "3 engines across 4 targets" is the DuckDB pair writing two table
  // formats, and a table format is not an engine.
  if (targets.size > n) sentence += ` across **${targets.size} dbt targets**`;
  if (made) sentence += `: ${made}`;
  if (rows !== null) {
    // With the breakdown present its closing dash already separates this; without one the phrase
    // needs its own comma, and with no preceding clause at all it opens the sentence instead.
    const r = `**${fmt(rows, 0)} row${rows !== 1 ? "s" : ""}**`;
    sentence += made ? `${shape ? " " : ", "}totalling ${r}` : `: ${r} in total`;
  }
  return para(`${sentence}.`, "lede");
}

// ------------------------------------------------------------------------------------- the whole

/**
 * The whole page BODY as one HTML string.
 *
 * NUMBERS FIRST. What this page is for is the charts and the table under them; a reader who already
 * knows what a capacity unit is should not have to scroll past a paragraph explaining it and a
 * provenance table to reach them.
 *
 * AND ANALYTICS FIRST OF THE TWO, which is the point of the whole project. Fabric smooths BACKGROUND
 * operations — the build — over 24 hours, so a heavy ETL leg is absorbed. Query CU is INTERACTIVE,
 * smoothed over minutes, and it is what throttles: it is the CU a user waits behind and a capacity
 * admin notices. An engine that builds cheaply and queries expensively has optimised the half that
 * does not hurt.
 */
export function renderPage(cols, runs, ledger, opts = {}) {
  const repo = opts.repo || DEFAULTS.repo;
  const martTable = opts.table || DEFAULTS.table;
  const now = opts.now === undefined ? null : opts.now;
  const perCol = {};
  for (const { col, rec } of cols) perCol[col] = runCu(rec, ledger).cells;

  const newest = cols.map(({ rec }) => (rec.run || {}).started || "").sort().pop() || "";
  const asOf = String(ledger.updated || newest || "?").slice(0, 16).replace("T", " ");
  // FIRST — the scale of the thing, under the `<h1>` the shell carries. `Capacity units` names the
  // measure and stays where it is, now heading the section it always described rather than the page.
  const out = [pageLede(cols, { table: martTable }),
    `<h2>Capacity units <span class="asof">the latest run per engine, as of ` +
    `${esc(asOf)}</span></h2>`];

  // EVERY run maps to its column, not just the one the column was named after: the chart's mean is
  // over an engine's whole history at that configuration, and matching on the chosen record's filename
  // would have collapsed every sample but the newest.
  const byVariant = new Map(cols.map(({ col, rec }) =>
    [JSON.stringify([baseEngine(col), variant(rec)]), col]));
  const keyOf = (rec) => byVariant.get(JSON.stringify([rec.engine, variant(rec)]));

  // ONE ENTRY PER RUN, carrying that run's own analytics CU and a key into its own query timings. The
  // analytics half groups on the parquet a run MEASURED, and two runs of one column can write different
  // parquet — grouping the columns and averaging their runs is what put a 3-file and a 4-file
  // `duckrun sorted` in one bar, at a mean belonging to neither. `qid` is the entry's index because a
  // record has no id of its own that is guaranteed present.
  const anaEntries = [];
  for (const rec of runs) {
    const col = keyOf(rec);
    if (col === undefined || col === null) continue;
    anaEntries.push({ col, rec, qid: String(anaEntries.length),
      cu: classTotal(runCu(rec, ledger).cells, "analytics") });
  }
  const groups = layoutGroups(anaEntries, martTable);

  // ONE BAR PER LAYOUT, not per engine — Power BI never sees the engine. It opens parquet through
  // Direct Lake and transcodes row groups, so what a query costs belongs to what was written and the
  // writer is metadata; the caption carries it. The ETL chart is the exact opposite and stays per
  // column, because there the writer and the compute it was given ARE the subject.
  //
  // ONE CHART, FULL WIDTH, and it is the analytics one. There was a second bar chart beside it
  // ranking what each column cost to BUILD, and dropping it is this page's own thesis applied to
  // itself: `About these numbers` says analytics is the half that matters — background CU is
  // smoothed over 24 hours and nobody waits for it, query CU is interactive and is what throttles —
  // while the layout gave both equal visual weight and put the half that does not hurt beside it.
  // The ETL numbers did not go anywhere: `Cost by engine` carries them per bucket on the run that
  // measured them, and `Analysis` ranks them with a margin and a verdict, which is more than a bar
  // ever said. What is bought is the prose measure instead of a 53rem flex child — the widest a
  // chart gets here, since the SVG is drawn at a 660-unit viewBox and a wider box would inflate
  // every label with it.
  // The title names the measure and the grouping, and the subtitle says which way is good. It used
  // to be `Analytics — what querying each LAYOUT cost` over `capacity units, lower is better —
  // INTERACTIVE CU, and Power BI sees only the parquet`: three qualifications carried because there
  // was a second chart to be told apart from. With one chart the contrast has nobody to draw, and
  // both facts it carried are already stated where they can be read properly — `Analytics is the
  // half that matters` in the methodology explains INTERACTIVE against background CU, and `per
  // parquet layout` is itself the statement that Power BI sees the parquet and not the engine.
  out.push(chartSvg("Capacity units per parquet layout", "lower is better",
    groupRows(groups, martTable)));
  // The one place the ADAPTERS are named and linked. The bars stopped captioning them because the
  // column name already implies the adapter — this line is where that implication resolves.
  //
  // ONE PER LINE. Four `name — what it is` pairs joined with `·` ran together as a single wrapped
  // paragraph, where the separator between two entries looked exactly like the separator inside one;
  // a reader scanning for "which adapter is dwh" had to parse the line to find out. Broken, each row
  // is one engine and the em dash only ever separates a name from its description.
  out.push(note("The adapters:<br>" + ENGINES
    .filter((e) => ADAPTER_URLS[e])
    .map((e) => `[${STACK[e][0]}](${ADAPTER_URLS[e]}) — ${STACK[e][1]}`)
    .join("<br>")));

  // The mart block and the fit chart quote the SAME numbers as the analytics bars above — the same
  // groups, the same members, the same mean, all of it through `martPoints`. They are one measurement
  // described three ways, and a page printing dwh at 1,916 in a bar and 1,960 in the row under it
  // would be inviting the reader to work out which one it meant. The timings are keyed per RUN for the
  // same reason the CU is: a group's tiers are its own runs' mean, not its column's newest record.
  const { times, counts } = queryTime(anaEntries.map(({ qid, rec }) => ({ col: qid, rec })));

  // What each layout cost to query and how long it took, together. `Table layout` is physical layout
  // alone; these are the numbers that used to sit beside it.
  out.push(renderFit(groups, times, TIERS.map(([l]) => l).filter((l) => l in counts)));

  out.push("<h3>Cost by engine</h3>");
  const secsCol = Object.fromEntries(cols.map(({ col, rec }) =>
    [col, runCu(rec, ledger, "seconds").cells]));
  out.push(engineTable(perCol, cols, secsCol));
  out.push(renderLayouts(cols, groups, times, counts, martTable));

  // WHAT WENT IN, then the per-run table, then the prose. Every TABLE the page has comes before every
  // paragraph about them: a reader arrives for the numbers, and `About these numbers` sat between the
  // layout tables and the run table pushing the last one below a screen of methodology.
  out.push(renderInput(cols));

  out.push(renderSources(cols, anaEntries, ledger, repo, now,
    { dropped: opts.dropped, reference: opts.reference, table: martTable, ref: opts.ref,
      times, counts }));
  // A record that is not a whole generation — a failed run that never benchmarked, a build half
  // that never reported — is skipped, and NAMED HERE with its reason. It used to be only a count in
  // the live status line, which the offline copy does not even have: a page that quietly ignores a
  // record is indistinguishable from a page that never had it. Visible, not folded — same rule as
  // the generation exclusions above.
  const skipped = opts.skipped || [];
  if (skipped.length) {
    out.push(note(`**${skipped.length} record(s) skipped as incomplete** — a run has to be built ` +
      "and benchmarked to be comparable, and a partial one would render an empty column that reads " +
      "as “this engine was free”: " +
      skipped.map((s) => {
        // `file: reason` — the file half links to the committed record so the reason can be
        // checked against what the run actually filed.
        const at = s.indexOf(": ");
        if (at < 0) return `\`${s}\``;
        const file = s.slice(0, at);
        return `[\`${file}\`](${recordUrl(repo, file, opts.ref)}) — ${s.slice(at + 2)}`;
      }).join(" · ")));
  }
  // LAST OF THE TABLES, and after the run table on purpose: it is the only section that reads the
  // others rather than reporting a measurement of its own, and its verdicts are unreadable until a
  // reader has seen what they are verdicts ON. It sits ABOVE the methodology because it carries
  // tables, and every table on this page comes before every paragraph.
  out.push(renderAnalysis(cols, anaEntries, groups, times,
    { runs, ledger, keyOf, table: martTable, counts, reference: opts.reference }));
  // FOOTNOTES, LAST — after every chart and every table. This block explains the measure rather than
  // reporting one, so it belongs where a reader goes looking for it rather than in the middle of the
  // page they came for.
  const n = new Set(cols.map(({ col }) => baseEngine(col))).size;
  out.push("<h3>About these numbers</h3>");
  out.push(para("**Capacity units (CU-seconds) are what this page leads with** — Fabric's own " +
    "billing measure, read from the Capacity Metrics model. Not milliseconds and not rows: what the " +
    `work COST. One dbt project, ${n} engine${n !== 1 ? "s" : ""}, one landed copy of the data: this ` +
    "is what each engine charged to build the same tables and to answer the same queries. Attribution " +
    "is by Fabric ITEM GUID — each run records what it created and then deletes it — so no " +
    "number here is a guess about which engine an item belonged to."));
  out.push(fold("what's comparable, and why analytics leads",
    "**The CU columns are directly comparable, and the two time measures need reading " +
    "with more care.** The engines were handed different compute — a 64-vCore notebook, a Livy " +
    "pool, a warehouse — and a capacity unit already prices that in, which is the whole reason " +
    "to lead with cost. Duration does not: billed operation seconds SUM across concurrent operations, " +
    "so spark's five Livy REPLs total more than the clock they ran on, and query milliseconds are one " +
    "sample of a shared capacity rather than a bill. They are on the page because they answer a " +
    "question CU cannot — how long a person waits, and how hard the engine drew while they did " +
    "— and each says where its own number bends.",
    "**Analytics is the half that matters**, and it leads for that reason. Fabric smooths " +
    "BACKGROUND operations — everything the build does — over 24 hours, so a heavy ETL leg " +
    "is absorbed and nobody waits for it. Query CU is INTERACTIVE, smoothed over minutes, and it is " +
    "what THROTTLES: the CU a user sits behind and a capacity admin asks about. An engine that builds " +
    "cheaply and queries expensively has optimised the half that does not hurt."));

  const reads = (ledger.reads || []).length;
  // `runs` here is already filtered to one source generation, so the count would UNDERSTATE what was
  // read. Say both — a footer that quietly drops three records is the silence this whole section is
  // built to avoid.
  const excluded = (opts.dropped || []).length;
  out.push(para([`[source](${SERVER}/${repo})`,
    `\`history/runs/\` — ${runs.length} run(s)` +
    (excluded ? ` (+${excluded} excluded)` : "") +
    (skipped.length ? ` (+${skipped.length} skipped)` : "") + `, ${cols.length} on this page`,
    `\`history/cu.json\` — ${Object.keys(ledger.items).length} item GUID(s) over ${reads} read(s)`,
  ].join(" · ")));
  return out.filter(Boolean).join("\n");
}

/**
 * Nothing to render, so say what the contract is rather than printing an empty page. This is the
 * dashboard's only failure mode that is not a network one, and it is always the same: nothing has been
 * measured yet.
 */
export function renderEmpty(repo = DEFAULTS.repo) {
  return [
    "<h2>Capacity units</h2>",
    para("**No run records in `history/runs/`.** This page renders what a run filed and what the " +
      "capacity ledger (`history/cu.json`) says those items cost. It reads nothing else and spends no " +
      "capacity, so an empty directory means nothing has been recorded yet — not that the " +
      "capacity was idle."),
    para(`Dispatch **Benchmark** ([${repo}](${SERVER}/${repo}/actions)). It builds one engine, ` +
      "benchmarks it, deletes what it created and commits one record; the **Capacity units** " +
      "workflow then reads the capacity for those item GUIDs and commits the ledger this page joins " +
      "against — it runs straight after that build, and daily thereafter."),
  ].join("\n");
}

/**
 * Records + ledger -> the page body. The one entry point both the browser boot and the offline build
 * go through, so a snapshot and a live read cannot render differently.
 */
export function compose(records, ledgerDoc, opts = {}) {
  const ledger = normaliseLedger(ledgerDoc);
  const { runs: whole, skipped } = selectRuns(records);
  if (!whole.length) return { html: renderEmpty(opts.repo || DEFAULTS.repo), skipped, cols: [] };
  const pick = (opts.record || "").trim();
  if (pick) {
    // Pinning a run means asking for THAT run, so the generation filter does not apply — the whole
    // point of `?record=` is reproducing a page as it was, including one from an older source.
    let hits = whole.filter((r) => String(r._file || "").includes(pick));
    if (!hits.length) hits = whole.slice(-1);
    const rec = hits[hits.length - 1];
    const cols = [{ col: ENGINE_LABEL[rec.engine] || rec.engine || "?", engine: rec.engine, rec }];
    return { html: renderPage(cols, whole, ledger, { ...opts, skipped }), skipped, cols, dropped: [] };
  }
  // BEFORE `columnsFor`, and the order is load-bearing twice over. `columnsFor` takes the latest run
  // per (engine, config), so filtering afterwards would let a stale-generation run hold a column; and
  // `spreadFor` walks this whole array to build the charts' means and ranges, so filtering the array
  // is what stops a mean blending two generations. Both come free from filtering here.
  const { runs, dropped, reference } = sameGeneration(whole, opts.table || DEFAULTS.table);
  const cols = columnsFor(runs);
  return {
    html: renderPage(cols, runs, ledger, { ...opts, dropped, reference, skipped }),
    skipped, cols, dropped,
  };
}

// ------------------------------------------------------------------------------------ the loader
//
// Live data comes from raw.githubusercontent.com, which serves the repo's own files with
// `Access-Control-Allow-Origin: *` and a ~5 minute CDN TTL. The DIRECTORY LISTING cannot come from
// there — raw serves files, not indexes — so it comes from the contents API, which is also CORS-open
// and rate-limited to 60 requests per hour per IP unauthenticated. One call per page load.

const jsonOf = async (url, fetchImpl) => {
  const r = await fetchImpl(url, { headers: { Accept: "application/json" } });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} for ${url}`);
  return r.json();
};

export async function loadRemote(opts = {}) {
  const repo = opts.repo || DEFAULTS.repo;
  const ref = opts.ref || DEFAULTS.ref;
  const fetchImpl = opts.fetch || (typeof fetch !== "undefined" ? fetch : null);
  if (!fetchImpl) throw new Error("no fetch available");
  const raw = `https://raw.githubusercontent.com/${repo}/${ref}/`;
  const api = `https://api.github.com/repos/${repo}/contents/history/runs` +
    `?ref=${encodeURIComponent(ref)}`;
  // `legacy/` is a directory and is filtered out here, which is the same thing the old loader did by
  // only reading `history/runs/*.json` at the top level: those records predate the item GUIDs and
  // cannot be joined to a ledger at all.
  const listing = await jsonOf(api, fetchImpl);
  const names = (Array.isArray(listing) ? listing : [])
    .filter((e) => e.type === "file" && e.name.endsWith(".json"))
    .map((e) => e.name).sort();
  const [ledger, ...records] = await Promise.all([
    jsonOf(raw + "history/cu.json", fetchImpl).catch(() => null),
    ...names.map((n) => jsonOf(raw + "history/runs/" + n, fetchImpl)
      .then((r) => Object.assign(r, { _file: n })).catch(() => null)),
  ]);
  return { ledger, records: records.filter(Boolean), names };
}

/** `?record=`, `?repo=`, `?ref=`, `?table=` — the dispatch inputs the old workflow carried, as query
 *  params. A link to one run is now a link, not a workflow run. */
export function optsFromSearch(search) {
  const p = new URLSearchParams(search || "");
  return {
    repo: p.get("repo") || DEFAULTS.repo,
    ref: p.get("ref") || DEFAULTS.ref,
    table: p.get("table") || DEFAULTS.table,
    record: p.get("record") || DEFAULTS.record,
  };
}

/**
 * Turn every `table(…, filter)` into a spreadsheet-style autofilter: a search box, a dropdown per
 * declared column, sortable headers, and a live count.
 *
 * **Built here, not in the markup, and that is the point.** The dropdown's options are the DISTINCT
 * VALUES ALREADY IN THE COLUMN, read off the DOM, so the list cannot describe a column it no longer
 * matches and the render layer stays a pure string function with the data in it once. Nothing is
 * removed from the DOM either — filtering only sets `display`, so ctrl-F, the offline snapshot and
 * every test still see all of it, the same rule the CSS-only tab strip follows.
 *
 * Progressive enhancement throughout: with scripts off there is no bar and the whole table is simply
 * there, which is the state a reader can always fall back to.
 */
export function wireTables(root, doc = null) {
  const d = doc || (root && root.ownerDocument) || (typeof document === "undefined" ? null : document);
  if (!root || !d || !root.querySelectorAll) return 0;
  let wired = 0;
  // Two selectors, not `".filtered, .sortable"` — the offline test's stub DOM resolves one plain
  // class per query, and a combined selector would silently match nothing there.
  for (const box of [...root.querySelectorAll(".filtered"), ...root.querySelectorAll(".sortable")]) {
    const tbl = box.querySelector("table");
    if (!tbl || !tbl.tHead || !tbl.tBodies[0]) continue;
    const heads = [...tbl.tHead.rows[0].cells];
    const body = tbl.tBodies[0];
    const all = [...body.rows];
    const text = all.map((r) => [...r.cells].map(cellText));
    if (box.classList.contains("sortable")) {
      wireSort(heads, body, all, text);
      wired++;
      continue;
    }

    const bar = d.createElement("div");
    bar.className = "filterbar";
    const find = d.createElement("input");
    find.type = "search";
    find.className = "ffind";
    find.placeholder = box.dataset.find || "filter";
    find.setAttribute("aria-label", find.placeholder);
    bar.appendChild(find);

    const picks = new Map();
    for (const raw of String(box.dataset.menus || "").split(",")) {
      const i = Number(raw);
      if (raw === "" || !Number.isInteger(i) || !heads[i]) continue;
      const sel = d.createElement("select");
      sel.className = "fpick";
      const label = cellText(heads[i]) || `column ${i + 1}`;
      sel.setAttribute("aria-label", `filter by ${label}`);
      const seen = [...new Set(text.map((cells) => cells[i]))].sort((a, b) => compareCells(a, b));
      // `createElement("option")`, never `new Option(…)`: that constructor is a browser GLOBAL, so it
      // would put this function out of reach of an offline test for no gain.
      const opt = (label_, value) => {
        const o = d.createElement("option");
        o.value = value;
        o.textContent = label_;
        return o;
      };
      sel.appendChild(opt(`all ${label}`, ""));
      for (const v of seen) sel.appendChild(opt(v, v));
      picks.set(i, sel);
      bar.appendChild(sel);
    }
    const count = d.createElement("span");
    count.className = "fcount";
    bar.appendChild(count);
    box.insertBefore(bar, box.firstChild);

    const apply = () => {
      const chosen = {};
      for (const [i, sel] of picks) chosen[i] = sel.value;
      let shown = 0;
      all.forEach((r, k) => {
        const ok = matchesFilter(text[k], find.value, chosen);
        // `display`, never `remove()` — a filtered row is hidden, not gone, so nothing the page says
        // about how many runs it read stops being true while a filter is on.
        r.style.display = ok ? "" : "none";
        if (ok) shown++;
      });
      count.textContent = shown === all.length ? `${all.length} rows` : `${shown} of ${all.length} rows`;
    };
    find.addEventListener("input", apply);
    for (const [, sel] of picks) sel.addEventListener("change", apply);

    wireSort(heads, body, all, text);
    apply();
    wired++;
  }
  return wired;
}

/** Clickable, keyboard-reachable column sort on one table — shared by the autofilter and the
 *  sort-only `.sortable` box. Click sorts ascending, a second click reverses, a caret marks the
 *  current column. Reordering is `appendChild` on the existing rows, so a filter's `display`
 *  state and the row objects survive a sort. */
function wireSort(heads, body, all, text) {
  let at = -1, dir = 1;
  const sortBy = (i) => {
    dir = at === i ? -dir : 1;
    at = i;
    const order = all.map((r, k) => k)
      .sort((a, b) => compareCells(text[a][i], text[b][i]) * dir);
    for (const k of order) body.appendChild(all[k]);
    heads.forEach((th, k) => {
      th.classList.toggle("asc", k === i && dir > 0);
      th.classList.toggle("desc", k === i && dir < 0);
    });
  };
  heads.forEach((th, i) => {
    th.tabIndex = 0;
    th.setAttribute("role", "button");
    th.addEventListener("click", () => sortBy(i));
    th.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); sortBy(i); }
    });
  });
}

/**
 * The browser entry point. An inlined snapshot wins when present — that is the offline artifact copy,
 * which has to open from a local disk years later with no network — and otherwise the page reads
 * `history/` live.
 */
export async function boot(doc = document, loc = location) {
  const app = doc.getElementById("app");
  const status = doc.getElementById("status");
  const opts = optsFromSearch(loc.search);
  const snap = doc.getElementById("snapshot");
  const say = (html) => { if (status) status.innerHTML = html; };
  try {
    let records, ledger, live;
    if (snap && snap.textContent.trim()) {
      const s = JSON.parse(snap.textContent);
      records = s.records; ledger = s.ledger; live = false;
      say(inline(`Offline copy — frozen at \`${s.built || "?"}\`. ` +
        `[The live page](${pagesUrl(opts.repo)}) reads \`history/\` on every load.`));
    } else {
      say("Reading <code>history/</code> from GitHub…");
      const got = await loadRemote(opts);
      records = got.records; ledger = got.ledger; live = true;
    }
    const { html, skipped } = compose(records, ledger, opts);
    app.innerHTML = html;
    wireTables(app, doc);
    if (live) {
      say(inline(`Live — read from \`${opts.repo}@${opts.ref}\` at ` +
        `${new Date().toISOString().slice(0, 16).replace("T", " ")} UTC. ` +
        `Reload for new data; nothing needs republishing.` +
        (skipped.length ? ` ${skipped.length} record(s) skipped as incomplete.` : "")));
    }
  } catch (ex) {
    // A page that fails has to say what it could not read, because every plausible cause — the API's
    // 60/hour anonymous rate limit, a renamed branch, a private fork — looks identical from here.
    app.innerHTML = [
      "<h2>Capacity units</h2>",
      para(`**Could not read the data.** \`${String(ex && ex.message || ex)}\``),
      para(`This page reads \`history/runs/\` and \`history/cu.json\` from ` +
        `[${opts.repo}](${SERVER}/${opts.repo}) at view time, over ` +
        "`raw.githubusercontent.com` and the GitHub contents API. The API allows 60 requests per hour " +
        "per IP without a token, which is the usual reason this fails — wait, or open the " +
        "`dashboard` artifact from a **Dashboard** run, which carries a frozen copy of the data."),
    ].join("\n");
    say("");
  }
}

if (typeof document !== "undefined" && typeof window !== "undefined") {
  window.addEventListener("DOMContentLoaded", () => boot());
}
