# `dashboard/` — the page

**It runs in the reader's browser and reads `history/` live.** `app.js` fetches
`history/runs/*.json` and `history/cu.json` from `raw.githubusercontent.com` on every load, joins
them on the Fabric item GUID, and writes the whole page — tables, both bar charts, every note — into
an empty shell.

**So publishing is what you do when the VISUALISATION changes, not when a number does.** A
`Benchmark` run that commits a record, or a `Dashboard` measure job that commits the ledger, appears
on the published page with no deploy at all. That is the point of the arrangement, and it replaced a
Python renderer whose output had to be republished for every measurement — which meant a page nobody
had looked at could be published by a workflow nobody had watched.

```
index.html   the shell: one stylesheet, three empty elements
app.js       the whole page — loader, join, layout grouping, render, charts
build.mjs    index.html + app.js -> one file, twice (live, and offline with data inlined)
app.test.mjs 69 offline tests, no browser, no network
```

Where the data comes from is [`cu/`](../cu/README.md) (the CU ledger) and the `Benchmark` workflow
(the run records). **Neither directory imports the other**; what passes between them is `history/`.

## Why raw.githubusercontent, and why the contents API

`raw.githubusercontent.com` serves this repo's own files with `Access-Control-Allow-Origin: *` and a
~5 minute CDN TTL, which is what lets a page hosted on `djouallah.github.io` read them at all. The
repo is public and `history/` has always been committed, so nothing here is a new disclosure.

**It must NOT be served from the Pages origin.** Copying `history/` into `site/` would put the data
back inside the published artifact and make every commit a republish again — the exact thing this
removes.

Raw serves files, not directory indexes, so the listing of `history/runs/` comes from the GitHub
contents API — also CORS-open, and rate-limited to **60 requests per hour per IP** without a token.
One call per page load. When it refuses, the page says so and names the limit, because an empty page
and a rate-limited API look identical to a reader and only one of them means "nothing has ever been
measured".

**DuckDB-WASM was considered and rejected.** The whole dataset is ~300 KB of JSON, already in the
shape the page wants; ~30 MB of wasm from a CDN to query it would be a cost with no matching
benefit. If an ad-hoc SQL explorer over the records is ever wanted, that is a different page.

## Two builds, one implementation

```
node dashboard/build.mjs --out site/index.html              # live: fetches history/ at load time
node dashboard/build.mjs --out dashboard.html --snapshot    # offline: history/ inlined
```

The offline copy exists because the `dashboard` artifact has to open off a local disk, with no
network, years later. It is the **same module and the same render path** — `app.js` prefers an
inlined `#snapshot` over the network when one is present — so a frozen copy and the live page cannot
disagree about a number. The build reads the snapshot back out of the finished document and parses
it, because a truncated one renders as "no run records", which is indistinguishable from a repo that
has never been measured.

The live shell is checked in CI for the opposite property: it must NOT carry inlined data. A page
that ships its own data is a page that goes stale silently.

To work on it locally, serve the directory rather than opening the file — `index.html` loads
`app.js` as a module, which `file://` refuses:

```
python -m http.server -d dashboard 8000   # then http://localhost:8000/
```

It reads the real `history/` off GitHub from there, so a local edit is checked against real records
without a build, a token or a dispatch.

## Query parameters, not workflow inputs

| | |
|---|---|
| `?record=30776174056` | render one run alone — a substring of the record's filename, so a run id or a date both work |
| `?ref=some-branch` | read `history/` from another branch |
| `?repo=owner/name` | read another fork's records entirely |
| `?table=fct_scada` | which table leads the layout section |

`?record=` used to be a workflow dispatch input. A link to one run's page is now a link.

## The page

- **Bar charts first**, analytics then ETL, **cheapest first** because "lower is better" makes the
  ranking the finding. A **zero sorts to the bottom**, never the top: zero means the engine did no
  such work, and at the top under that caption it would read as the winner.
- **The two charts are keyed on DIFFERENT THINGS, and that is the design.** **Analytics is one bar
  per PARQUET LAYOUT**, because Power BI never sees the engine — it opens parquet through Direct Lake
  and transcodes row groups, so what a query costs belongs to what was written and the writer is
  metadata. The bar is **named for its writer and captioned with the shape** — `spark V-Order` over
  `V-Order · 10–11 files · 10–11 RG`: the grouping is the layout, but a file count is a poor name
  even when it is the real subject, so the shape sits underneath where it explains why two writers
  would ever share a bar. **ETL is one bar per column**, because there the writer and the compute it
  was given are the entire subject, and it keeps the adapter-and-vCores caption.
  What forced this: duckrun at 64 cores and at 32 wrote 4 files and 27 row groups either way, so two
  bars 50% apart was not a comparison — it was one layout measured twice, presented as two results.
  Grouping merges them and the range says what the gap really was.
  **Grouping is MEASURED, labelling is DECLARED.** The key is
  `(V-Order, power-of-two band of files, power-of-two band of row groups)` read off the parquet as
  `stats.py` saw it, so two unrelated engines that wrote the same shape *do* share a bar. The caption
  comes from `LAYOUT_CONFIG`, so it does not re-word itself every time a record lands. On the current
  records that yields five groups from nine columns, and it surfaces two things the old chart hid:
  V-Order on and off sit in the same file band and differ 2.8× (1,332 against 3,769), which is the
  sharpest experiment on the page; and NEE on and off produce the same layout, so the gap between
  them was never an NEE effect.
  Banded, not exact: exact equality splits dwh's own two runs from each other (78 files and 80) and
  splits duckrun on 1.1 MB of size. The accepted cost is the boundary — 15 row groups and 17 land in
  different bands. A record with **no** file count keys to `null` and keeps a bar of its own; two
  unmeasured layouts are not one identical layout.
- **A column header is an engine plus the SHORTEST config that still tells it apart** (`variantTag`).
  It appears in every table and both charts, so width is a real cost, and it used to read
  `spark·readHeavyForPBI+NEE` — Microsoft's name for an intended workload plus a double negative on
  its sibling. Two rules cut it to `spark·V-Order+NEE` / `spark·V-Order` / `spark·default+NEE` /
  `spark·default`. The profile is named by its **effect**, through the same `PROFILE_LABEL` the layout
  captions use, so a profile is called the same thing wherever it appears on the page. And a flag that
  is **off is absent** rather than negated — `+noNEE` spends header width saying nothing happened, and
  the contrast with the run that did enable it is what a reader is looking for.
  Absence-means-off is only unambiguous while every config of that engine RECORDS the flag, so
  `columnsFor` checks it: where two configs would collapse to one header — a record predating the
  dispatch input has no key at all and would collide with an explicit `false` — the whole engine falls
  back to the explicit spelling. A page printing one column name twice is unreadable and silent about
  why. The tag still never contains `COL_SEP`; `baseEngine` splits on it.
  The **engine half** takes `ENGINE_LABEL` too, so a column reads `duckdb iceberg·64c` and the page
  calls that engine one thing throughout — the layout rows had said `duckdb iceberg` while the columns
  said `iceberg`, which read as two subjects. That is only safe because **`baseEngine` reverses the
  label**: `STACK`, the adapter caption and the (engine, variant) join to a record are all keyed on
  `iceberg`, and without the reversal each would silently miss — a blank caption, a chart row quietly
  gone — rather than raise.
- **Engine-major table**, engines across, **`compute` and `storage`** down, class subtotals in bold.
  The split comes from the OPERATION, and it has to: compute and storage share an ITEM. Measured
  against the live model — `dbt_spark` [Lakehouse] bills 188,636 CU of `High Concurrency Session Livy
  Run` and 20,268 of `OneLake Write via Redirect` against one GUID; `dbt_dwh` [Warehouse] bills
  129,177 of `Warehouse Query` beside its own OneLake writes. **Every `OneLake …` operation is
  storage; everything else is compute.** A dash means no operation of that kind was billed there at
  all — an iceberg lakehouse is 40,832 CU of pure OneLake, because its compute is the notebook, a
  different item. A class is only decomposed when some column holds more than one bucket, so
  `analytics` stays a single bold row.
- **Every lakehouse has a paired SQL analytics endpoint**, a separate billable `Warehouse` item with
  the same display name: `dbt_spark` 306.3 CU, `dbt_iceberg` 245.7, `dbt_delta` 278.9, all of it
  `SQL Endpoint Query`. It was invisible to the ledger until `provision.py` started recording it —
  the GUID is not the lakehouse's. It is never deleted by the teardown: Fabric removes it with its
  parent. **`dbt_landing` has one too, and it is the one door landing CU got onto the page through:**
  its role is `sql_endpoint`, not `landing`, so the role filter never saw it, and the same item
  appeared in every run record charging every engine 130.4 CU it did not spend. `landingGuids()`
  catches it by NAME against the record's own `landing` items, leaving an engine's own endpoint
  alone. It distorted more than a total: that endpoint bills 130.4 CU over 83.2 s, a rate of 1.6,
  against a 64-vCore notebook's 32.0, so blending them made duckrun and iceberg — the same DuckDB in
  the same notebook at the same vCores — read 28.5 and 26.4. Excluded, both read 32.0.
- **Engine-major is what makes the width work**: item-major needs a column per Fabric item and every
  run creates different ones. **No total column and no grand-total row** — both would sum ACROSS
  engines, which is the one sum on this page that answers nothing, since the engines are alternatives
  to each other.
- **`landing` CU is not on the page at all.** The page compares ENGINES. `dbt_landing` is the
  ingestion staging area — no run deletes it, every run reads it — so its CU is one cumulative figure
  belonging to no engine, and it answers no question this page asks. It was briefly given a row of
  its own; the same number repeated under every column read as "each of them spent this". The
  archive's SIZE is still reported, because input volume is a different question from what ingesting
  it cost.
- **Input archive, LAST on the page**: files and bytes in the landing archive, from `stats.py`'s
  listing. Every other number describes what came OUT, and this is the one copy of what went in —
  shared by every engine, so it belongs with the provenance rather than among the columns it is not
  one of. It used to sit between the engine table and the layout, where a table with no engine in it
  read as a column that had gone missing.
- **Table layout**, every shared table, mart first, **one row per WRITER, and no `writer` column** —
  the row label IS the writer, so a `duckdb (iceberg)` cell beside a `duckdb iceberg` label was one
  fact printed twice. `spark V-Order`, `spark default`, `duckrun`, not `spark·V-Order+NEE` and
  `duckrun·64c`. The resource profile is named by what it does to the parquet rather than by
  Microsoft's name for the workload it was designed for, and the core count and NEE flag are dropped
  because two runs each showed they never reach it. duckrun's two core counts and spark's two NEE
  settings therefore collapse to one row — they had written identical layouts, so the rows they
  replaced were the same row printed twice. This is also what makes the table agree with the chart
  above it: the table groups by the DECLARED writer and the chart by the MEASURED parquet, two
  directions onto the same rows, and a disagreement between them would be worth knowing.
  The mart block alone carries the analytics CU and the three query-time columns — both are one
  number per writer, not per table — and it quotes the **same** CU as the chart above it, the mean
  over every run, not that column's latest.
  **The row count is in the heading, not a column**: it is identical on every row by design, which is
  the parity statement the whole project rests on, and 143,980,961 repeated down a table is a wide
  column carrying one fact. When the engines DISAGREE the heading says so and the column comes back,
  because that disagreement is the loudest signal this page has. `rows per RG` is abbreviated
  (`13.1M`, `122.9K`) — that number spans four orders of magnitude across these engines and the ratio
  is the finding, not the twelve digits.
- **`cold` / `warm` / `hot` are THREE COLUMNS OF THE MART BLOCK, not a section.** The one thing on
  the page that is not capacity units, and it comes from the run records rather than the ledger:
  `benchmark.timings.<model>.<query>` is already on every record. `benchmark/render_report.py`
  renders it per dispatch, but a dispatch builds ONE engine, so that report always has a single
  column and a degenerate ranking — composed here from every engine's latest run, this is the only
  place the three tiers can be read ACROSS engines at all.
  They were briefly a table of their own. **That was wrong, and the placement is the whole point:** a
  separate table put the layout and the speed it produced on two different tables, when the only
  question worth asking of these numbers is whether one explains the other. On one row, `files`,
  `row groups`, `size MB` and `V-Order` sit beside the milliseconds they produced, per engine, and a
  reader can see for themselves whether a smaller file count bought a faster first visit — iceberg's
  369 files and 122k-row row groups next to its 101,861 ms cold, against duckrun's 4 files and
  27,675 ms. **cold** is the first visit to a freshly deployed semantic model, **warm** the second,
  **hot** the median of the passes after that; the record's own `tier` field is something else
  entirely (the query CATEGORY — `probe`/`composite`/`raw`/`hot_only`) and must not be confused with
  them. Cold is the tier layout can actually MOVE — it is the one that transcodes columns out of
  parquet, while warm and hot converge on what the model already holds in memory.
  Mart block only, for the same reason the CU column is: one number per ENGINE, not per table, so on
  every block it would read as one measurement per table. Each tier is summed over the queries
  **every column carries at that tier**, and the closing note counts them, because it genuinely
  differs — the selectivity-ladder queries have no `cold_ms` at all, the top DUID being resolved only
  after pass 1, so cold is two queries short of warm and hot.
  Deliberately **reimplemented rather than imported** — `render_report._totals`/`rank` take exactly
  this shape, and importing `benchmark/` would end the isolation that makes this directory deletable.
- **`compute CU per second` is a ROW OF THE ENGINE TABLE, not a section, and the raw seconds are
  not shown at all.** It comes off the SAME Capacity Metrics row as the CU above it — same GUIDs,
  same roles, same compute/storage split — so a table of its own restated the whole join to add two
  numbers per class. The seconds themselves are gone because they are BILLED OPERATION seconds that
  sum across concurrent operations — spark's five Livy REPLs total more than the clock they ran on —
  so the number needed four sentences of hedging to be read at all, while the rate needs none: the
  concurrency is in the numerator and the denominator alike, so it cancels. The rate was the only
  thing the seconds were there to support. A high rate is a WIDE engine, not a slow one.
  **It is COMPUTE ÷ COMPUTE, and that is not a refinement — a total-over-total rate is wrong.** A
  storage operation bills real CU over a duration of essentially nothing (one `OneLake Write via
  Redirect`: 383.25 CU in **0.049 s**), so including storage does not dilute the rate, it detonates
  it, by an amount tracking only how much OneLake traffic the engine made. `CU (s)` is literally
  capacity-units × seconds, so `CU ÷ duration` is capacity units DRAWN — for a single-node Python
  notebook that is **`cores` ÷ 2**, fixed for a given core count and not a constant: 32.0 at the 64
  vCores dispatched by default, 16.0 at 32. The check when this reads oddly is two DuckDB legs at the
  **same** `cores` reading the **same** number, never that they read 32; `vcores` is part of
  `variant()`, so two core counts are two columns and the caption names each. The row is **absent**
  when the ledger has no seconds — a ledger written before the duration read, or a model that does
  not expose the column — because absent says "not measured" and a zero would say "instant". Same
  rule on a class subtotal: a column the ledger has not read yet is a **dash**, never `0.0`, which
  would say the engine did that work for free.
- **There is no chart of the seconds and no third bar.** The page carries two, and both are capacity
  units — the measure it leads with and can defend. A third in the same visual language, drawn from
  numbers that need a caveat, invites exactly the reading the note beneath it withdraws.
- **A record has to be built and benchmarked to reach the page.** `incomplete()` skips anything else
  and names why — a run with no benchmark shows an empty analytics column, which reads as "querying
  this engine was free" rather than "nobody measured it".
- **A run that was never TORN DOWN still renders, with a caveat.** Its items are alive and Fabric
  keeps billing them, so its total creeps upward and is an upper bound on that run rather than a
  measurement of it. It was briefly rejected outright; the creep is small and a column that
  disappears costs more than one carrying a caveat, so `drifting()` marks it **still billing** in the
  sources table instead — the loudest of the three states, because it is the only one that does not
  resolve by waiting. Deleting the items settles it.
- **Columns are each engine's latest run, once per config.** One dispatch builds one engine, so
  rendering the newest record alone would give a comparison page with one column. spark under
  `readHeavyForPBI` and spark under `writeHeavy` are two columns, because one number cannot answer
  for both. The cost — columns are different dispatches, days apart — is stated in the sources table
  rather than smoothed over.

## The CU columns are comparable, and that is the point of the unit

The engines are handed different compute — a 64-vCore notebook, a Livy pool, a warehouse — and it
does not qualify the comparison. **A capacity unit already prices that in.** 64 vCores for ten
minutes costs more CU than 8 vCores for ten minutes, which is exactly why CU leads: it is the bill.

**The two time measures do not have that property, and the page says which is which.** Billed
operation seconds SUM across concurrent operations, so a spark leg totals more than the clock it ran
on; query milliseconds are one sample of a shared capacity rather than a bill. They are on the page
because they answer a question CU cannot — how long a person waits, and how hard the engine drew
while they did — and each states where its own number bends. Do not flatten the three into one
ranking.

The chart captions still name the configuration (`dbt-duckrun · 64 vCores`) because it says which
setting produced the number — a run at a different core count is a different data point, not an
invalid one.

## Things that will bite

- **`app.js` must not touch `document` at import time.** It exports pure functions that return
  STRINGS and boots only under `DOMContentLoaded`; that is what lets the whole page — join, layout
  grouping, both charts — be tested under `node --test` with no browser and no jsdom.
- **The render layer escapes before it interprets markdown.** A Fabric display name containing `<`
  is text, and link hrefs are restricted to `http(s)://`. Pinned by a test.
- **A tag must never contain `COL_SEP`** (`·`). `baseEngine` splits a column id on it to recover the
  engine, so a tag carrying one would make `STACK` and the (engine, variant) join silently miss.
- **The page build checks out `ref: <branch>`, not the triggering SHA.** The measure job commits the
  ledger; a default checkout would freeze the OFFLINE copy from the version before that commit. The
  live page is immune — it reads the branch head at view time — which is exactly the class of
  one-dispatch-stale bug this whole arrangement removes.
- **Rounding ties differ from the old Python page by one in the last digit.** Python rounds
  half-to-even, JavaScript half-up, so a value of exactly 1,378.5 printed `1,378` before and prints
  `1,379` now. Display only; the underlying numbers are identical, verified row-for-row against the
  last Python render.
- **`node --test dashboard/app.test.mjs` is the gate**, and the page job runs it. There is no Python
  in that job at all, which is what proves by running that the render path reaches no network of its
  own beyond the two documents it fetches.

## Isolation

No imports from `benchmark/`, none from `cu/`, and no third-party package of any kind — no bundler,
no framework, no CDN. `build.mjs` is string substitution over `index.html`. It is built to be deleted
by removing one directory; the exporter in `cu/` keeps working and the ledger keeps being committed.
