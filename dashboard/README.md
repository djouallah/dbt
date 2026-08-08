# `dashboard/` — the page

**It runs in the reader's browser and reads `history/` live.** `app.js` fetches
`history/runs/*.json` and `history/cu.json` from `raw.githubusercontent.com` on every load, joins
them on the Fabric item GUID, and writes the whole page — tables, the chart, every note — into
an empty shell.

**So publishing is what you do when the VISUALISATION changes, not when a number does.** A
`Benchmark` run that commits a record, or a `Capacity units` run that commits the ledger, appears
on the published page with no deploy at all. That is the point of the arrangement, and it replaced a
Python renderer whose output had to be republished for every measurement — which meant a page nobody
had looked at could be published by a workflow nobody had watched.

```
index.html   the shell: one stylesheet, the title, three empty elements
dag.html     the dbt DAG -- `dbt docs generate --static` output, one self-contained file
app.js       the whole page — loader, join, layout grouping, render, charts
build.mjs    index.html + app.js -> one file, twice (live, and offline with data inlined)
app.test.mjs offline tests, no browser, no network (a count here goes stale silently)
```

Where the data comes from is [`cu/`](../cu/README.md) (the CU ledger) and the `Benchmark` workflow
(the run records). **Neither directory imports the other**; what passes between them is `history/`.

## When it publishes

**On a push to `dashboard/**` on `main`, and on dispatch. Nothing else.** Push a change to the page
and it deploys itself; that is the only automation, and you start nothing by hand in the normal case.

This is a reversal of the repo's "nothing runs on push" rule, and it is safe for a reason that does
**not** generalise — do not copy the trigger to another workflow. That rule exists because the
workflows that COMMIT would otherwise pay for their own commits. `Dashboard` commits nothing (it
deploys to Pages), and `dashboard/**` never matches the `history/` paths that `Benchmark` and
`Capacity units` write. So no commit can trigger a publish and no publish can make a commit: the loop
is not reachable. Two things must stay true —

- **`history/` must never appear in the path filter.** That single edit builds the loop.
- **`Benchmark` and `Capacity units` must never gain a `push:` trigger.** They commit, and one of
  them spends capacity.

The filter is the whole directory even though only `app.js`, `index.html` and `build.mjs` reach the
published bytes. A narrower one is tempting, but if someone later adds `dashboard/theme.css` and
forgets to extend it, the page **silently** stops updating; the broad filter's worst case is a free
no-op deploy for a README edit. `paths:` is also evaluated per PUSH, not per file, so a mixed commit
fires it anyway — which is the same reason and the same cost.

One wrinkle when this trigger is first added, or if it is ever changed: **a commit that edits
`dashboard.yml` but nothing under `dashboard/` does not fire it.** Dispatch once by hand to bootstrap.

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

- **The page says what it IS before what it measures.** It opened on `Capacity units` and went
  straight into the numbers, so it named its measure and never its subject: a reader arriving on a
  link met four columns of CU with no statement of the scale any of it describes. Now an `<h1>`
  **Fabric dbt benchmark** with the repo link under it, then one sentence of scale, then
  `Capacity units` — kept, and heading the section it always described rather than the page.
  **The title is in the SHELL and the sentence is in `app.js`**, and the split is what each needs:
  the title needs no data, so putting it in `index.html` means it is already there while the page
  says `Loading…`, on the empty-records page and on the boot error page, with one copy to maintain
  instead of four. The sentence needs the records, so it cannot live there.
  **Every number in it is DERIVED** — engines from the columns, GB and files from `layout.landing`,
  the table count and its `1 fact, 2 dimensions, 4 staging and a log` breakdown from
  `layout.tables`, the row total summed over that same list. A hardcoded `170 GB`
  goes stale the first dispatch that runs with `skip_download` off, and goes stale **silently**.
  **The `fct_` prefix is NOT the classifier**, and reading it as one printed `4 facts … and a mart`
  — wrong twice over. Four of the five `fct_*` tables (`fct_price`, `fct_scada` and their `_today`
  siblings) are raw AEMO CSV landed in the **`landing`** schema; only `fct_summary` reaches
  **`mart`**, and it is the one actual fact table, the `(date, time, DUID)` grain Power BI queries.
  The split is the mart table against everything else, and the record's own `schema` field is what
  says so.
  It reads the archive through `landingBlocks`, the same call the *Input archive* table at the foot
  of the page makes, so the top and the bottom cannot quote different archives — a test asserts they
  agree rather than asserting which record wins, so it survives a change to that rule.
  Three things it refuses to say. **An absent input is an absent clause, never a zero** — no landing
  block, no size; the same rule as the `compute seconds` row. **A partial row total is dropped
  entirely**: seven tables of eight labelled "in total" is a *wrong* number, not an incomplete one,
  and it would sit there looking perfectly plausible. And a **breakdown that does not account for
  every table** is dropped while the count stays, because a decomposition quietly short of the
  number beside it contradicts it. With nothing measurable at all there is no lede rather than a
  sentence of dashes.
  On the unit: `stats.py` stores `bytes / 1048576`, so `size_mb` is really MiB and the archive is
  178.8 GB decimal. The lede prints `size_mb / 1000` because that is the figure which agrees on
  sight with the `170,491.5 MB` in the *Input archive* table on the same page; raw bytes are
  discarded inside `landing_stats()` and never reach the record, so there is no exact byte figure to
  print instead. A test pins the `/1000`, so a later "fix" to `/1024` is a visible change.
- **The DAG is the real one, linked from the title.** `dag.html` is `dbt docs generate --static`
  output — one self-contained file, manifest inlined, no sidecar JSON and no CDN, which is the only
  form that fits a page with no third-party runtime. Regenerate it by hand when the models change:

  ```
  FILES_PATH=./landing ONELAKE_TABLES_PATH=./warehouse WAREHOUSE_PATH=./wh \
  ONELAKE_ENDPOINT=http://localhost ONELAKE_TOKEN=x \
  dbt docs generate --target iceberg --static --no-compile --empty-catalog
  cp target/static_index.html dashboard/dag.html
  ```

  `--no-compile --empty-catalog` is what makes it free: no warehouse, no credentials, no capacity —
  the lineage and the model descriptions all come from the parsed manifest. The placeholder env vars
  are only there because parsing reads `profiles.yml`; nothing connects. The cost is that
  column-level detail is empty. It lives under `dashboard/` so that regenerating it republishes the
  page on its own, with no new trigger and no edit to a path filter this repo is careful about; the
  workflow copies it into `site/` beside `index.html`.
  The link is **relative** on the live page and rewritten by `build.mjs --snapshot` to the absolute
  Pages URL, because the offline copy is one loose file with no sibling to point at — and a 404 off
  a local disk looks like nothing happened at all. The build fails if that link ever stops matching.
- **EVERY CHART AND TABLE COMES BEFORE EVERY PARAGRAPH, and the methodology is LAST.** The order is
  *Cost and speed by layout* (its table, then its line chart), *Cost by engine*,
  *Table layout*, *Input archive*,
  *Every run*, then *About these numbers* and the provenance line. `About these numbers` used to sit
  between the layout tables and the run table, so the last table on the page was below a screen of
  prose explaining the measure. A reader arrives for the numbers; the explanation is where they go
  looking for it. Pinned by a test that asserts the section order outright.
  *Every run* gained an `<h3>` of its own in the same move: it opened with a bare note, so it read as
  a continuation of whatever sat above it.
- **Numbers are visible, methodology is opt-in.** The long how-to-read notes are folded behind a
  one-line `<details>` each (`fold()` in `app.js`); every sentence stays in the DOM — the tests and
  ctrl-F still see it all — but the page reads numbers-first. Two things are deliberately NEVER
  folded, because they are the page's alarms: the excluded-runs block (a different source generation)
  and a **still billing** drifter note. Both are pinned by tests.
- **The layout tables are a tab strip, one table visible at a time.** Eight stacked tables buried
  the mart under seven blocks that explain it. The tabs are CSS-only — radio inputs paired to panels
  by enumerated `nth-of-type` rules, no JS — so the offline snapshot and a script-blocked browser
  behave identically, every panel stays in the DOM, and print shows all of them. The pairing is
  enumerated to 12 panels in the stylesheet; past that `renderLayouts` falls back to stacked blocks
  rather than render tabs whose panels could never show. The `?table=` param still picks which table
  leads, i.e. which tab is first and checked.
- **Every ranked table is cheapest first**, because "lower is better" makes the ranking the
  finding. A **zero sorts to the bottom**, never the top: zero means the engine did no such work,
  and at the top under that heading it would read as the winner.
- **`Column encoding` sits under `Table layout`, and is the half shape could not explain.** One row
  per `fct_summary` column, one column per layout: what each column is ENCODED as in parquet, read
  from the footers by `stats.py` and aggregated over every row group. Shape does not predict the
  CU — duckrun writes the densest parquet here and does not win, dwh writes UNCOMPRESSED and beats
  a SNAPPY spark build, and spark's two profiles sit in one row-group band 2.6x apart — while what
  Direct Lake pays for on a cold pass is transcoding parquet into VertiPaq segments, which depends
  on the encoding. A column whose writer gave up dictionary-encoding partway still LISTS
  `PLAIN_DICTIONARY`, so `dict_pages < chunks` is flagged separately; no dictionary at all is
  flagged loudest. **Absent, never empty**, when no record carries `encodings` — which is every
  record written before `stats.py` learned to profile the mart.
- **Cost and speed by layout is a TABLE, above its chart and *Cost by engine*.** One row per
  layout, cheapest first: `analytics CU`, `etl CU`, `cold ms`, `warm ms`, `hot ms`. Most of these
  were columns of the mart's layout block, which made one table answer two questions — what the
  parquet looks like, and what querying it cost. *Table layout* is now physical layout alone and this
  is the other half. It has a title and no commentary. Built from the same `martPoints` as the chart
  and the layout rows, so all three quote the same median.
  **THE TWO CU COLUMNS ARE NOT THE SAME KIND OF NUMBER, and only one of them summarises the whole
  row.** `analytics CU` is what QUERYING the layout cost and is what the table is ranked by — it
  belongs to the parquet, which is why every run in the group can be summarised into it. `etl CU` is
  what BUILDING it cost, and that belongs to the engine and the machine it was given.
  **So `etl CU` is filtered to ONE core count and the header prints which** — `etl CU (8 vCores)`.
  `layoutKey` does not carry `vcores` (it is about the parquet, and duckrun writes the same files at
  every core count), so a layout group genuinely holds runs from several machines: measured on the
  real records one duckrun layout reads **9,986 CU at 8 vCores against 22,547 blended** across
  8/16/32/64. A median over all of them describes none of them. A filter a reader cannot see is the
  one that lies, which is why it is in the header and not only here.
  **A run that records NO core count is kept, not filtered out.** `FABRIC_CORES` sizes the notebook
  the DuckDB legs run in, so only `duckrun` and `iceberg` record `vcores`; spark's compute is the
  workspace Livy pool and dwh's is the warehouse, and neither reads the input. Filtering on the value
  alone would have emptied the column for two of the four engines rather than narrowing it.
  **A layout nobody has built at that size is a DASH, never a blend and never a zero** — 7 of 17
  groups today, all duckrun, and the nightly fills them in since it runs at 8. `ETL_VCORES` is a
  constant that has to be kept in step with the dispatch default by hand.
- **Under it, ONE LINE CHART where there were two scatters.** Each layout is a single horizontal
  line: **warm ms at its left end, cold ms at its right, on one shared linear time axis**, at the
  height of its analytics CU. **The LENGTH is what the cold transcode costs.** Same `martPoints` and
  same `groupMid` as the table above, so the chart and the table cannot disagree about a number.
  It replaced *CU against cold* stacked on *cold against warm*. Both of those plotted `cold ms` on
  x, so a reader wanting all three numbers carried one between two panels — and the quantity worth
  knowing was on neither: the first had no warm at all, the second had it as a distance from a
  45-degree line nobody drew. A subtraction became a length.
  **The ends carry no markers, and that is the second simplification, not an oversight.** A first
  version drew a filled dot at cold and a hollow one at warm, sized by row-group size — which meant
  learning two more encodings (which fill is which tier, what the area means) before the length
  could be read at all. **There is no size channel now**: row-group size is a column of the table
  directly above and a line of every hover, so nothing was lost from the page.
  **One time axis is not a compromise, it is the correct encoding.** Both tiers are milliseconds,
  and two scales for two measures of the same kind is the dual-axis mistake: a length spanning two
  scales is not a quantity. Warm is always the LEFT end because it is always the smaller number.
  **BOTH AXES ARE LOG, and on x that changes what a length MEANS — from a difference to a RATIO.**
  That is the better quantity and the reason for it: "the cold pass is 6x the warm one" is a
  property of the layout, where "18,000 ms slower" mostly tracks how big the query happened to be.
  It also fixed real crowding — warm at 3,000–6,500 against cold at 20,000–37,000 pinned every warm
  end into the left eighth of a linear plot, so the ends could not be compared with each other at
  all; on log the lines span 84→882 instead of 107→820, and near-coincident pairs fell from four to
  two. On y it un-squashes the cheap layouts, which sit within half a decade of each other.
  **`logScale` does NOT snap the bound out to whole decades**, for the same reason `niceScale` snaps
  the step and not the bound one function above it: CU spans 1,332–3,769, so a 1,000–10,000 axis
  would put every layout on the page in the bottom half. Ticks come from mantissa sets, coarsest
  that still fills the axis — `[1,2,5]` yields a single tick over half a decade, and an axis with no
  numbers reads as a rendering failure rather than as a narrow range.
- **Every line is named at its COLD END, and what the name says depends on the writer.** The writer
  itself when that identifies the line, which `uniqueName` decides — `dwh`, the three spark
  profiles. `delta_rs` is most of the lines, so its name separates nothing; it is in the legend, and
  what actually tells its lines apart takes its place: the sort key and the row group count,
  `date, time · 9 RG`. That string is `layoutLabel`, the same one the analytics bar is captioned
  with, minus its leading `by ` — so a line and the mart's own rows cannot describe one parquet two
  different ways, and three characters each is what keeps eleven of them off each other.
  **The cold end because that is where the eye already is**: the cold ends are what the chart is
  ranked by and what spreads out, while the warm ends bunch toward the y axis. The warm end is the
  FALLBACK — both ends are free on a line with no writer name, so a label that cannot fit beside the
  cold end moves rather than being forced somewhere it overlaps. On the real records 10 of 11 sit
  beside a cold end and 1 falls back; that one's line runs 8.5 units from its neighbour's, less than
  a line of text, so both cannot sit on the same side of their own marks. A name is never dropped —
  an overlapping one is recoverable by hovering, an absent one is the bug this was built to fix —
  and `force()` flips side rather than running off the plot, which the bounds-free version did once,
  25 units past the y axis and across an unrelated line.
  **The x axis reserves a LABEL GUTTER on its high side (`padHi` 1.55), and only when something goes
  in it.** Names read rightward from the cold end and the cold ends are the far right of the plot;
  at the symmetric pad the rightmost had 22 units of room for a name needing 139. Widening the axis
  moves every line left together — it cannot mislead, because the ticks come from the same scale.
  Reserving it unconditionally is the part that had to be conditional: on a dense cluster of plain
  dots, squeezing the x span squeezed the gaps a label has to find and eleven names that used to fit
  started colliding. A gutter with nothing in it is pure loss.
  **THE KNOWN COST OF DROPPING THE MARKERS: two layouts at nearly the same CU now overlap along
  their whole length.** A dot marked one x, so two of them at the same height still sat apart; a
  line spans most of the plot. On the real records four pairs land within 3 units of each other, all
  `delta_rs`, i.e. the same hue. `.pair` is drawn at `opacity: .8` so an overlap composites darker
  rather than reading as a single line — a mitigation, not a fix. If it ever misleads, the honest
  levers are jittering y or going back to end markers, **not** dropping runs from the chart.
  **`iceberg` is still off it (`SCATTER_OMIT`), and the shared axis makes that stronger** — its cold
  pass is 124,036 ms against 20,000–37,000, so its line would be four times the next longest and
  squash every other one into a fraction of the plot. A named constant, never a computed outlier
  rule, and the subtitle says what was left out and says nothing when nothing was.
  **A layout with no warm pass is still plotted, as a plain dot** — one shape per point, never both,
  and unmeasured is an absent thing rather than a zero: a line run back to x=0 would read as "this
  layout answered instantly". The axis reaching 0 on today's data is a *consequence* (the combined
  span snaps `niceScale`'s step to 5,000), not a zero-baseline policy.
  One `<title>` per layout, on the line itself — which is the mark a reader is pointing at.
- **THE TWO CU BAR CHARTS ARE DELETED, and what they drew is not.** They were
  `Capacity units per parquet layout` and `Capacity units per engine build`, stacked, analytics
  first. Removing them is not a judgement on the build half — that is still where the sharpest
  operational result on the page lives (duckrun costs 1.8x at 64 cores for the same wall time, which
  the analytics keying structurally cannot show, because both runs wrote identical parquet). It is
  that **both drew a figure the page already prints as a figure one block away**: the analytics bar
  was the `CU` column of *Cost and speed by parquet layout*, the build bar was the `etl` row of
  *Cost by engine*. A bar length is a worse way to read a number you can simply be told.
  What went with them: `chartSvg`, `barPath`, `groupRows`, the `.bar`/`.bar-label` rules and four
  unit tests about bar geometry. What did NOT: `spreadFor`, which the noise floor still uses, and
  every grouping rule below, which now surfaces as table rows and as the line chart's marks.
  **The no-loss claim is the thing to re-check before restoring one.** A test pins it — if either
  number ever stops being printed, a chart brought back for it is a different argument from the one
  that removed these.
- **A LAYOUT GROUP IS ONE ROW, and the grouping is the same one the line chart plots.** Power BI
  never sees the engine — it opens parquet through Direct Lake and transcodes row groups — so what a
  query costs belongs to what was WRITTEN, and the writer is metadata. The row is named for its
  writer; the `ordering` and `row group size` cells are what tell two rows of one writer apart.
  **Grouping is MEASURED, labelling is DECLARED.** The key is
  `(V-Order, power-of-two band of files, power-of-two band of row groups, sort columns)` read off
  the parquet as `stats.py` saw it, so two unrelated engines that wrote the same shape *do* share a
  row. The sort element is the run's own COLUMN LIST, not a boolean, so two sorts on different keys
  can never merge — the `['date','time','DUID']` and `['date','time']` runs split by file band today
  only by luck. **The columns come off the RECORD, never a constant here**: the key is a property of
  the COMMIT, and the model declared `['date','time','DUID']` for a while and `['date','time']`
  since. Two spellings are read, both legitimate: `dbt.<engine>.sort_by` is what the run DECLARED
  (`stats.py`), `dbt.<engine>.sort_by_auto` what duckrun's picker RESOLVED (`fabric_run.py`'s log
  scrape, the only witness for an `'auto'` run).
  **It groups RUNS, not columns, and that distinction is load-bearing.** A column is
  `(engine, config)`, so two of its runs can write different parquet — `duckrun·64c+sorted` wrote
  3 files / 26 row groups under an explicit `sort_by=['date','time','DUID']` and 4 files / 25 under
  the `sort_by='auto'` the picker resolved to `['date','time']`. Grouping the columns and averaging
  every run of each put those two together at their mean (2,041.8 — a number neither run measured),
  described by only the newer one's shape. Per run they are two rows sharing a writer, and the key
  cells are what tell them apart. A run with no file count at all falls back to its column rather
  than to a row of its own — two *unmeasured* layouts are still never merged, but one column's own
  runs are not split with nothing able to say why.
  Banded, not exact: exact equality splits dwh's own two runs from each other (78 files and 80) and
  splits duckrun on 1.1 MB of size. The accepted cost is the boundary — 15 row groups and 17 land in
  different bands. A record with **no** file count keys to `null` and keeps a row of its own.
  It surfaces two things a per-engine view hid: V-Order on and off sit in the same file band and
  differ 2.8x (1,332 against 3,769), the sharpest experiment on the page; and NEE on and off produce
  the same layout, so the gap between them was never an NEE effect.
- **The figure is the MEDIAN of the group's runs, never the mean** — `groupMid`, called by *Cost and
  speed by parquet layout*, the mart rows and the line chart alike, so the three cannot disagree. One
  dispatch is a sample of a shared capacity and a bad sample is not a property of the layout: run
  30966983384 read 2,629.3 against 1,331.5/1,577.1/1,586.7 for byte-identical parquet, because its
  XMLA read billed 49s against ~33s and its refresh took 28.4s against ~8s — Fabric being busy, not
  the parquet being slow. A mean let it lift that figure 11% and dwh's 16%. **It is not a noise fix,
  and the page should not be read as if it were**: at n=1 and n=2 the median IS the mean, and four of
  nine groups are that thin. The min/max are still measured and still reachable, in `Every run`'s
  per-run rows, so the outlier is findable rather than quietly averaged away.
- **A column header is an engine plus the SHORTEST config that still tells it apart** (`variantTag`).
  It appears in every table and in the chart, so width is a real cost. One rule keeps it short: a flag
  that is **off is absent** rather than negated — `spark·readHeavyForPBI+NEE` against
  `spark·readHeavyForPBI`, never `+noNEE`, which spends header width saying nothing happened when the
  contrast with the run that did enable it is what a reader is looking for.
  **The RESOURCE PROFILE is printed verbatim, and a second rule that shortened it is gone.** A
  `PROFILE_LABEL` map renamed the two in use by their effect — `readHeavyForPBI` → `V-Order`,
  `writeHeavy` → `default` — and it has been removed in both directions. Those strings are what the
  dispatch input takes, what `profiles.yml` sets and what Microsoft's own profile reference publishes,
  so the rename made a reader translate to match this page against a run's inputs, and the page and
  the record called one setting two things. `default` was the worse half: it named the workspace's
  *choice* rather than the profile, so it would silently become a lie the day that default changed,
  and it hid which profile a bare dispatch actually got. The effect is still said — **where it is
  measured rather than declared**: `layoutLabel` reads `vorder` off the parquet, so a label reads
  `spark readHeavyForPBI` over `V-Order · 10–11 RG`. The label names the knob that was
  turned, the caption states what came out — a split that also survives a profile whose name misleads,
  which is not hypothetical, since `readHeavyForSpark` reads like it enables V-Order and sets no
  vorder at all. One cost, worth knowing: column order is alphabetical, so renaming moved
  `readHeavyForPBI` ahead of `writeHeavy` where `V-Order` had followed `default` — an order that
  changed with a label rather than with anything measured, which is one more argument for the
  verbatim spelling. `CONFIG_LABEL` is now the only relabelling left, and it exists because
  `sorted=true` has no name of its own to print.
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
- **Input archive**: files and bytes in the landing archive, from `stats.py`'s listing. Every other number describes what came OUT, and this is the one copy of what went in —
  shared by every engine, so it belongs with the provenance rather than among the columns it is not
  one of. It used to sit between the engine table and the layout, where a table with no engine in it
  read as a column that had gone missing.
- **Table layout**, every shared table, mart first, **one row per WRITER, and no `writer` column** —
  the row label IS the writer, so a `duckdb (iceberg)` cell beside a `duckdb iceberg` label was one
  fact printed twice. `spark readHeavyForPBI`, `spark writeHeavy`, `duckrun`, not
  `spark·readHeavyForPBI+NEE` and `duckrun·64c`. The resource profile is printed verbatim; the core
  count and NEE flag are dropped because two runs each showed they never reach the parquet. duckrun's two core counts and spark's two NEE
  settings therefore collapse to one row — they had written identical layouts, so the rows they
  replaced were the same row printed twice.
  **The MART block is the exception: its rows ARE the chart's bars**, same grouping and same members,
  which is what keeps a writer that produced two different shapes on two rows — `duckrun sorted` wrote
  3 files/26 RG and 4/25, which is two layouts and not one, and the `files`/`row groups` columns say
  which is which. **The block is PHYSICAL LAYOUT ONLY**: the analytics CU and the three query tiers
  that used to sit beside the mart are now the *Cost and speed by layout* table above, so one table
  no longer answers both what the parquet looks like and what querying it cost. Rows are FEWEST FILES
  FIRST — it sorted by the CU column, and ordering by a column that is no longer printed is a ranking
  a reader cannot check.
  Every other block stays one row per writer: they are physical layout alone, describing a table the
  mart's shape says nothing about, so splitting them the same way would print one row twice for a
  difference that is not in it.
  **The row count is in the heading, not a column**: it is identical on every row by design, which is
  the parity statement the whole project rests on, and 143,980,961 repeated down a table is a wide
  column carrying one fact. When the engines DISAGREE the heading says so and the column comes back —
  though **for the mart that branch is now unreachable**, because the generation filter below has
  already dropped anything that disagrees. It still fires for every other table. `rows per RG` is
  abbreviated (`13.1M`, `122.9K`) — that number spans four orders of magnitude across these engines
  and the ratio is the finding, not the twelve digits.
- **THE PAGE SHOWS ONE SOURCE GENERATION, AND THE NEWEST RUN DEFINES IT.** `sameGeneration()` reads
  the mart's `total_rows` from the latest record and drops every run that disagrees. The columns are
  different dispatches days apart and nothing else made them comparable: change the AEMO archive and
  an engine nobody has rebuilt keeps its column, with its numbers sitting beside engines built from
  different data — in the tables, and inside the chart's own bars.
  **Newest wins, not the most common value.** Right after a genuine source change the old count is
  still the majority, which is exactly the case this exists for; a mode would keep the stale
  generation and drop the new run.
  It runs **before `columnsFor`**, which matters twice: `columnsFor` takes the latest run per
  (engine, config), so filtering later would let a stale run hold a column, and `spreadFor` walks the
  whole array for the chart's marks, so filtering the array is what stops a group blending two
  generations.
  **The exclusion is loud on purpose, and must stay that way.** It bought its silence from the
  `row counts DISAGREE` heading, so it pays it back: every dropped run is named with its engine, run
  id, own count and delta against current, plus the reference, plus `(+N excluded)` in the footer.
  Named, it is sharper than the heading was — "duckrun wrote 143,980,960 against the current
  143,980,961" beats "row counts DISAGREE".
  A run recording **no** count is KEPT (unmeasured is a different claim from different), with no
  reference anywhere **nothing** is filtered rather than everything vanishing, and `?record=` bypasses
  it entirely because pinning a run means asking for that run.
  **Its failure mode is stated on the page:** newest-wins cannot tell "the source changed" from "the
  newest run is broken", so a bad newest run excludes all the good history. Survivable because it is
  loud — the note says `N of M runs were excluded` and that the newest is then the likelier anomaly —
  and because the next good run reverses it.
- **`cold` / `warm` / `hot` appear TWICE, and the two answer different questions.** They are the one
  thing on the page that is not capacity units, and they come from the run records rather than the
  ledger: `benchmark.timings.<model>.<query>` is already on every record. `benchmark/render_report.py`
  renders it per dispatch, but a dispatch builds ONE engine, so that report always has a single column
  and a degenerate ranking — composed here, this is the only place the tiers can be read across
  engines at all.
  **Per LAYOUT** in *Cost and speed by layout*, beside the CU, which is a group's median over its runs
  — and the cold and warm medians are also the two ends of the line chart directly below it, same
  `martPoints`, same `groupMid`, so the chart and the table cannot disagree about a number.
  **Per RUN** in the sources table, which is what actually measured them: one dispatch, against one
  semantic model it had just deployed. They were columns of the mart's layout block and are not any
  more — there they had to be a group mean sitting on a row about parquet, and no single run recorded
  that number.
  **cold** is the first visit to a freshly deployed semantic model, **warm** the second, **hot** the
  median of the passes after that; the record's own `tier` field is something else entirely (the
  query CATEGORY — `probe`/`composite`/`raw`/`hot_only`) and must not be confused with them. Each is
  summed over the queries every run carries at that tier, and the note counts them, because it
  genuinely differs — the selectivity-ladder queries have no `cold_ms` at all, the top DUID being
  resolved only after pass 1, so cold is two queries short of warm and hot.
  Deliberately **reimplemented rather than imported** — `render_report._totals`/`rank` take exactly
  this shape, and importing `benchmark/` would end the isolation that makes this directory deletable.
- **`compute seconds` is ONE ROW, ON THE `etl` HALF ONLY** — how long the build billed for, read
  from `Duration (s)` in the same Capacity Metrics row as the CU, so it costs no extra query. It was
  removed once and is back: billed operation seconds SUM across concurrent operations, which is a
  real objection and unchanged, but "how long did the build take" deserves an answer and the hedge
  now rides in the row's own label (`compute seconds` — *billed, not wall clock*) instead of in a
  note four rows below where it is attached to nothing. A duckrun leg is one long notebook run so its
  seconds land close to the clock; spark's five Livy REPLs under one session sum to more than the
  wall time anyone waited. Compare it freely between two runs of the same engine, across engines only
  knowing that.
  **`analytics` gets no such row on purpose:** the query half already reports latency as the
  `cold`/`warm`/`hot` milliseconds beside the layout that produced them, and those are time a user
  actually waited. A second, differently-defined duration next to them would invite a comparison.
  **COMPUTE seconds, never total**, which also makes the column reconcile against itself: `compute`
  CU ÷ `compute seconds` is exactly the rate underneath (duckrun·64c: 20,665.6 ÷ 646 = 32.0).
- **`compute CU per second` is a ROW OF THE ENGINE TABLE, not a section.** It comes off the SAME
  Capacity Metrics row as the CU above it — same GUIDs, same roles, same compute/storage split — so a
  table of its own restated the whole join to add two numbers per class. It is the sturdiest number
  here: the concurrency that makes the seconds awkward is in the numerator and the denominator alike,
  so it cancels. A high rate is a WIDE engine, not a slow one.
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
- **There is no chart of the seconds — which is exactly why they are a table row.**
  The page carries two bars and both are capacity units, the measure it leads with and can defend. A
  third in the same visual language, drawn from numbers that need a caveat, invites precisely the
  cross-engine ranking the caveat withdraws. A number that needs a caveat belongs where the caveat
  can sit beside it — in the row label — not in a mark, where length alone reads as a ranking.
- **A record has to be built and benchmarked to reach the page.** `incomplete()` skips anything else
  and names why — a run with no benchmark shows an empty analytics column, which reads as "querying
  this engine was free" rather than "nobody measured it". The skipped records are **listed by file
  and reason in the sources section**, visible and never folded — they used to be only a count in
  the live status line, which the offline copy does not even have.
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

The core count still reaches the chart because a run at a different size is a different data point —
but through the column tag (`duckrun·64c`), not a caption. An ETL caption states only what the
column name does not already say, which in practice is the vCores of a single-config engine whose
bare column carries no tag: `dbt-fabricspark · writeHeavy · NEE off` under a column already labelled
`spark·writeHeavy` was three facts the label carries (the profile named by its effect, an off flag
absent, the adapter implied by the engine name).

## Things that will bite

- **`app.js` must not touch `document` at import time.** It exports pure functions that return
  STRINGS and boots only under `DOMContentLoaded`; that is what lets the whole page — join, layout
  grouping, the chart — be tested under `node --test` with no browser and no jsdom.
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
