# To do

Open work that needs a decision or a dispatch. Not a wishlist — an item earns a place here by being
something a future session would otherwise have to rediscover.

[README.md](README.md) states the thesis, [LEARNINGS.md](LEARNINGS.md) records the investigations,
[CLAUDE.md](CLAUDE.md) records the rules those imply, [RETROSPECTIVE.md](RETROSPECTIVE.md) what the
exercise cost. This file is what has not been done.

---

## `etl CU (8 vCores)` is a dash on 7 of 17 layout groups

*Cost and speed by parquet layout* reports build cost at ONE core count, because build cost tracks
the machine and `layoutKey` does not carry `vcores` — a group holds runs from several machines and a
median over them describes none of them (measured: one duckrun layout reads **9,986 CU at 8 vCores
against 22,547 blended** across 8/16/32/64). See the `ETL_VCORES` comment in `dashboard/app.js`.

Seven groups have never been built at 8 vCores, so their cell is a dash. All seven are duckrun; every
one exists only at 64 cores.

**⚠️ The nightly does NOT fill these in, and an earlier note claiming it would was wrong.** The
nightly builds one layout — `sort_by=date,time,price` at `row_group_size=2000000`, 72 row groups —
and that group already has 8-core runs (it reads 9,897). A layout group is keyed on the sort columns
and a band of the row-group count, so every one of the seven below is a layout the nightly never
writes. Filling them needs a deliberate dispatch each.

| `sort_by` | row groups | `row_group_size` to dispatch | runs in the group |
|---|---:|---:|---:|
| `date,time` | 19–25 | `6000000` | 6 |
| `date,time,DUID` | 19–24 | `6000000` | 2 |
| `date,DUID,time` | 24 | `6000000` | 5 |
| `date,time,price` | 24 | `6000000` | 1 |
| `date,time` | 72 | `2000000` | 1 |
| `date,time,price` | 144 | `1000000` | 1 |
| `date,time` | 144 | `1000000` | 1 |

`row_group_size` is DERIVED (`143,980,961 ÷ row groups`) because most of these records predate that
dispatch input and carry no `inputs.row_group_size` to copy. The 19–25 band is one group, so one
dispatch at `6000000` covers it.

Each row is one `Benchmark` dispatch at **`cores=8`** with that `sort_by` and `row_group_size`,
everything else default.

**The cost is real and is the reason this is a to-do rather than a task.** Seven from-scratch builds
of 370M rows plus their query passes. At 8 vCores the CU rate is `cores / 2` = 4/s and a build reads
~10,000 CU, so each is ~40 minutes of compute and the set is roughly **70,000 CU**.

Three ways to close it, in rough order of preference:

1. **Dispatch the seven.** Complete data, known cost, no code change.
2. **Do nothing.** A dash is honest — it says "never built at this size", which is true and is not
   the same as a zero or a blend. The column is already comparable across the rows that have it.
3. **Lower `ETL_VCORES` coverage by re-pinning it.** Only worth it if the fleet's usual core count
   moves; the constant already has to be kept in step with the dispatch default by hand, and moving
   it to chase coverage would make the column mean whatever happens to be best populated.

Do **not** close it by widening the filter to blend core counts. That is the thing the column was
built to stop.
