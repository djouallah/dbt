-- Uniqueness of the fct_summary merge key: (date, time, DUID). T-SQL dialect (Fabric Warehouse).
-- Same assertion as tests/duckdb/assert_fct_summary_grain.sql — read that one for why it reads
-- fct_summary and nothing else, and what that deliberately leaves unwatched.
--
-- This is the copy that matters most. Fabric Warehouse is the ONE engine whose write path can
-- actually produce a duplicate: duckrun, iceberg and spark check the commit and fail loudly on a
-- real overlap, while under snapshot isolation two dwh transactions overlapping in time can both
-- insert. The merge on the full grain shrinks that window to the transaction overlap, and T-SQL
-- offers nothing stronger without application locks Fabric DW lacks — so this test is the detector
-- for the remainder. It used to be unreachable here (the singular suite was DuckDB SQL, gated off
-- dwh) and the standing advice was to run it by hand after any run that could have overlapped.
--
-- Two dialect differences from the DuckDB copy, both mandatory:
--   * no GROUP BY ALL in T-SQL — the key columns are spelled out;
--   * [date] and [time] are reserved words, so every reference is bracketed. The dwh model writes
--     them under exactly these names (unique_key=['[date]', '[time]', '[DUID]']).

SELECT [date], [time], [DUID], COUNT(*) AS n
FROM {{ ref('fct_summary') }}
GROUP BY [date], [time], [DUID]
HAVING COUNT(*) > 1
