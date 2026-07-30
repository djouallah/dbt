-- Uniqueness of the fct_summary merge key: (date, time, DUID). Spark SQL dialect (Fabric Spark).
-- Same assertion as tests/duckdb/assert_fct_summary_grain.sql — read that one for why it reads
-- fct_summary and nothing else, and what that deliberately leaves unwatched.
--
-- Spark checks the Delta commit, so a real concurrent overlap fails the write loudly rather than
-- duplicating; this is the belt to dwh's braces. It still earns its place: `skip_matched_step`
-- makes the merge insert-only, so a source batch carrying the same key twice is not caught by the
-- write, only here.
--
-- Dialect note: GROUP BY ALL exists on Spark 3.4+ and would work, but the columns are spelled out
-- to match the dwh copy — and `date`/`time` are backticked, which is unconditionally safe whatever
-- ANSI mode the session runs in.

SELECT `date`, `time`, DUID, COUNT(*) AS n
FROM {{ ref('fct_summary') }}
GROUP BY `date`, `time`, DUID
HAVING COUNT(*) > 1
