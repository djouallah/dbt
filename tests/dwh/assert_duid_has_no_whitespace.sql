-- No DUID may contain whitespace — leading, trailing, or embedded. T-SQL dialect (Fabric
-- Warehouse). See tests/duckdb/assert_duid_has_no_whitespace.sql for the incident this guards.
--
-- This is the copy that had to exist. The bug is a *T-SQL* pathology — ANSI trailing-space padding
-- means 'ERB01' = 'ERB01 ' is TRUE here and FALSE in DuckDB and Spark — so gating the guard to the
-- duckdb targets put it on the two engines that cannot exhibit it. dim_duid is built from the same
-- CSVs everywhere, so a dirty key does trip the DuckDB copy too, but only this one runs against the
-- table where the padding actually changes a join result.
--
-- Two dialect differences from the DuckDB copy, both mandatory:
--
--   * No regexp_matches. LIKE with a character class is the portable T-SQL equivalent, and it is
--     the right tool here specifically because LIKE does NOT pad: every character of the value is
--     significant, so a trailing space in DUID is available to match. Do not "simplify" this to
--     `DUID <> LTRIM(RTRIM(DUID))` — that is a comparison, comparisons pad, and it is therefore
--     always FALSE for exactly the trailing-space case this test exists to catch.
--   * LEN() ignores trailing spaces, so it would report the padded and clean values as the same
--     length — the one thing the failure output needs to show. DATALENGTH counts them.

SELECT
  DUID,
  DATALENGTH(DUID) AS len,
  '[' + DUID + ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_duid') }}
WHERE DUID LIKE '%[' + CHAR(9) + CHAR(10) + CHAR(13) + ' ]%'
