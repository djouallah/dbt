-- No DUID may contain whitespace — leading, trailing, or embedded. Spark SQL dialect (Fabric
-- Spark). See tests/duckdb/assert_duid_has_no_whitespace.sql for the incident this guards.
--
-- Spark compares like DuckDB does ('ERB01' = 'ERB01 ' is FALSE), so a padded key here loses rows
-- silently rather than gaining them. That is the failure mode the original incident produced on
-- three of four engines, and it is invisible without this test: nothing errors, the unit simply
-- never appears.
--
-- Dialect note: RLIKE, not regexp_matches. '\\s' is a SQL string literal, and Spark's parser turns
-- \\ into a single backslash before the regex engine sees it (escapedStringLiterals defaults to
-- false), so the pattern that runs is \s — leading and trailing spaces, tabs, newlines, and
-- embedded whitespace. Writing '\s' would depend on that setting; keep both backslashes.

SELECT
  DUID,
  length(DUID) AS len,
  concat('[', DUID, ']') AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_duid') }}
WHERE DUID RLIKE '\\s'
