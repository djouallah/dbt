-- No DUID may contain whitespace — leading, trailing, or embedded.
--
-- This is not cosmetic. DUID is the join key from dim_duid into fct_scada, and the engines do
-- not agree on what equality means when one side is padded:
--
--   T-SQL   'ERB01' = 'ERB01 '  ->  TRUE   (ANSI trailing-space padding on comparison)
--   DuckDB  'ERB01' = 'ERB01 '  ->  FALSE
--   Spark   'ERB01' = 'ERB01 '  ->  FALSE
--
-- So a single trailing space in the AEMO registration CSV split the engines in half. `dwh`
-- joined the unit and carried it into fct_summary (113,959 rows, from 2025-03-04 onward);
-- duckrun, iceberg and spark dropped it silently and had never included it at all. Nothing
-- failed, nothing warned — the only symptom was fct_summary row counts differing by ~250 per
-- date, which read as write-path drift on the one engine that was actually correct.
--
-- A padded key here means at minimum a silent cross-engine disagreement, and at worst a real
-- generating unit missing from three of four outputs. Catch it at the dimension, where it enters,
-- rather than inferring it from a row-count gap downstream. See LEARNINGS.md.
--
-- DuckDB dialect (duckrun + iceberg). tests/dwh and tests/spark carry the same assertion; the dwh
-- one is the load-bearing copy, since T-SQL is the only dialect where a padded key joins anyway.
--
-- \s covers leading and trailing spaces, tabs and newlines, and embedded whitespace too — no
-- DUID legitimately contains any of them.
SELECT
  DUID,
  length(DUID) AS len,
  '[' || DUID || ']' AS boxed   -- makes the padding visible in the failure output
FROM {{ ref('dim_duid') }}
WHERE regexp_matches(DUID, '\s')
