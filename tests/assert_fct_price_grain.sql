-- Grain tripwire for the fct_price merge key. Catches three things at once:
--   * a duplicate that predates the append -> merge conversion;
--   * a NULL-key leak — SQL NULL != NULL, so a row with NULL INTERVENTION can never match
--     in the merge ON clause and would be re-inserted on any reprocess. GROUP BY groups
--     NULLs together, so this test still sees it;
--   * the residual concurrent-writer race on dwh, where snapshot isolation still lets two
--     overlapping transactions both insert. duckrun/iceberg/spark fail the commit loudly
--     instead, so for those three engines a hit here means a pre-existing duplicate.
--
-- Scoped to a rolling 30-day window, and deliberately NOT tagged heavy. The tagged tests
-- are excluded from the CI test job, which would make this tripwire decorative; the window
-- is what buys it a place in every run. A race can only duplicate rows a run is currently
-- writing, and runs only ever write recent files, so 30 days covers the exposure with room
-- for a backlog catch-up while letting delta_scan prune away nearly all of the table.
-- For a full-history sweep (worth doing once, before the first merge run), drop the WHERE.
-- Remediation: `dbt run --full-refresh -s fct_price` on duckrun/iceberg/spark; a targeted
-- DELETE on dwh — never --full-refresh there.

SELECT file, REGIONID, SETTLEMENTDATE, INTERVENTION, COUNT(*) AS n
FROM {{ ref('fct_price') }}
WHERE "DATE" >= current_date - INTERVAL 30 DAY
GROUP BY ALL
HAVING COUNT(*) > 1
