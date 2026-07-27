-- Crater tripwire: a date left PARTIALLY filled by an interrupted intraday run.
-- A completed date must cover (nearly) all 288 five-minute intervals; the latest date
-- is excluded (legitimately still filling).
-- The mechanism that made craters permanent is fixed: the daily branch used to offer
-- only dates missing ENTIRELY from the summary, so a half-filled date was skipped
-- forever (observed 2026-06-07 / 2026-06-14). fct_summary now re-emits the complete
-- recomputation for a trailing 7-day window, which repairs a crater as soon as the
-- daily file lands. This test defends that: a hit now means either a crater older than
-- the rebuild window, or the window logic itself regressed.
-- Scoped to a rolling 12-month window: craters only form going forward and get
-- remediated once flagged, so there is no point re-scanning frozen history every
-- run — the window also lets Iceberg prune the scan. The known by-design SOURCE
-- gaps (a missing daily archive file clips ~4h off one date and ~20h off the
-- next: 2018-08-30/31, 2019-12-31/2020-01-01 — pairs summing to 288) sit far
-- outside this window, so they can never fail it.
-- Deliberately NOT tagged heavy: unlike the scada-vs-summary assertions this only
-- reads fct_summary itself, so a partially drained backlog can't false-positive it.
-- Remediation: inside the 7-day rebuild window it heals itself on the next run. Older
-- than that, `dbt run --full-refresh -s fct_summary` (on dwh, REBUILD_SUMMARY=1).

WITH per_date AS (
  SELECT
    date,
    COUNT(DISTINCT time) AS intervals
  FROM {{ ref('fct_summary') }}
  WHERE date >= current_date - INTERVAL 12 MONTH
  GROUP BY date
)

SELECT
  date,
  intervals
FROM per_date
WHERE date < (SELECT MAX(date) FROM per_date)
  AND intervals < 280
ORDER BY date
