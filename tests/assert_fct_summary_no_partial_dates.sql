-- Crater tripwire: a date that lands PARTIALLY in the summary is never revisited.
-- Mechanism: the intraday branch tops a date up hour by hour; if the pipeline stops
-- mid-day (outage), the date is left half-filled — and the daily backfill skips it
-- forever because its filter is `date NOT IN (SELECT date FROM summary)` and the
-- insert-only merge never updates matched keys (observed 2026-06-07 / 2026-06-14).
-- A completed date must cover (nearly) all 288 five-minute intervals; the latest
-- date is excluded (legitimately still filling).
-- Scoped to a rolling 12-month window: craters only form going forward and get
-- remediated once flagged, so there is no point re-scanning frozen history every
-- run — the window also lets Iceberg prune the scan. The known by-design SOURCE
-- gaps (a missing daily archive file clips ~4h off one date and ~20h off the
-- next: 2018-08-30/31, 2019-12-31/2020-01-01 — pairs summing to 288) sit far
-- outside this window, so they can never fail it.
-- Deliberately NOT tagged heavy: unlike the scada-vs-summary assertions this only
-- reads fct_summary itself, so a partially drained backlog can't false-positive it.
-- Remediation: DELETE the flagged dates from fct_summary — the next incremental
-- run's daily branch re-inserts them in full from fct_scada.

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
