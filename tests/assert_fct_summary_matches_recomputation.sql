-- Determinism tripwire: within the trailing 7 days of daily data (plus the intraday tail
-- beyond it), the stored fct_summary must EXACTLY equal a clean recomputation of the
-- model's full-refresh logic from its inputs. Any row returned means the incremental
-- write path let the table drift from f(inputs) — the bug class that fossilized three
-- different row counts across four engines fed identical inputs. Zero tolerance.
--
-- The window is 7 days because that is fct_summary's rebuild window: this test must
-- never check a date the model is not allowed to repair, or a single drifted day would
-- hold CI red until someone ran --full-refresh by hand. Widen both together or neither.
-- Deliberately NOT tagged heavy — the window keeps it a bounded scan, so CI's neutral
-- reader runs it against every engine. Older history is settled once correct; the
-- cross-engine row-count parity dashboard is what guards it.
WITH bounds AS (
  SELECT
    MAX(s.DATE) - INTERVAL 6 DAY AS d0,
    MAX(CAST(s.SETTLEMENTDATE AS TIMESTAMPTZ)) AS daily_ts
  FROM {{ ref('fct_scada') }} s
),

-- Must stay byte-for-byte equivalent to fct_summary's own dispatch_duids CTE: the model
-- gates its intraday branch on this set, so a test that did not would fail by construction.
-- The two change together or not at all. Rationale (26 non-scheduled units that publish
-- SCADA telemetry but are never dispatched) is in the model header.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),

-- The model's own grain-level logic, recomputed from upstream (same joins, same
-- filters, same casts as fct_summary's full-refresh branch).
recomputed AS (
  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    s.DUID,
    CAST(MAX(s.INITIALMW) AS DECIMAL(18, 4)) AS mw,
    CAST(MAX(p.RRP) AS DECIMAL(18, 4)) AS price
  FROM {{ ref('fct_scada') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.DATE >= (SELECT d0 FROM bounds)
  GROUP BY ALL

  UNION ALL

  SELECT
    s.DATE AS date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) AS time,
    s.DUID,
    CAST(MAX(s.INITIALMW) AS DECIMAL(18, 4)) AS mw,
    CAST(MAX(p.RRP) AS DECIMAL(18, 4)) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Mirrors the model's intraday gate. Without it this test fails by construction.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT daily_ts FROM bounds)
  GROUP BY ALL
),

expected AS (
  SELECT date, COUNT(*) AS n, SUM(mw) AS mw, SUM(price) AS price
  FROM recomputed
  GROUP BY date
),

actual AS (
  SELECT date, COUNT(*) AS n, SUM(mw) AS mw, SUM(price) AS price
  FROM {{ ref('fct_summary') }}
  WHERE date >= (SELECT d0 FROM bounds)
  GROUP BY date
)

SELECT
  date,
  e.n AS expected_rows,
  a.n AS actual_rows,
  e.mw AS expected_mw,
  a.mw AS actual_mw,
  e.price AS expected_price,
  a.price AS actual_price
FROM expected e
FULL OUTER JOIN actual a USING (date)
-- Row count stays EXACT. It is dialect-independent, and a wrong count is the bug this test was
-- written for — three engines fed identical inputs holding three different counts. A NULL on
-- either side (a date present in one and not the other) is caught here too, via IS DISTINCT FROM.
WHERE a.n IS DISTINCT FROM e.n
-- Sums cannot be exact across engines, and no rebuild will make them so. `expected` is
-- recomputed by the neutral DuckDB reader; `actual` was written by Spark or Fabric Warehouse.
-- The three disagree on how DOUBLE -> DECIMAL(18,4) breaks a tie — Spark rounds HALF_UP (away
-- from zero, confirmed on negatives), DuckDB HALF_EVEN, T-SQL a third way — so a value sitting
-- exactly on a half at the 5th decimal lands 0.0001 apart. Measured: ~146 rows per date of
-- ~65,000, deltas of EXACTLY +/-0.0001 and nothing else, netting ~0.012 per date. See
-- LEARNINGS.md. The engines are not drifting; the assertion was un-satisfiable as written.
--
-- Tolerance is n * 0.0001, not a hand-picked constant: one ULP per row, every row tied, all in
-- the same direction. That is the largest divergence rounding CAN produce, so anything above it
-- is provably something else. On a real date that is ~6.5 against sums of ~6.9M — still 1e-6
-- relative, and ~550x the drift actually observed, while a single wrong row moves the sum by
-- whole MW. Widen this only with a measurement that says why.
   OR abs(a.mw - e.mw) > e.n * 0.0001
   OR abs(a.price - e.price) > e.n * 0.0001
ORDER BY date
