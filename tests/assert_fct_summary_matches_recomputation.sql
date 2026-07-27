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
WHERE a.n IS DISTINCT FROM e.n
   OR a.mw IS DISTINCT FROM e.mw
   OR a.price IS DISTINCT FROM e.price
ORDER BY date
