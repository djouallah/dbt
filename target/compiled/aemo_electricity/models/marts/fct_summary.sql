



-- Full refresh from daily data
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM "ducklake"."aemo"."fct_scada") as cutoff,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM "ducklake"."aemo"."fct_scada" s
  LEFT JOIN "ducklake"."aemo"."dim_duid" d ON s.DUID = d.DUID
  LEFT JOIN "ducklake"."aemo"."fct_price" p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  cutoff
FROM daily_summary
ORDER BY date

