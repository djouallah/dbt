-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

{% if is_incremental() %}

-- Intraday incremental: append new records after max cutoff
WITH max_cutoff AS (
  SELECT MAX(cutoff) as cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.DATE as date,
    s.SETTLEMENTDATE,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.DATE >= CAST(mc.cutoff AS DATE)
    AND s.SETTLEMENTDATE > mc.cutoff
    AND p.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
),

final_with_cutoff AS (
  SELECT
    date,
    SETTLEMENTDATE,
    DUID,
    CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT) AS time,
    CAST(mw AS DECIMAL(18, 4)) AS mw,
    CAST(price AS DECIMAL(18, 4)) AS price,
    MAX(SETTLEMENTDATE) OVER () AS cutoff
  FROM incremental_data
)

SELECT
  date,
  time,
  DUID,
  mw,
  price,
  cutoff
FROM final_with_cutoff

{% else %}

-- Full refresh from daily data
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}) as cutoff,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
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

{% endif %}
