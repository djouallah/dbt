-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE EXISTS (SELECT 1 FROM {{ ref('fct_scada') }} s WHERE s.SETTLEMENTDATE > (SELECT MAX(cutoff) FROM {{ this }}) AND s.INTERVENTION = 0 LIMIT 1){% endif %}"
) }}

{% if is_incremental() %}

{%- set has_new_daily_query -%}
SELECT COUNT(*) as cnt FROM {{ ref('fct_scada') }}
WHERE SETTLEMENTDATE > (SELECT MAX(cutoff) FROM {{ this }})
  AND INTERVENTION = 0
LIMIT 1
{%- endset -%}

{%- set result = run_query(has_new_daily_query) -%}
{%- set has_new_daily = result and result.rows[0][0] > 0 -%}

{% if has_new_daily %}

-- New daily data found: full rebuild from daily
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

{% else %}

-- No new daily data: append intraday after cutoff
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
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
)

SELECT
  date,
  CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT) AS time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  MAX(SETTLEMENTDATE) OVER () AS cutoff
FROM incremental_data

{% endif %}

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
