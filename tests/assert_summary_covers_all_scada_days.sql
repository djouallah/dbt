-- Summary should have at least as many distinct days as scada
SELECT
  scada_days,
  summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
    (SELECT COUNT(DISTINCT date) FROM {{ ref('fct_summary') }}) as summary_days
)
WHERE scada_days > summary_days
