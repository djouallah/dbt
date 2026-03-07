-- Distinct days in fct_summary should equal distinct days from fct_scada + fct_scada_today combined
SELECT
  scada_days,
  summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT date) FROM (
      SELECT DISTINCT DATE as date FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0
      UNION
      SELECT DISTINCT DATE as date FROM {{ ref('fct_scada_today') }}
    )) as scada_days,
    (SELECT COUNT(DISTINCT date) FROM {{ ref('fct_summary') }}) as summary_days
)
WHERE scada_days != summary_days
