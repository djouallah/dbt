-- Grain check: the merge key of fct_summary must be unique — this is the
-- tripwire for any residual concurrent-writer race.
{{ config(tags=['heavy']) }}

SELECT date, time, DUID, COUNT(*) AS n
FROM {{ ref('fct_summary') }}
GROUP BY ALL
HAVING COUNT(*) > 1
