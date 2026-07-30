-- Grain tripwire for the fct_scada key. See the header of assert_fct_price_grain.sql
-- for what this catches, why it is scoped to a rolling 30-day window, and why it is not
-- tagged heavy.

SELECT file, DUID, SETTLEMENTDATE, INTERVENTION, COUNT(*) AS n
FROM {{ ref('fct_scada') }}
WHERE "DATE" >= current_date - INTERVAL 30 DAY
GROUP BY ALL
HAVING COUNT(*) > 1
