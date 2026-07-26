{{ config(
    materialized='incremental',
    incremental_strategy='append',
    unique_key='date'
) }}

-- One-off, fixed calendar dimension. Built in full on the first run; once the table
-- exists, every later run selects nothing (WHERE 1=0) so it's a no-op — dbt's idiom
-- for "create if not exists, otherwise skip".
--
-- DuckDB's generate_series(...INTERVAL 1 DAY) has no T-SQL equivalent that survives
-- dbt-fabric's subquery wrapping (OPTION(MAXRECURSION) can't live in a derived table),
-- so build a 0..9999 tally by cross-joining digit tables and offset from the start date.
WITH digits(n) AS (
  SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
  UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
numbers AS (
  SELECT (d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n) AS num
  FROM digits d1 CROSS JOIN digits d2 CROSS JOIN digits d3 CROSS JOIN digits d4
),
calendar AS (
  SELECT DATEADD(DAY, num, CAST('2018-04-01' AS DATE)) AS d
  FROM numbers
  WHERE num <= DATEDIFF(DAY, CAST('2018-04-01' AS DATE), CAST('2026-12-31' AS DATE))
)
SELECT
  CAST(d AS DATE) AS [date],
  CAST(YEAR(d) AS INT) AS [year],
  CAST(MONTH(d) AS INT) AS [month]
FROM calendar
{% if is_incremental() %}
WHERE 1 = 0
{% endif %}
