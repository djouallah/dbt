-- Uniqueness of the fct_summary merge key: (date, time, DUID). The only assertion on this table.
--
-- It reads fct_summary and NOTHING else — no join to dim_duid, no recomputation from fct_scada,
-- no expectation about intervals per day, unit counts, or which dates should exist. That is the
-- point: the test knows nothing about the source, so it cannot go red because AEMO published a
-- short day, a backlog drained halfway, or a unit stopped reporting. The only thing it can ever
-- report is the same key written twice, which is always a defect no matter what landed.
--
-- What that costs, so nobody is surprised later: everything the deleted assertions covered is now
-- unwatched — drift of the stored table from f(inputs), craters in a half-filled date, NULL prices
-- from a broken region join, and duplicates in fct_price/fct_scada. A duplicate in the facts still
-- surfaces here IF it lands on a distinct grain key; one that doesn't, doesn't.
--
-- Full table, no date window and not tagged `heavy` — a window would be an assumption about where
-- duplicates live (recent writes), which is exactly the kind of source knowledge this test is
-- meant to be free of, and a tag would exclude it from every CI leg, leaving fct_summary with no
-- assertion at all. Cost is one GROUP BY over the table, run by the engine that just wrote it.
--
-- DuckDB dialect (duckrun + iceberg). tests/dwh and tests/spark hold the same assertion in their
-- own dialects; `data_tests` in dbt_project.yml enables exactly one folder per target, the same
-- way models/ is gated. Keep the three in step -- this is the only assertion fct_summary has, on
-- any engine, and dwh is the one whose writes can genuinely race.

SELECT date, time, DUID, COUNT(*) AS n
FROM {{ ref('fct_summary') }}
GROUP BY ALL
HAVING COUNT(*) > 1
