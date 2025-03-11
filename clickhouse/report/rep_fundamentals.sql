CREATE MATERIALIZED VIEW mv_fundamentals
TO rep_fundamentals AS
SELECT
	f.*,
	e.sector,
	e.country
FROM v_fundamentals AS f
LEFT JOIN v_emitents AS e ON e.inn = f.inn
SETTINGS join_use_nulls = 1
