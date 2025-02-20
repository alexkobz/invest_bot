-- TODO replace view add current year and last_value
CREATE MATERIALIZED VIEW mv_fundamentals
TO rep_fundamentals AS
SELECT
	f.*,
	e.id_emitent,
	e.sector,
	e.country
FROM fundamentals f
LEFT JOIN v_emitents AS e ON e.inn = f.inn
