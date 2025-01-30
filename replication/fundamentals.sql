INSERT INTO fundamentals
WITH e AS (
    SELECT * FROM (
        SELECT
            id_emitent,
            inn,
            sector,
            country,
            row_number() OVER(PARTITION BY inn ORDER BY id_emitent DESC) AS rn
        FROM pg_emitents
        )
    WHERE rn = 1
)
SELECT 
	f.*,
	e.id_emitent,
	e.sector,
	e.country
FROM pg_fundamentals f
LEFT JOIN e ON e.inn = f.inn
WHERE NOT EXISTS (
    SELECT 1
    FROM fundamentals
    JOIN f.inn = fundamentals.inn AND f.year = fundamentals.year
)
