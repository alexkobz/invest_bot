INSERT INTO fundamentals
SELECT f.*
FROM pg_fundamentals f
WHERE NOT EXISTS (
    SELECT 1
    FROM fundamentals
    JOIN f.inn = fundamentals.inn AND f.year = fundamentals.year
)
