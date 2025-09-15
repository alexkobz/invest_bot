CREATE VIEW v_emitents AS
SELECT
    inn,
    shortname_rus,
    sector,
    country
FROM (
    SELECT
        id_emitent,
        inn,
        shortname_rus,
        sector,
        country,
        row_number() OVER(PARTITION BY inn ORDER BY id_emitent DESC) AS rn
    FROM emitents
    )
WHERE rn = 1
