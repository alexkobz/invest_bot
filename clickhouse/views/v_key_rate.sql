CREATE VIEW v_key_rate AS
SELECT
    s.generated_date AS "date",
    argMax(k.rate, s.generated_date) OVER (ORDER BY s.generated_date) as rate
FROM (
    SELECT
        toDate(arrayJoin(range(toUInt32(min("date")), toUInt32(today()) + 1))) AS generated_date
    FROM key_rate) s
LEFT JOIN key_rate AS k ON k."date" = s.generated_date
SETTINGS join_use_nulls = 1
