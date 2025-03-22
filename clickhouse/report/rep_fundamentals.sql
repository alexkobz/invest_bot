CREATE MATERIALIZED VIEW mv_fundamentals
TO rep_fundamentals AS
--WITH AS (
--    SELECT
--        f.inn,
--        f."year",
--        argMax(sec.secid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secid,
--        argMax(sec.boardid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as boardid,
--        argMax(sec.sectype, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as sectype,
--        argMax(sec.secgroup, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secgroup,
--        argMax(sec.issuesize, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as issuesize
--    FROM v_fundamentals AS f
--    ASOF LEFT JOIN v_moex_securities AS sec
--        ON sec.inn = f.inn
--        AND year(sec.settledate) >= f."year"
--    SETTINGS join_use_nulls = 1
--)
SELECT
    f.*,
    (f."1200" - f."1500")/nullif(f."1200", 0) AS wca,
    (f."1200")/nullif(f."1500", 0) AS cr,
    (f."1200" - f."1210")/nullif(f."1500", 0) AS qr,
    (f."1200" - f."1210" - f."1260")/nullif(f."1500", 0) AS qar,
    (f."1250")/nullif(f."1500", 0) AS cashr,
    (f."2410")/nullif(f."2110", 0)*100 AS gpm,
    (f."2400")/nullif(f."2110", 0)*100 AS npm,
    (f."2400")/nullif(f."1600", 0)*100 AS roa,
    (f."2400")/nullif(f."1300", 0)*100 AS roe,
    (f."2300" + f."2330")/nullif((f."1600" - f."1500"), 0)*100 AS roce,
    (f."2400")/nullif((f."1300" + f."1400"), 0)*100 AS roi,
    (f."2300" + f."2330")/nullif(f."2110", 0)*100 AS ros,
    (f."1230")*365/nullif(f."2110", 0) AS tdd,
    (f."1230")*365/nullif(f."2120", 0) AS toc,
    (f."2120")/nullif(f."1210", 0) AS itr,
    (f."2110")/nullif(f."1600", 0) AS atr,
    (f."2110")/nullif(f."1230", 0) AS rtr,
    (f."1300")/nullif(f."1600", 0)*100 AS er,
    (f."1400")/nullif(f."1300", 0) AS de,
    (f."1400")/nullif(f."1600", 0) AS da,
    (f."2300" + f."2330")/nullif(f."2330", 0) AS icr,
    (f."2400")/nullif(argMax(sec.issuesize, f."year") OVER (PARTITION BY f.inn ORDER BY f."year"), 0) AS eps,
    argMax(sec.secid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secid,
    argMax(sec.boardid, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as boardid,
    argMax(sec.sectype, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as sectype,
    argMax(sec.secgroup, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as secgroup,
    argMax(sec.issuesize, f."year") OVER (PARTITION BY f.inn ORDER BY f."year") as issuesize,
    e.sector,
    e.country
FROM v_fundamentals AS f
LEFT JOIN v_emitents AS e ON e.inn = f.inn
ASOF LEFT JOIN v_moex_securities AS sec
    ON sec.inn = f.inn
    AND year(sec.settledate) >= f."year"
SETTINGS join_use_nulls = 1
