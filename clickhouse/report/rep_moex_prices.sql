CREATE MATERIALIZED VIEW mv_moex_prices
TO rep_moex_prices AS
WITH generated_dates AS (
    SELECT
        secid,
        boardid,
        arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(min_date), toUInt32(max_date) + 1, 1))) AS tradedate
    FROM (
        SELECT secid, boardid, MIN(tradedate) AS min_date, MAX(tradedate) AS max_date
        FROM moex_prices
        GROUP BY secid, boardid
    ) AS p
)
, prices AS (
    SELECT
        d.secid AS secid,
        d.boardid AS boardid,
        d.tradedate AS tradedate,
        year(d.tradedate) AS "year",
        p.tradedate IS NOT NULL AS is_workday,
        argMax(sec.issuesize, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS issuesize,
        argMax(sec.shortname, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS shortname,
        argMax(sec.name, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS name,
        argMax(sec.isin, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS isin,
        argMax(sec.is_security_traded, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS is_security_traded,
        argMax(sec.inn, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS inn,
        argMax(p.numtrades, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS numtrades,
        argMax(p.value, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS value,
        argMax(p.open, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS open,
        argMax(p.low, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS low,
        argMax(p.high, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS high,
        argMax(p.close, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS close,
        argMax(p.volume, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS volume,
        argMax(b.title, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS board_title,
        argMax(b.is_traded, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS is_board_traded,
        argMax(f."2400", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "2400",
        argMax(f."2110", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "2110",
        argMax(f."1300", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "1300",
        argMax(f."2200", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "2200",
        argMax(f."1510", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "1510",
        argMax(f."1250", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "1250",
        argMax(f."2330", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "2330",
        argMax(f."3227", d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS "3227"
    FROM generated_dates AS d
    LEFT JOIN moex_prices AS p
        ON d.secid = p.secid
        AND d.boardid = p.boardid
        AND d.tradedate = p.tradedate
    ASOF LEFT JOIN v_moex_securities AS sec
        ON sec.secid = d.secid
        AND sec.boardid = d.boardid
        AND sec.settledate <= d.tradedate
    LEFT JOIN moex_boards AS b
        ON b.boardid = p.boardid
    LEFT JOIN v_fundamentals AS f
        ON f.inn = sec.inn
        AND f."year" = year(d.tradedate)
    WHERE d.boardid IN ('EQBS', 'EQBR', 'TQBS', 'TQBR')
    SETTINGS join_use_nulls = 1
)
SELECT
    secid,
    boardid,
    tradedate,
    "year",
    is_workday,
    shortname,
    name,
    isin,
    is_security_traded,
    inn,
    numtrades,
    "value",
    "open",
    low,
    high,
    "close",
    volume,
    board_title,
    is_board_traded,
    issuesize,
    "close"*issuesize AS capitalization,
    "2200" AS ebit,
    "2200" + "2330" AS ebitda,
    ("close"*issuesize)/nullIf("2400", 0) AS p_e,
    ("close"*issuesize)/nullIf("2110", 0) AS p_s,
    ("close"*issuesize)/nullIf("1300", 0) AS p_b,
    ("close"*issuesize)/nullIf("2200", 0) AS p_ebit,
    ("close"*issuesize + "1510" - "1250")/nullIf(("2200" + "2330"), 0) AS ev_ebitda,
    ("3227")/nullif(issuesize, 0)/"close" AS dy
FROM prices
