CREATE MATERIALIZED VIEW mv_moex_prices
TO rep_moex_prices AS
WITH generated_dates AS (
    SELECT
        secid,
        arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(min_date), toUInt32(max_date) + 1, 1))) AS tradedate
    FROM (
        SELECT secid, MIN(tradedate) AS min_date, MAX(tradedate) AS max_date
        FROM moex_prices
        WHERE boardid IN ('EQBS', 'EQBR', 'TQBS', 'TQBR')
        GROUP BY secid
    ) AS p
)
, tmp_prices AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER(PARTITION BY secid, tradedate ORDER BY volume DESC) AS rn
        FROM moex_prices
        WHERE boardid IN ('EQBS', 'EQBR', 'TQBS', 'TQBR')
    )
    WHERE rn = 1
)
, prices AS (
    SELECT
        d.secid AS secid,
        argMax(p.boardid, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS boardid,
        d.tradedate AS tradedate,
        year(d.tradedate) AS "year",
        p.tradedate IS NOT NULL AS is_workday,
        argMax(sec.issuesize, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS issuesize,
        argMax(sec.shortname, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS shortname,
        argMax(sec.name, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS name,
        argMax(sec.isin, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS isin,
        argMax(sec.is_security_traded, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS is_security_traded,
        argMax(sec.inn, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS inn,
        argMax(p.numtrades, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS numtrades,
        argMax(p.value, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS value,
        argMax(p.open, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS open,
        argMax(p.low, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS low,
        argMax(p.high, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS high,
        argMax(p.close, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS close,
        argMax(p.volume, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS volume,
        argMax(b.title, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS board_title,
        argMax(b.is_traded, d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS is_board_traded,
        argMax(f."2400", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2400",
        argMax(f."2110", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2110",
        argMax(f."1300", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "1300",
        argMax(f."2200", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2200",
        argMax(f."2330", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2330",
        argMax(f."2340", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2340",
        argMax(f."2350", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2350",
        argMax(f."1510", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "1510",
        argMax(f."1250", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "1250",
        argMax(f."2330", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "2330",
        argMax(f."3227", d.tradedate) OVER (PARTITION BY d.secid ORDER BY d.tradedate) AS "3227"
    FROM generated_dates AS d
    LEFT JOIN tmp_prices AS p
        ON d.secid = p.secid
        AND d.tradedate = p.tradedate
    ASOF LEFT JOIN v_moex_securities AS sec
        ON sec.secid = d.secid
        AND sec.boardid = p.boardid
        AND sec.settledate <= d.tradedate
    LEFT JOIN moex_boards AS b
        ON b.boardid = p.boardid
    LEFT JOIN v_fundamentals AS f
        ON f.inn = sec.inn
        AND f."year" = year(d.tradedate)
    SETTINGS join_use_nulls = 1
)
SELECT
    secid,
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
    boardid,
    board_title,
    is_board_traded,
    issuesize,
    "close"*issuesize AS capitalization,
    ("close"*issuesize)/nullIf("2400", 0) AS p_e,
    ("close"*issuesize)/nullIf("2110", 0) AS p_s,
    ("close"*issuesize)/nullIf("1300", 0) AS p_b,
    ("close"*issuesize)/nullIf("2200" + "2340" - "2350", 0) AS p_ebit,
    ("close"*issuesize)/nullIf("2200" + "2330" + "2340" - "2350", 0) AS p_ebitda,
    ("close"*issuesize + "1510" - "1250")/nullIf("2200" + "2330" + "2340" - "2350", 0) AS ev_ebitda,
    ("3227")/nullif(issuesize, 0)/"close" AS dy
FROM prices
