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
    )
), v_moex_prices AS (
    SELECT
      d.tradedate,
      d.boardid,
      d.secid,
      argMax(p.numtrades, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS numtrades,
      argMax(p.value, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS value,
      argMax(p.open, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS open,
      argMax(p.low, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS low,
      argMax(p.high, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS high,
      argMax(p.legalcloseprice, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS legalcloseprice,
      argMax(p.waprice, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS waprice,
      argMax(p.close, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS close,
      argMax(p.volume, d.tradedate) OVER (PARTITION BY d.secid, d.boardid ORDER BY d.tradedate) AS volume
    FROM generated_dates AS d
    LEFT JOIN moex_prices AS p ON d.secid = p.secid AND d.boardid = p.boardid AND d.tradedate = p.tradedate
    SETTINGS join_use_nulls = 1
)
, prices AS (
    SELECT
        p.boardid AS boardid,
        p.tradedate,
        p.secid AS secid,
        p.numtrades,
        p."value",
        p."open",
        p.low,
        p.high,
        p.waprice,
        p."close",
        p.volume,
        sec.shortname,
        sec.name,
        sec.isin,
        sec.is_security_traded,
        sec.inn AS inn,
        sec.issuesize AS issuesize,
        b.id AS id_board,
        b.title,
        b.is_traded AS is_board_traded,
        year(p.tradedate) AS "year",
        f."2400",
        f."2110",
        f."1300",
        f."2200",
        f."1510",
        f."1250",
        f."2330",
        f."3227"
    FROM v_moex_prices AS p
    LEFT JOIN v_moex_securities AS sec
        ON sec.secid = p.secid
        AND sec.boardid = p.boardid
        AND sec."year" = year(p.tradedate)
    LEFT JOIN moex_boards AS b
        ON b.boardid = p.boardid
    LEFT JOIN v_fundamentals AS f
        ON f.inn = sec.inn
        AND f."year" = year(p.tradedate)
    WHERE boardid IN ('EQBS', 'EQBR', 'TQBS', 'TQBR')
)
SELECT
    boardid,
    tradedate,
    secid,
    numtrades,
    "value",
    "open",
    low,
    high,
    waprice,
    "close",
    volume,
    shortname,
    name,
    isin,
    is_security_traded,
    inn,
    id_board,
    title,
    is_board_traded,
    "year",
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
