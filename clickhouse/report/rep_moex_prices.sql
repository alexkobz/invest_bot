CREATE MATERIALIZED VIEW mv_moex_prices
TO rep_moex_prices AS
WITH prices AS (
    SELECT
        p.boardid AS boardid,
        p.tradedate,
        p.secid AS secid,
        p.numtrades,
        p.value,
        p.open,
        p.low,
        p.high,
        p.waprice,
        p.close,
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
        f."2330"
    FROM moex_prices AS p
    LEFT JOIN v_moex_securities AS sec
        ON sec.secid = p.secid
        AND sec.boardid = p.boardid
        AND sec."year" = year(p.tradedate)
    LEFT JOIN moex_boards AS b
        ON b.boardid = p.boardid
    LEFT JOIN v_fundamentals AS f
        ON f.inn = sec.inn
        AND f."year" = year(p.tradedate)
    WHERE COALESCE(sec.inn, '') != ''
)
SELECT
    boardid,
    tradedate,
    secid,
    numtrades,
    value,
    open,
    low,
    high,
    waprice,
    close,
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
    close*issuesize AS capitalization,
    "2200" AS ebit,
    "2200" + "2330" AS ebitda,
    (close*issuesize)/nullIf("2400", 0) AS p_e,
    (close*issuesize)/nullIf("2110", 0) AS p_s,
    (close*issuesize)/nullIf("1300", 0) AS p_b,
    (close*issuesize)/nullIf("2200", 0) AS p_ebit,
    (close*issuesize + "1510" - "1250")/nullIf(("2200" + "2330"), 0) AS ev_ebitda
FROM prices
SETTINGS join_use_nulls = 1
