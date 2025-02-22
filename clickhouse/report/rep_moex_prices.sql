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
        s.shortname,
        s.name,
        s.isin,
        s.is_traded AS is_security_traded,
        s.inn AS inn,
        s.issuesize,
        b.id AS id_board,
        b.title,
        b.is_traded AS is_board_traded,
        year(p.tradedate) AS "year",
        yearweek(p.tradedate) AS year_week,
        f."2400",
        f."2110",
        f."1300",
        f."2200",
        f."1510",
        f."1250",
        f."2330"
    FROM moex_prices AS p
    LEFT JOIN pg_moex_securities AS s ON s.secid = p.secid AND s.boardid = p.boardid
    LEFT JOIN pg_moex_boards AS b ON b.boardid = s.boardid
    LEFT JOIN v_fundamentals AS f ON f.inn = s.inn AND f."year" = year(p.tradedate)
    WHERE COALESCE(s.inn, '') != ''
), result AS (
    SELECT DISTINCT
        prices.*,
        argMin(sec.issuesize, sec.settledate) OVER (PARTITION BY sec.secid, sec.boardid ORDER BY sec.settledate) AS issuesize
    FROM prices
    LEFT JOIN v_moex_securities AS sec ON prices.inn = sec.inn AND prices.year_week = sec.year_week
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
    close*issuesize/"2400" AS p_e,
    close*issuesize/"2110" AS p_s,
    close*issuesize/"1300" AS p_b,
    close*issuesize/"2200" AS p_ebit,
    (close*issuesize + "1510" - "1250")/("2200" + "2330") AS ev_ebitda
FROM result
