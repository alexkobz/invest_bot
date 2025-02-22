CREATE VIEW v_moex_securities AS
SELECT DISTINCT
    s.inn,
    s_i.secid,
    s_i.boardid,
    argMin(s_i.issuesize, s_i.settledate) OVER (PARTITION BY s_i.secid, s_i.boardid ORDER BY s_i.settledate) AS issuesize,
    toYear(s_i.settledate) AS year,
    toYYYYMM(s_i.settledate) AS year_month,
    yearweek(s_i.settledate) AS year_week,
    s_i.settledate AS settledate
FROM moex_securities_info AS s_i
JOIN moex_securities AS s ON s.secid = s_i.secid AND s.boardid = s_i.boardid
WHERE s_i.issuesize IS NOT NULL OR s_i.issuesize != 0
