CREATE VIEW v_moex_securities AS
SELECT
    s.inn AS inn,
    s_i.secid AS secid,
    s_i.boardid AS boardid,
    toYear(s_i.settledate) AS "year",
    s.shortname AS shortname,
    s.name AS name,
    s.isin AS isin,
    s.is_traded AS is_security_traded,
    argMin(s_i.issuesize, s_i.settledate) AS issuesize
FROM moex_securities_info AS s_i
JOIN moex_securities AS s ON s.secid = s_i.secid AND s.boardid = s_i.boardid
WHERE s_i.issuesize IS NOT NULL OR s_i.issuesize != 0
GROUP BY s.inn, s_i.secid, s_i.boardid, toYear(s_i.settledate),s.shortname,
    s.name,
    s.isin,
    s.is_traded
