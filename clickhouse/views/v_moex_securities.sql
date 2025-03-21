CREATE VIEW v_moex_securities AS
SELECT
    s_i.secid AS secid,
    s_i.boardid AS boardid,
    toYear(s_i.settledate) AS "year",
    s.inn AS inn,
    s.shortname AS shortname,
    s.name AS name,
    s.isin AS isin,
    s.is_traded AS is_security_traded,
    s.type AS sectype,
    s.grp AS secgroup,
    argMin (s_i.imputed_issuesize, s_i.settledate) AS issuesize
FROM moex_securities_info AS s_i
JOIN moex_securities AS s ON s.secid = s_i.secid AND s.boardid = s_i.boardid
WHERE s_i.boardid IN ('EQBS', 'EQBR', 'TQBS', 'TQBR') AND coalesce(s_i.imputed_issuesize, 0) != 0
GROUP BY
  s_i.secid,
  s_i.boardid,
  toYear(s_i.settledate),
  s.inn,
  s.shortname,
  s.name,
  s.isin,
  s.is_traded,
  s.type,
  s.grp
