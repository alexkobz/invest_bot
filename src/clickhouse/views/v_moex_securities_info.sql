CREATE VIEW v_moex_securities_info AS
SELECT
	secid,
	boardid,
    issuesize,
    min(settledate::date) AS settledate
FROM moex_securities_info
GROUP BY
    secid,
	boardid,
    issuesize
SETTINGS join_use_nulls = 1
