-- TODO replace view add issuesize
CREATE MATERIALIZED VIEW mv_moex_prices
TO rep_moex_prices AS
SELECT
	p.boardid AS boardid,
	p.tradedate,
	p.secid AS secid,
	p.numtrades,
	p.value,
	p.open,
	p.low,
	p.high,
	p.legalcloseprice,
	p.waprice,
	p.close,
	p.volume,
	p.marketprice2,
	p.marketprice3,
	p.admittedquote,
	p.mp2valtrd,
	p.marketprice3tradesvalue,
	p.admittedvalue,
	p.waval,
	p.tradingsession,
	p.trendclspr,
	s.id AS id_security,
    s.shortname,
    s.regnumber,
    s.name,
    s.isin,
    s.is_traded AS is_security_traded,
    s.id_emitent,
    s.inn,
    s.type,
    s.grp,
    b.id AS id_board,
    b.board_group_id,
    b.title,
    b.is_traded AS is_board_traded
FROM moex_prices AS p
LEFT JOIN pg_moex_securities AS s ON s.secid = p.secid AND s.boardid  = p.boardid
LEFT JOIN pg_moex_boards AS b ON b.boardid  = s.boardid
