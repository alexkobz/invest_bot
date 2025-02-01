TRUNCATE TABLE moex_securities_trading;

INSERT INTO moex_securities_trading
SELECT *
FROM pg_moex_securities_trading
