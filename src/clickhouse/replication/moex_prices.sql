INSERT INTO moex_prices
SELECT *
FROM pg_moex_prices
WHERE tradedate::DATE NOT IN (SELECT tradedate FROM moex_prices)
