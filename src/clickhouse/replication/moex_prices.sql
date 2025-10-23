INSERT INTO moex_prices
SELECT *
FROM pg_moex_prices
WHERE tradedate::DATE > (SELECT max(tradedate) FROM moex_prices)
