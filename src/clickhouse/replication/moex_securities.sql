TRUNCATE TABLE moex_securities;

INSERT INTO moex_securities
SELECT *
FROM pg_moex_securities
