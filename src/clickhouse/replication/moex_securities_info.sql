TRUNCATE TABLE moex_securities_info;

INSERT INTO moex_securities_info
SELECT *
FROM pg_moex_securities_info
