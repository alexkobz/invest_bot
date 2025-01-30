TRUNCATE TABLE moex_boards;

INSERT INTO moex_boards
SELECT *
FROM pg_moex_boards
