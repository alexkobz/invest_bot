TRUNCATE TABLE moex_boards;

INSERT INTO moex_boards
SELECT
    id,
    board_group_id,
    boardid,
    title,
    is_traded
FROM pg_moex_boards
WHERE NOW() > dbt_valid_from
