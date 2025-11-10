TRUNCATE TABLE shares;

INSERT INTO shares
SELECT
   *
FROM pg_moex_shares
