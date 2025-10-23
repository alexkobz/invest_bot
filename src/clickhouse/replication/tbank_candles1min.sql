INSERT INTO tbank_candles1min
SELECT *
FROM pg_tbank_candles1min
WHERE "date" > (SELECT max("date") FROM tbank_candles1min)
