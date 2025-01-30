TRUNCATE TABLE emitents;

INSERT INTO emitents
SELECT *
FROM pg_emitents
