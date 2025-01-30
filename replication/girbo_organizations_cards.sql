TRUNCATE TABLE girbo_organizations_cards;

INSERT INTO girbo_organizations_cards
SELECT *
FROM pg_girbo_organizations_cards
