{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ticker'],
    merge_update_columns=['shortname', 'latname', 'secname', 'isin', 'issuesize', 'lotsize',
    'facevalue', 'faceunit', 'currency', 'regnumber', 'figi', 'ipo_date', 'country_code',
    'sector', 'emitent_inn']
  )
}}
WITH moex AS (
    SELECT DISTINCT ON (secid)
        secid,
        shortname,
        latname,
        secname,
        UPPER(isin) isin,
        issuesize,
        lotsize,
        facevalue,
        UPPER(faceunit) faceunit,
        regnumber,
        UPPER(currencyid) currencyid
    from {{ ref('moex_shares') }}
    WHERE dbt_valid_to IS NULL AND boardid = 'TQBR'
    ORDER BY secid, dbt_valid_from DESC
)
, tbank AS (
    SELECT DISTINCT ON (ticker)
        figi,
        ticker,
        UPPER(isin) isin,
        lot,
        UPPER(currency) currency,
        name,
        ipo_date,
        issue_size,
        country_of_risk,
        sector
    FROM {{ ref('tbank_shares') }}
    WHERE dbt_valid_to IS NULL
    ORDER BY ticker, dbt_valid_from DESC
)
, rudata AS (
    SELECT DISTINCT ON (secid)
        secid,
        emitent_inn
    FROM {{ ref('rudata_stocks') }}
    WHERE dbt_valid_to IS NULL
    ORDER BY secid, dbt_valid_from DESC
)
SELECT
    COALESCE(m.secid, t.ticker) ticker,
    m.shortname,
    COALESCE(m.latname, t.name, '') AS latname,
    m.secname,
    COALESCE(m.isin, t.isin, '') AS isin,
    COALESCE(m.issuesize, t.issue_size, 0) AS issuesize,
    COALESCE(m.lotsize, t.lot, 0) AS lotsize,
    COALESCE(m.facevalue, 0.0) AS facevalue,
    m.faceunit AS faceunit,
    COALESCE(m.currencyid, t.currency, '') AS currency,
    m.regnumber,
    t.figi AS figi,
    t.ipo_date AS ipo_date,
    COALESCE(t.country_of_risk, '') AS country_code,
    COALESCE(t.sector, '') AS sector,
    r.emitent_inn
FROM moex m
FULL JOIN tbank t ON m.secid = t.ticker
LEFT JOIN rudata r ON COALESCE(m.secid, t.ticker) = r.secid
