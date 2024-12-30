{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid'],
    merge_update_columns=['id', 'shortname', 'regnumber', 'name', 'isin', 'is_traded', 'id_emitent', 'type', 'grp']
  )
}}
WITH prices AS (
    SELECT DISTINCT
        s.id,
        s.secid,
        s.shortname,
        s.regnumber,
        s.name,
        s.isin,
        s.is_traded,
        s.emitent_inn,
        s.type,
        s.group AS grp,
        s.primary_boardid AS boardid,
        row_number() OVER(PARTITION BY s.secid, s.primary_boardid ORDER BY s.id DESC) AS rn
    FROM {{ ref('stg_moex_securities_trading') }} AS s
)
SELECT DISTINCT
    cast(prices.id AS INTEGER) AS id,
    prices.secid,
    prices.shortname,
    prices.regnumber,
    prices.name,
    prices.isin,
    prices.is_traded,
    d.id_emitent,
    prices.type,
    prices.grp AS grp,
    prices.boardid AS boardid
FROM prices
LEFT JOIN (
    SELECT
    id_emitent
    , inn
    , row_number() OVER(PARTITION BY inn ORDER BY update_date DESC) AS rn
    FROM {{ ref('dim_rudata_emitents') }}
) AS d ON d.inn = prices.emitent_inn
WHERE prices.rn = 1 AND d.rn = 1