{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid'],
    merge_update_columns=['id', 'shortname', 'regnumber', 'name', 'isin', 'is_traded', 'id_emitent', 'type', 'grp']
  )
}}
WITH prices AS (
    SELECT
        secid,
        boardid,
        row_number() OVER(PARTITION BY secid, boardid ORDER BY tradedate DESC, volume DESC) AS rn
    FROM {{ ref('stg_moex_prices') }}
)
SELECT DISTINCT
    d.id,
    prices.secid,
    d.shortname,
    d.regnumber,
    d.name,
    d.isin,
    coalesce(cast(cast(d.is_traded AS INTEGER) AS BOOLEAN), true) AS is_traded,
    d.id_emitent,
    d.type,
    d.grp,
    prices.boardid
FROM prices 
LEFT JOIN {{ ref('dim_moex_securities_trading') }} AS d
    ON prices.secid = d.secid AND prices.boardid = d.boardid
WHERE prices.rn = 1