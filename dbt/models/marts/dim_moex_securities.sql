{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid'],
    merge_update_columns=['id', 'shortname', 'regnumber', 'name', 'isin', 'is_traded', 'id_emitent', 'inn',
    'type', 'grp']
  )
}}
SELECT DISTINCT
    d.id,
    d.secid,
    d.shortname,
    d.regnumber,
    d.name,
    d.isin,
    d.is_traded,
    d.id_emitent,
    d.inn,
    d.type,
    d.grp,
    d.boardid
FROM {{ ref('dim_moex_securities_trading') }} AS d