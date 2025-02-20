{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid'],
    merge_update_columns=['id', 'shortname', 'regnumber', 'name', 'isin', 'is_traded', 'id_emitent', 'inn',
    'type', 'grp', 'issuesize']
  )
}}
WITH securities AS (
    SELECT *
    FROM (
        SELECT
            s.id,
            s.secid,
            s.shortname,
            s.regnumber,
            s.name,
            s.isin,
            s.is_traded,
            s.emitent_id,
            s.emitent_inn,
            s.type,
            s.group AS grp,
            s.primary_boardid AS boardid,
            row_number() OVER(PARTITION BY s.secid, s.primary_boardid ORDER BY s.id DESC) AS rn
        FROM {{ ref('stg_moex_securities_trading') }} AS s
    )
    WHERE rn = 1
)
, prices AS (
    SELECT * FROM (
        SELECT
            secid,
            boardid,
            row_number() OVER(PARTITION BY secid, boardid ORDER BY tradedate DESC, volume DESC) AS rn
        FROM {{ ref('stg_moex_prices') }}
        )
    WHERE rn = 1
)
, emitents AS (
    SELECT * FROM (
        SELECT
            id_emitent,
            inn,
            row_number() OVER(PARTITION BY inn ORDER BY id_emitent DESC) AS rn
        FROM {{ ref('dim_emitents') }}
        )
    WHERE rn = 1
)
, securities_info AS (
    SELECT * FROM (
        SELECT
            secid,
            boardid,
            issuesize,
            row_number() OVER(PARTITION BY secid, boardid ORDER BY settledate DESC) AS rn
        FROM {{ ref('dim_moex_securities_info') }}
        )
    WHERE rn = 1
)
SELECT DISTINCT
    cast(securities.id AS BIGINT) AS id,
    coalesce(securities.secid, prices.secid, '') AS secid,
    securities.shortname,
    securities.regnumber,
    securities.name,
    securities.isin,
    coalesce(CAST(CAST(securities.is_traded AS INTEGER) AS BOOLEAN), true) AS is_traded,
    coalesce(d.id_emitent, CAST(NULLIF(securities.emitent_id, '') AS BIGINT), 0) AS id_emitent,
    coalesce(d.inn, securities.emitent_id, '') AS inn,
    securities.type,
    securities.grp AS grp,
    coalesce(securities.boardid, prices.boardid, '') AS boardid,
    securities_info.issuesize AS issuesize
FROM securities
FULL JOIN prices ON securities.secid = prices.secid AND securities.boardid = prices.boardid
LEFT JOIN emitents AS d ON d.inn = securities.emitent_inn
LEFT JOIN securities_info ON securities.secid = securities_info.secid AND securities.boardid = securities_info.boardid