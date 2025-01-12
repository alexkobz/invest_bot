{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    merge_update_columns=['board_group_id', 'boardid', 'title', 'is_traded']
  )
}}

SELECT
    CAST(id AS BIGINT) AS id,
    CAST(board_group_id AS BIGINT) AS board_group_id,
    upper(boardid) AS boardid,
    title AS title,
    CAST(CAST(is_traded AS INTEGER) AS BOOLEAN) AS is_traded
FROM {{ ref('stg_moex_boards') }}
