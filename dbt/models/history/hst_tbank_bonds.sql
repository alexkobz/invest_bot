{{
  config(
    materialized='incremental',
    incremental_strategy='append')
}}
SELECT
    *,
    NOW() AS load_ts
FROM {{ ref('stg_tbank_bonds') }}