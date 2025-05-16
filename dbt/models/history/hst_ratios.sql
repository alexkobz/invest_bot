{{
  config(
    materialized='incremental',
    incremental_strategy='append')
}}
SELECT *
FROM {{ ref('stg_ratios') }}
