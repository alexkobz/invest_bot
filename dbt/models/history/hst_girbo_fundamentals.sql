{{
  config(
    materialized='incremental',
    incremental_strategy='append')
}}
SELECT *
FROM {{ ref('stg_girbo_fundamentals') }}
