{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['value']
  )
}}
SELECT DISTINCT ON ((date_trunc('month', "date") - interval '1 month')::date)
    (date_trunc('month', "date") - interval '1 month')::date AS "date",
    "Inflation"::double precision AS value
FROM {{ ref('stg_cbr_main_info') }}
ORDER BY "date"