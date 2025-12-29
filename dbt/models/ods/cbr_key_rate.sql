{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['rate']
  )
}}
SELECT DISTINCT ON (date::date)
	date::date,
	rate::double precision
FROM {{ ref('stg_cbr_key_rate') }}
ORDER BY date::date