{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['rate']
  )
}}
SELECT
	date::date,
	rate::double precision
FROM {{ ref('stg_cbr_key_rate') }}
