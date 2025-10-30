{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'code'],
    merge_update_columns=['metal_name', 'price']
  )
}}
SELECT DISTINCT ON (date::date, code::bigint)
	date::date,
	code::bigint,
	lower(metal_name) metal_name,
	price::double precision
FROM {{ ref('stg_cbr_precious_metals') }}
ORDER BY date::date, code::bigint