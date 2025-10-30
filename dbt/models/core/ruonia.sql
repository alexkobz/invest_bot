{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['rate', 'volume']
  )
}}
SELECT
	"date"::date AS "date",
	"rate"::double precision AS "rate",
	"volume"::double precision AS "volume"
FROM {{ ref('stg_cbr_ruonia') }}