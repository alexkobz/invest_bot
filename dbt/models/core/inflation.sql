{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['value']
  )
}}
SELECT DISTINCT
	to_date("date", 'DD.MM.YYYY') AS "date",
	"value"::double precision AS "value"
FROM {{ ref('stg_cbr_main_info') }}
WHERE lower(indicator) = 'inflation'