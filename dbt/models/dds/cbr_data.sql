{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date'],
    merge_update_columns=['rate', 'inflation']
  )
}}
SELECT
	COALESCE(r.date, i.date) AS date,
	r.rate,
	i.value AS inflation
FROM {{ ref('cbr_key_rate') }} r
FULL JOIN {{ ref('cbr_inflation') }} i ON TO_CHAR(r.date, 'YYYYMM') = TO_CHAR(i.date, 'YYYYMM')
