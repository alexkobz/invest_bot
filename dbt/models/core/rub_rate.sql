{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'code'],
    merge_update_columns=['name', 'nom', 'curs', '"chCode"', '"unitRate"']
  )
}}
SELECT DISTINCT ON (date::date, code::bigint)
	"date"::date AS "date",
	"code"::bigint AS "code",
	lower(name) "name",
	nom::double precision,
	curs::double precision,
	upper("chCode") "chCode",
	"unitRate"::double precision "unitRate"
FROM {{ ref('stg_cbr_rub_rate') }}
ORDER BY date::date, code::bigint