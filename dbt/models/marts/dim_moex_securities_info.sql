{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid', 'settledate'],
    merge_update_columns=['issuesize', 'isin']
  )
}}
SELECT
	secid,
	boardid,
    issuesize::float::bigint as issuesize,
    isin,
    settledate::date AS settledate
FROM {{ ref('stg_moex_securities_info') }}