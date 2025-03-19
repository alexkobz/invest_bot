{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid', 'settledate'],
    merge_update_columns=['shortname', 'prevprice', 'lotsize', 'facevalue',
       'status', 'boardname', 'decimals', 'secname', 'remarks', 'marketcode',
       'instrid', 'sectorid', 'minstep', 'prevwaprice', 'faceunit', 'prevdate',
       'issuesize', 'isin', 'latname', 'regnumber', 'prevlegalcloseprice',
       'currencyid', 'sectype', 'listlevel']
  )
}}
SELECT
	secid,
	boardid,
	shortname,
	prevprice::float,
	lotsize::float::bigint,
	facevalue::float,
    status,
    boardname,
    decimals::float::bigint,
    secname,
    remarks,
    marketcode,
    instrid,
    sectorid,
    minstep::float,
    prevwaprice::float,
    faceunit,
    prevdate,
    issuesize::float::bigint as issuesize,
    issuesize::float::bigint as imputed_issuesize,
    isin,
    latname,
    regnumber,
    prevlegalcloseprice::float,
    currencyid,
    sectype,
    listlevel::float::bigint,
    settledate::date AS settledate
FROM {{ ref('stg_moex_securities_info') }}