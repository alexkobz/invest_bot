{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid', 'tradedate'],
    merge_update_columns=['numtrades', 'value', 'open', 'low', 'high', 'legalcloseprice', 'waprice', 'close',
    'volume', 'marketprice2', 'marketprice3', 'admittedquote', 'mp2valtrd', 'marketprice3tradesvalue',
    'admittedvalue', 'waval', 'tradingsession', 'trendclspr']
  )
}}
SELECT
	boardid,
	cast(tradedate as date) as tradedate,
	secid,
	numtrades,
	value,
	open,
	low,
	high,
	legalcloseprice,
	waprice,
	close,
	volume,
	marketprice2,
	marketprice3,
	admittedquote,
	mp2valtrd,
	marketprice3tradesvalue,
	admittedvalue,
	waval,
	tradingsession,
	trendclspr
FROM {{ ref('stg_moex_prices') }}
WHERE volume > 0