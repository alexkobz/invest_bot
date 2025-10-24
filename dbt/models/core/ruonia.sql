{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['secid', 'boardid', 'tradedate'],
    merge_update_columns=['numtrades', 'value', 'open', 'low', 'high', 'legalcloseprice', 'waprice', 'close',
    'volume', 'marketprice2', 'marketprice3', 'admittedquote', 'mp2valtrd', 'marketprice3tradesvalue',
    'admittedvalue', 'waval', 'tradingsession', 'trendclspr', 'trade_session_date']
  )
}}
SELECT
	UPPER(secid) secid,
	UPPER(boardid) boardid,
	tradedate::date AS tradedate,
	NULLIF(numtrades, '')::bigint numtrades,
	NULLIF(value, '')::float "value",
	NULLIF(open, '')::float "open",
	NULLIF(close, '')::float "close",
	NULLIF(low, '')::float low,
	NULLIF(high, '')::float high,
	NULLIF(legalcloseprice, '')::float AS legalcloseprice,
	NULLIF(waprice, '')::float waprice,
	NULLIF(volume, '')::bigint volume,
	NULLIF(marketprice2, '')::float marketprice2,
    NULLIF(marketprice3, '')::float marketprice3,
    NULLIF(admittedquote, '')::float admittedquote,
	NULLIF(mp2valtrd, '')::float mp2valtrd,
	NULLIF(marketprice3tradesvalue, '')::float marketprice3tradesvalue,
	NULLIF(admittedvalue, '')::float admittedvalue,
	NULLIF(waval, '')::float waval,
	NULLIF(tradingsession, '')::bigint tradingsession,
	currencyid currencyid,
	NULLIF(trendclspr, '')::float trendclspr,
	NULLIF(trade_session_date, '')::date AS trade_session_date
FROM {{ ref('stg_cbr_ruonia') }}
