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
	sec.boardid,
	CAST(prices.tradedate AS DATE) AS tradedate,
	sec.secid,
	prices.numtrades,
	prices.value,
	prices.open,
	prices.low,
	prices.high,
	CAST(prices.legalcloseprice AS DOUBLE PRECISION) AS legalcloseprice,
	prices.waprice,
	prices.close,
	prices.volume,
	prices.marketprice2,
	prices.marketprice3,
	prices.admittedquote,
	prices.mp2valtrd,
	prices.marketprice3tradesvalue,
	prices.admittedvalue,
	prices.waval,
	prices.tradingsession,
	prices.trendclspr,
	prices.trade_session_date::date AS trade_session_date
FROM {{ ref('stg_moex_prices') }} AS prices
JOIN {{ ref('dim_moex_securities') }} AS sec ON prices.secid = sec.secid AND prices.boardid = sec.boardid
WHERE prices.volume > 0
