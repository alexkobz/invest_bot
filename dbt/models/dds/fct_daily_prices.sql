{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ticker', 'boardid', 'date'],
    merge_update_columns=['open', 'close', 'low', 'high', 'volume']
  )
}}
WITH moex AS (
    SELECT DISTINCT ON (secid, boardid, tradedate)
        secid,
        boardid,
        tradedate,
        "value",
        "open",
        "close",
        low,
        high,
        volume
    FROM {{ ref('moex_prices') }}

    {% if is_incremental() %}

    WHERE tradedate > (SELECT MAX(date) FROM {{ this }} )

    {% endif %}

    ORDER BY secid, boardid, tradedate, volume DESC
)
, tbank AS (
    SELECT DISTINCT ON (ticker, "date")
        UPPER(s.ticker) AS ticker,
        c."date",
        c.volume,
        c."open",
        c."close",
        c."low",
        c."high"
    FROM {{ ref('tbank_historic_candles1min') }} c
    LEFT JOIN {{ ref('tbank_shares') }} s ON c.figi = s.figi
    WHERE COALESCE(s.ticker, '') != ''

    {% if is_incremental() %}

    AND c."date" > (SELECT MAX(date) FROM {{ this }} )

    {% endif %}

    ORDER BY ticker, "date", "timestamp" DESC
)
SELECT
	COALESCE(m.secid, t.ticker, '') AS ticker,
	COALESCE(m.boardid, 'TQBR') boardid,
	COALESCE(m.tradedate, t."date") AS date,
	COALESCE(m."open", t."open") AS "open",
	COALESCE(m."close", t."close") AS "close",
	COALESCE(m.low, t.low) AS low,
	COALESCE(m.high, t.high) AS high,
	COALESCE(m.volume, t.volume) AS volume
FROM moex m
FULL JOIN tbank t on m.secid = t.ticker AND m.tradedate = t."date"
