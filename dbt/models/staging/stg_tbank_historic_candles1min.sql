SELECT
    *
FROM {{ source('tbank', 'Candles1MinYesterday') }}
