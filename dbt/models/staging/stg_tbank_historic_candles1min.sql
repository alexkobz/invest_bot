SELECT
    *
FROM {{ source('tbank', 'historic_candles1') }}
