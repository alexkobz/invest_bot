SELECT
    *
FROM {{ source('tbank', 'etfs') }}