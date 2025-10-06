SELECT
    *
FROM {{ source('tbank', 'bonds') }}