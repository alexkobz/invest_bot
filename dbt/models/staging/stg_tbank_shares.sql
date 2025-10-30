SELECT
    *
FROM {{ source('tbank', 'shares') }}