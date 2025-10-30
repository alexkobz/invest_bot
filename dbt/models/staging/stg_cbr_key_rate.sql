SELECT
    *
FROM {{ source('cbr', 'KeyRate') }}