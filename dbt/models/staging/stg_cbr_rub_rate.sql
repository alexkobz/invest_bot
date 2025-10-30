SELECT
    *
FROM {{ source('cbr', 'GetCursOnDate') }}