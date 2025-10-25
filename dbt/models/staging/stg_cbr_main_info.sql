SELECT
    *
FROM {{ source('cbr', 'MainInfoXML') }}