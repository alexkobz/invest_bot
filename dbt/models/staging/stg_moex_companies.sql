SELECT
    *
FROM {{ source('moex', 'Companies') }}