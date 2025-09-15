SELECT
    *
FROM {{ source('moex', 'api_moex_boards') }}