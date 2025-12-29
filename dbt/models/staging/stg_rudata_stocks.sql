SELECT
    *
FROM {{ source('rudata', 'MoexStocks') }}