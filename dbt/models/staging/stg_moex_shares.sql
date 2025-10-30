SELECT
    *
FROM {{ source('moex', 'StockSharesSecurities') }}