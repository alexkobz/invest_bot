SELECT
    *
FROM {{ source('moex', 'HistoryStockSharesSecurities') }}