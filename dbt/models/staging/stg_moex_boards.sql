SELECT
    *
FROM {{ source('moex', 'StockSharesBoards') }}