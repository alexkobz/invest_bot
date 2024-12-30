SELECT
    *,
    NOW() AS load_ts
FROM {{ source('moex', 'api_moex_securities_trading') }}