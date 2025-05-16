SELECT
    *,
    NOW() AS load_ts
FROM {{ source('finance_marker', 'ratios') }}