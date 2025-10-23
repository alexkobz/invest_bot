SELECT
    *
FROM {{ source('finance_marker', 'ratios') }}