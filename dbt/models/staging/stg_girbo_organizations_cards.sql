SELECT
    *,
    NOW() AS load_ts
FROM {{ source('girbo', 'api_girbo_organizations_cards') }}