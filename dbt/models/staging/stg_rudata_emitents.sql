SELECT
    *,
    NOW() AS load_ts
FROM {{ source('rudata', 'api_rudata_emitents') }}