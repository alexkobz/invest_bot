SELECT
    *
FROM {{ source('rudata', 'api_rudata_emitents') }}