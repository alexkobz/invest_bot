SELECT
    *
FROM {{ source('cbr', 'DragMetDynamic') }}