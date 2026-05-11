WITH source AS (
    SELECT * FROM {{ source('raw', 'product_category_name_translation') }}
)

SELECT
    product_category_name AS category_name,
    product_category_name_english AS category_name_english,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE product_category_name IS NOT NULL
