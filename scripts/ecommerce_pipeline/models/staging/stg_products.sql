WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_products_dataset') }}
)

SELECT
    product_id,
    product_category_name AS category_name,
    product_name_lenght AS name_length,
    product_description_lenght AS description_length,
    product_photos_qty AS photos_qty,
    product_weight_g AS weight_g,
    product_length_cm AS length_cm,
    product_height_cm AS height_cm,
    product_width_cm AS width_cm,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE product_id IS NOT NULL
