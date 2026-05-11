WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_order_items_dataset') }}
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date AS shipping_limit_at,
    price,
    freight_value AS freight,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE order_id IS NOT NULL
