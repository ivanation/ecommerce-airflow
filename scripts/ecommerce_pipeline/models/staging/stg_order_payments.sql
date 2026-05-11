WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_order_payments_dataset') }}
)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE order_id IS NOT NULL
