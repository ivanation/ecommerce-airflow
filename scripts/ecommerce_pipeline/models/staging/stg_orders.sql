WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_orders_dataset') }}
)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp AS purchased_at,
    order_approved_at AS approved_at,
    order_delivered_carrier_date AS delivered_carrier_at,
    order_delivered_customer_date AS delivered_customer_at,
    order_estimated_delivery_date AS estimated_delivery_at,
    -- Columnas de auditoría (opcionales pero recomendadas)
    _ingested_at,
    _source_file
FROM source
WHERE order_id IS NOT NULL
