{{
  config(
    materialized='incremental',
    unique_key='order_id'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_orders_dataset') }}
    
    {% if is_incremental() %}
    -- Solo traemos datos cargados después de la última ejecución exitosa
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
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
