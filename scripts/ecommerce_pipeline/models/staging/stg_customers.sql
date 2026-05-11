WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_customers_dataset') }}
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix AS zip_code,
    customer_city AS city,
    customer_state AS state,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE customer_id IS NOT NULL
