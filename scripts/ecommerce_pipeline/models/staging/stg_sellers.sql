WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_sellers_dataset') }}
)

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code_prefix,
    seller_city AS city,
    seller_state AS state,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE seller_id IS NOT NULL
