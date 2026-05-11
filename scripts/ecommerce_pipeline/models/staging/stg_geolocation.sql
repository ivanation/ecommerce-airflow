WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_geolocation_dataset') }}
)

SELECT
    geolocation_zip_code_prefix AS zip_code_prefix,
    geolocation_lat AS latitude,
    geolocation_lng AS longitude,
    geolocation_city AS city,
    geolocation_state AS state,
    -- Columnas de auditoría
    _ingested_at,
    _source_file
FROM source
WHERE geolocation_zip_code_prefix IS NOT NULL
