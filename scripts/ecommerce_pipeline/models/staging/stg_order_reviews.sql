WITH source AS (
    SELECT * FROM {{ source('raw', 'olist_order_reviews_dataset') }}
),

renamed AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date AS created_at,
        review_answer_timestamp AS answered_at,
        -- Columnas de auditoría
        _ingested_at,
        _source_file
    FROM source
    WHERE review_id IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id 
            ORDER BY answered_at DESC
        ) AS row_num
    FROM renamed
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    created_at,
    answered_at,
    _ingested_at,
    _source_file
FROM deduplicated
WHERE row_num = 1
