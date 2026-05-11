WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

translation AS (
    SELECT * FROM {{ ref('stg_category_name_translation') }}
),

joined AS (
    SELECT
        p.product_id,
        COALESCE(t.category_name_english, p.category_name) AS category_name,
        p.name_length,
        p.description_length,
        p.photos_qty,
        p.weight_g,
        p.length_cm,
        p.height_cm,
        p.width_cm
    FROM products p
    LEFT JOIN translation t ON p.category_name = t.category_name
)

SELECT * FROM joined
