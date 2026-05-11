WITH products AS (
    SELECT * FROM {{ ref('int_products_translated') }}
)

SELECT
    product_id,
    category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm
FROM products
