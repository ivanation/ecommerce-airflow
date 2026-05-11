WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
)

SELECT
    order_id,
    COUNT(order_item_id) AS total_items,
    SUM(price) AS total_price,
    SUM(freight) AS total_freight,
    MIN(shipping_limit_at) AS earliest_shipping_limit,
    MAX(shipping_limit_at) AS latest_shipping_limit
FROM order_items
GROUP BY order_id
