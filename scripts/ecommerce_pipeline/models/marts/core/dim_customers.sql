WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state
FROM customers
