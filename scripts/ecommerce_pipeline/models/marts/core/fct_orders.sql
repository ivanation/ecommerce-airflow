{{
  config(
    materialized='incremental',
    unique_key='order_id'
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

items AS (
    SELECT * FROM {{ ref('int_order_items_aggregated') }}
),

payments AS (
    SELECT * FROM {{ ref('int_order_payments_pivoted') }}
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.delivered_carrier_at,
        o.delivered_customer_at,
        o.estimated_delivery_at,
        COALESCE(i.total_items, 0) AS total_items,
        COALESCE(i.total_price, 0) AS total_item_price,
        COALESCE(i.total_freight, 0) AS total_freight,
        COALESCE(p.credit_card_amount, 0) AS credit_card_amount,
        COALESCE(p.boleto_amount, 0) AS boleto_amount,
        COALESCE(p.voucher_amount, 0) AS voucher_amount,
        COALESCE(p.debit_card_amount, 0) AS debit_card_amount,
        COALESCE(p.total_payment_value, 0) AS total_payment_value
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN items i ON o.order_id = i.order_id
    LEFT JOIN payments p ON o.order_id = p.order_id
)

SELECT * FROM final
