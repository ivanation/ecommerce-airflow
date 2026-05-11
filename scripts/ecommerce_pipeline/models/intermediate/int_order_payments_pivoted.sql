WITH payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
),

pivoted AS (
    SELECT
        order_id,
        SUM(CASE WHEN payment_type = 'credit_card' THEN payment_value ELSE 0 END) AS credit_card_amount,
        SUM(CASE WHEN payment_type = 'boleto' THEN payment_value ELSE 0 END) AS boleto_amount,
        SUM(CASE WHEN payment_type = 'voucher' THEN payment_value ELSE 0 END) AS voucher_amount,
        SUM(CASE WHEN payment_type = 'debit_card' THEN payment_value ELSE 0 END) AS debit_card_amount,
        SUM(payment_value) AS total_payment_value
    FROM payments
    GROUP BY order_id
)

SELECT * FROM pivoted
