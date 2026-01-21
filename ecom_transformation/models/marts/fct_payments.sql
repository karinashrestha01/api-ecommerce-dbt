WITH payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
),

orders AS (
    SELECT order_id, customer_id 
    FROM {{ ref('stg_orders') }}
)

SELECT
    MD5(payments.order_id || '-' || payments.payment_sequential) AS payment_sk,
    payments.order_id,
    COALESCE(orders.customer_id, 'Unknown') AS customer_id,
    payments.payment_installments AS installments,
    payments.payment_value
FROM payments
LEFT JOIN orders ON payments.order_id = orders.order_id