--The total amount paid by the customer (fct_payements) must match the total cost of the order

WITH order_totals AS (
    SELECT 
        order_id, 
        (total_order_price + total_freight_value) as expected_total
    FROM {{ ref('fact_orders') }}
),

payment_totals AS (
    SELECT 
        order_id, 
        SUM(payment_value) as actual_paid
    FROM {{ ref('fct_payments') }} 
    GROUP BY 1
)

SELECT 
    o.order_id,
    o.expected_total,
    p.actual_paid
FROM order_totals o
JOIN payment_totals p ON o.order_id = p.order_id
WHERE ABS(o.expected_total - p.actual_paid) > 1.00