WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_totals AS (
    SELECT 
        order_id,
        SUM(price) AS total_order_price,
        SUM(freight_value) AS total_freight_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    -- surrogate keys
    MD5(orders.order_id) AS order_key,
    orders.order_id,
    MD5(orders.customer_id) AS customer_key,
    MD5(orders.order_status) AS order_status_key,
    
    -- Date Keys (Using clean names from Staging)
    CAST(TO_CHAR(orders.purchase_at, 'YYYYMMDD') AS INTEGER) AS order_purchase_timestamp_key,
    CAST(TO_CHAR(orders.approved_at, 'YYYYMMDD') AS INTEGER) AS order_approved_at_key,
    CAST(TO_CHAR(orders.delivered_at, 'YYYYMMDD') AS INTEGER) AS order_delivered_customer_date_key,
    CAST(TO_CHAR(orders.estimated_delivery_at, 'YYYYMMDD') AS INTEGER) AS order_estimated_delivery_date_key,

    -- Metrics
    EXTRACT(EPOCH FROM (orders.approved_at - orders.purchase_at)) / 3600 AS time_to_approve_hours,
    EXTRACT(DAY FROM (orders.delivered_at - orders.purchase_at)) AS days_to_ship,
    EXTRACT(DAY FROM (orders.estimated_delivery_at - orders.delivered_at)) AS actual_vs_estimated_difference,
    COALESCE(totals.total_order_price, 0) AS total_order_price,
    COALESCE(totals.total_freight_value, 0) AS total_freight_value

FROM orders
LEFT JOIN order_totals totals 
    ON orders.order_id = totals.order_id