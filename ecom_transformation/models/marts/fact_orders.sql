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
    COALESCE(CAST(TO_CHAR(orders.purchase_at, 'YYYYMMDD') AS INTEGER),-1) AS order_purchase_timestamp_key,
    COALESCE(CAST(TO_CHAR(orders.approved_at, 'YYYYMMDD') AS INTEGER),-1) AS order_approved_at_key,
    COALESCE(CAST(TO_CHAR(orders.delivered_at, 'YYYYMMDD') AS INTEGER),-1) AS order_delivered_customer_date_key,
    COALESCE(CAST(TO_CHAR(orders.estimated_delivery_at, 'YYYYMMDD') AS INTEGER),-1) AS order_estimated_delivery_date_key,

    -- Metrics
   ROUND(
        CAST(
            GREATEST(COALESCE(EXTRACT(EPOCH FROM (orders.approved_at - orders.purchase_at)) / 3600, 0), 0) 
        AS NUMERIC), 
    2) AS time_to_approve_hours,
    
    -- days_to_ship (Delivered - Purchase)
    GREATEST(
        COALESCE(
            EXTRACT(DAY FROM (orders.delivered_at - orders.purchase_at)), 
            0
        ), 
        0
    ) AS days_to_ship,
    
    -- actual_vs_estimated_difference (Estimated - Actual)
    COALESCE(
        EXTRACT(DAY FROM (orders.estimated_delivery_at - orders.delivered_at)), 
        0
    ) AS actual_vs_estimated_difference,
    GREATEST(COALESCE(totals.total_order_price, 0), 0) AS total_order_price,
    GREATEST(COALESCE(totals.total_freight_value, 0), 0) AS total_freight_value
FROM orders
LEFT JOIN order_totals totals 
    ON orders.order_id = totals.order_id