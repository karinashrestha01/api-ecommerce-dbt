WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT 
    order_id,
    order_status,
    -- FIX: Use the name from stg_orders
    order_purchase_at AS purchase_timestamp,
    order_delivered_at AS delivery_date,
    order_estimated_delivery_at AS estimated_delivery_date
FROM stg_orders