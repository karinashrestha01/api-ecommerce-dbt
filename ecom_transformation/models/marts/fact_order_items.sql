WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT 
        order_id, 
        customer_id, 
        -- FIX: Use the actual name from stg_orders
        order_purchase_at 
    FROM {{ ref('stg_orders') }}
)

SELECT
    -- Surrogate Key (MD5)
    MD5(items.order_id || '-' || items.order_item_id) AS order_item_sk,
    items.order_id,
    items.product_id,
    items.seller_id,
    
    COALESCE(orders.customer_id, 'Unknown') AS customer_id,
    
    -- FIX: Use the correct column name here too
    CAST(orders.order_purchase_at AS DATE) AS order_date,
    
    items.price,
    items.freight_value,
    (items.price + items.freight_value) AS total_item_value

FROM items
LEFT JOIN orders ON items.order_id = orders.order_id