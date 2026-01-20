WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT order_id, customer_id 
    FROM {{ ref('stg_orders') }}
)

SELECT
    -- Surrogate Key
    MD5(items.order_id || '-' || CAST(items.order_item_id AS VARCHAR)) as order_item_sk,

    -- Natural Key
    items.order_item_id,

    -- Foreign Keys
    items.order_id,     
    items.product_id,   
    items.seller_id,    
    
    -- Handle missing customer_id because of the data mismatch
    COALESCE(orders.customer_id, 'Unknown') as customer_id, 

    -- Metrics
    items.price,
    items.freight_value,
    
    -- Total Value
    (items.price + items.freight_value) AS total_item_value

FROM items
-- CHANGE TO LEFT JOIN
-- This keeps the item data even if the order header is missing
LEFT JOIN orders 
    ON items.order_id = orders.order_id