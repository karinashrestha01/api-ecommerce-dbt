WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT 
        order_id, 
        customer_id, 
        purchase_at 
    FROM {{ ref('stg_orders') }}
)

SELECT
    -- Surrogate Key (MD5)
    MD5(items.order_id || '-' || items.order_item_id) AS order_item_sk,
    MD5(CAST(items.order_id AS VARCHAR)) AS order_key,
    MD5(CAST(items.product_id AS VARCHAR)) AS product_key,
    MD5(CAST(items.seller_id AS VARCHAR)) AS seller_key,
    COALESCE(orders.customer_id, 'Unknown') AS customer_id,
    -- Natural Keys (The tests in schema.yml are looking for these!)
    items.order_id,    
    CAST(orders.purchase_at AS DATE) AS order_date,
    GREATEST(COALESCE(items.price,0),0) AS price,
    GREATEST(COALESCE(items.freight_value,0),0) AS freight_value,
    GREATEST(COALESCE((items.price + items.freight_value),0),0) AS total_item_value
FROM items
LEFT JOIN orders ON items.order_id = orders.order_id