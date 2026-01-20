WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT order_id, customer_id 
    FROM {{ ref('stg_orders') }}
)

SELECT
    -- Native PostgreSQL Hashing:
    MD5(items.order_id || '-' || CAST(items.order_item_id AS VARCHAR)) as order_item_sk,

    -- 2. Natural Key
    items.order_item_id,

    -- 3. Foreign Keys (Links to Dimensions)
    items.order_id,     
    items.product_id,   
    items.seller_id,    
    orders.customer_id, 

    -- 4. Metrics / Measures
    items.price,
    items.freight_value,
    
    -- 5. Calculated Metric (Total Value)
    (items.price + items.freight_value) AS total_item_value

FROM items
-- Inner join because an item cannot exist without a parent order header
INNER JOIN orders 
    ON items.order_id = orders.order_id