WITH source AS (
    SELECT DISTINCT ON (order_id, order_item_id) *
    FROM {{ source('ecommerce_raw', 'order_items') }}
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    
    -- FIX: Robust Date Handling for mixed formats
    CASE 
        -- 1. Handle garbage / empty
        WHEN shipping_limit_date IS NULL THEN NULL
        WHEN shipping_limit_date = 'invalid-date' THEN NULL
        WHEN shipping_limit_date = '' THEN NULL
        
        -- 2. Handle Epochs (only digits, e.g., "1534327818")
        WHEN shipping_limit_date ~ '^\d+$' 
            THEN to_timestamp(shipping_limit_date::double precision)
            
        -- 3. Handle DD/MM/YYYY format (e.g., "10/07/2018 12:30:45")
        WHEN shipping_limit_date ~ '^\d{2}/\d{2}/\d{4}' 
            THEN to_timestamp(shipping_limit_date, 'DD/MM/YYYY HH24:MI:SS')
            
        -- 4. Fallback to standard ISO format (e.g., "2018-05-03...")
        ELSE CAST(shipping_limit_date AS TIMESTAMP)
    END AS shipping_limit_date,

    CAST(price AS NUMERIC) AS price,
    CAST(freight_value AS NUMERIC) AS freight_value
FROM source