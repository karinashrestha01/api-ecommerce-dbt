WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'order_items') }}
),

json_extraction AS (
    SELECT
        data->>'order_id' as order_id,
        data->>'order_item_id' as order_item_id,
        data->>'product_id' as product_id,
        data->>'seller_id' as seller_id,
        data->>'shipping_limit_date' as shipping_limit_date,
        data->>'price' as price,
        data->>'freight_value' as freight_value
    FROM source
),

deduplicated AS (
    SELECT DISTINCT ON (order_id, order_item_id) *
    FROM json_extraction
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CASE 
        -- 1. Handle garbage / empty
        WHEN shipping_limit_date IS NULL THEN NULL
        WHEN shipping_limit_date = 'invalid-date' THEN NULL
        WHEN shipping_limit_date = '' THEN NULL
        
        -- 2. Handle Epochs
        WHEN shipping_limit_date ~ '^\d+$' 
            THEN to_timestamp(shipping_limit_date::double precision)
            
        -- 3. Handle DD/MM/YYYY
        WHEN shipping_limit_date ~ '^\d{2}/\d{2}/\d{4}' 
            THEN to_timestamp(shipping_limit_date, 'DD/MM/YYYY HH24:MI:SS')
            
        -- 4. Fallback
        ELSE CAST(shipping_limit_date AS TIMESTAMP)
    END AS shipping_limit_date,

    CAST(price AS NUMERIC) AS price,
    CAST(freight_value AS NUMERIC) AS freight_value
FROM deduplicated