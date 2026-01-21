WITH source AS (
    SELECT DISTINCT ON (order_id) * FROM {{ source('ecommerce_raw', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) as order_status,
    
    -- CLEANING: order_purchase_timestamp
    CASE 
        WHEN order_purchase_timestamp IS NULL THEN NULL
        WHEN order_purchase_timestamp = 'None' THEN NULL         
        WHEN order_purchase_timestamp = 'invalid-date' THEN NULL
        WHEN order_purchase_timestamp = '' THEN NULL
        -- 1. Handle Epochs
        WHEN order_purchase_timestamp ~ '^\d+$' 
            THEN to_timestamp(order_purchase_timestamp::bigint)
        -- 2. Handle DD/MM/YYYY
        WHEN order_purchase_timestamp ~ '^\d{2}/\d{2}/\d{4}' 
            THEN to_timestamp(order_purchase_timestamp, 'DD/MM/YYYY HH24:MI:SS')
        -- 3. Fallback
        ELSE order_purchase_timestamp::timestamp
    END AS purchase_at,

    CASE 
        WHEN order_approved_at IS NULL OR order_approved_at = 'invalid-date' OR order_approved_at = 'None' THEN NULL
        WHEN order_approved_at ~ '^\d+$' THEN to_timestamp(order_approved_at::bigint)
        ELSE CAST(order_approved_at AS TIMESTAMP)
    END AS approved_at,

    -- CLEANING: order_delivered_customer_date
    CASE 
        WHEN order_delivered_customer_date IS NULL THEN NULL
        WHEN order_delivered_customer_date = 'None' THEN NULL    
        WHEN order_delivered_customer_date = 'invalid-date' THEN NULL
        WHEN order_delivered_customer_date = '' THEN NULL
        WHEN order_delivered_customer_date ~ '^\d+$' 
            THEN to_timestamp(order_delivered_customer_date::bigint)
        WHEN order_delivered_customer_date ~ '^\d{2}/\d{2}/\d{4}' 
            THEN to_timestamp(order_delivered_customer_date, 'DD/MM/YYYY HH24:MI:SS')
        ELSE order_delivered_customer_date::timestamp
    END AS delivered_at,
    
    -- CLEANING: order_estimated_delivery_date
    CASE 
        WHEN order_estimated_delivery_date IS NULL THEN NULL
        WHEN order_estimated_delivery_date = 'None' THEN NULL    
        WHEN order_estimated_delivery_date = 'invalid-date' THEN NULL
        WHEN order_estimated_delivery_date = '' THEN NULL
        WHEN order_estimated_delivery_date ~ '^\d{2}/\d{2}/\d{4}' 
            THEN to_timestamp(order_estimated_delivery_date, 'DD/MM/YYYY HH24:MI:SS')
        ELSE order_estimated_delivery_date::timestamp
    END AS estimated_delivery_at

FROM source