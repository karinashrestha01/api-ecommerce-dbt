WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'orders') }}
),

json_extraction AS (
    SELECT
        data->>'order_id' as order_id,
        data->>'customer_id' as customer_id,
        data->>'order_status' as order_status,
        data->>'order_purchase_timestamp' as order_purchase_timestamp,
        data->>'order_approved_at' as order_approved_at,
        data->>'order_delivered_customer_date' as order_delivered_customer_date,
        data->>'order_estimated_delivery_date' as order_estimated_delivery_date
    FROM source
),

deduplicated AS (
    SELECT DISTINCT ON (order_id) * FROM json_extraction
)

SELECT
    order_id,
    customer_id,
    LOWER(TRIM(order_status)) as order_status,
    
    -- CLEANING: order_purchase_timestamp
    CASE 
        -- 1. Handle Empty/Nulls
        WHEN order_purchase_timestamp IS NULL OR order_purchase_timestamp IN ('None', 'invalid-date', '') THEN NULL
        
        -- 2. Handle Epochs (digits only)
        WHEN order_purchase_timestamp ~ '^\d+(\.\d+)?$' 
            THEN to_timestamp(order_purchase_timestamp::double precision)
            
        -- 3. Handle DD/MM/YYYY (Specific format causing error "26/04/2017")
        WHEN order_purchase_timestamp ~ '^\d{1,2}/\d{1,2}/\d{4}' 
            THEN to_timestamp(order_purchase_timestamp, 'DD/MM/YYYY HH24:MI:SS')
            
        -- 4. Handle Standard ISO (YYYY-MM-DD)
        WHEN order_purchase_timestamp ~ '^\d{4}-\d{2}-\d{2}' 
            THEN CAST(order_purchase_timestamp AS TIMESTAMP)
            
        -- 5. Safe Fallback (Return NULL instead of crashing)
        ELSE NULL
    END AS purchase_at,

    -- CLEANING: order_approved_at
    CASE 
        WHEN order_approved_at IS NULL OR order_approved_at IN ('None', 'invalid-date', '') THEN NULL
        WHEN order_approved_at ~ '^\d+(\.\d+)?$' THEN to_timestamp(order_approved_at::double precision)
        WHEN order_approved_at ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN to_timestamp(order_approved_at, 'DD/MM/YYYY HH24:MI:SS')
        WHEN order_approved_at ~ '^\d{4}-\d{2}-\d{2}' THEN CAST(order_approved_at AS TIMESTAMP)
        ELSE NULL
    END AS approved_at,

    -- CLEANING: order_delivered_customer_date
    CASE 
        WHEN order_delivered_customer_date IS NULL OR order_delivered_customer_date IN ('None', 'invalid-date', '') THEN NULL
        WHEN order_delivered_customer_date ~ '^\d+(\.\d+)?$' THEN to_timestamp(order_delivered_customer_date::double precision)
        WHEN order_delivered_customer_date ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN to_timestamp(order_delivered_customer_date, 'DD/MM/YYYY HH24:MI:SS')
        WHEN order_delivered_customer_date ~ '^\d{4}-\d{2}-\d{2}' THEN CAST(order_delivered_customer_date AS TIMESTAMP)
        ELSE NULL
    END AS delivered_at,
    
    -- CLEANING: order_estimated_delivery_date
    CASE 
        WHEN order_estimated_delivery_date IS NULL OR order_estimated_delivery_date IN ('None', 'invalid-date', '') THEN NULL
        WHEN order_estimated_delivery_date ~ '^\d{1,2}/\d{1,2}/\d{4}' THEN to_timestamp(order_estimated_delivery_date, 'DD/MM/YYYY HH24:MI:SS')
        WHEN order_estimated_delivery_date ~ '^\d{4}-\d{2}-\d{2}' THEN CAST(order_estimated_delivery_date AS TIMESTAMP)
        ELSE NULL
    END AS estimated_delivery_at

FROM deduplicated