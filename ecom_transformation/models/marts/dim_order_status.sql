WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

distinct_statuses AS (
    SELECT DISTINCT order_status AS status_name
    FROM stg_orders
)

SELECT
    -- Surrogate Key
    MD5(status_name) AS order_status_key,
    
    status_name,
    
    -- Derived Flags as shown in Image
    CASE 
        WHEN status_name = 'canceled' THEN true 
        ELSE false 
    END AS is_canceled,
    
    CASE 
        WHEN status_name = 'delivered' THEN true 
        ELSE false 
    END AS is_completed
FROM distinct_statuses