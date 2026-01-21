WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT 
    order_id,
    order_status,
    purchase_at AS purchase_timestamp,
    delivered_at AS delivery_date,
    -- Derived Column: Delivery Speed
    EXTRACT(DAY FROM (delivered_at - purchase_at)) AS days_to_deliver
FROM stg_orders