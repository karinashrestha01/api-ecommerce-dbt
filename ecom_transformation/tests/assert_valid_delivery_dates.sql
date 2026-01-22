--An order cannot be delivered before it was purchased.

SELECT 
    order_id,
    purchase_at,
    delivered_at
FROM {{ ref('stg_orders') }} 
WHERE delivered_at < purchase_at
  AND delivered_at IS NOT NULL