WITH source AS (
    -- Deduplicate based on the composite key
    SELECT DISTINCT ON (order_id, payment_sequential) * 
    FROM {{ source('ecommerce_raw', 'order_payments') }}
)

SELECT
    -- ENSURE THIS COLUMN EXISTS HERE:
    order_id,
    
    CAST(NULLIF(payment_sequential, 'None') AS INTEGER) AS payment_sequential,
    
    -- Base64 Decoding Logic
    CASE 
        WHEN payment_type ~ '^[A-Za-z0-9+/]+={0,2}$' 
             AND LENGTH(payment_type) % 4 = 0 
        THEN INITCAP(REPLACE(convert_from(decode(payment_type, 'base64'), 'UTF8'), '_', ' '))
        ELSE INITCAP(REPLACE(TRIM(payment_type), '_', ' '))
    END AS payment_type,

    CAST(NULLIF(payment_installments, 'None') AS INTEGER) AS payment_installments,
    CAST(NULLIF(payment_value, 'None') AS NUMERIC) AS payment_value
FROM source