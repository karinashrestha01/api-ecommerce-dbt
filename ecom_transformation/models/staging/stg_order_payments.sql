WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'order_payments') }}
)

SELECT
    order_id,
    CAST(NULLIF(payment_sequential, 'None') AS INTEGER) AS payment_sequential,
    INITCAP(REPLACE(payment_type, '_', ' ')) AS payment_type,
    CAST(NULLIF(payment_installments, 'None') AS INTEGER) AS payment_installments,
    CAST(NULLIF(payment_value, 'None') AS NUMERIC) AS payment_value
FROM source