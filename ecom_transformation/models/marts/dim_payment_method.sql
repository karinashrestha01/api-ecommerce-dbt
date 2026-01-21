WITH stg_payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
),

distinct_methods AS (
    SELECT DISTINCT payment_type
    FROM stg_payments
    WHERE payment_type IS NOT NULL
)

SELECT
    -- Surrogate Key
    MD5(payment_type) AS payment_method_key,
    payment_type
FROM distinct_methods