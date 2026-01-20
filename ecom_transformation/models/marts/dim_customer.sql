WITH stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT 
    customer_id,
    customer_unique_id,
    zip_code AS customer_zip,
    COALESCE(INITCAP(customer_city), 'Unknown') AS customer_city,
    UPPER(customer_state) AS customer_state
FROM stg_customers