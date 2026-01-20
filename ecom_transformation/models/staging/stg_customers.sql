WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'customers')}}
)

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix AS zip_code,
     COALESCE(
            INITCAP(NULLIF(customer_city, '')), 
            'Unknown'
        ) AS customer_city,
    INITCAP(customer_state) AS customer_state
FROM source