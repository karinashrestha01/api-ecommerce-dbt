WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'customers')}}
),

json_extraction AS (
    SELECT
        data->>'customer_id' as customer_id,
        data->>'customer_unique_id' as customer_unique_id,
        data->>'customer_zip_code_prefix' as customer_zip_code_prefix,
        data->>'customer_city' as customer_city,
        data->>'customer_state' as customer_state
    FROM source
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
FROM json_extraction