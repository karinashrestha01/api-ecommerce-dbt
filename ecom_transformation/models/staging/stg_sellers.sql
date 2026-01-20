WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'sellers') }}
)

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    COALESCE(
            INITCAP(NULLIF(seller_city, '')), 
            'Unknown'
        ) AS seller_city
    INITCAP(seller_state) AS state
FROM source