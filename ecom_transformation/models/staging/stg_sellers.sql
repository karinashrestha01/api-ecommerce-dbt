WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'sellers') }}
),

json_extraction AS (
    SELECT
        data->>'seller_id' as seller_id,
        data->>'seller_zip_code_prefix' as seller_zip_code_prefix,
        data->>'seller_city' as seller_city,
        data->>'seller_state' as seller_state
    FROM source
)

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    COALESCE(
            INITCAP(NULLIF(seller_city, '')), 
            'Unknown'
        ) AS seller_city,
    INITCAP(seller_state) AS state
FROM json_extraction