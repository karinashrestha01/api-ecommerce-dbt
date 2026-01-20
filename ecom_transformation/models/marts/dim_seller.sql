WITH stg_sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT 
    seller_id,
    zip_code AS seller_zip_code,
    seller_city AS seller_city,
    state AS seller_state
FROM stg_sellers