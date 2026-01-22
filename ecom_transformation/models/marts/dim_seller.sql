WITH stg_sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT 
    MD5(CAST(seller_id AS VARCHAR)) AS seller_key,
    seller_id,
    zip_code AS seller_zip_code,
    seller_city AS seller_city,
    state AS seller_state
FROM stg_sellers