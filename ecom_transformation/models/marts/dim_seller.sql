WITH stg_sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)

SELECT 
    seller_id,
    -- FIX: Use 'zip_code' from staging
    zip_code AS seller_zip_code,
    -- FIX: Use 'city' from staging
    city AS seller_city,
    -- FIX: Use 'state' from staging
    state AS seller_state
FROM stg_sellers