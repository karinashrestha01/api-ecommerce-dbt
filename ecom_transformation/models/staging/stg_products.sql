WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'products') }}
)

SELECT
    product_id,
    -- CLEANING: Base64 Decoding
    CASE 
        -- Rule 1: Must contain only valid Base64 characters
        -- Rule 2: Must NOT have spaces
        -- Rule 3: Length must be divisible by 4 (CRITICAL FIX for "invalid end sequence")
        WHEN product_category_name ~ '^[a-zA-Z0-9+/=]+$' 
             AND position(' ' in product_category_name) = 0
             AND MOD(LENGTH(product_category_name), 4) = 0
        THEN convert_from(decode(product_category_name, 'base64'), 'UTF8')
        ELSE product_category_name 
    END AS category_name_pt,
    
    TRIM(product_category_name_english) AS category_name_en,
    
    -- Cast numbers (Handling empty strings)
    CAST(NULLIF(product_weight_g, '') AS NUMERIC) AS weight_g,
    CAST(NULLIF(product_length_cm, '') AS NUMERIC) AS length_cm,
    CAST(NULLIF(product_height_cm, '') AS NUMERIC) AS height_cm,
    CAST(NULLIF(product_width_cm, '') AS NUMERIC) AS width_cm

FROM source