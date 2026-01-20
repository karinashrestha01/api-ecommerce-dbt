WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'products') }}
)

SELECT
    product_id,
    
    -- 1. Portuguese Category (Keep your existing Base64 logic)
    CASE 
        WHEN product_category_name ~ '^[a-zA-Z0-9+/=]+$' 
             AND position(' ' in product_category_name) = 0
             AND MOD(LENGTH(product_category_name), 4) = 0
        THEN 
            INITCAP(REPLACE(convert_from(decode(product_category_name, 'base64'), 'LATIN1'), '_', ' '))
        ELSE 
            INITCAP(REPLACE(product_category_name, '_', ' '))
    END AS category_name_pt,
    
    -- 2. English Category (Keep your existing Base64 logic)
    CASE 
        WHEN product_category_name_english ~ '^[a-zA-Z0-9+/=]+$' 
             AND position(' ' in product_category_name_english) = 0
             AND MOD(LENGTH(product_category_name_english), 4) = 0
        THEN 
            INITCAP(REPLACE(convert_from(decode(product_category_name_english, 'base64'), 'LATIN1'), '_', ' '))
        ELSE 
            INITCAP(REPLACE(TRIM(product_category_name_english), '_', ' '))
    END AS category_name_en,
    
    -- FIX: Handle "1.0" by casting to Numeric first, then Integer
    CAST(CAST(NULLIF(product_photos_qty, '') AS NUMERIC) AS INTEGER) AS photos_qty,

    CAST(NULLIF(product_weight_g, '') AS NUMERIC) AS weight_g,
    CAST(NULLIF(product_length_cm, '') AS NUMERIC) AS length_cm,
    CAST(NULLIF(product_height_cm, '') AS NUMERIC) AS height_cm,
    CAST(NULLIF(product_width_cm, '') AS NUMERIC) AS width_cm

FROM source