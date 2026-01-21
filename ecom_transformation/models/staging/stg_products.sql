WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'products') }}
),

json_extraction AS (
    SELECT
        data->>'product_id' as product_id,
        data->>'product_category_name' as product_category_name,
        data->>'product_category_name_english' as product_category_name_english,
        data->>'product_description_length' as product_description_length,
        data->>'product_photos_qty' as product_photos_qty,
        data->>'product_weight_g' as product_weight_g,
        data->>'product_length_cm' as product_length_cm,
        data->>'product_height_cm' as product_height_cm,
        data->>'product_width_cm' as product_width_cm
    FROM source
)

SELECT
    product_id,
    -- 1. Portuguese Category
    CASE 
        WHEN product_category_name ~ '^[a-zA-Z0-9+/=]+$' 
             AND position(' ' in product_category_name) = 0
             AND MOD(LENGTH(product_category_name), 4) = 0
        THEN INITCAP(REPLACE(convert_from(decode(product_category_name, 'base64'), 'LATIN1'), '_', ' '))
        ELSE INITCAP(REPLACE(product_category_name, '_', ' '))
    END AS category_name_pt,
    
    -- 2. English Category
    CASE 
        WHEN product_category_name_english ~ '^[a-zA-Z0-9+/=]+$' 
             AND position(' ' in product_category_name_english) = 0
             AND MOD(LENGTH(product_category_name_english), 4) = 0
        THEN INITCAP(REPLACE(convert_from(decode(product_category_name_english, 'base64'), 'LATIN1'), '_', ' '))
        ELSE INITCAP(REPLACE(TRIM(product_category_name_english), '_', ' '))
    END AS category_name_en,
    
    -- Fix: Explicitly handle 'NaN' before casting to Numeric/Integer
    CAST(NULLIF(NULLIF(product_description_length, ''), 'NaN') AS NUMERIC) AS product_description_length,
    CAST(NULLIF(NULLIF(product_photos_qty, ''), 'NaN') AS NUMERIC)::INTEGER AS photos_qty,
    CAST(NULLIF(NULLIF(product_weight_g, ''), 'NaN') AS NUMERIC) AS weight_g,
    CAST(NULLIF(NULLIF(product_length_cm, ''), 'NaN') AS NUMERIC) AS length_cm,
    CAST(NULLIF(NULLIF(product_height_cm, ''), 'NaN') AS NUMERIC) AS height_cm,
    CAST(NULLIF(NULLIF(product_width_cm, ''), 'NaN') AS NUMERIC) AS width_cm
FROM json_extraction