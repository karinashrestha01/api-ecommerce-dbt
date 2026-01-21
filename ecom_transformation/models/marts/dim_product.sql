WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    MD5(CAST(product_id AS VARCHAR)) AS product_key,
    product_id,
    COALESCE(category_name_en, 'Uncategorized') AS product_category,
    product_description_length ,
    photos_qty AS product_photos_qty,
    weight_g AS product_weight_g,
    (COALESCE(length_cm,0) * COALESCE(height_cm,0) * COALESCE(width_cm,0)) AS volume_cm3
FROM stg_products