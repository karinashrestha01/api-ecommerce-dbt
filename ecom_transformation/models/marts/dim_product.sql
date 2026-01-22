WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    MD5(CAST(product_id AS VARCHAR)) AS product_key,
    product_id,
    COALESCE(category_name_en, 'Uncategorized') AS product_category,
    COALESCE(product_description_length,0) AS product_description_length ,
    photos_qty AS product_photos_qty,
    ROUND(COALESCE(weight_g,0),0) AS product_weight_g,
   ROUND((COALESCE(length_cm,0) * COALESCE(height_cm,0) * COALESCE(width_cm,0)),0) AS volume_cm3
FROM stg_products