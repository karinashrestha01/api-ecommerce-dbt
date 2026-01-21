WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    product_id,
    -- Use the cleaned English name
    COALESCE(category_name_en, 'Uncategorized') AS product_category,
    photos_qty AS product_photos_qty,
    weight_g AS product_weight_g,
    --derived 
    (length_cm * height_cm * width_cm) AS volume_cm3
FROM stg_products