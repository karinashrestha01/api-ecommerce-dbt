WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    product_id,
    COALESCE(category_name_en, 'Uncategorized') AS product_category,
    photos_qty AS product_photos_qty,
    weight_g AS product_weight_g,
    -- Handle cases where dimensions might be null to avoid calculation errors
    (COALESCE(length_cm,0) * COALESCE(height_cm,0) * COALESCE(width_cm,0)) AS volume_cm3
FROM stg_products