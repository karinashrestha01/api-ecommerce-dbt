WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    product_id,
    -- FIX: Use the cleaned english column from staging
    COALESCE(INITCAP(REPLACE(category_name_en, '_', ' ')), 'Uncategorized') AS product_category,
    
    -- NOTE: 'product_photos_qty' does not exist in your stg_products SQL.
    -- If you want it, you must go back and add it to stg_products.sql first!
    -- For now, I will omit it to prevent error.
    
    -- FIX: Use 'weight_g' (the name in staging), not 'product_weight_g'
    weight_g AS product_weight_g,
    
    -- FIX: Use the 'length_cm', 'height_cm', etc from staging
    CAST(length_cm AS VARCHAR) || 'x' || 
    CAST(height_cm AS VARCHAR) || 'x' || 
    CAST(width_cm AS VARCHAR) AS product_dims
FROM stg_products