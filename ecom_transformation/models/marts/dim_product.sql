WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }} 
)

SELECT 
    product_id,
    -- Use the cleaned English name
    COALESCE(category_name_en, 'Uncategorized') AS product_category,
    
    -- Now we can select this because we added it to staging!
    photos_qty AS product_photos_qty,
    
    weight_g AS product_weight_g,
    
    -- Dimensions
    CAST(length_cm AS VARCHAR) || 'x' || 
    CAST(height_cm AS VARCHAR) || 'x' || 
    CAST(width_cm AS VARCHAR) AS product_dims
FROM stg_products