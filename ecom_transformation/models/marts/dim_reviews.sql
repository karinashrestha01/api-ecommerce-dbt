SELECT 
    review_id,
    order_id,
    review_score,
    review_message AS review_comment
FROM {{ ref('stg_order_reviews') }}