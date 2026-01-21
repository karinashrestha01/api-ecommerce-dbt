WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_order_reviews') }}
)

SELECT
    MD5(review_id) AS review_detail_key,
    review_id,
    review_comment_title,
    review_message AS review_comment_message 
FROM stg_reviews