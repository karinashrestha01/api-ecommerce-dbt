WITH stg_reviews AS (
    SELECT * FROM {{ ref('stg_order_reviews') }}
)

SELECT
    MD5(review_id) AS review_detail_key,
    review_id,
    review_comment_title,
    review_message AS review_comment_message ,
      -- Derived Sentiment Flag
    CASE 
        WHEN review_score <= 2 THEN 'Negative'
        WHEN review_score = 3 THEN 'Neutral'
        ELSE 'Positive' 
    END AS sentiment
FROM stg_reviews