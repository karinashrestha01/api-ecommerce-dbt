WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'order_reviews') }}
)

SELECT
    review_id,
    order_id,
    -- Handle 'None' in scores before casting
    CAST(NULLIF(review_score, 'None') AS INTEGER) AS review_score,
    
    COALESCE(review_comment_title, 'No Title') AS review_comment_title,
    
    REPLACE(REPLACE(review_comment_message, '\n', ' '), '\r', '') AS review_comment_message,
    
    -- FIX: Handle 'None' string for Creation Date
    CASE 
        WHEN review_creation_date IS NULL THEN NULL
        WHEN review_creation_date = '' THEN NULL
        WHEN review_creation_date = 'None' THEN NULL 
        WHEN review_creation_date = 'invalid-date' THEN NULL
        ELSE CAST(review_creation_date AS TIMESTAMP) 
    END AS review_creation_date,
    
    -- FIX: Handle 'None' string for Answer Timestamp
    CASE 
        WHEN review_answer_timestamp IS NULL THEN NULL
        WHEN review_answer_timestamp = '' THEN NULL
        WHEN review_answer_timestamp = 'None' THEN NULL
        WHEN review_answer_timestamp = 'invalid-date' THEN NULL
        ELSE CAST(review_answer_timestamp AS TIMESTAMP) 
    END AS review_answer_timestamp

FROM source