WITH source AS (
    SELECT * FROM {{ source('ecommerce_raw', 'order_reviews') }}
),

json_extraction AS (
    SELECT
        data->>'review_id' as review_id,
        data->>'order_id' as order_id,
        data->>'review_score' as review_score,
        data->>'review_comment_title' as review_comment_title,
        data->>'review_comment_message' as review_comment_message,
        data->>'review_creation_date' as review_creation_date,
        data->>'review_answer_timestamp' as review_answer_timestamp
    FROM source
),

deduplicated AS (
    SELECT DISTINCT ON (review_id) *  
    FROM json_extraction
)

SELECT
    review_id,
    order_id,
    CAST(NULLIF(review_score, 'None') AS INTEGER) AS review_score,
    COALESCE(review_comment_title, 'No Title') AS review_comment_title,
    REPLACE(REPLACE(review_comment_message, '\n', ' '), '\r', '') AS review_message,
    
    -- Creation Date Cleaning
    CASE 
        WHEN review_creation_date IS NULL OR review_creation_date IN ('', 'None', 'invalid-date') THEN NULL
        
        -- 1. Handle Epochs (digits only)
        WHEN review_creation_date ~ '^\d+(\.\d+)?$' 
            THEN to_timestamp(review_creation_date::double precision)
            
        -- 2. Handle DD/MM/YYYY (Fix for "25/04/2018 00:00:00")
        WHEN review_creation_date ~ '^\d{1,2}/\d{1,2}/\d{4}' 
            THEN to_timestamp(review_creation_date, 'DD/MM/YYYY HH24:MI:SS')
            
        -- 3. Standard ISO
        ELSE CAST(review_creation_date AS TIMESTAMP) 
    END AS review_creation_date,
    
    -- Answer Timestamp Cleaning
    CASE 
        WHEN review_answer_timestamp IS NULL OR review_answer_timestamp IN ('', 'None', 'invalid-date') THEN NULL
        
        -- 1. Handle Epochs
        WHEN review_answer_timestamp ~ '^\d+(\.\d+)?$' 
            THEN to_timestamp(review_answer_timestamp::double precision)
            
        -- 2. Handle DD/MM/YYYY
        WHEN review_answer_timestamp ~ '^\d{1,2}/\d{1,2}/\d{4}' 
            THEN to_timestamp(review_answer_timestamp, 'DD/MM/YYYY HH24:MI:SS')
            
        -- 3. Standard ISO
        ELSE CAST(review_answer_timestamp AS TIMESTAMP) 
    END AS review_answer_timestamp

FROM deduplicated