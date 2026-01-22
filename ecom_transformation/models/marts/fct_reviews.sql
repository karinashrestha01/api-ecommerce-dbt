WITH reviews AS (SELECT * FROM {{ ref('stg_order_reviews') }}),
     orders AS (SELECT order_id, customer_id FROM {{ ref('stg_orders') }})

SELECT
    reviews.review_id,
    reviews.order_id,
    COALESCE(orders.customer_id, 'Unknown') AS customer_id,
    reviews.review_score,
       -- Creation Date Key: Handle NULLs by pointing to -1
    COALESCE(
        CAST(TO_CHAR(review_creation_date, 'YYYYMMDD') AS INTEGER), 
        -1
    ) AS review_creation_date_key,
    COALESCE(
    CAST(TO_CHAR(review_answer_timestamp, 'YYYYMMDD') AS INTEGER), -1) AS review_answer_timestamp_key,

    -- Derived Metric: Response Time in Hours
    -- Logic: Extract the total seconds between creation and answer, divide by 3600
    ROUND(CAST(GREATEST(COALESCE(EXTRACT(EPOCH FROM (review_answer_timestamp - review_creation_date)) / 3600, 0), 0) 
        AS NUMERIC), 
    2) AS response_time_hours


FROM reviews
LEFT JOIN orders ON reviews.order_id = orders.order_id