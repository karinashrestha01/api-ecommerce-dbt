WITH reviews AS (SELECT * FROM {{ ref('stg_order_reviews') }}),
     orders AS (SELECT order_id, customer_id FROM {{ ref('stg_orders') }})

SELECT
    reviews.review_id,
    reviews.order_id,
    COALESCE(orders.customer_id, 'Unknown') AS customer_id,
    
    reviews.review_score,
    -- Derived Metric: Sentiment Flag
    CASE WHEN reviews.review_score <= 2 THEN 'Negative'
         WHEN reviews.review_score = 3 THEN 'Neutral'
         ELSE 'Positive' 
    END AS sentiment

FROM reviews
LEFT JOIN orders ON reviews.order_id = orders.order_id