

WITH business_stats AS (
    SELECT
        b.business_id,
        b.name,
        b.city,
        b.state,
        b.avg_rating,
        b.total_reviews,
        COUNT(DISTINCT r.user_id) as unique_customers,
        AVG(r.stars) as actual_avg_rating
    FROM yelp_transform.stg_businesses b
    JOIN yelp_transform.stg_reviews r ON b.business_id = r.business_id
    WHERE b.total_reviews >= 10  -- Minimum review threshold
    GROUP BY 1,2,3,4,5,6
)

SELECT
    business_id,
    name,
    city,
    state,
    avg_rating,
    total_reviews,
    unique_customers,
    actual_avg_rating,
    CASE
        WHEN avg_rating >= 4.5 THEN 'Elite'
        WHEN avg_rating >= 4.0 THEN 'Excellent'
        WHEN avg_rating >= 3.5 THEN 'Good'
        WHEN avg_rating >= 3.0 THEN 'Average'
        ELSE 'Below Average'
    END as rating_category
FROM business_stats
ORDER BY avg_rating DESC, total_reviews DESC