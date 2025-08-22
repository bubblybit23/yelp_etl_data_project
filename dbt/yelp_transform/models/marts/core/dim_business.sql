{{ config(materialized='table') }}

WITH business_reviews AS (
    SELECT
        business_id,
        COUNT(*) as total_reviews,
        AVG(stars) as avg_rating
    FROM {{ source('yelp_transform','stg_reviews') }}
    GROUP BY 1
)

SELECT
    b.business_id,
    b.name,
    b.address,
    b.city,
    b.state,
    b.postal_code,
    b.latitude,
    b.longitude,
    b.original_stars,
    b.original_review_count,
    b.is_open,
    b.categories,
    COALESCE(r.total_reviews, 0) as total_reviews,
    ROUND(COALESCE(r.avg_rating, 0), 2) as avg_rating
FROM {{ source('yelp_transform','stg_businesses') }} b
LEFT JOIN business_reviews r ON b.business_id = r.business_id