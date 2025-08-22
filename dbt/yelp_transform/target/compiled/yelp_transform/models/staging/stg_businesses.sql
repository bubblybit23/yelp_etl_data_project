

SELECT
    business_id,
    name,
    address,
    city,
    state,
    postal_code,
    latitude,
    longitude,
    stars as original_stars,
    review_count as original_review_count,
    is_open,
    categories
FROM yelp_raw.businesses