{{ config(materialized='view') }}

SELECT
    review_id,
    user_id,
    business_id,
    stars,
    date,
    text,
    useful,
    funny,
    cool
FROM {{ source('yelp_raw', 'reviews') }}