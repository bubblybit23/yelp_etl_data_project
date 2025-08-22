{{ config(materialized='table') }}

SELECT
    review_id,
    user_id,
    business_id,
    stars,
    date,
    text,
    useful,
    funny,
    cool,
    YEAR(date) as review_year,
    MONTH(date) as review_month
FROM {{ source('yelp_transform','stg_reviews') }}