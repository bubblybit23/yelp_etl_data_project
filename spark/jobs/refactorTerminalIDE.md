# Create the analytics layer with compatible SQL syntax
>> docker exec spark-master /opt/bitnami/spark/bin/spark-sql -e "
>> -- Create target database for transformed data
>> CREATE DATABASE IF NOT EXISTS yelp_transform;
>>
>> -- Create staging layer (views)
>> USE yelp_transform;
>>
>> CREATE OR REPLACE VIEW stg_businesses AS
>> SELECT
>>     business_id,
>>     name,
>>     address,
>>     city,
>>     state,
>>     postal_code,
>>     latitude,
>>     longitude,
>>     stars,
>>     review_count,
>>     is_open,
>>     categories
>> FROM yelp_raw.businesses;
>>
>> CREATE OR REPLACE VIEW stg_reviews AS
>> SELECT
>>     review_id,
>>     user_id,
>>     business_id,
>>     stars,
>>     useful,
>>     funny,
>>     cool,
>>     text,
>>     review_date,
>>     review_year
>> FROM yelp_raw.reviews;
>>
>> -- Create mart layer (tables) - using DROP IF EXISTS + CREATE approach
>> DROP TABLE IF EXISTS dim_business;
>> CREATE TABLE dim_business AS
>> WITH business_reviews AS (
>>     SELECT
>>         business_id,
>>         COUNT(*) as total_reviews,
>>         AVG(stars) as avg_rating
>>     FROM stg_reviews
>>     GROUP BY business_id
>> )
>> SELECT
>>     b.business_id,
>>     b.name,
>>     b.address,
>>     b.city,
>>     b.state,
>>     b.postal_code,
>>     b.latitude,
>>     b.longitude,
>>     b.stars as original_stars,
>>     b.review_count as original_review_count,
>>     b.is_open,
>>     b.categories,
>>     r.total_reviews,
>>     ROUND(r.avg_rating, 2) as avg_rating
>> FROM stg_businesses b
>> LEFT JOIN business_reviews r ON b.business_id = r.business_id;
>>
>> DROP TABLE IF EXISTS fact_reviews;
>> CREATE TABLE fact_reviews AS
>> SELECT
>>     review_id,
>>     user_id,
>>     business_id,
>>     stars,
>>     useful,
>>     funny,
>>     cool,
>>     text,
>>     review_date,
>>     review_year
>> FROM stg_reviews;
>>
>> -- Verify the transformation pipeline
>> SHOW DATABASES;
>> USE yelp_transform;
>> SHOW TABLES;
>> SELECT 'stg_businesses count:', COUNT(*) FROM stg_businesses;
>> SELECT 'stg_reviews count:', COUNT(*) FROM stg_reviews;
>> SELECT 'dim_business count:', COUNT(*) FROM dim_business;
>> SELECT 'fact_reviews count:', COUNT(*) FROM fact_reviews;
>>
>> -- Data quality tests
>> SELECT 'TEST: Null business_id in dim_business', COUNT(*) as null_count
>> FROM dim_business WHERE business_id IS NULL;
>>
>> SELECT 'TEST: Null review_id in fact_reviews', COUNT(*) as null_count
>> FROM fact_reviews WHERE review_id IS NULL;
>>
>> SELECT 'TEST: dim_business unique business_ids',
>> COUNT(*) as total,
>> COUNT(DISTINCT business_id) as unique_count,
>> CASE WHEN COUNT(*) = COUNT(DISTINCT business_id) THEN 'PASS' ELSE 'FAIL' END as status
>> FROM dim_business;
>>
>> SELECT 'TEST: fact_reviews unique review_ids',
>> COUNT(*) as total,
>> COUNT(DISTINCT review_id) as unique_count,
>> CASE WHEN COUNT(*) = COUNT(DISTINCT review_id) THEN 'PASS' ELSE 'FAIL' END as status
>> FROM fact_reviews;
>> " 2>&1