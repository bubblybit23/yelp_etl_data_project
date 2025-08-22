from pyspark.sql import SparkSession

def create_spark_session():
    """Create Spark session with proper Hive configuration"""
    return SparkSession.builder \
        .appName("YelpDataProcessing") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def create_staging_views(spark):
    """Create staging views from raw tables"""
    print(">>> Creating staging views in yelp_transform database...")
    
    # Create database and views
    spark.sql("CREATE DATABASE IF NOT EXISTS yelp_transform")
    spark.sql("USE yelp_transform")
    
    # Create staging business view
    spark.sql("""
    CREATE OR REPLACE VIEW stg_businesses AS 
    SELECT 
        business_id,
        name,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        stars,
        review_count,
        is_open,
        categories
    FROM yelp_raw.businesses
    """)
    
    # Create staging reviews view
    spark.sql("""
    CREATE OR REPLACE VIEW stg_reviews AS
    SELECT
        review_id,
        user_id,
        business_id,
        stars,
        useful,
        funny,
        cool,
        text,
        review_date,
        review_year
    FROM yelp_raw.reviews
    """)
    
    print("✅ Staging views created successfully!")

def verify_staging_layer(spark):
    """Verify staging layer was created correctly"""
    print(">>> Verifying staging layer...")
    
    # Check views exist
    views = spark.sql("SHOW VIEWS IN yelp_transform").collect()
    view_names = [row['viewName'] for row in views]
    
    if 'stg_businesses' in view_names and 'stg_reviews' in view_names:
        print("✅ Both staging views exist")
        
        # Count records
        biz_count = spark.sql("SELECT COUNT(*) FROM stg_businesses")
        rev_count = spark.sql("SELECT COUNT(*) FROM stg_reviews")
        
        print(f"✅ stg_businesses count: ")
        biz_count.show(truncate=False)
        print(f"✅ stg_reviews count: ")
        rev_count.show(truncate=False)

        return True
    else:
        print("❌ Staging views missing")
        return False

def main():
    """Main function to create staging layer"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_staging_views(spark)
        success = verify_staging_layer(spark)
        
        if success:
            print(">>> Staging layer creation completed successfully!")
        else:
            print(">>> Staging layer creation completed with issues!")
            
    except Exception as e:
        print(f"❌ Error creating staging layer: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()