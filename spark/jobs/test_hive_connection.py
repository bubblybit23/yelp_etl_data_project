from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("YelpDataProcessing") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("Testing Hive connection...")
    
    try:
        # Test basic Hive operation
        databases = spark.sql("SHOW DATABASES").collect()
        print("✅ Available databases:")
        for db in databases:
            print(f"   - {db['namespace']}")
        
        # Refresh the table metadata first
        print("\n🔄 Refreshing table metadata...")
        try:
            spark.sql("REFRESH TABLE yelp_raw.reviews")
            print("✅ Refreshed yelp_raw.reviews metadata")
        except Exception as refresh_error:
            print(f"⚠️  Could not refresh reviews table: {refresh_error}")
        
        try:
            spark.sql("REFRESH TABLE yelp_raw.businesses")
            print("✅ Refreshed yelp_raw.businesses metadata")
        except Exception as refresh_error:
            print(f"⚠️  Could not refresh businesses table: {refresh_error}")
        
        # Now try to count
        print("\n🔢 Counting records after refresh...")
        try:
            print(f"✅ yelp_raw.reviews count:")
            spark.sql("SELECT COUNT(*) FROM yelp_raw.reviews").show(truncate=False)
        except Exception as count_error:
            print(f"❌ Still cannot count reviews: {count_error}")
            print("Trying alternative approach...")
            
            # Try a different approach - use spark.sql directly
            result = spark.sql("SELECT COUNT(*) as count FROM yelp_raw.reviews")
            result.show()
            
    except Exception as e:
        print(f"❌ Hive test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()