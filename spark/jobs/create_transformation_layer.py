from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

def create_spark_session():
    """Create Spark session with Hive support"""
    return SparkSession.builder \
        .appName("YelpDataProcessing") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

def create_dim_business(spark):
    """Create dimension business table"""
    print(">>> Creating dim_business table...")
    
    # Calculate review metrics
    business_reviews_df = spark.table("yelp_transform.stg_reviews") \
        .groupBy("business_id") \
        .agg(
            count("*").alias("total_reviews"),
            avg("stars").alias("avg_rating")
        )
    
    # Join with business data
    stg_businesses_df = spark.table("yelp_transform.stg_businesses")
    
    dim_business_df = stg_businesses_df \
        .join(business_reviews_df, "business_id", "left") \
        .select(
            col("business_id"),
            col("name"),
            col("address"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("latitude"),
            col("longitude"),
            col("stars").alias("original_stars"),
            col("review_count").alias("original_review_count"),
            col("is_open"),
            col("categories"),
            col("total_reviews"),
            round(col("avg_rating"), 2).alias("avg_rating")
        )
    
    # Write to table
    dim_business_df.write \
        .mode("overwrite") \
        .saveAsTable("yelp_marts.dim_business")
    
    return dim_business_df

def create_fact_reviews_simple(spark):
    """Simple optimized approach for 6.9M records"""
    print(">>> Creating fact_reviews table...")
    
    fact_reviews_df = spark.table("yelp_transform.stg_reviews")
    
    # For 6.9M records, we can handle this with proper partitioning
    # Use about 100 partitions (about 69K records per partition)
    fact_reviews_df = fact_reviews_df.repartition(100)
    
    # Write with optimizations
    fact_reviews_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .option("parquet.block.size", 128 * 1024 * 1024) \
        .saveAsTable("yelp_marts.fact_reviews") # 128 mb block
    
    # Verify count
    result_count = spark.sql("SELECT COUNT(*) FROM yelp_marts.fact_reviews").collect()[0][0]
    print(f"Written {result_count:,} records to fact_reviews")
    
    return fact_reviews_df

def run_data_quality_checks(spark):
    """Run data quality checks on transformed tables"""
    print(">>> Running data quality checks...")
    
    checks = []
    
    try:
        # Check dim_business
        dim_count = spark.sql("SELECT COUNT(*) FROM yelp_marts.dim_business").collect()[0][0]
        print(f"✅ dim_business count: {dim_count:,}")
        checks.append(dim_count > 0)
        
        # Check fact_reviews
        fact_count = spark.sql("SELECT COUNT(*) FROM yelp_marts.fact_reviews").collect()[0][0]
        print(f"✅ fact_reviews count: {fact_count:,}")
        checks.append(fact_count > 0)
        
        # Check null business_ids
        null_biz = spark.sql("SELECT COUNT(*) FROM yelp_marts.dim_business WHERE business_id IS NULL").collect()[0][0]
        print(f"✅ Null business_id count: {null_biz}")
        checks.append(null_biz == 0)
        
        return all(checks)
    except Exception as e:
        print(f"❌ Quality checks failed: {e}")
        return False

def main():
    """Main function to create transformation layer"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Create the marts database if it doesn't exist
        spark.sql("CREATE DATABASE IF NOT EXISTS yelp_marts")
        
        # Create mart tables
        create_dim_business(spark)
        create_fact_reviews_simple(spark)
        
        # Run quality checks
        quality_passed = run_data_quality_checks(spark)
        
        if quality_passed:
            print(">>> Transformation layer creation completed successfully!")
            
            # Show sample data
            print(">>> Sample from dim_business:")
            spark.sql("""
                SELECT business_id, name, city, state, avg_rating 
                FROM yelp_marts.dim_business
                LIMIT 5
            """).show(truncate=False)
            
        else:
            print(">>> Transformation layer creation completed with quality issues!")
            exit(1)
            
    except Exception as e:
        print(f"❌ Error creating transformation layer: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()