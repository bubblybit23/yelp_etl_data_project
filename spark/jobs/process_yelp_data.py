from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp
from pyspark.sql.types import StringType, FloatType, IntegerType

def main():
    # Initialize Spark Session with Hive support enabled.
    # Spark will use its default Derby metastore, stored in the container's ./metastore_db.
    spark = SparkSession.builder \
        .appName("YelpDataProcessing") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(">>> Starting processing of Yelp datasets...")

    # Create the database
    spark.sql("CREATE DATABASE IF NOT EXISTS yelp_raw")

    # Process and write Business data
    print(">>> Processing business data...")
    business_df = spark.read.json("/data/raw/yelp_academic_dataset_business.json")
    business_processed_df = business_df.select(
        col("business_id").cast(StringType()),
        col("name").cast(StringType()),
        col("address").cast(StringType()),
        col("city").cast(StringType()),
        col("state").cast(StringType()),
        col("postal_code").cast(StringType()),
        col("latitude").cast(FloatType()),
        col("longitude").cast(FloatType()),
        col("stars").cast(FloatType()),
        col("review_count").cast(IntegerType()),
        col("is_open").cast(IntegerType()),
        col("categories").cast(StringType())
    )
    business_processed_df.write \
        .format("orc") \
        .mode("overwrite") \
        .saveAsTable("yelp_raw.businesses")

    # Process and write Review data
    print(">>> Processing review data...")
    review_df = spark.read.json("/data/raw/yelp_academic_dataset_review.json")
    review_processed_df = review_df.select(
        col("review_id").cast(StringType()),
        col("user_id").cast(StringType()),
        col("business_id").cast(StringType()),
        col("stars").cast(FloatType()),
        col("useful").cast(IntegerType()),
        col("funny").cast(IntegerType()),
        col("cool").cast(IntegerType()),
        col("text").cast(StringType()),
        to_timestamp(col("date")).alias("review_date")
    ).withColumn("review_year", year("review_date"))
    
    review_processed_df.write \
        .format("orc") \
        .mode("overwrite") \
        .partitionBy("review_year") \
        .saveAsTable("yelp_raw.reviews")

    print(">>> Data processing completed successfully!")
    print(">>> Sample from yelp_raw.businesses:")
    spark.sql("SELECT business_id, name, city, stars FROM yelp_raw.businesses LIMIT 5").show(truncate=False)
    print(">>> Sample from yelp_raw.reviews:")
    spark.sql("SELECT review_id, business_id, stars, review_year FROM yelp_raw.reviews LIMIT 5").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()