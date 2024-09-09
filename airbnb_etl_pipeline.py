import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit, isnull
import logging

# Initialize SparkSession
spark = SparkSession.builder.appName("NYC Airbnb ETL").getOrCreate()

# Paths
raw_data_path = "/raw"
processed_data_path = "/processed"
log_path = "/logs/processed_files_log.csv"

# Set up logging
logging.basicConfig(filename='/logs/etl_pipeline.log', level=logging.INFO, format='%(asctime)s %(message)s')

# Load the log of processed files
if os.path.exists(log_path):
    processed_files_log = pd.read_csv(log_path)
else:
    processed_files_log = pd.DataFrame(columns=["file_name"])


def is_new_file(file_name):
    return file_name not in processed_files_log['file_name'].values


# Set up streaming to monitor new CSV files in the raw folder
raw_data_stream = spark.readStream.option("header", "true").csv(raw_data_path)


# Function to ingest new files and process them
def process_stream_data(df, batch_id):
    try:
        logging.info(f"Starting batch {batch_id}")

        # Filter out rows where price is 0 or negative
        df = df.filter(col("price") > 0)

        # Convert 'last_review' to date type
        df = df.withColumn("last_review", to_date(col("last_review"), "yyyy-MM-dd"))

        # Fill missing last_review values with the earliest date in the dataset or a default value
        earliest_review = \
        df.select("last_review").filter(col("last_review").isNotNull()).orderBy("last_review").first()[0]
        df = df.fillna({"last_review": earliest_review})

        # Fill missing 'reviews_per_month' with 0
        df = df.fillna({"reviews_per_month": 0})

        # Drop rows with missing latitude or longitude
        df = df.dropna(subset=["latitude", "longitude"])

        # Add price category column
        df = df.withColumn("price_category", when(col("price") < 100, "budget")
                           .when((col("price") >= 100) & (col("price") < 300), "mid-range")
                           .otherwise("luxury"))

        # Add price_per_review column
        df = df.withColumn("price_per_review", col("price") / (col("number_of_reviews") + lit(1)))

        # Register the DataFrame as a temp SQL table for queries
        df.createOrReplaceTempView("airbnb_data")

        # SQL Query 1: Listings by neighbourhood_group
        neighbourhood_count = spark.sql("""
            SELECT neighbourhood_group, COUNT(*) AS listing_count
            FROM airbnb_data
            GROUP BY neighbourhood_group
            ORDER BY listing_count DESC
        """)

        # SQL Query 2: Top 10 most expensive listings
        top_expensive_listings = spark.sql("""
            SELECT id, name, price
            FROM airbnb_data
            ORDER BY price DESC
            LIMIT 10
        """)

        # SQL Query 3: Average price by room type grouped by neighbourhood_group
        avg_price_by_room_type = spark.sql("""
            SELECT neighbourhood_group, room_type, AVG(price) AS avg_price
            FROM airbnb_data
            GROUP BY neighbourhood_group, room_type
        """)

        # Show the SQL query results
        neighbourhood_count.show()
        top_expensive_listings.show()
        avg_price_by_room_type.show()

        # Repartition the data by neighbourhood_group and save as Parquet
        df.repartition("neighbourhood_group").write.partitionBy("neighbourhood_group").mode('append').parquet(
            processed_data_path)

        # After successful processing, update the log with the newly processed files
        new_file_log = pd.DataFrame([[f"batch_{batch_id}"]], columns=["file_name"])
        updated_log = pd.concat([processed_files_log, new_file_log])
        updated_log.to_csv(log_path, index=False)

        logging.info(f"Batch {batch_id} processed successfully")

    except Exception as e:
        logging.error(f"Error processing batch {batch_id}: {e}")


# Stream processing
query = raw_data_stream.writeStream.foreachBatch(process_stream_data).start()

# Await termination of the stream
query.awaitTermination()


# Data Quality Checks

def data_quality_checks(df):
    # Row Count Validation
    row_count = df.count()
    print(f"Row count: {row_count}")

    # Null Value Checks
    null_checks = df.filter(col("price").isNull() | col("minimum_nights").isNull() | col("availability_365").isNull())
    if null_checks.count() > 0:
        raise ValueError("Null values found in critical columns")

    print("Data quality checks passed")

