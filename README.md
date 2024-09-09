# Airbnb ETL Pipeline

## Overview
This PySpark ETL pipeline automates the process of extracting, transforming, and loading Airbnb data from CSV files. The job is designed to handle incremental processing of new CSV files, clean and transform the data, run SQL queries, and save the processed data in partitioned Parquet format.

## Prerequisites
- **Apache Spark 3.x** and **PySpark**
- **Python 3.x**
- A directory structure to store raw and processed data:
 ├── raw/ ├── processed/
- CSV file: `AB_NYC_2019.csv` (place this in the `raw/` directory)

## How to Run

1. **Place Raw CSV Files:**
 - Add your raw CSV files (including `AB_NYC_2019.csv`) to the `raw/` directory.

2. **Run the PySpark Job:**
 - Execute the `airbnb_etl_pipeline.py` script. This will:
   - Ingest new CSV files in the `raw/` directory.
   - Track processed files to avoid duplicate processing.
   - Clean and transform the data using PySpark DataFrames.
   - Run SQL queries using PySpark SQL.
   - Save the transformed data as Parquet files in the `processed/` directory, partitioned by `neighbourhood_group`.

3. **Log of Processed Files:**
 - The script tracks processed files in a `processed_files_log.txt` file to avoid reprocessing files that have already been ingested.

## Configuration
- **File paths:** You can change the `raw_dir` and `processed_dir` in the script to point to your custom directories.
- **Streaming Micro-Batches:** The script uses Structured Streaming to handle micro-batches. Adjust the trigger time in `spark.readStream` if necessary.

## Data Quality Checks
- The job performs the following validation checks:
- Row count validation: Ensures the number of records matches the expected number.
- Null value checks: Ensures no NULL values are present in critical columns like `price`, `minimum_nights`, and `availability_365`.

## SQL Queries
The script runs three SQL queries on the cleaned data:
1. **Listings by Neighborhood Group:** Counts listings per `neighbourhood_group`, sorted by the number of listings.
2. **Top 10 Most Expensive Listings:** Finds the 10 most expensive listings based on the `price` column.
3. **Average Price by Room Type:** Calculates the average price per room type (`room_type`), grouped by `neighbourhood_group`.

## Output
- The processed and transformed data is saved as partitioned Parquet files in the `/processed` directory, partitioned by `neighbourhood_group`.

## Error Handling and Logging
- Errors during data ingestion, transformation, or saving will be logged using PySpark's built-in logging mechanism.
- In case of issues like missing or corrupted files, the script will stop, and errors will be logged for troubleshooting.

## Monitoring
- Use Spark’s Web UI to monitor job execution, including task progress, stages, and data lineage.

## Refactoring
- The code can be refactored for better optimization and scalability. Some refactor suggestions:
- Introduce parallel processing for larger datasets.
- Use partitioning strategies for more efficient query execution.

## Notes
- This script is designed to be run periodically or in streaming mode to handle new data incrementally.
- You can test the pipeline by splitting the original CSV file into smaller chunks and adding them to the raw folder for processing.

