import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Enterprise Standard: Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

# Inject Iceberg & AWS dependencies to read the Silver table
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

def create_qc_session():
    """Initializes Spark to read the Iceberg Data Lake for Quality Control."""
    spark = SparkSession.builder \
        .appName("Data-Quality-Gate") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://cleansed-transactions/") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def run_quality_suite(spark):
    logger.info("Starting Data Quality Gate on Silver Layer...")
    
    # 1. Load the Silver Iceberg Table
    try:
        df = spark.table("local.db.transactions")
        total_records = df.count()
        logger.info(f"Scanning {total_records} records for anomalies.")
    except Exception as e:
        logger.error("Failed to load Silver table. Does it exist?")
        sys.exit(1)

    # 2. Define the exact Phase 4 Assertions
    valid_currencies = ['USD', 'EUR', 'GBP', 'NPR', 'JPY']
    
    # Assertion A: amount is always greater than zero
    failed_amounts = df.filter(col("amount") <= 0).count()
    
    # Assertion B: currency is within a recognized list
    failed_currencies = df.filter(~col("currency").isin(valid_currencies)).count()
    
    # Assertion C: transaction_id is never null
    failed_ids = df.filter(col("transaction_id").isNull()).count()

    # 3. Evaluate the Suite
    logger.info("--- Quality Gate Results ---")
    logger.info(f"Amount > 0 Check: {failed_amounts} violations")
    logger.info(f"Valid Currency Check: {failed_currencies} violations")
    logger.info(f"Null Transaction ID Check: {failed_ids} violations")

    if failed_amounts > 0 or failed_currencies > 0 or failed_ids > 0:
        logger.error("CRITICAL: Data Quality Gate FAILED. Halting pipeline.")
        sys.exit(1) # This tells Airflow the task failed
    else:
        logger.info("SUCCESS: All data quality checks passed. Safe to proceed to Gold Layer.")
        sys.exit(0) # This tells Airflow the task succeeded

if __name__ == "__main__":
    spark = create_qc_session()
    run_quality_suite(spark)