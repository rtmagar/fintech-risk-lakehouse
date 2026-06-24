import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Enterprise Standard: Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

# Enterprise Standard: Inject Iceberg & AWS dependencies
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

def create_iceberg_spark_session():
    """Initializes Spark with Apache Iceberg Catalog configurations for MinIO."""
    logger.info("Initializing PySpark Session with Apache Iceberg...")
    spark = SparkSession.builder \
        .appName("Silver-Transaction-Cleansing") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://cleansed-transactions/") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_silver_layer(spark):
    """Reads Bronze data, cleans it, and writes to Silver Iceberg tables."""
    logger.info("Reading raw bronze data from S3...")
    
    try:
        # 1. Read the raw Parquet data
        bronze_df = spark.read.format("parquet").load("s3a://raw-transactions/data/")
        
        initial_count = bronze_df.count()
        logger.info(f"Loaded {initial_count} raw records.")

        # 2. Enterprise Schema Definition
        # We must explicitly tell Spark how to unpack the raw JSON string
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("ip_address", StringType(), True)
        ])

        # 3. Unpack and Cleanse
        logger.info("Unpacking JSON and applying data quality rules...")
        silver_df = bronze_df \
            .withColumn("parsed_data", from_json(col("json_payload"), transaction_schema)) \
            .select("parsed_data.*") \
            .dropDuplicates(["transaction_id"]) \
            .filter(col("amount") > 0) \
            .filter(col("currency").isNotNull())

        final_count = silver_df.count()
        logger.info(f"Retained {final_count} clean records after processing.")

        # 4. Write to Iceberg
        logger.info("Writing to Silver layer using Apache Iceberg format...")
        silver_df.writeTo("local.db.transactions") \
            .using("iceberg") \
            .createOrReplace()
            
        logger.info("Silver pipeline execution completed successfully!")
        
    except Exception as e:
        logger.error(f"Silver pipeline failed: {str(e)}")

if __name__ == "__main__":
    spark = create_iceberg_spark_session()
    process_silver_layer(spark)