import os
import logging
from pyspark.sql import SparkSession

# Enterprise Standard: Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

# Enterprise Standard: Auto-download exact Kafka and AWS S3 drivers
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

def create_spark_session():
    """Initializes a Spark Session configured for MinIO (Local S3)."""
    logger.info("Initializing PySpark Session...")
    spark = SparkSession.builder \
        .appName("Bronze-Transaction-Ingestion") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def start_bronze_stream(spark):
    """Reads from Redpanda and streams raw JSON to MinIO."""
    logger.info("Connecting to Redpanda topic: live_transactions...")
    
    # 1. Read the stream from Kafka (UPDATED to Redpanda internal Docker DNS)
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "redpanda:9092") \
        .option("subscribe", "live_transactions") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Kafka stores data as binary. Cast the 'value' column to a String (JSON).
    raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload")

    logger.info("Starting stream to S3 (MinIO)...")
    
    # 3. Write the stream to S3 using the Parquet format
    query = raw_json_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://raw-transactions/data/") \
        .option("checkpointLocation", "s3a://raw-transactions/_checkpoints/") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    spark = create_spark_session()
    start_bronze_stream(spark)