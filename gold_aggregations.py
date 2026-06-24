import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell'

def create_gold_session():
    spark = SparkSession.builder \
        .appName("Gold-Risk-Aggregations") \
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

def process_gold_layer(spark):
    logger.info("Connecting to certified Silver Iceberg Table...")
    
    df = spark.table("local.db.transactions")
    df.createOrReplaceTempView("transactions")

    logger.info("Calculating Risk Metrics (Phase 4)...")
    
    # Calculate total transaction volume and distinct merchants per user
    gold_df = spark.sql("""
        SELECT 
            user_id,
            COUNT(transaction_id) as total_transactions,
            COUNT(DISTINCT merchant_id) as unique_merchants_visited,
            ROUND(SUM(amount), 2) as total_volume_spent,
            currency
        FROM transactions
        GROUP BY user_id, currency
    """)
    
    logger.info("Writing Aggregated Risk Data to Gold S3 Bucket...")
    
    # Write the final aggregated report to the Gold bucket
    gold_df.write \
        .mode("overwrite") \
        .parquet("s3a://risk-aggregates/hourly_user_risk_metrics/")
        
    logger.info("Gold pipeline execution completed successfully!")
    
    # Show a quick preview in the terminal
    gold_df.orderBy("total_volume_spent", ascending=False).show(5, truncate=False)

if __name__ == "__main__":
    spark = create_gold_session()
    process_gold_layer(spark)