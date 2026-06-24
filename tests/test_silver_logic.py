import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

# 1. Enterprise Standard: Use a pytest fixture to spin up a tiny, fast Spark session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-pyspark-local") \
        .master("local[1]") \
        .getOrCreate()

def test_silver_cleansing_logic(spark):
    """
    Business Requirement: The Silver layer MUST drop duplicate transaction_ids
    and MUST filter out any transaction where the amount is <= 0.
    """
    
    # 1. Arrange: Create mock data with intentional errors (duplicates and negative amounts)
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])
    
    mock_data = [
        ("txn-123", 100.50, "USD"),  # Valid
        ("txn-123", 100.50, "USD"),  # INVALID: Duplicate ID
        ("txn-456", -50.00, "EUR"),  # INVALID: Negative amount
        ("txn-789", 0.00, "GBP"),    # INVALID: Zero amount
        ("txn-999", 250.00, "NPR")   # Valid
    ]
    
    df = spark.createDataFrame(mock_data, schema)
    
    # 2. Act: Apply the exact transformation logic used in our silver_cleansing.py
    clean_df = df \
        .dropDuplicates(["transaction_id"]) \
        .filter(col("amount") > 0) \
        .filter(col("currency").isNotNull())
        
    results = clean_df.collect()
    
    # 3. Assert: Prove the logic worked mathematically
    assert len(results) == 2, f"Expected 2 rows, but got {len(results)}"
    
    valid_ids = [row["transaction_id"] for row in results]
    assert "txn-123" in valid_ids
    assert "txn-999" in valid_ids
    assert "txn-456" not in valid_ids # Proves negative amounts are dropped