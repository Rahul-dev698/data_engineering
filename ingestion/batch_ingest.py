from pyspark.sql import SparkSession
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("BatchIngestion") \
    .getOrCreate()

# Define file paths
input_path = "/app/data/raw/E commerce.csv"
output_path = "/app/data/processed/ecommerce_cleaned.parquet"

# Load raw CSV data
df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

# Show schema and few rows (for debugging)
df.printSchema()
df.show(5)

# Save as Parquet format
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Ingestion complete. File saved to: {output_path}")

spark.stop()

