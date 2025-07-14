from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkTransformation") \
    .getOrCreate()

# Input and output paths
input_path = "/app/data/processed/ecommerce_cleaned.parquet"
output_path = "/app/data/processed/category_sales_summary.parquet"

# Read cleaned Parquet data
df = spark.read.parquet(input_path)

# Show schema and sample data
df.printSchema()
df.show(5)

# Transformation: Aggregate total sales per product category
category_sales = df.groupBy("Product_Category").agg(
    sum(col("Sales")).alias("Total_Sales"),
    sum(col("Profit")).alias("Total_Profit")
)

# Show results
category_sales.show()

# Save result to new Parquet file
category_sales.write.mode("overwrite").parquet(output_path)

print(f"✅ Aggregated data written to: {output_path}")

# Stop Spark session
spark.stop()
