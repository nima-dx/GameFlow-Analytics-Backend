from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("move_and_transform").getOrCreate()

# Input path (bucket 1)
input_path = os.getenv("RAW_BUCKET")

# Output path (bucket 2)
output_path = os.getenv("PROCESSED_BUCKET")

# Read parquet
df = spark.read.parquet(input_path)

# Example transformation
df_transformed = df.dropDuplicates()

# Write parquet
df_transformed.write.mode("overwrite").parquet(output_path)
