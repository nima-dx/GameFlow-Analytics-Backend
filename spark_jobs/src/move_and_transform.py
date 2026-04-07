from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os
from pathlib import Path

print("PROCESSED_BUCKET =", os.getenv("PROCESSED_BUCKET"))
print("GOOGLE_APPLICATION_CREDENTIALS =", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# spark = (
#     SparkSession.builder
#     .appName("move_and_transform")
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#     .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
#     .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
#     .getOrCreate()
# )

spark = (
    SparkSession.builder
    .appName("gcs_write_test")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .getOrCreate()
)

# Input path (bucket 1)
#input_path = "/app/spark_jobs/data/leagues.json"
#input_path=os.getenv("RAW_BUCKET")
input_path="gs://gameflow-ingestion-raw/leagues.json"

# Output path (bucket 2)
output_path = f"{os.getenv('PROCESSED_BUCKET')}/leagues_parquet"
output_path="gs://gameflow-ingestion-processed/leagues_parquet"

# Read parquet
#df = spark.read.parquet(input_path)
df = spark.read.option("multiline", "true").json(input_path)

# Example transformation
df_flat = (
    df
    .select(explode(col("leagues")).alias("league"))
    .select(
        col("league.idLeague").alias("league_id"),
        col("league.strLeague").alias("league_name"),
        col("league.strSport").alias("sport_name"),
        col("league.strLeagueAlternate").alias("league_alternate_name"),
    )
    .dropDuplicates()
)

print(f"Read from local file: {input_path}")
print(f"Wrote to bucket: {output_path}")

# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
