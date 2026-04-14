from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,trim,current_timestamp,regexp_replace,when,explode,to_timestamp,input_file_name,to_date
import os
from pathlib import Path
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]

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
input_path="gs://gameflow-ingestion-raw/seasons*.json"

# Output path (bucket 2)
output_path = f"{os.getenv('PROCESSED_BUCKET')}/seasons"
output_path="gs://gameflow-ingestion-processed/seasons"

# Read parquet
#df = spark.read.parquet(input_path)
df = spark.read.option("multiline", "true").json(input_path)

# Example transformation
# df_flat = (
#     df
#     .select(explode(col("leagues")).alias("league"))
#     .select(
#         col("league.idLeague").alias("league_id"),
#         col("league.strLeague").alias("league_name"),
#         col("league.strSport").alias("sport_name"),
#         col("league.strLeagueAlternate").alias("league_alternate_name"),
#     )
#     .dropDuplicates()
# )

df_flat = (
    df
    .select(explode(col("list")).alias("season"))
    .select(
        trim(col("season.strSeason")).alias("season_name"),
        trim(col("season.strPoster")).alias("poster_url"),
        trim(col("season.strBadge")).alias("badge_url"),
        trim(col("season.strDescriptionEN")).alias("description_en"),
        input_file_name().alias("source_file"),
        current_timestamp().alias("ingested_at"),
    )
    .dropDuplicates()
    .filter(col("season_name").isNotNull())
)



print(f"Read from local file: {input_path}")
print(f"Wrote to bucket: {output_path}")

# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
