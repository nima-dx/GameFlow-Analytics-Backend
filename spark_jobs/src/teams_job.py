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
input_path="gs://gameflow-ingestion-raw/teams*.json"

# Output path (bucket 2)
output_path = f"{os.getenv('PROCESSED_BUCKET')}/teams"
output_path="gs://gameflow-ingestion-processed/teams"

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
    .select(explode(col("list")).alias("team"))
    .select(
        trim(col("team.idTeam")).cast("long").alias("team_id"),
        trim(col("team.strTeam")).alias("team_name"),
        trim(col("team.strTeamShort")).alias("team_short_name"),
        trim(col("team.strColour1")).alias("color_1"),
        trim(col("team.strColour2")).alias("color_2"),
        trim(col("team.strColour3")).alias("color_3"),
        trim(col("team.idLeague")).cast("long").alias("league_id"),
        trim(col("team.strLeague")).alias("league_name"),
        trim(col("team.strBadge")).alias("badge_url"),
        trim(col("team.strLogo")).alias("logo_url"),
        trim(col("team.strBanner")).alias("banner_url"),
        trim(col("team.strFanart1")).alias("fanart_1_url"),
        trim(col("team.strEquipment")).alias("equipment_url"),
        trim(col("team.strCountry")).alias("country_name"),
        input_file_name().alias("source_file"),
        current_timestamp().alias("ingested_at"),
    )
    .dropDuplicates()
    .filter(col("team_id").isNotNull())
)

print(f"Read from local file: {input_path}")
print(f"Wrote to bucket: {output_path}")

# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
