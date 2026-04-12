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
input_path="gs://gameflow-ingestion-raw/events*.json"

# Output path (bucket 2)
output_path = f"{os.getenv('PROCESSED_BUCKET')}/events"
output_path="gs://gameflow-ingestion-processed/events"

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
    .select(explode(col("schedule")).alias("event"))
    .select(
        trim(col("event.idEvent")).cast("long").alias("event_id"),
        trim(col("event.strEvent")).alias("event_name"),
        trim(col("event.idLeague")).cast("long").alias("league_id"),
        trim(col("event.strLeague")).alias("league_name"),
        trim(col("event.strSport")).alias("sport_name"),
        trim(col("event.strHomeTeam")).alias("home_team_name"),
        trim(col("event.strAwayTeam")).alias("away_team_name"),
        trim(col("event.idHomeTeam")).cast("long").alias("home_team_id"),
        trim(col("event.idAwayTeam")).cast("long").alias("away_team_id"),
        trim(col("event.intRound")).cast("int").alias("round_number"),
        trim(col("event.intHomeScore")).cast("int").alias("home_score"),
        trim(col("event.intAwayScore")).cast("int").alias("away_score"),
        to_timestamp(col("event.strTimestamp"), "yyyy-MM-dd'T'HH:mm:ss").alias("event_timestamp_utc"),
        to_date(col("event.dateEvent"), "yyyy-MM-dd").alias("event_date"),
        to_date(col("event.dateEventLocal"), "yyyy-MM-dd").alias("event_date_local"),
        trim(col("event.strTime")).alias("event_time_utc"),
        trim(col("event.strTimeLocal")).alias("event_time_local"),
        trim(col("event.strVenue")).alias("venue_name"),
        trim(col("event.strCountry")).alias("country_name"),
        trim(col("event.strThumb")).alias("thumb_url"),
        trim(col("event.strPoster")).alias("poster_url"),
        trim(col("event.strVideo")).alias("video_url"),
        when(trim(col("event.strPostponed")) == "yes", True)
            .otherwise(False)
            .alias("is_postponed"),
        trim(col("event.strFilename")).alias("source_filename"),
        trim(col("event.strStatus")).alias("match_status"),
        input_file_name().alias("source_file"),
        current_timestamp().alias("ingested_at"),
    )
    .dropDuplicates()
    .filter(col("event_id").isNotNull())
)

print(f"Read from local file: {input_path}")
print(f"Wrote to bucket: {output_path}")

# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
