from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,trim,current_timestamp,regexp_replace,when,explode,input_file_name,to_date
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
input_path="gs://gameflow-ingestion-raw/event-timeline*.json"

# Output path (bucket 2)
output_path = f"{os.getenv('PROCESSED_BUCKET')}/event_timeline"
output_path="gs://gameflow-ingestion-processed/event_timeline"

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
    .select(explode(col("lookup")).alias("timeline"))
    .select(
        trim(col("timeline.idTimeline")).cast("long").alias("timeline_id"),
        trim(col("timeline.idEvent")).cast("long").alias("event_id"),
        trim(col("timeline.idAPIfootball")).cast("long").alias("api_football_id"),
        trim(col("timeline.strTimeline")).alias("timeline_type"),
        trim(col("timeline.strTimelineDetail")).alias("timeline_detail"),
        trim(col("timeline.strHome")).alias("is_home"),
        trim(col("timeline.strEvent")).alias("event_name"),
        trim(col("timeline.idPlayer")).cast("long").alias("player_id"),
        trim(col("timeline.strPlayer")).alias("player_name"),
        trim(col("timeline.idAssist")).cast("long").alias("assist_id"),
        trim(col("timeline.strAssist")).alias("assist_name"),
        trim(col("timeline.intTime")).cast("int").alias("minute"),
        trim(col("timeline.strPeriod")).alias("period"),
        trim(col("timeline.idTeam")).cast("long").alias("team_id"),
        trim(col("timeline.strTeam")).alias("team_name"),
        trim(col("timeline.strComment")).alias("comment"),
        to_date(col("timeline.dateEvent"), "yyyy-MM-dd").alias("event_date"),
        trim(col("timeline.strSeason")).alias("season"),
        trim(col("timeline.strCutout")).alias("player_cutout"),
        input_file_name().alias("source_file"),
        current_timestamp().alias("ingested_at"),
    )
    .dropDuplicates()
    .filter(
        col("timeline_id").isNotNull() &
        col("event_id").isNotNull()
    )
)

print(f"Read from local file: {input_path}")
print(f"Wrote to bucket: {output_path}")

# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
