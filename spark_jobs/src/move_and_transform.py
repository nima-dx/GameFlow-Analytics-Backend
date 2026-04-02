from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import os

spark = SparkSession.builder.appName("move_and_transform").getOrCreate()

# Input path (bucket 1)
input_path = "data/leagues.json" #os.getenv("RAW_BUCKET")

# Output path (bucket 2)
output_path =  "data/output/leagues_parquet" #os.getenv("PROCESSED_BUCKET")

# Read parquet
#df = spark.read.parquet(input_path)
df = spark.read.option("multiline", "true").json(input_path)

# Example transformation
df_flat = (
    df
    .select(explode(col("all")).alias("league"))
    .select(
        col("league.idLeague").alias("league_id"),
        col("league.strLeague").alias("league_name"),
        col("league.strSport").alias("sport_name"),
        col("league.strLeagueAlternate").alias("league_alternate_name"),
    )
    .dropDuplicates()
)


# Write parquet
df_flat.write.mode("overwrite").parquet(output_path)
