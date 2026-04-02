
source .env

bq load --autodetect \
    --source_format=PARQUET \
    --location=EU \
    $GCP_PROJECT_ID:$BQ_DATASET_RAW.leagues \
    dbt_gameflow/data/leagues.parquet
