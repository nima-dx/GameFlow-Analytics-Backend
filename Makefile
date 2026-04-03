.PHONY: create-dataset load-leagues dbt-run

create-dataset:
	@source .env && bq mk --location=EU --dataset $$GCP_PROJECT_ID:$$BQ_DATASET_RAW

load-leagues:
	@source .env && bq load \
		--autodetect \
		--source_format=PARQUET \
		--location=EU \
		$$GCP_PROJECT_ID:$$BQ_DATASET_RAW.leagues \
		dbt_gameflow/data/leagues.parquet

dbt-run-most-leagues:
	@dbt run --select most_leagues_in_sport
