SHELL := /bin/bash

.PHONY: create-dataset load-leagues dbt-run-most-leagues

create-dataset-raw:
	@source .env && bq mk --location=$$GCP_LOCATION --dataset $$GCP_PROJECT_ID:$$BQ_DATASET_RAW

create-dataset-analytics:
	@source .env && bq mk --location=$$GCP_LOCATION --dataset $$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS

load-leagues:
	@source .env && bq load \
		--autodetect \
		--source_format=PARQUET \
		--location=$$GCP_LOCATION \
		$$GCP_PROJECT_ID:$$BQ_DATASET_RAW.leagues \
		dbt_gameflow/data/leagues.parquet

load-grandprix:
	@source .env && bq load \
		--autodetect \
		--source_format=PARQUET \
		--location=$$GCP_LOCATION \
		$$GCP_PROJECT_ID:$$BQ_DATASET_RAW.grandprix \
		dbt_gameflow/data/f1_season_2026.parquet

dbt-run-most-leagues:
	@source .env && dbt run --select most_leagues_in_sport --project-dir /home/arthurdeetu/code/joaquin-ortega84/GameFlow-Analytics-Backend/dbt_gameflow/
