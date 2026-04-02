#!/bin/bash

source .env

bq mk --location=EU --dataset $GCP_PROJECT_ID:$BQ_DATASET_RAW


# run in setup/ folder: bash create_dataset.sh
