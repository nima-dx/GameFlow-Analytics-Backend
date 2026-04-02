

# start by creating a bucket with the gcloud CLI, remember that bucket names must be globally unique,


gcloud storage buckets create gs://gameflow-ingestion-raw \
    --location=EU \
    --uniform-bucket-level-access

# Create a service account that we’ll use for this challenge:

gcloud iam service-accounts create svc-acc-gameflow-etl \
    --display-name="GameFlow analytics Data Pipeline"

# To see all your service accounts:

gcloud iam service-accounts list

# Now you need to add some permissions:
# Bucket Level Storage Role for GCS: roles/storage.objectAdmin

gcloud storage buckets add-iam-policy-binding gs://gameflow-ingestion-raw \
    --member="serviceAccount:svc-acc-gameflow-etl@$le-wagon-data-atelier.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# And finally, download a service account key for this service account onto your VM:

gcloud iam service-accounts keys create ~/.gcp_keys/svc-acc-gameflow-etl.json \
--iam-account="svc-acc-gameflow-etl@le-wagon-data-atelier.iam.gserviceaccount.com" \
--verbosity=debug
