export $(grep -v '^#' ../.env | xargs)
echo $GCP_PROJECT_ID                  # should print lwdataengineer
echo $GOOGLE_APPLICATION_CREDENTIALS  # should print the json path
dbt run --select most_leagues_in_sport
