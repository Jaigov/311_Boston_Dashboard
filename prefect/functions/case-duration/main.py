# create/update a view to associate case duration with features for a regression task

import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import aiplatform
import duckdb
import pandas as pd
import datetime

# settings
project_id = 'group2-ba882'
project_region = 'us-central1'
secret_id = 'project_key'
version_id = 'latest'
bucket_name = 'group2-ba882-project'
ml_bucket_name = 'group2-ba882-vertex-models'
ml_dataset_path = '/training-data/case-duration/'

# db setup
db = 'city_services_boston'
stage_db_schema = f"{db}.stage"
ml_schema = f"{db}.ml"
ml_view_name = "case_duration"

############################################################### helpers

## define the SQL
ml_view_sql = f"""
CREATE OR REPLACE VIEW {ml_schema}.{ml_view_name} 
AS
SELECT 
    case_id,
    EXTRACT(EPOCH FROM (closed_dt - open_dt)) / 3600 AS duration_hours,
    EXTRACT(YEAR FROM open_dt) AS open_year,
    EXTRACT(MONTH FROM open_dt) AS open_month,
    EXTRACT(DAY FROM open_dt) AS open_day,
    EXTRACT(HOUR FROM open_dt) AS open_hour,
    fire_district,
    pwd_district,
    city_council_district,
    police_district,
    neighborhood,
    CURRENT_TIMESTAMP AS created_at
FROM {stage_db_schema}.cases
WHERE closed_dt IS NOT NULL AND open_dt IS NOT NULL;
"""

############################################################### main task

@functions_framework.http
def task(request):
    # No data will be passed in through the request

    # Instantiate services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Connect to Motherduck, the cloud data warehouse
    print("connecting to Motherduck")
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # Create the view
    print("creating the schema if it doesn't exist and creating/updating the view")
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {ml_schema};")
    md.sql(ml_view_sql)

    # Grab the view as a pandas dataframe with features and the target variable
    df = md.sql(f"SELECT case_id, duration_hours, open_year, open_month, open_day, open_hour, fire_district, pwd_district, city_council_district, police_district, neighborhood FROM {ml_schema}.{ml_view_name};").df()

    # Write the dataset to the training dataset path on GCS
    print("writing the csv file to gcs")
    dataset_path = "gcs://" + ml_bucket_name + ml_dataset_path + "case-duration.csv"
    df.to_csv(dataset_path, index=False)

    return {"dataset_path": dataset_path}, 200
