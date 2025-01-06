# this pipeline will trigger a single function whose task is to score new records
# think of this as offline bulk prediction

import functions_framework
from google.cloud import secretmanager
import duckdb
import requests
import json
import pandas as pd

# settings
project_id = 'group2-ba882'
secret_id = 'project_key'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'city_services_boston'
schema = "ml"
db_schema = f"{db}.{schema}"

# prediction service url
# QUESTION:  What would you do to avoid below?
EP = "https://us-central1-group2-ba882.cloudfunctions.net/mlops-caseduration-prediction"


@functions_framework.http
def task(request):

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # create the table for the predictions
    # this table is model specific
    raw_tbl_name = f"{db_schema}.caseduration_predictions"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        id VARCHAR
        ,length_pred FLOAT
        ,job_id VARCHAR
        ,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    md.sql(raw_tbl_sql)

    # get the records from the posts table that haven't been scored yet
    sql = """ 
    select id, title 
    from city_services_boston.stage.requests 
    where id not in (select id from city_services_boston.ml.caseduration_predictions)
    """
    df = md.sql(sql).df()

    if len(df) > 0:

        # get the array
        titles = {'data': df['title'].to_list()}

        # score the records
        resp = requests.post(EP, json=titles)
        resp.raise_for_status()

        # predictions
        preds = resp.json().get("predictions")

        # create the records
        df['length_pred'] = preds 
        df['created_at'] = pd.Timestamp.now()
        df['job_id'] = resp.json().get('model_info').get('job_id')

        # scored records for insert
        scored = df[['id', 'length_pred', 'job_id', 'created_at']]

        # insert into the table
        sql = f"INSERT INTO {raw_tbl_name} SELECT * from scored"
        print(sql)
        md.sql(sql)

        # return
        return {'num_records': len(preds)}, 200
    
    else:

        return{'num_records': 0}, 200