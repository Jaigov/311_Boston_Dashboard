# imports
import functions_framework
import joblib
import json
from gcsfs import GCSFileSystem
import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
import duckdb


# db setup
db = 'city_services_boston'
schema = "mlops"
db_schema = f"{db}.{schema}"

# settings
project_id = 'group2-ba882'
secret_id = 'project_key'   #<---------- this is the name of the secret you created
version_id = 'latest'
gcp_region = 'us-central1'

# instantiate the services 
sm = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()

# Build the resource name of the secret version
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# initiate the MotherDuck connection through an access token through
md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# get the last, best model
sql = f""" 
select m.*, r.model_path
from {db_schema}.job_metrics m 
inner join {db_schema}.model_runs r on m.job_id = r.job_id
where m.metric_name = 'mape'
and r.name like '%case duration%'
order by m.metric_value asc,
m.created_at desc
limit 1
"""

results = md.sql(sql).df()
model_path = results['model_path'][0]
print(f"Getting model: {model_path}")

# load the model
with GCSFileSystem().open(model_path, 'rb') as f:
    model_pipeline = joblib.load(f)

# flatten for inclusion in the result
json_output = results.iloc[0].to_dict()
json_output['created_at'] = json_output['created_at'].isoformat()

@functions_framework.http
def task(request):
    "Make predictions for the model"

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(request_json)

    # load the data key which is a list of text for inference
    data_list = request_json.get('data')
    print(f"data: {data_list}")

    preds = model_pipeline.predict(data_list)

    # convert to a list for return
    preds_list = preds.tolist()

    return {'predictions':preds_list, 'model_info': json_output}, 200