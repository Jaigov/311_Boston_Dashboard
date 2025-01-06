# imports
import functions_framework
import os
import pandas as pd 
import joblib
import uuid
import datetime

from gcsfs import GCSFileSystem
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

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

##################################################### helpers

def load_sql(p):
    with open(p, "r") as f:
        sql = f.read()
        return sql

##################################################### task

@functions_framework.http
def task(request):
    "Using Cloud Functions as our compute layer - train models in a pipeline"

    # Generate a unique job_id for this run (based on timestamp and UUID)
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Build the resource name of the secret version for MotherDuck connection token
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version for MotherDuck token
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Initiate the MotherDuck connection through an access token
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # Load dataset from MotherDuck using SQL query (assumed to be stored in a file)
    sql = load_sql("dataset.sql")
    df = md.sql(sql).df()

    # Parse incoming request to get parameters (if any)
    try:
        request_json = request.get_json()
    except Exception:
        request_json = {}

    # Model parameters with default values (can be overridden by request parameters)
    n_estimators = request_json.get('n_estimators', 100)
    max_depth = request_json.get('max_depth', None)
    model_name = request_json.get('name', '311 Service Requests Random Forest Regressor')

    # Select relevant columns for features and target variable (adjust based on your dataset)
    columns_of_interest = ['fire_district', 'pwd_district', 'city_council_district', 'police_district', 
                           'neighborhood', 'reason', 'type', 'on_time']
    
    df_features = df[columns_of_interest]
    
    # Target variable: duration_hours (adjust based on your dataset)
    df_target = df['duration_hours']

    # One-hot encode categorical variables (optional, adjust based on your dataset)
    df_features_encoded = pd.get_dummies(df_features, drop_first=True)

    # Split the dataset into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(df_features_encoded, df_target, test_size=0.2, random_state=882)

    # Create a Random Forest Regressor model with specified parameters
    model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=882)

    # Fit the model on training data
    model.fit(X_train, y_train)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate evaluation metrics for the model performance
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)

    print(f"r2: {r2}")
    print(f"mae: {mae}")
    print(f"mape: {mape}")

    ############################################# write the model to GCS as an artifact

    # Define GCS bucket and path for saving artifacts (each run gets its own folder)
    GCS_BUCKET = "group2-ba882-vertex-models"
    GCS_PATH_RUN_FOLDER = f"pipeline/runs/{job_id}"
    FNAME_MODEL_ARTIFACT = "model/model.joblib"
    GCS_MODEL_PATH_FULL = f"gs://{GCS_BUCKET}/{GCS_PATH_RUN_FOLDER}/{FNAME_MODEL_ARTIFACT}"

    # Save the trained model to GCS using GCSFileSystem
    with GCSFileSystem().open(GCS_MODEL_PATH_FULL, 'wb') as f:
        joblib.dump(model, f)
    
    ############################################# write metadata and metrics to MotherDuck warehouse

    # Insert metadata about this run into `model_runs` table in MotherDuck warehouse
    insert_query_model_run = f"""
    INSERT INTO {db_schema}.model_runs (job_id, name, gcs_path, model_path)
    VALUES ('{job_id}', '{model_name}', '{GCS_BUCKET + "/" + GCS_PATH_RUN_FOLDER}', '{GCS_MODEL_PATH_FULL}');
    """
    print(f"Inserting into model_runs: {insert_query_model_run}")
    md.sql(insert_query_model_run)

    # Prepare metrics data for insertion into `job_metrics` table in MotherDuck warehouse
    ingest_timestamp = pd.Timestamp.now()
    metrics_data_dict = {
        'job_id': job_id,
        'r2': r2,
        'mae': mae,
        'mape': mape,
        'created_at': ingest_timestamp
    }
   
    metrics_df = pd.DataFrame([metrics_data_dict])
   
    # Reshape metrics dataframe to match schema of `job_metrics`
    metrics_df_melted = pd.melt(metrics_df, 
                                id_vars=['job_id', 'created_at'], 
                                value_vars=['r2', 'mae', 'mape'], 
                                var_name='metric_name', 
                                value_name='metric_value')
   
    metrics_df_melted_final_cols_ordered = metrics_df_melted[['job_id', 'metric_name', 'metric_value', 'created_at']]

    # Ensure correct data types
    metrics_df_melted_final_cols_ordered['job_id'] = metrics_df_melted_final_cols_ordered['job_id'].astype(str)
    metrics_df_melted_final_cols_ordered['created_at'] = metrics_df_melted_final_cols_ordered['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Insert metrics into `job_metrics` table in MotherDuck warehouse
    md.sql(f"""
        INSERT INTO {db_schema}.job_metrics 
        (job_id, metric_name, metric_value, created_at)
        SELECT job_id, metric_name, metric_value, created_at 
        FROM metrics_df_melted_final_cols_ordered
    """)

    ############################################# write hyperparameters to `job_parameters` table

    # Prepare parameters data for insertion into `job_parameters` table
    params_data = [
        {'job_id': str(job_id), 'parameter_name': 'n_estimators', 'parameter_value': str(n_estimators), 'created_at': ingest_timestamp},
        {'job_id': str(job_id), 'parameter_name': 'max_depth', 'parameter_value': str(max_depth), 'created_at': ingest_timestamp},
        {'job_id': str(job_id), 'parameter_name': 'model', 'parameter_value': 'RandomForestRegressor', 'created_at': ingest_timestamp}
    ]

    params_df = pd.DataFrame(params_data)

    # Ensure correct data types
    params_df['job_id'] = params_df['job_id'].astype(str)
    params_df['parameter_value'] = params_df['parameter_value'].astype(str)
    params_df['created_at'] = params_df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Insert parameters into `job_parameters` table in MotherDuck warehouse
    md.sql(f"""
        INSERT INTO {db_schema}.job_parameters 
        (job_id, parameter_name, parameter_value, created_at)
        SELECT job_id, parameter_name, parameter_value, created_at 
        FROM params_df
    """)

    return f"Model training completed. Job ID: {job_id}", 200