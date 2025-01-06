# imports
import functions_framework
import os
import pandas as pd
import joblib
import json

from gcsfs import GCSFileSystem
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score

# settings
project_id = 'group2-ba882'  # <------- change this to your value
project_region = 'us-central1'

@functions_framework.http
def main(request):
    """Fit the Random Forest model and save it to GCS"""

    # Path to the dataset in GCS (Google Cloud Storage)
    GCS_PATH = "gs://group2-ba882-vertex-models/training-data/case-duration/case_duration_data.csv"

    # Load the dataset from GCS
    df = pd.read_csv(GCS_PATH)
    print(df.head())

    # Calculate duration in hours (target variable)
    df['duration_hours'] = (pd.to_datetime(df['closed_dt']) - pd.to_datetime(df['open_dt'])).dt.total_seconds() / 3600

    # Select relevant columns for features and target variable
    columns_of_interest = ['fire_district', 'pwd_district', 'city_council_district', 'police_district', 'neighborhood', 
                           'reason', 'type', 'on_time']
    df = df[columns_of_interest + ['duration_hours']]

    # Handle categorical variables by one-hot encoding
    df = pd.get_dummies(df, drop_first=True)

    # Split the dataset into training (80%) and testing (20%) sets
    train_set, test_set = train_test_split(df, test_size=0.2, random_state=882)

    # Separate features (X) and target variable (y)
    X_train = train_set.drop(columns=['duration_hours'])
    y_train = train_set['duration_hours']
    
    X_test = test_set.drop(columns=['duration_hours'])
    y_test = test_set['duration_hours']

    # Create a Random Forest Regressor model
    model = RandomForestRegressor(n_estimators=100, random_state=882)

    # Fit the model to the training data
    model.fit(X_train, y_train)

    # Make predictions on the test data
    y_pred = model.predict(X_test)

    # Calculate evaluation metrics
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    mape = mean_absolute_percentage_error(y_test, y_pred)

    print(f"r2: {r2}")
    print(f"mae: {mae}")
    print(f"mape: {mape}")

    # Define GCS bucket and path for saving the trained model and evaluation metrics
    GCS_BUCKET = "group2-ba882-vertex-models"
    GCS_PATH = "models/case-duration/"
    
    # Save Random Forest Model to GCS
    model_fname = "rf_model.joblib"
    model_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{model_fname}"
    
    with GCSFileSystem().open(model_gcs_path, 'wb') as f:
        joblib.dump(model, f)  # Save the Random Forest model

    # Save evaluation metrics as a JSON file in GCS
    metrics_fname = "rf_metrics.json"
    metrics_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{metrics_fname}"
    
    metrics_data = {
        'r2': r2,
        'mae': mae,
        'mape': mape,
        "model_path": model_gcs_path
    }
    
    with GCSFileSystem().open(metrics_gcs_path, 'w') as f:
        json.dump(metrics_data, f)  # Save evaluation metrics as JSON

    return 'Random Forest Regressor model and metrics saved successfully to GCS', 200