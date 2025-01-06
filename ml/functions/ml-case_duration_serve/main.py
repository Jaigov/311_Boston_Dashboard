# imports
import functions_framework
import joblib
import json
import pandas as pd
import numpy as np
from gcsfs import GCSFileSystem

# the hardcoded model on GCS
GCS_BUCKET = "group2-ba882-vertex-models"
GCS_PATH = "models/case-duration/"
FNAME = "rf_model.joblib"
GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

# load the model
with GCSFileSystem().open(GCS, 'rb') as f:
    rf_model = joblib.load(f)
print("loaded the Random Forest model from GCS")

@functions_framework.http
def task(request):
    "Make predictions using the Random Forest model"

    # Parse the request data (expecting structured data with multiple features)
    request_json = request.get_json(silent=True)

    # Ensure that we have received valid input data
    if not request_json or 'data' not in request_json:
        return {'error': 'Invalid input data'}, 400

    # Load the data key which should be a list of dictionaries (structured feature values)
    data_list = request_json.get('data')
    print(f"Input data: {data_list}")

    # Convert list of dictionaries into a pandas DataFrame for prediction
    df = pd.DataFrame(data_list)
    print(f"DataFrame columns before preprocessing: {df.columns.tolist()}")

    # Handle missing values (fill with default values)
    df.fillna("", inplace=True)  # For categorical columns
    df.fillna(0, inplace=True)   # For numerical columns

    # Preprocess input data as needed (e.g., one-hot encoding for categorical variables)
    columns_of_interest = ['fire_district', 'pwd_district', 'city_council_district', 'police_district', 'neighborhood', 
                           'reason', 'type', 'on_time']
    
    # Ensure all required columns are present
    for col in columns_of_interest:
        if col not in df.columns:
            df[col] = ""  # or an appropriate default value

    # Select only the columns of interest
    df = df[columns_of_interest]

    # Handle categorical variables using one-hot encoding
    df_encoded = pd.get_dummies(df, columns=columns_of_interest)
    print(f"DataFrame columns after encoding: {df_encoded.columns.tolist()}")

    # Ensure all features used during training are present
    expected_features = rf_model.feature_names_in_
    for feature in expected_features:
        if feature not in df_encoded.columns:
            df_encoded[feature] = 0

    # Select only the features used during training
    df_final = df_encoded[expected_features]
    print(f"Final DataFrame columns: {df_final.columns.tolist()}")

    # Check if DataFrame is empty after preprocessing
    if df_final.empty:
        return {'error': 'Processed data has no features for prediction'}, 400

    # Make predictions using the loaded Random Forest model
    preds = rf_model.predict(df_final)

    # Convert predictions to a list for return
    preds_list = preds.tolist()

    return {'predictions': preds_list}, 200