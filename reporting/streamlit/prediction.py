import streamlit as st
import duckdb
import pandas as pd
from google.cloud import secretmanager
import numpy as np
from datetime import datetime  # <-- Importing datetime module
from PIL import Image
from streamlit_lottie import st_lottie
import json
import requests
from io import BytesIO
from datetime import datetime
import base64
import ast
import re

from itertools import combinations
import networkx as nx
from collections import Counter
import matplotlib.pyplot as plt

############################################ Streamlit App

import streamlit as st
import base64

# Function to load and encode the background image
def load_background_image(image_file):
    with open(image_file, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return data

# Load the local image file (e.g., "Boston city.webp")
background_image = load_background_image("Boston city.webp")

# Set the background image using CSS
st.markdown(f"""
    <style>
    .stApp {{
        background-image: url("data:image/webp;base64,{background_image}");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
    }}
    </style>
""", unsafe_allow_html=True)

# Example content to display on the page
#st.title("Welcome to Boston 311")
#st.write("This is an example Streamlit application with a custom background image.")

# Google Cloud Secret Manager setup
project_id = 'group2-ba882'
secret_id = 'project_key'   #<---------- this is the name of the secret you created
version_id = 'latest'

db = 'city_services_boston'
schema = "stage"
schema_raw = "raw"
schema_ml = "ml"
schema_mlops = "mlops"
db_schema = f"{db}.{schema}"


# Accessing secrets from Google Cloud Secret Manager
sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# Connecting to DuckDB using the token from Secret Manager
md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# Define cloud Run URLs
ML_CASE_DURATION_HOURS_RUN = "https://us-central1-group2-ba882.cloudfunctions.net/ml-case-duration-serve"


# Query to fetch data from all required tables
def fetch_data():
    query = f"""
        SELECT 
            da.case_enquiry_id,
            da.department,
            loc.location,
            loc.fire_district,
            loc.pwd_district,
            loc.city_council_district,
            loc.police_district,
            loc.neighborhood,
            loc.neighborhood_services_district,
            loc.ward,
            loc.precinct,
            loc.location_street_name,
            loc.location_zipcode,
            req.case_title,
            req.subject,
            req.reason,
            req.type,
            req.queue,
            req.source,
            rt.on_time as case_status,
            sh.open_dt,
            sh.sla_target_dt,
            sh.closed_dt,
            sh.case_status AS SH_Case_Status,
            sh.closure_reason

        FROM city_services_boston.raw.department_assignment da
        LEFT JOIN city_services_boston.raw.locations loc 
          ON da.case_enquiry_id = loc.case_enquiry_id
        LEFT JOIN city_services_boston.raw.requests req 
          ON da.case_enquiry_id = req.case_enquiry_id
        LEFT JOIN city_services_boston.raw.response_time rt 
          ON da.case_enquiry_id = rt.case_enquiry_id
        LEFT JOIN city_services_boston.raw.status_history sh 
          ON da.case_enquiry_id = sh.case_enquiry_id;
        
    """
    try:
        return md.execute(query).df()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

############################################################################ CODE FOR THE MAIN PAGE CONTENT

# Streamlit Title and Subheader
st.markdown("<h1 style='font-size: 36px;'>PREDICT 311: Service Request Duration Predictor</h1>", unsafe_allow_html=True)


################################################################################# FOR PREICTION HOURS FROM CLOUD RUN

# Function to fetch predictions from Cloud Run
def fetch_predictions(data):
    try:
        # Send a POST request to the Cloud Run endpoint with input data
        response = requests.post(ML_CASE_DURATION_HOURS_RUN, json={"data": data})
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
        predictions = response.json().get("predictions", [])
        return predictions
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching predictions: {e}")
        return []

# Function to apply conditional formatting to the DataFrame
def style_table(row):

    styles = [""] * len(row)
    # Highlight Case Status column (green for On Time, red for Overdue)
    if row["Case Status"] == "Overdue":
        styles[row.index.get_loc("Case Status")] = "background-color: red; color: white"
    elif row["Case Status"] == "On Time":
        styles[row.index.get_loc("Case Status")] = "background-color: green; color: white"

    # Highlight Case State column based on conditions
    if row["Case State"] == "Severe":
        styles[row.index.get_loc("Case State")] = "background-color: darkred; color: white"
    elif row["Case State"] == "Moderate":
        styles[row.index.get_loc("Case State")] = "background-color: orange; color: black"
    elif row["Case State"] == "On Track":
        styles[row.index.get_loc("Case State")] = "background-color: green; color: white"
    return styles

# User input for case details
st.sidebar.header("Input Case Details For Prediction")
fire_district = st.sidebar.text_input("Fire District", "")
pwd_district = st.sidebar.text_input("PWD District", "")
city_council_district = st.sidebar.text_input("City Council District", "")
police_district = st.sidebar.text_input("Police District", "")
neighborhood = st.sidebar.text_input("Neighborhood", "")
reason = st.sidebar.text_input("Reason", "")
case_type = st.sidebar.text_input("Type", "")
sla_hours = st.sidebar.number_input("SLA Target (Hours)", min_value=1, step=1, value=48)  # User-defined SLA target

# Prepare input data for prediction
input_data = [{
    "fire_district": fire_district,
    "pwd_district": pwd_district,
    "city_council_district": city_council_district,
    "police_district": police_district,
    "neighborhood": neighborhood,
    "reason": reason,
    "type": case_type
}]


################################################### For LOGO Image

# Function to fetch an image from a URL
def fetch_image_from_url(url: str):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return Image.open(BytesIO(response.content))
        else:
            st.error("Failed to fetch the image. Please check the URL.")
            return None
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return None

# URL of the Boston 311 image
boston_311_image_url = "https://cdn.myportfolio.com/7437048a-6b6b-4e67-917e-6cc9367aa63d/005c3894-34f7-413a-aefd-009dbe70d7ef.png?h=6c07280d3095dd9d88a72490b8d2d31b"

# Fetch and display the image
image = fetch_image_from_url(boston_311_image_url)
if image:
    st.image(image, caption="Boston 311", use_container_width=True)


# Bold Introduction using markdown
st.markdown("**INTRODUCTION**")

# Reduced description size and aligned text as justified using HTML/CSS
st.markdown("""
<div style='text-align: justify; font-size: 14px;'>
Welcome to PREDICT 311, your go-to tool for predicting service request durations in Boston's 311 Service Requests System. 
This app leverages advanced machine learning models to estimate the time it will take to resolve a service request based on its case inquiry ID. 
Whether you're a city official or a curious citizen, PREDICT 311 provides quick and reliable insights into expected service completion times, 
helping you plan and manage requests, resources more efficiently.
</div>
""", unsafe_allow_html=True)

st.write("\n" * 2)  # Adds three newlines
st.markdown("---")
st.write("\n" * 2)  # Adds three newlines


# Button to fetch predictions
if st.button("Predict Duration"):
    if any(value.strip() for value in input_data[0].values()):  # Ensure at least one field is filled
        predictions = fetch_predictions(input_data)
        
        if predictions:
            # Create a DataFrame with predictions and calculate On Time/Overdue status
            df_predictions = pd.DataFrame({"Duration Hours": predictions})  # Calculate Case Status (On Time/Overdue)
            df_predictions["Case Status"] = df_predictions["Duration Hours"].apply(
                lambda x: "On Time" if x <= sla_hours else "Overdue"
            )
            
             # Calculate Case State (Severe/Moderate/On Track)

            df_predictions["Case State"] = df_predictions["Duration Hours"].apply(

                lambda x: (

                    "Severe" if x > sla_hours * 1.7 else 

                    ("Moderate" if x > sla_hours else "On Track")

                )
            )
            # Display styled DataFrame with conditional formatting applied

            styled_df = df_predictions.style.apply(style_table, axis=1)

            st.subheader("Predicted Case Durations")

            st.dataframe(styled_df, use_container_width=True)
        else:
            st.warning("No predictions available. Please check your input or try again.")
    else:
        st.warning("Please provide at least one input field before predicting.")



# SQL query to get date range (min and max published dates)
sql = """
select 
    min(published) as min,
    max(published) as max,
from
    awsblogs.stage.posts
"""
date_range = md.sql(sql).df()

# Extracting start_date and end_date from the query result
start_date = date_range['min'].to_list()[0]
end_date = date_range['max'].to_list()[0]

# Check if 'min' or 'max' dates are NaT and replace them with today's date if necessary
if pd.isna(start_date):
    start_date = datetime.today()  # Replace NaT with today's date

if pd.isna(end_date):
    end_date = datetime.today()  # Replace NaT with today's date

# Convert start_date and end_date to proper date format if needed (optional)
start_date = pd.to_datetime(start_date).date()
end_date = pd.to_datetime(end_date).date()


########################################### CODE FOR LEFT PANEL

#st.sidebar.button("A button to control inputs")
#st.sidebar.file_uploader("Users can upload files that your app analyzes!")
#st.sidebar.markdown("These controls are not wired up to control data, just highlighting you have a lot of control!")



############ There are some chat support features, more coming

#prompt = st.chat_input("Say something")
#if prompt:
   # st.write(f"User has sent the following prompt: {prompt}")


################################################################################################

# Streamlit app to display data
def main():
    st.title("City Services Data Viewer")

    # Fetch data
    st.write("Fetching data...")
    data_df = fetch_data()

    # Check if data is empty and stop execution if so
    if data_df.empty:
        st.warning("No data available. Please check your database or query.")
        st.stop()

    # Sidebar filters
    st.sidebar.header("Filters for Dynamic Data View")
    
    # Case Enquiry ID Filter
    case_enquiry_filter = st.sidebar.text_input("Enter Case Enquiry ID (optional):")
    
    # Dropdown for Source Filter
    source_options = data_df['source'].dropna().unique()
    source_filter = st.sidebar.selectbox("Select Source", options=["All"] + list(source_options))

    # Ensure 'open_dt' column exists and calculate date range
    if 'open_dt' in data_df.columns:
        min_date, max_date = pd.to_datetime(data_df['open_dt']).min(), pd.to_datetime(data_df['open_dt']).max()
    else:
        st.error("'open_dt' column not found in the dataset.")
        st.stop()

    # Date Range Filter (using `open_dt`)
    date_range_filter = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date.date(), max_date.date()),  # Default range
        min_value=min_date.date(),
        max_value=max_date.date()
    )

    # Check if input is valid
    if isinstance(date_range_filter, tuple) and len(date_range_filter) == 2:
        start_date, end_date = date_range_filter
    else:
        st.error("Please select a valid date range.")
        st.stop()

    # Apply filters to the data
    filtered_data_df = data_df.copy()

    if case_enquiry_filter:
        filtered_data_df = filtered_data_df[filtered_data_df['case_enquiry_id'] == case_enquiry_filter]

    if source_filter != "All":
        filtered_data_df = filtered_data_df[filtered_data_df['source'] == source_filter]

    # Filter by selected date range
    filtered_data_df['open_dt'] = pd.to_datetime(filtered_data_df['open_dt'])
    filtered_data_df = filtered_data_df[
        (filtered_data_df['open_dt'] >= pd.to_datetime(start_date)) &
        (filtered_data_df['open_dt'] <= pd.to_datetime(end_date))
    ]

    # Display selected filters and filtered data table
    st.write(f"You selected: {source_filter}")
    
    if not filtered_data_df.empty:
        st.subheader("Filtered Data Table")
        st.dataframe(filtered_data_df)
    
        # Add any additional visualizations or graphs here
    
    else:
        st.warning("No data available for the selected filters.")

if __name__ == "__main__":
    main()