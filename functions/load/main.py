###############################################
# TASK: process the json from the previous step
###############################################

# imports
import functions_framework
import datetime
from google.cloud import storage
import json 
from io import BytesIO
from dateutil import parser
from google.cloud import secretmanager
import duckdb
import pandas as pd
import re         

# setup
project_id = 'group2-ba882'
secret_id = 'project_key'
version_id = 'latest'
bucket_name = 'group2-ba882-project'                            

# db setup
db = 'city_services_boston'
raw_db_schema = f"{db}.raw"
stage_db_schema = f"{db}.stage"

def get_latest_job_file(bucket_name, dataset='boston_data'):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all blobs in the dataset folder
    blobs = list(bucket.list_blobs(prefix=f"{dataset}/"))
    
    if not blobs:
        return None
    
    # Extract job IDs and find the latest one
    job_ids = set()
    for blob in blobs:
        match = re.search(r'(\d{12}-[a-f0-9-]+)/', blob.name)
        if match:
            job_ids.add(match.group(1))
    
    if not job_ids:
        return None
    
    latest_job_id = max(job_ids)
    
    # Find the specific JSON file in the latest job folder
    target_blob = next((blob for blob in blobs if f"{latest_job_id}/data.json" in blob.name), None)
    
    if target_blob:
        return f"gs://{bucket_name}/{target_blob.name}"
    else:
        return None

############################################################### main task
@functions_framework.http
def main(request):
    # get the raw json from gcs from the previous step
    request_json = request.get_json(silent=True)

    # setup                                 
    storage_client = storage.Client()
    sm = secretmanager.SecretManagerServiceClient()
    
    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # create db if not exists
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
    md.sql(create_db_sql)

    # drop if exists and create the raw schema for 
    #create_schema = f"DROP SCHEMA IF EXISTS {raw_db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {raw_db_schema};"
    #md.sql(create_schema)

    # create stage schema if first time running function
    #create_schema = f"CREATE SCHEMA IF NOT EXISTS {stage_db_schema};"
    #md.sql(create_schema)

    print(md.sql("SHOW DATABASES;").show())  

    # read in from gcs
    json_path = get_latest_job_file(bucket_name)
    print(f"Processing data from {json_path}")
            
    # Download the JSON file
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = '/'.join(json_path.split('/')[3:])   # Remove 'gs://bucket_name/' from the path
    print(blob_name)
    blob = bucket.blob(blob_name)
    print(blob)
    blob_content = blob.download_as_text()
    lines = blob_content.splitlines()

    # parse the JSONL (json lines) file
    records = [json.loads(line) for line in lines]

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: requests
                                               
    # # parse the JSONL (json lines) file
    # requests = [json.loads(line) for line in lines]

    # parse the feed elements we want
    parsed_requests = []
    for record in records:
        parsed_requests.append({
            '_id': record.get('_id'),
            'case_enquiry_id': record.get('case_enquiry_id'),
            'case_title': record.get('case_title'),
            'subject': record.get('subject'),
            'reason': record.get('reason'),
            'type': record.get('type'),
            'queue': record.get('queue'),
            'source': record.get('source'),
            'submitted_photo': record.get('submitted_photo'),
            'closed_photo': record.get('closed_photo'),
            'job_id': request_json.get('jobid'),
            'ingest_timestamp': datetime.datetime.now().isoformat()
        })

    # Convert parsed records to DataFrame for easirer ingestion
    requests_df = pd.DataFrame(parsed_requests)

    # Upload the parsed data to MotherDuck
    raw_tbl_name = f"{raw_db_schema}.requests" #  API data          
    stage_tbl_name = f"{stage_db_schema}.requests" 

    # table logic
                                                                           
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM requests_df WHERE CAST (case_enquiry_id as VARCHAR) NOT IN (SELECT CAST(case_enquiry_id as VARCHAR) FROM {stage_tbl_name});                          
                                                                               
    """     
                              
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    # Ingest the parsed DataFrame into the raw schema
    ingest_sql = f"INSERT INTO {stage_tbl_name} SELECT * FROM {raw_tbl_name}"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del requests_df
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: location
    # locations = [json.loads(line) for line in lines]

    # parse the feed elements we want
    parsed_locations = []
    for record in records:
        parsed_locations.append({
            'case_enquiry_id': record.get('case_enquiry_id'),
            'location': record.get('location'),
            'fire_district': record.get('fire_district'),
            'pwd_district': record.get('pwd_district'),
            'city_council_district': record.get('city_council_district'),
            'police_district': record.get('police_district'),
            'neighborhood': record.get('neighborhood'),
            'neighborhood_services_district': record.get('neighborhood_services_district'),
            'ward': record.get('ward'),
            'precinct': record.get('precinct'),
            'location_street_name': record.get('location_street_name'),
            'location_zipcode': record.get('location_zipcode'),
            'latitude': record.get('latitude'),
            'longitude': record.get('longitude'),
            'geom_4326': record.get('geom_4326'),
            'job_id': request_json.get('jobid'),
            'ingest_timestamp': datetime.datetime.now().isoformat()
        })

    # Convert parsed records to DataFrame for easirer ingestion
    locations_df = pd.DataFrame(parsed_locations)

    # Upload the parsed data to MotherDuck
    raw_tbl_name = f"{raw_db_schema}.locations" #  API data
    stage_tbl_name = f"{stage_db_schema}.locations"
    # table logic
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM locations_df WHERE CAST (case_enquiry_id as VARCHAR) NOT IN (SELECT CAST(case_enquiry_id as VARCHAR) FROM {stage_tbl_name});
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # Upload the parsed data to MotherDuck
     #  API data  

    # Ingest the parsed DataFrame into the raw schema
    ingest_sql = f"INSERT INTO {stage_tbl_name} SELECT * FROM {raw_tbl_name}"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del locations_df
    
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: department_assignment
    
    # department_assignment = [json.loads(line) for line in lines]

    # parse the feed elements we want
    parsed_department_assignment = []
    for record in records:
        parsed_department_assignment.append({
            'case_enquiry_id': record.get('case_enquiry_id'),
            'department': record.get('department'),
            'job_id': request_json.get('jobid'),
            'ingest_timestamp': datetime.datetime.now().isoformat()
        })

    # Convert parsed records to DataFrame for easirer ingestion
    department_assignment_df = pd.DataFrame(parsed_department_assignment)

    # Upload the parsed data to MotherDuck
    raw_tbl_name = f"{raw_db_schema}.department_assignment" # API data
    stage_tbl_name = f"{stage_db_schema}.department_assignment"

    # table logic
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM department_assignment_df WHERE CAST (case_enquiry_id as VARCHAR) NOT IN (SELECT CAST(case_enquiry_id as VARCHAR) FROM {stage_tbl_name});
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)


    # Ingest the parsed DataFrame into the raw schema
    ingest_sql = f"INSERT INTO {stage_tbl_name} SELECT * FROM {raw_tbl_name}"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del department_assignment_df
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: response_time

    # response_time = [json.loads(line) for line in lines]

    # parse the feed elements we want
    parsed_response_time = []
    for record in records:
        parsed_response_time.append({
            'case_enquiry_id': record.get('case_enquiry_id'),
            'on_time': record.get('on_time'),
            'job_id': request_json.get('jobid'),
            'ingest_timestamp': datetime.datetime.now().isoformat()
        })

    # Convert parsed records to DataFrame for easirer ingestion
    response_time_df = pd.DataFrame(parsed_response_time)

    # Upload the parsed data to MotherDuck
    raw_tbl_name = f"{raw_db_schema}.response_time" # API data
    stage_tbl_name = f"{stage_db_schema}.response_time" 
    # table logic
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM response_time_df WHERE CAST (case_enquiry_id as VARCHAR) NOT IN (SELECT CAST(case_enquiry_id as VARCHAR) FROM {stage_tbl_name});
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # Upload the parsed data to MotherDuck
    #  API data  

    # Ingest the parsed DataFrame into the raw schema
    ingest_sql = f"INSERT INTO {stage_tbl_name} SELECT * FROM {raw_tbl_name}"
    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del response_time_df
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ tbl: status_history
    
    # status_history = [json.loads(line) for line in lines]

    # Now use this function in your script
    parsed_status_history = []
    for record in records:
        parsed_status_history.append({
            'case_enquiry_id': record.get('case_enquiry_id'),
            'open_dt': record.get('open_dt'),
            'sla_target_dt': record.get('sla_target_dt'),
            'closed_dt': record.get('closed_dt'),
            'case_status': record.get('case_status'),
            'closure_reason': record.get('closure_reason'),
            'job_id': request_json.get('jobid'),
            'ingest_timestamp': datetime.datetime.now().isoformat()
    })

    # Convert parsed records to DataFrame for easirer ingestion
    status_history_df = pd.DataFrame(parsed_status_history)

    # Upload the parsed data to MotherDuck
    raw_tbl_name = f"{raw_db_schema}.status_history" # API
    stage_tbl_name = f"{stage_db_schema}.status_history"
    # table logic
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM status_history_df WHERE CAST (case_enquiry_id as VARCHAR) NOT IN (SELECT CAST(case_enquiry_id as VARCHAR) FROM {stage_tbl_name});
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # Ingest the parsed DataFrame into the raw schema
    ingest_sql = f"INSERT INTO {stage_tbl_name} SELECT * FROM {raw_tbl_name}"

    print(f"Import statement: {ingest_sql}")
    md.sql(ingest_sql)
    del status_history_df

    return {}, 200