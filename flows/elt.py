# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError:
        print("Response did not contain valid JSON")
        return None

@task(retries=2)
def schema_department_assignment():
    """Setup the stage schema for department assignment"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-schema-department_assignment"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_location():
    """Setup the stage schema for location"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-schema-location"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_requests():
    """Setup the stage schema for requests"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-schema-requests"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_response_time():
    """Setup the stage schema for response time"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-schema-response_time"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_status_history():
    """Setup the stage schema for status history"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-schema-status_history"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def extract():
    """Extract the RSS feeds into JSON on GCS"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-extract"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def load(payload):
    """Load the tables into the raw schema, ingest new records into stage tables"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/group2-load"
    resp = invoke_gcf(url, payload=payload)
    return resp

# Prefect Flow
@flow(name="311-service-requests-elt-flow", log_prints=True)
def elt_flow():
    """The ELT flow which orchestrates Cloud Functions"""

    result = schema_department_assignment()
    print("The schema setup for department assignment completed")

    result = schema_location()
    print("The schema setup for location completed")

    result = schema_requests()
    print("The schema setup for requests completed")

    result = schema_response_time()
    print("The schema setup for response time completed")

    result = schema_status_history()
    print("The schema setup for status history completed")
    
    extract_result = extract()
    print("Data extracted onto GCS")
    print(f"{extract_result}")

    result = load(extract_result)
    print("The data were loaded into the raw schema and changes added to stage")

# the job
if __name__ == "__main__":
    elt_flow()
