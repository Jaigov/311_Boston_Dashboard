# The Machine Learning Datasets job

# imports
import requests
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def case_duration():
    """Creates or updates ML views for case duration"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/ml-case-duration"
    resp = invoke_gcf(url, payload={})
    return resp

# The main job
@flow(name="case-duration-ml-datasets", log_prints=True)
def ml_datasets():
    # Execute the task to create/update ML view
    case_duration()

# The job execution
if __name__ == "__main__":
    ml_datasets()