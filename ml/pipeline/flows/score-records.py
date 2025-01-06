# Score new records

# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def score_records():
    """Flag and score new records"""
    url = "https://us-central1-group2-ba882.cloudfunctions.net/mlops-caseduration-batch"
    resp = invoke_gcf(url, payload={})
    return resp


# Prefect Flow
@flow(name="mlops-batch-predict", log_prints=True)
def batch_flow():
    """The ETL flow which batch scores new records"""
    
    result = score_records()
    print(result)



# the job
if __name__ == "__main__":
    batch_flow()