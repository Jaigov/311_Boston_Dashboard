import requests
import pandas as pd
from google.cloud import storage
import datetime
import uuid
from io import BytesIO
import functions_framework
from bs4 import BeautifulSoup

# Google Cloud Storage bucket name
bucket_name = "group2-ba882-project"

# CSV download URL
def latest_api_link():
    # URL of the Boston 311 Service Requests page
    url = "https://data.boston.gov/dataset/311-service-requests/resource/dff4d804-5031-443a-8409-8344efd0e5c8"
 
    # Send an HTTP request to the page
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the page content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all anchor tags (<a>) in the page
        links = soup.find_all('a', href=True)
 
        # Use a set to store unique links containing 'download'
        download_links = set()
        for link in links:
            if 'download' in link['href']:
                download_links.add(link['href'])
 
        # Check if all found links are the same or not
        if len(download_links) == 1:
            return download_links.pop()  # Return the single found link
 
        elif len(download_links) > 1:
            print("Multiple different download links found. Please choose which one to use:")
 
            for link in download_links:
                print(link)
 
            return None  # Return None since multiple links were found
 
        else:
            print("No download links found.")
            return None  # Return None if no links were found
 
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return None  # Return None if the request failed

# Printing the API Link from the above Funciton 
csv_url = latest_api_link()
if csv_url:
    print(f"Latest CSV URL: {csv_url}")
else:
    print("No valid CSV URL found.")


# Function to download CSV data
def download_csv(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to download CSV. Status code: {response.status_code}")

# Function to upload data to GCS as JSON
def upload_to_gcs(data, bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(BytesIO(data), content_type='application/json')

# Main function to execute the process as a Cloud Function
@functions_framework.http
def main(request):
    try:
        # Generate job ID
        job_id = datetime.datetime.now().strftime('%Y%m%d%H%M') + '-' + str(uuid.uuid4())

        # Download CSV data
        csv_data = download_csv(csv_url)

        # Convert CSV to DataFrame and then to JSON lines format
        df = pd.read_csv(BytesIO(csv_data))
        
        # Attempt to sort by date and get the latest 100,000 rows
        if 'open_dt' in df.columns:
            #df['open_dt'] = pd.to_datetime(df['open_dt'])
            df = df.sort_values('open_dt', ascending=False).head(100000)
        else:
            # If no date column, just take the last 100,000 rows
            df = df.tail(100000)

        json_buffer = BytesIO()
        df.to_json(json_buffer, orient='records', lines=True)
        json_buffer.seek(0)

        # Define the blob name with job ID
        blob_name = f"boston_data/{job_id}/data.json"

        # Upload JSON data to GCS
        upload_to_gcs(json_buffer.getvalue(), bucket_name, blob_name)

        print(f"Data successfully uploaded to gs://{bucket_name}/{blob_name}")

        return {
            'filepath': f"gs://{bucket_name}/{blob_name}",
            'jobid': job_id,
            'bucket_id': bucket_name,
            'blob_name': blob_name,
            'total_records': len(df)
        }

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {'error': str(e)}, 500