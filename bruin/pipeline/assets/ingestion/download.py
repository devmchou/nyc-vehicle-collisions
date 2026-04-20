""" @bruin

name: ingestion.download
type: python
description: "Download yearly collisions CSV and upload to GCS"
depends: []

@bruin """
import os
import pandas as pd
from sodapy import Socrata
from google.cloud import storage
from dotenv import load_dotenv

# This loads variables from .env into os.environ
load_dotenv()

start_date = os.environ.get("BRUIN_START_DATE")
end_date = os.environ.get("BRUIN_END_DATE")
start_date_string = start_date.replace("-", "")  # YYYYMMDD
end_date_string = end_date.replace("-", "")  # YYYYMMDD

api_token = os.environ.get("SOCRATA_APP_TOKEN")
url = "data.cityofnewyork.us"
dataset_id = "h9gi-nx95"
where = f"crash_date >= '{start_date}' AND crash_date < '{end_date}'"

bucket_name = os.environ["GCS_BUCKET_NAME"]
destination = f"Collisions_{start_date_string}_to_{end_date_string}.csv"

FETCH_ITERATION_LIMIT = 20000  # Number of records to fetch per API call
UPLOAD_TIMEOUT = 300 # Timeout for GCS upload in seconds (5 minutes)

try:
    client = Socrata(url, api_token)
    print(f"Downloading {url} with {where}")
    # Query between two timestamps
    all_results = []
    offset = 0
    limit = FETCH_ITERATION_LIMIT  # Smaller chunk size

    while True:
        print(f"Fetching data with offset {offset} and limit {limit}...")
        results = client.get(
            dataset_id,
            limit=limit,
            offset=offset,
            where=where
        )
        if not results:
            break
        all_results.extend(results)
        offset += limit

    print(f"Total records downloaded: {len(all_results)}")

    # Convert to DataFrame and save
    df = pd.DataFrame.from_records(all_results)
except Exception as e:
    print(f"Error during data download or processing: {e}")
    exit(1)

try:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)

    if blob.exists():
        print(f"Already exists: gs://{bucket_name}/{destination}, skipping upload")
    else:
        print(f"Beginning upload to gs://{bucket_name}/{destination}")
        blob.upload_from_string(df.to_csv(index=False), content_type='text/csv', timeout=UPLOAD_TIMEOUT)
        print(f"Uploaded to gs://{bucket_name}/{destination}")
except Exception as e:
    print(f"Error during GCS upload: {e}")
    exit(1)
