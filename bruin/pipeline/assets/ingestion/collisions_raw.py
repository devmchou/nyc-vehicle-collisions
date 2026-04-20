""" @bruin

name: ingestion.collisions_raw
type: python
description: "Create BigQuery external table pointing to all CSVs in GCS"
depends: [ingestion.download]

@bruin """
import os
from google.cloud import bigquery
from dotenv import load_dotenv

# This loads variables from .env into os.environ
load_dotenv() 

project_id  = os.environ["GCP_PROJECT_ID"]
bucket_name = os.environ["GCS_BUCKET_NAME"]
files_pattern = "Collisions_*.csv"

client = bigquery.Client(project=project_id)

# (
#   crash_date    STRING,
#   crash_time     STRING,
#   borough     STRING,
#   zip_code     STRING,
#   latitude    STRING,
#   longitude    STRING,
#   location STRING,
#   cross_street_name STRING,
#   number_of_persons_injured STRING,
#   number_of_persons_killed STRING,
#   number_of_pedestrians_injured STRING,
#   number_of_pedestrians_killed STRING,
#   number_of_cyclist_injured STRING,
#   number_of_cyclist_killed STRING,
#   number_of_motorist_injured STRING,
#   number_of_motorist_killed STRING,
#   contributing_factor_vehicle_1 STRING,
#   contributing_factor_vehicle_2 STRING,
#   collision_id STRING,
#   vehicle_type_code1 STRING,
#   vehicle_type_code2 STRING,
#   on_street_name STRING,
#   off_street_name STRING,
#   contributing_factor_vehicle_3 STRING,
#   contributing_factor_vehicle_4 STRING,
#   contributing_factor_vehicle_5 STRING,
#   vehicle_type_code3 STRING,
#   vehicle_type_code4 STRING,
#   vehicle_type_code5 STRING
# )

# OPTIONS (
#   format                = 'CSV',
#   skip_leading_rows     = 1,  -- each file has a header row
#   source_column_match   = NAME,  -- use column names from the header row
#   uris                  = ['gs://{bucket_name}/{files_pattern}'],
#   allow_jagged_rows     = true,
#   ignore_unknown_values = true
#   )

ddl = f"""
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.ingestion.collisions_raw`
OPTIONS
(
  format                = 'CSV',
  skip_leading_rows     = 1,  -- each file has a header row
  source_column_match   = NAME,  -- use column names from the header row
  uris                  = ['gs://{bucket_name}/{files_pattern}'],
  allow_jagged_rows     = true,
  ignore_unknown_values = true
  )
"""

job = client.query(ddl)
job.result()
print(f"Created external table {project_id}.ingestion.collisions_raw")
print(f"Loaded data, source: gs://{bucket_name}/{files_pattern}")