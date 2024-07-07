from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from google.cloud import storage

import sys
import os
import json

# Add the scripts directory to the PYTHONPATH
sys.path.append('/opt/airflow/scripts')

from extraction_cricapi import series_info, match_info
from utils import write_csv, flatten_series_info

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')


output_file = f'series_info.csv'

def upload_to_gcs(bucket_name, local_file_name, destination_object_name):
        print('bucket_name: ', bucket_name)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name=bucket_name)

        blob = bucket.blob(destination_object_name)
        blob.upload_from_filename(local_file_name)

        print(
            f"File {local_file_name} uploaded to {destination_object_name}."
        )
    
def get_series_info():
        series_info_data = series_info(id='d13235de-1bd4-4e5e-87e8-766c21f11661')
        flatten_series_info_df = flatten_series_info(series_info_data)
        write_csv(flatten_series_info_df, output_file)

# Define default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 7, 6),
    "retries": 1,
    "depends_on_past": False
}

# Define the DAG using the decorator
with DAG(
    dag_id="crickapi_extraction_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=2,
    tags=['crickapi-data-analysis']
) as dag:
    
    cricapi_series_info_task = PythonOperator(
           task_id='cricapi_series_info',
           python_callable=get_series_info
    )

    local_to_gcs_task = PythonOperator(
           task_id='local_to_gcs',
           python_callable=upload_to_gcs,
           op_kwargs={
                "bucket_name": BUCKET,
                "local_file_name": output_file,
                "destination_object_name": output_file
           }
    )

    remove_local_file_task = BashOperator(
           task_id='remove_local_file',
           bash_command=f"rm /opt/airflow/{output_file}"
    )


    cricapi_series_info_task >> local_to_gcs_task >> remove_local_file_task


