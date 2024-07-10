import sys
sys.path.append('/opt/airflow/code/api_handler')
sys.path.append('/opt/airflow/code/utils')

from cricbuzzapi_utils import normalize_series_matches, normalize_match
from file_utils import write_csv
from gcp_utils import upload_to_gcs
from cricbuzz_api import CricBuzzHandler
from logger_utils import logging

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# from code.api_handler.cricbuzz_api import CricBuzzHandler
# from code.utils.cricbuzzapi_utils import normalize_series_matches, normalize_match
# from code.utils.file_utils import write_csv
# from code.utils.gcp_utils import upload_to_gcs

import os
import pandas as pd


PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
CSV_FILE_FOLDER = "/opt/airflow/cricbuzz_api_data"
GSUTIL = '/home/google-cloud-sdk/bin/gsutil'

def fetch_cricet_series_data_from_cricbuzz_api(series_id):
    logging.info(f'serisesid: {series_id}')
    cricbuzz = CricBuzzHandler()
    series_data = cricbuzz.get_series_matches(series_id=series_id)
    if series_data:
        series_df, match_ids = normalize_series_matches(series_data)
        logging.info(f"MatchIds: {match_ids}")
        # count = 1
        match_df = None
        for id in list(match_ids):
            match_data = cricbuzz.get_match_info(match_id=id)
            match_data = normalize_match(match_data)
            
            if match_df is None:
                match_df = match_data.copy()  # Initialize match_df with the first DataFrame
            else:
                match_df = pd.concat([match_df, match_data], ignore_index=True)  # Concatenate new DataFrame
            
            # if count == 5:
            #     break

            # count += 1


    if 'series_df' in locals() and not series_df.empty:
        write_csv(series_df, f'{CSV_FILE_FOLDER}/series/series_{datetime.now().strftime("%y_%m_%d")}.csv')
    if match_df is not None and not match_df.empty:
        write_csv(match_df, f'{CSV_FILE_FOLDER}/matches/match_{datetime.now().strftime("%y_%m_%d")}.csv')

def print_env():
    logging.info(f'GOOGLE_APPLICATION_CREDENTIALS: {GOOGLE_APPLICATION_CREDENTIALS}')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 7, 6),
    "retries": 1,
    'retry_exponential_backoff': True,
    'retry_delay': timedelta(minutes=1),  # Initial delay of 1 minute
    "depends_on_past": False
}

with DAG(
    dag_id="data_ingestion_gcs_cricbuzz_api",
    schedule_interval=None,
    start_date=datetime(2024, 7, 8),
    default_args=default_args,
    max_active_runs=3,
    tags=['cricapi-data-ingestion']
) as dag:
    
    extract_cricbuzz_api = PythonOperator(
        task_id='download_data_from_cricbuzz_api',
        python_callable=fetch_cricet_series_data_from_cricbuzz_api,
        op_kwargs={
            "series_id": "7476"
        }
    ) 

    
    local_to_gcs_task = BashOperator(
           task_id='local_to_gcs',
           bash_command=f'{GSUTIL} -m cp -r /opt/airflow/cricbuzz_api_data/ gs://{BUCKET}/',
           env={'GOOGLE_APPLICATION_CREDENTIALS': GOOGLE_APPLICATION_CREDENTIALS},
    )

    remove_local_file_task = BashOperator(
           task_id='remove_local_file',
           bash_command=f"rm -r {CSV_FILE_FOLDER}/"
    )

    extract_cricbuzz_api >> local_to_gcs_task >> remove_local_file_task


