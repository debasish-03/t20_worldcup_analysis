import sys
sys.path.append('/opt/airflow/code/api_handler')
sys.path.append('/opt/airflow/code/utils')

from cricapp_api import CricApiHandler
from file_utils import write_csv
from gcp_utils import upload_to_gcs
from cricapi_utils import flatten_series_info

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# from code.api_handler.cricapp_api import series_info
# from code.utils.file_utils import write_csv, flatten_series_info
# from code.utils.gcp_utils import upload_to_gcs

import sys
import os

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')


output_file = f'series_info.csv'

    
def get_series_info():
       cricapp = CricApiHandler()
       series_info_data = cricapp.series_info(id='d13235de-1bd4-4e5e-87e8-766c21f11661')
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


