# track folder data containing our useful functions

import sys
sys.path.append("/home/ubuntu/SolarPanelsNasa/data_france")

# import in the dag file any librairies needed for the project

import pandas as pd
import datetime
import urllib.request
import unidecode
from unidecode import unidecode
from datetime import datetime, timedelta
from france_etl_functions import *

from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'donatello',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 5, 21, 15, 0, 0),
}

# Create the DAG object
dag = DAG(
    'france_weather_data_pipeline',
    default_args=default_args,
    description='An end-to-end france weather data pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Define the extract_data task
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Define the transform_data task
transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    op_args=[extract_task.output],
    provide_context=True,
    dag=dag,
)

# Define the save_to_local task
load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    op_args=[transform_task.output],
    op_kwargs={'execution_date': '{{ ts }}'},
    dag=dag,
)

# Set the task dependencies
extract_task >> transform_task >> load_task
