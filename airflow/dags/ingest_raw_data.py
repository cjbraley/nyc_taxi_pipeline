from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


import os
from datetime import datetime, timedelta


# ENV variables
AIRFLOW_HOME_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_BUCKET_RAW = os.environ.get("GCS_BUCKET_RAW")

GCS_BUCKET_PATH = f"{GCP_PROJECT_ID}_{GCS_BUCKET_NAME}"


def workflow(dag, config):

    with dag:
        # download the data to local
        task_download_data = BashOperator(
            task_id="download_from_src",
            bash_command=f"curl -sSlf {config['src_url']}/{config['file_name_template']} > {AIRFLOW_HOME_DIR}/{config['file_name_template']}"

        )

        # upload to gcs
        task_upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id="upload_to_gcs",
            src=f"{AIRFLOW_HOME_DIR}/{config['file_name_template']}",
            bucket=GCS_BUCKET_PATH,
            dst=f"{GCS_BUCKET_RAW}/{config['dest_url']}/{config['file_name_template']}"
        )

        # remove the tmp file
        task_remove_local_file = BashOperator(
            task_id="remove_local_file",
            bash_command=f"rm {AIRFLOW_HOME_DIR}/{config['file_name_template']}"
        )

        task_download_data >> task_upload_to_gcs >> task_remove_local_file


# DAG
default_args = {
    "owner": "airflow",
    "retries": "3",
    "retry_delay": timedelta(minutes=30),
    "catchup": True,
    "tags": ['nyc_taxi']
}


# Yellow taxi
YELLOW_TAXI_CONFG = {
    "src_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
    "file_name_template": "yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet",
    "dest_url": "yellow_tripdata"
}

yellow_taxi_dag = DAG(
    dag_id="ingest_yellow_taxi_data",
    description="This dag fownloads the raw taxi data and copies it into GCS",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    schedule_interval="0 6 2 * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
)

workflow(dag=yellow_taxi_dag, config=YELLOW_TAXI_CONFG)


# Green taxi
GREEN_TAXI_CONFG = {
    "src_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
    "file_name_template": "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet",
    "dest_url": "green_tripdata"
}

green_taxi_dag = DAG(
    dag_id="ingest_green_taxi_data",
    description="This dag downloads the raw taxi data and copies it into GCS",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    schedule_interval="0 6 2 * *",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
)

workflow(dag=green_taxi_dag, config=GREEN_TAXI_CONFG)


# zones
ZONES_TAXI_CONFG = {
    "src_url": "https://d37ci6vzurychx.cloudfront.net/misc",
    "file_name_template": "taxi+_zone_lookup.csv",
    "dest_url": "taxi_zone"
}

zones_taxi_dag = DAG(
    dag_id="ingest_zone_taxi_data",
    description="This dag downloads the taxi zone lookup and copies it into GCS",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
)

workflow(dag=zones_taxi_dag, config=ZONES_TAXI_CONFG)
