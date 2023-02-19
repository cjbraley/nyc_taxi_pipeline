from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
import os
from datetime import timedelta

# ENV variables
AIRFLOW_HOME_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_BUCKET_RAW = os.environ.get("GCS_BUCKET_RAW")
GCS_BUCKET_TRANSFORM = os.environ.get("GCS_BUCKET_TRANSFORM")

GCS_BUCKET_PATH = f"{GCP_PROJECT_ID}_{GCS_BUCKET_NAME}"


default_args = {
    "owner": "airflow",
    "retries": "3",
    "retry_delay": timedelta(minutes=30),
    "catchup": True,
    "tags": ['nyc_taxi']
}

config = [
    {
        "table_name": "yellow_trips",
        "gcs_src_path": "yellow_tripdata/*.parquet",
        "partition_field": "DATE(tpep_pickup_datetime)"
    },
    {
        "table_name": "green_trips",
        "gcs_src_path": "green_tripdata/*.parquet",
        "partition_field": "DATE(lpep_pickup_datetime)"
    },
    {
        "table_name": "zones",
        "gcs_src_path": "taxi_zone/*.parquet",
        "partition_field": None
    }
]

with DAG(
    dag_id="create_bigquery_tables",
    description="Create the external tables for taxi data in BigQuery",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
) as dag:

    for dataset in config:
        table_name, gcs_src_path, partition_field = dataset[
            "table_name"], dataset["gcs_src_path"], dataset["partition_field"]

        task_external_table = BigQueryCreateExternalTableOperator(
            task_id=f"create_external_table_{table_name}",
            table_resource={
                "type": "EXTERNAL",
                "tableReference": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": GCS_BUCKET_TRANSFORM,
                    "tableId": f"{table_name}_external"
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{GCS_BUCKET_PATH}/transform/{gcs_src_path}"]
                },
            },
        )

        if partition_field is not None:

            SQL_QUERY = f"CREATE OR REPLACE TABLE transform.{table_name} PARTITION BY {partition_field} AS SELECT * FROM {GCS_BUCKET_TRANSFORM}.{table_name}_external;"

            task_partitioned_table = BigQueryInsertJobOperator(
                task_id=f"create_partitioned_table_{table_name}",
                configuration={
                    "query": {
                        "query": SQL_QUERY,
                        "useLegacySql": False
                    }
                }
            )

            task_external_table >> task_partitioned_table
