import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator

# ENV variables
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_REGION = os.environ.get("GCP_REGION")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_BUCKET_RAW = os.environ.get("GCS_BUCKET_RAW")
GCP_DATAPROC_CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME")
SPARK_HOME = os.environ.get("SPARK_HOME")

CREATE_DESTROY_INFRA = False


GCS_BUCKET_PATH = f"{GCP_PROJECT_ID}_{GCS_BUCKET_NAME}"

# CONFIG
cluster_config = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
}


"""
    1. Transfer spark script to gcs so that it can be accessed by the dataproc cluster
    2. Create cluster
    3. Run spark job
    4. Destroy Cluster

    If doing a historical run, can disabled the creation and destruction of the cluster on every instace using CREATE_DESTROY_INFRA = False above
"""


def workflow(dag, config):

    ingest_task_id = config["ingest_task_id"]
    spark_file = config["spark_file"]
    spark_arguments = config["spark_arguments"]

    with dag:
        task_ingest_sensor = ExternalTaskSensor(
            task_id="ingest_sensor",
            external_dag_id=ingest_task_id,
            timeout=600,
            allowed_states=["success"],
            mode="reschedule",
        )

        task_upload_spark_file = LocalFilesystemToGCSOperator(
            task_id="upload_pyspark_file",
            src=f"{SPARK_HOME}/{spark_file}",
            dst=f"spark/{spark_file}",
            bucket=GCS_BUCKET_PATH
        )

        if CREATE_DESTROY_INFRA:
            task_create_dataproc_cluster = DataprocCreateClusterOperator(
                task_id="create_cluster",
                project_id=GCP_PROJECT_ID,
                cluster_name=GCP_DATAPROC_CLUSTER_NAME,
                region=GCP_REGION,
                cluster_config=cluster_config
            )

        task_run_spark_job = DataprocSubmitPySparkJobOperator(
            task_id="submit_dataproc_spark_job_task",
            main=f"gs://{GCS_BUCKET_PATH}/spark/{spark_file}",
            arguments=spark_arguments,
            cluster_name=GCP_DATAPROC_CLUSTER_NAME,
            region=GCP_REGION,
            dataproc_jars=[]
        )

        if CREATE_DESTROY_INFRA:
            task_delete_dataproc_cluster = DataprocDeleteClusterOperator(
                task_id="delete_cluster",
                project_id=GCP_PROJECT_ID,
                cluster_name=GCP_DATAPROC_CLUSTER_NAME,
                region=GCP_REGION,
                trigger_rule="all_done",
            )

        task_ingest_sensor >> task_upload_spark_file >> task_run_spark_job

        if CREATE_DESTROY_INFRA:
            task_ingest_sensor >> task_upload_spark_file >> task_create_dataproc_cluster >> task_run_spark_job >> task_delete_dataproc_cluster


# Create DAG instances for different data types
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "tags": ['nyc_taxi']
}

# YELLOW TAXI
yellow_taxi_dag = DAG(
    dag_id="transform_yellow_taxi_data",
    description="Transform raw yellow data using spark on dataproc",
    catchup=True,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    default_args=default_args
)

YELLOW_TAXI_CONFIG = {
    "ingest_task_id": "ingest_yellow_taxi_data",
    "spark_file": "transform_yellow_taxi.py",
    "spark_arguments": [GCS_BUCKET_PATH, "yellow_tripdata/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet", "yellow_tripdata/{{ execution_date.strftime(\'%Y-%m\') }}/", "{{ execution_date.strftime(\'%Y-%m-%d\') }}"]
}

workflow(yellow_taxi_dag, YELLOW_TAXI_CONFIG)

# GREEN TAXI
green_taxi_dag = DAG(
    dag_id="transform_green_taxi_data",
    description="Transform raw green data using spark on dataproc",
    catchup=True,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2022, 1, 1),
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    default_args=default_args
)

GREEN_TAXI_CONFIG = {
    "ingest_task_id": "ingest_green_taxi_data",
    "spark_file": "transform_green_taxi.py",
    "spark_arguments": [GCS_BUCKET_PATH, "green_tripdata/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet", "green_tripdata/{{ execution_date.strftime(\'%Y-%m\') }}/", "{{ execution_date.strftime(\'%Y-%m-%d\') }}"]
}

workflow(green_taxi_dag, GREEN_TAXI_CONFIG)

# TAXI ZONE
taxi_zone_dag = DAG(
    dag_id="transform_zone_taxi_data",
    description="Transform raw taxi zone data using spark on dataproc",
    catchup=True,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    max_active_runs=1,
    default_args=default_args
)

ZONES_TAXI_CONFIG = {
    "ingest_task_id": "ingest_zone_taxi_data",
    "spark_file": "transform_taxi_zone.py",
    "spark_arguments": [GCS_BUCKET_PATH, "taxi_zone/taxi+_zone_lookup.csv", "taxi_zone/"]
}

workflow(taxi_zone_dag, ZONES_TAXI_CONFIG)
