from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

import os
from datetime import datetime, timedelta


# ENV variables
DBT_HOME = os.environ.get("DBT_HOME")
DBT_TAXI_DIR = f"{DBT_HOME}taxi/"

default_args = {
    "owner": "airflow",
    "retries": "3",
    "retry_delay": timedelta(minutes=30),
    "catchup": True,
    "tags": ['nyc_taxi']
}


with DAG(
    dag_id='dbt',
    description="DBT execute run and test",
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=True,
    max_active_runs=1,
    default_args=default_args
) as dag:

    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_TAXI_DIR} && dbt run"
    )

    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_TAXI_DIR} && dbt test"
    )

    task_dbt_run >> task_dbt_test
