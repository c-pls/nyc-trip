from datetime import timedelta

from airflow import DAG
from airflow.decorators import task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from pull_raw_data import pull_raw_data
from s3 import push_raw_data_s3, delete_raw_data
from sf import (
    create_external_stage_s3,
    load_to_raw_table,
    truncate_raw_table,
)
from dbt_task import dbt_task

default_args = {
    "owner": "Chinh Pham",
    "retries": 1,
    "retry_delay": timedelta(seconds=0),
    "depends_on_past": False,
    "email_on_failure": False,
}


@task_group(group_id="ingestion")
def ingestion_groups():
    trip_type_list = ["yellow", "green"]

    def build_group(trip_type: str):
        @task_group(group_id=f"{trip_type}")
        def ingestion():
            @task_group(group_id="snowflake", tooltip="Snowflake workflow")
            def sf_task(s3_uri: str):
                [
                    truncate_raw_table(trip_type),
                    create_external_stage_s3(trip_type, s3_uri),
                ] >> load_to_raw_table(trip_type)

            raw_output_file_path = pull_raw_data(trip_type)

            s3_uri = push_raw_data_s3(trip_type, raw_output_file_path)

            chain(
                s3_uri,
                sf_task(s3_uri),
                delete_raw_data(s3_uri),
            )

        return ingestion()

    return list(map(build_group, trip_type_list))


with DAG(
    dag_id="full_pipeline",
    default_args=default_args,
    start_date=days_ago(n=0),
    schedule=None,
    catchup=False,
    template_searchpath="include",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> ingestion_groups() >> dbt_task() >> end