from datetime import timedelta

from airflow import DAG
from airflow.decorators import task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from conf import config
from tasks.dbt_task import dbt_task
from tasks.pull_raw_data import pull_raw_data
from tasks.s3 import delete_raw_data, push_raw_data_s3
from tasks.sf import (
    create_external_stage_s3,
    insert_to_production_table,
    load_to_raw_table,
    truncate_raw_table,
)
from tasks.soda_task import soda_scan

TRIP_TYPE_LIST = ["yellow", "green", "fhvhv"]


default_args = {
    "owner": "Chinh Pham",
    "retries": 1,
    "retry_delay": timedelta(seconds=0),
    "depends_on_past": False,
    "email_on_failure": False,
}


@task_group(group_id="ingestion")
def ingestion_groups():
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
                [
                    delete_raw_data(s3_uri),
                    soda_scan(
                        task_id=f"scan_{trip_type}_raw_data",
                        data_source=config.SODA_DATASOURCE,
                        soda_configuration_path=f"{config.PROJECT_ROOT}/soda/soda_config/raw/configuration.yml",
                        sodacl_yaml_file_path=f"{config.PROJECT_ROOT}/soda/checks/raw_ingest/{trip_type}/check.yml",
                    ),
                ],
            )

        return ingestion()

    return list(map(build_group, TRIP_TYPE_LIST))


@task_group(group_id="load_to_fact_table")
def load_to_fact_table_group():
    def build_group(trip_type: str):
        @task_group(group_id=f"{trip_type}")
        def load_to_fact_table():
            insert_to_production_table(trip_type)

        return load_to_fact_table()

    return list(map(build_group, TRIP_TYPE_LIST))


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
    trip_type = "yellow"
    (
        start
        >> ingestion_groups()
        >> soda_scan(
            task_id="scan_dim_table",
            data_source=config.SODA_DATASOURCE,
            soda_configuration_path=f"{config.PROJECT_ROOT}/soda/soda_config/prod/configuration.yml",
            sodacl_yaml_file_path=f"{config.PROJECT_ROOT}/soda/checks/dim_table/check.yml",
        )
        >> dbt_task()
        >> load_to_fact_table_group()
        >> end
    )

with DAG(
    dag_id="test",
    default_args=default_args,
    start_date=days_ago(n=0),
    schedule=None,
    catchup=False,
    template_searchpath="include",
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    (
        start
        >> soda_scan(
            task_id="test",
            data_source="nyc_trip",
            soda_configuration_path=f"{config.PROJECT_ROOT}/soda/configuration.yml",
            sodacl_yaml_file_path=f"{config.PROJECT_ROOT}/soda/checks/raw_data_check.yml",
        )
        >> end
    )
