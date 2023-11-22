from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from conf import config


def truncate_raw_table(trip_type: str):
    truncate_snowflake_raw_table = SnowflakeOperator(
        task_id="truncate_raw_table",
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_RAW_SCHEMA,
        sql="sql/common/truncate_table.sql",
        params={"table_name": config.SNOWFlAKE_RAW_TABLE.get(trip_type)},
    )

    return truncate_snowflake_raw_table


@task(task_id="create_external_stage")
def create_external_stage_s3(trip_type: str, s3_uri: str):
    with open(
        f"{config.HOME_DIR}/include/sql/common/snowflake_external_stage_s3.sql", "r"
    ) as f:
        sql = f.read().format(
            stage_name=f"{trip_type}_stage",
            s3_uri=s3_uri,
            storage_integration=config.SNOWFLAKE_AWS_INTEGRATION,
            file_format="(type='parquet')",
        )

        sf_hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_RAW_SCHEMA,
        )
        sf_hook.run(sql=sql)


@task(task_id="load_into_raw_table")
def load_to_raw_table(trip_type: str):
    stage_name = f"{trip_type}_stage"

    with open(
        f"{config.HOME_DIR}/include/sql/trip/{trip_type}/copy_command.sql", "r"
    ) as f:
        sql = f.read().format(
            table=config.SNOWFlAKE_RAW_TABLE.get(trip_type), stage_name=stage_name
        )

        sf_hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_RAW_SCHEMA,
        )
        sf_hook.run(sql=sql)


def insert_to_production_table(trip_type: str):
    insert_to_production_table = SnowflakeOperator(
        task_id="insert_to_production_table",
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        database=config.SNOWFLAKE_DATABASE,
        schema=config.SNOWFLAKE_PRODUCTION_SCHEMA,
        sql=f"sql/trip/{trip_type}/insert.sql",
        params={
            "target_table": f"{config.SNOWFLAKE_PRODUCTION_SCHEMA}.{config.SNOwFLAKE_FACT_TABLE.get(trip_type)}",
            "source_table": f"{config.SNOwFLAKE_STAGING_SCHEMA}.{config.SNOwFLAKE_FACT_TABLE.get(trip_type)}",
        },
    )

    return insert_to_production_table
