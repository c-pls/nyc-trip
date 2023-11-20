from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from airflow.decorators import task
from config import config
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os


@task(task_id="snowflake")
def sf():
    query = "SELECT count(*) FROM OPENADDRESS;"

    sf_hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
        database="WORLDWIDE_ADDRESS_DATA",
        schema="ADDRESS",
    )

    res = sf_hook.get_first(sql=query)

    print("Result: ")
    print(res[0])


def test():
    sf_operator = SnowflakeOperator(
        task_id="sf_test",
        sql=f"{os.getenv('AIRFLOW_HOME')}/include/test.sql",
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        database="WORLDWIDE_ADDRESS_DATA",
        schema="ADDRESS",
    )
    return sf_operator


def load_to_s3():
    key = "https://de-666ne92p.s3.ap-southeast-1.amazonaws.com/raw_data/2023/11/04/data.csv"

    copy_into_table = S3ToSnowflakeOperator(
        task_id="copy_from_s3_to_snowflake",
        snowflake_conn_id=config.SNOWFLAKE_CONN_ID,
        database="TWICE",
        table="HIRE_VEHICLES_RAW",
        schema="RAW",
        stage="HIRE_VEHICLES_STAGE",
        file_format="csv",
    )
    return copy_into_table
