import os
from dataclasses import dataclass


@dataclass
class Config:
    HOME_DIR = os.getenv("AIRFLOW_HOME")

    BASE_DATASOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    AWS_CONN_ID = "aws_conn"

    S3_BUCKET_NAME = "de-666ne92p"

    SNOWFLAKE_CONN_ID = "snowflake_conn"

    SNOWFLAKE_AWS_INTEGRATION = "AWS_INTEGRATION"

    SNOWFLAKE_DATABASE = "NYC_TRIP"

    SNOWFLAKE_RAW_SCHEMA = "RAW"

    SNOWFlAKE_RAW_TABLE = {
        "yellow": "YELLOW_TRIP_RECORD_RAW",
        "green": "GREEN_TRIP_RECORD_RAW",
    }

    SNOwFLAKE_STAGING_SCHEMA = "STAGING"

    SNOWFLAKE_PRODUCTION_SCHEMA = "PROD"

    SNOwFLAKE_FACT_TABLE = {
        "yellow": "FACT_YELLOW_TRIP",
        "green": "FACT_GREEN_TRIP",
    }


config = Config()
