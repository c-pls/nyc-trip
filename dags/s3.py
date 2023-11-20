import os

import pendulum
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from conf import config

AWS_CONN_ID = config.AWS_CONN_ID


@task(task_id="push_raw_data_s3", provide_context=True)
def push_raw_data_s3(trip_type, raw_output_file_path: str):
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        object_key = (
            f"raw_data/{pendulum.now().format('YYYY/MM/DD')}/{trip_type}/data.parquet"
        )
        s3_hook.load_file(
            filename=raw_output_file_path,
            bucket_name=config.S3_BUCKET_NAME,
            key=object_key,
            replace=True,
        )
        s3_uri = f"s3://{config.S3_BUCKET_NAME}/{object_key}"

        return s3_uri
    except Exception as e:
        raise e
    finally:
        os.remove(raw_output_file_path)


@task(task_id="delete_raw_data")
def delete_raw_data(s3_uri: str):
    def parse_s3_uri(s3_uri: str) -> (str, str):
        """
        Parse S3 URI and extract bucket name and object key.

        Args: s3_uri (str): S3 URI, e.g., s3://bucket-name/path/to/object

        Returns: tuple: (bucket_name, object_key)
        """

        from urllib.parse import urlparse

        parsed_uri = urlparse(s3_uri)

        # Check if the scheme is 's3'
        if parsed_uri.scheme != "s3":
            raise ValueError(
                f"Invalid S3 URI. Scheme must be 's3', got: {parsed_uri.scheme}"
            )

        # Extract bucket name and object key
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path.lstrip("/")

        return bucket_name, object_key

    bucket, object_key = parse_s3_uri(s3_uri)

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.delete_objects(bucket=bucket, keys=object_key)
