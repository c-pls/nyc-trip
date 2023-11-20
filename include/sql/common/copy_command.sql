COPY INTO {database}.{schema}.{table}
FROM {s3_key}
STORAGE_INTEGRATION = {storage_integration}
file_format={file_format};