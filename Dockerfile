FROM apache/airflow

WORKDIR "/opt/airflow"

RUN pip install --upgrade pip
COPY requirements.txt ./
RUN pip install -r requirements.txt


# useful when working with Managed Airflow (AWS MWAA )
# COPY dbt-requirements.txt ./
# RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-user --no-cache-dir -r dbt-requirements.txt && deactivate