from airflow.operators.bash import BashOperator
from conf import config


def soda_scan(
    task_id: str,
    data_source: str,
    soda_configuration_path: str,
    sodacl_yaml_file_path: str,
):
    activate_virtualenv_command = f"source {config.PROJECT_ROOT}/soda_venv/bin/activate"
    soda_scan_command = f"soda scan --data-source {data_source} --configuration  {soda_configuration_path} {sodacl_yaml_file_path}"
    deactivate_virtualenv_command = "deactivate"

    soda_scan = BashOperator(
        task_id=task_id,
        bash_command=f"{activate_virtualenv_command} && {soda_scan_command} && {deactivate_virtualenv_command}",
    )

    return soda_scan
