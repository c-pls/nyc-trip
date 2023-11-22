from airflow.decorators import task


@task.external_python(
    python="/opt/airflow/soda_venv/bin/python", task_id="run_soda_scan"
)
def check_load_raw():
    PROJECT_ROOT = "/opt/airflow"

    def run_soda_scan(project_root, scan_name, checks_subpath=None):
        from soda.scan import Scan

        print("Running Soda Scan ...")
        config_file = f"{project_root}/soda/configuration.yml"
        checks_path = f"{project_root}/soda/checks"

        if checks_subpath:
            checks_path += f"/{checks_subpath}"

        data_source = "nyc_trip"

        scan = Scan()
        scan.set_verbose()
        scan.add_configuration_yaml_file(config_file)
        scan.set_data_source_name(data_source)
        scan.add_sodacl_yaml_files(checks_path)
        scan.set_scan_definition_name(scan_name)

        result = scan.execute()
        print(scan.get_logs_text())

        if result != 0:
            raise ValueError("Soda Scan failed")

        return result

    run_soda_scan(PROJECT_ROOT, "check_ingest_raw", checks_subpath="raw_data_check.yml")
