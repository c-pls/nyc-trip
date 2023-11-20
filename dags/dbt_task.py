from pathlib import Path
from conf import config
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
        profile_args={
            "database": config.SNOWFLAKE_DATABASE,
            "schema": "RAW",
        },
    ),
)


def dbt_task():
    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
            dbt_project_path=Path(f"{config.HOME_DIR}/dbt/nyc_trip")
        ),
        profile_config=profile_config,
        default_args={"retries": 0},
    )
    return dbt_tg
