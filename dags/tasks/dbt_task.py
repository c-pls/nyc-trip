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
            "schema": "STAGING",
        },
    ),
)


def dbt_task():
    dbt_tg = DbtTaskGroup(
        group_id="dbt_transform_fact_table",
        project_config=ProjectConfig(
            dbt_project_path=Path(f"{config.PROJECT_ROOT}/dbt/nyc_trip")
        ),
        profile_config=profile_config,
        default_args={"retries": 0},
    )
    return dbt_tg
