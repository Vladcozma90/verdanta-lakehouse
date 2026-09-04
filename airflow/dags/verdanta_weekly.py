from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import Variable, dag
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_DIR = "/opt/airflow/dbt/verdanta_project"
VERDANTA_ENV = Variable.get("verdanta_env", "dev")

project_config = ProjectConfig(DBT_PROJECT_DIR)

profile_config = ProfileConfig(
    profile_name="verdanta_project",
    target_name=VERDANTA_ENV,
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={
            "catalog": f"verdanta_{VERDANTA_ENV}",
            "schema": "silver",
            "http_path": "/sql/1.0/warehouses/6b5884a11a6d64b4",
        }
    ),
)


@dag(
    dag_id="verdanta_weekly",
    schedule="0 5 * * 1",
    start_date=datetime(2026, 7, 28, tzinfo=ZoneInfo("Europe/Bucharest")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["verdanta", "weekly"],
)
def verdanta_weekly():

    bronze_weekly = DatabricksRunNowOperator(
        task_id="bronze_ingestion_weekly",
        databricks_conn_id="databricks_default",
        job_name=f"verdanta_bronze_ingestion_weekly_{VERDANTA_ENV}",
        job_parameters={"env": VERDANTA_ENV},
    )

    dbt_weekly = DbtTaskGroup(
        group_id="dbt_weekly",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "stg_hr__employees+",
                "stg_pim__suppliers+",
            ]
        ),
    )

    bronze_weekly >> dbt_weekly

verdanta_weekly()