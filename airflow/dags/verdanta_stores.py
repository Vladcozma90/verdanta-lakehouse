from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import Variable, dag
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DatabricksTokenProfileMapping

DBT_PROJECT_DIR = "/opt/airflow/dbt/verdanta_project"
VERDANTA_ENV = Variable.get("verdanta_var", "dev")

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
    dag_id="verdanta_stores",
    schedule=None,
    start_date=datetime(2026, 7, 28, tzinfo=ZoneInfo("Europe/Bucharest")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["verdanta", "on-demand"],
)
def verdanta_stores():

    bronze_stores = DatabricksRunNowOperator(
        task_id = "bronze_ingestion_stores",
        databricks_conn_id="databricks_default",
        job_name="verdanta_bronze_ingestion_stores",
        job_parameters={"ENV": VERDANTA_ENV},
    )

    dbt_stores = DbtTaskGroup(
        group_id="dbt_sources",
        profile_config=profile_config,
        project_config=project_config,
        render_config=RenderConfig(
            select=["stg_reference_stores+"]
        )
    )

    bronze_stores >> dbt_stores

verdanta_stores()