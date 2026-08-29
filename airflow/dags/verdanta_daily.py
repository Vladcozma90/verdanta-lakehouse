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
    dag_id="verdanta_daily",
    schedule="0 4 * * *",
    start_date=datetime(2026, 7, 28, tzinfo=ZoneInfo("Europe/Bucharest")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["verdanta", "daily"],
)
def verdanta_daily():

     land = DatabricksRunNowOperator(
          task_id="land_to_landing",
          databricks_conn_id="databricks_default",
          job_name="verdanta_land_to_landing",
          job_parameters={"env": VERDANTA_ENV, "ingest_date": "{{ ds }}"}
     )

     bronze_daily = DatabricksRunNowOperator(
          task_id="bronze_ingestion_daily",
          databricks_conn_id="databricks_default",
          job_name="verdanta_bronze_ingestion_daily",
          job_parameters={"env": VERDANTA_ENV}
     )

     dbt_daily = DbtTaskGroup(
          group_id="dbt_daily",
          project_config=project_config,
          profile_config=profile_config,
          render_config=RenderConfig(
               select=[
                    "stg_erp__orders+",
                    "stg_erp__order_items+",
                    "stg_erp__returns+",
                    "stg_crm__customers+",
                    "stg_pim__products+",
                    "snap_crm_customers+",
                    "snap_pim_products+",
                    "dim_country",
                    "dim_date",
               ]
          ),
     )

     land >> bronze_daily >> dbt_daily

verdanta_daily()