import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_employees": f"{load_settings.catalog}.bronze.hr_employees",
    }

def bootstrap_bronze_employees(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_employees"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_employees"]} (
                employee_id              STRING,
                first_name               STRING,
                last_name                STRING,
                work_email               STRING,
                job_title                STRING,
                department               STRING,
                store_id                 STRING,
                manager_employee_id      STRING,
                country_code             STRING,
                hire_date                STRING,
                termination_date         STRING,
                employment_status        STRING,
                created_at               STRING,
                updated_at               STRING,

                run_id                   STRING,
                _file_name               STRING,
                _source_system           STRING,
                _ingest_ts               TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_employees"])