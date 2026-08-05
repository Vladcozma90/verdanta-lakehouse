import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_stores": f"{load_settings.catalog}.bronze.reference_stores",
    }

def bootstrap_bronze_stores(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_stores"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_stores"]} (
                store_id                     STRING,
                store_name                   STRING,
                store_format                 STRING,
                address_line_1               STRING,
                city                         STRING,
                postal_code                  STRING,
                country_code                 STRING,
                latitude                     STRING,
                longitude                    STRING,
                selling_area_sqm             STRING,
                store_manager_employee_id    STRING,
                opening_date                 STRING,
                closing_date                 STRING,
                store_status                 STRING,
                created_at                   STRING,
                updated_at                   STRING,

                run_id                       STRING,
                _file_name                   STRING,
                _source_system               STRING,
                _ingest_ts                   TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_stores"])