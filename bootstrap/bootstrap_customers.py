import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_customers": f"{load_settings.catalog}.bronze.crm_customers",
    }

def bootstrap_bronze_customers(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_customers"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_customers"]} (
                customer_id              STRING,
                first_name               STRING,
                last_name                STRING,
                email                    STRING,
                phone_number             STRING,
                birth_date               STRING,
                preferred_language       STRING,
                loyalty_tier             STRING,
                loyalty_points_balance   STRING,
                marketing_opt_in         STRING,
                address_line_1           STRING,
                city                     STRING,
                postal_code              STRING,
                country_code             STRING,
                customer_status          STRING,
                signup_date              STRING,
                created_at               STRING,
                updated_at               STRING,

                run_id                   STRING,
                _file_name               STRING,
                _source_system           STRING,
                _ingest_ts               TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_customers"])