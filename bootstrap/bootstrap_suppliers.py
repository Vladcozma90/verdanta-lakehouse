import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_suppliers": f"{load_settings.catalog}.bronze.pim_suppliers",
    }

def bootstrap_bronze_suppliers(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_suppliers"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_suppliers"]} (
                supplier_id                  STRING,
                supplier_name                STRING,
                supplier_country_code        STRING,
                lead_time_days               STRING,
                is_preferred_supplier        STRING,
                payment_terms_days           STRING,
                onboarded_date               STRING,
                supplier_status              STRING,
                created_at                   STRING,
                updated_at                   STRING,

                run_id                       STRING,
                _file_name                   STRING,
                _source_system               STRING,
                _ingest_ts                   TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_suppliers"])