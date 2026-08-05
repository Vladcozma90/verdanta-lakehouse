import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_order_items": f"{load_settings.catalog}.bronze.bronze_order_items",
    }

def bootstrap_bronze_order_items(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_order_items"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_order_items"]} (
                order_item_id            STRING,
                order_id                 STRING,
                line_number              STRING,
                product_id               STRING,
                quantity                 STRING,
                unit_price_local         STRING,
                discount_amount_local    STRING,
                line_gross_amount_local  STRING,
                line_net_amount_local    STRING,
                vat_rate                 STRING,
                vat_amount_local         STRING,
                unit_cost_eur            STRING,
                currency_code            STRING,
                created_at               STRING,
                updated_at               STRING,

                run_id                STRING,
                _file_name            STRING,
                _source_system        STRING,
                _ingest_ts            TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_order_items"])