import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_returns": f"{load_settings.catalog}.bronze.erp_returns",
    }

def bootstrap_bronze_returns(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_returns"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_returns"]} (
                return_id                STRING,
                order_id                 STRING,
                order_item_id            STRING,
                product_id               STRING,
                return_store_id          STRING,
                return_ts                STRING,
                return_reason_code       STRING,
                quantity_returned        STRING,
                refund_amount_local      STRING,
                currency_code            STRING,
                restock_flag             STRING,
                created_at               STRING,
                updated_at               STRING,

                run_id                   STRING,
                _file_name               STRING,
                _source_system           STRING,
                _ingest_ts               TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_returns"])