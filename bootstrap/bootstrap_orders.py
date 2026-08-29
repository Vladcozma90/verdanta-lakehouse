import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_orders" : f"{load_settings.catalog}.bronze.erp_orders",
    }

def bootstrap_bronze_orders(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_orders"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_orders"]} (
                order_id                STRING,
                order_number             STRING,
                customer_id              STRING,
                store_id                 STRING,
                employee_id              STRING,
                sales_channel            STRING,
                order_ts                 STRING,
                order_status             STRING,
                country_code             STRING,
                shipping_country_code    STRING,
                currency_code            STRING,
                payment_method           STRING,
                promotion_code           STRING,
                order_gross_amount       STRING,
                order_discount_amount    STRING,
                order_net_amount         STRING,
                order_vat_amount         STRING,
                created_at               STRING,
                updated_at               STRING,

                run_id                STRING,
                _file_name            STRING,
                _source_system        STRING,
                _ingest_ts            TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", f"{cfg['bronze_orders']}")