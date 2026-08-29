import logging

from pyspark.sql import SparkSession

from src.verdanta.common.config import Settings

logger = logging.getLogger(__name__)

def _build_config(load_settings: Settings) -> dict[str, str]:
    return {
        "bronze_products": f"{load_settings.catalog}.bronze.pim_products",
    }

def bootstrap_bronze_products(spark: SparkSession, load_settings: Settings) -> None:

    cfg = _build_config(load_settings=load_settings)

    logger.info("Creating/validating table %s", cfg["bronze_products"])

    spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {cfg["bronze_products"]} (
                product_id            STRING,
                product_name          STRING,
                brand                 STRING,
                is_own_brand          STRING,
                category              STRUCT<
                                           category_l1: STRING,
                                           category_l2: STRING,
                                           category_l3: STRING
                                       >,
                supplier_ids          ARRAY<STRING>,
                unit_cost_eur         STRING,
                list_price_eur        STRING,
                vat_class             STRING,
                attributes            STRUCT<
                                           colour: STRING,
                                           material: STRING,
                                           weight_kg: STRING,
                                           is_seasonal: STRING,
                                           peak_season: STRING
                                       >,
                launch_date           STRING,
                discontinued_date     STRING,
                product_status        STRING,
                created_at            STRING,
                updated_at            STRING,

                run_id                STRING,
                _file_name            STRING,
                _source_system        STRING,
                _ingest_ts            TIMESTAMP
            )
            USING DELTA
            """)

    logger.info("Ensure table exists: %s", cfg["bronze_products"])