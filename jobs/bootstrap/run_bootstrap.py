
from pyspark.sql import SparkSession

from bootstrap.bootstrap_customers import bootstrap_bronze_customers
from bootstrap.bootstrap_employees import bootstrap_bronze_employees
from bootstrap.bootstrap_order_items import bootstrap_bronze_order_items
from bootstrap.bootstrap_orders import bootstrap_bronze_orders
from bootstrap.bootstrap_products import bootstrap_bronze_products
from bootstrap.bootstrap_returns import bootstrap_bronze_returns
from bootstrap.bootstrap_stores import bootstrap_bronze_stores
from src.verdanta.common.config import Settings, load_settings


def run_bootstrap() -> None:


    spark = SparkSession.builder.getOrCreate()
    cfg: Settings = load_settings()

    bootstrap_bronze_customers(spark=spark, load_settings=cfg)
    bootstrap_bronze_employees(spark=spark, load_settings=cfg)
    bootstrap_bronze_order_items(spark=spark, load_settings=cfg)
    bootstrap_bronze_orders(spark=spark, load_settings=cfg)
    bootstrap_bronze_products(spark=spark, load_settings=cfg)
    bootstrap_bronze_returns(spark=spark, load_settings=cfg)
    bootstrap_bronze_stores(spark=spark, load_settings=cfg)

if __name__ == '__main__':
    run_bootstrap()

    