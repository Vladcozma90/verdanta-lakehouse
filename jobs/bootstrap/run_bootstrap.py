import argparse
import logging
import sys
from pathlib import Path


def run_bootstrap() -> None:

    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--repo_root", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from pyspark.sql import SparkSession

    from bootstrap.bootstrap_customers import bootstrap_bronze_customers
    from bootstrap.bootstrap_employees import bootstrap_bronze_employees
    from bootstrap.bootstrap_order_items import bootstrap_bronze_order_items
    from bootstrap.bootstrap_orders import bootstrap_bronze_orders
    from bootstrap.bootstrap_products import bootstrap_bronze_products
    from bootstrap.bootstrap_returns import bootstrap_bronze_returns
    from bootstrap.bootstrap_stores import bootstrap_bronze_stores
    from bootstrap.bootstrap_suppliers import bootstrap_bronze_suppliers
    from src.verdanta.common.config import load_settings
    from src.verdanta.common.logger import setup_log

    setup_log(name="INFO")
    logger = logging.getLogger(__name__)

    settings = load_settings(args.env)
    spark = SparkSession.builder.getOrCreate()

    logger.info("bootstrap run start | env=%s", args.env)

    bootstrap_bronze_customers(spark=spark, load_settings=settings)
    bootstrap_bronze_employees(spark=spark, load_settings=settings)
    bootstrap_bronze_order_items(spark=spark, load_settings=settings)
    bootstrap_bronze_orders(spark=spark, load_settings=settings)
    bootstrap_bronze_products(spark=spark, load_settings=settings)
    bootstrap_bronze_returns(spark=spark, load_settings=settings)
    bootstrap_bronze_stores(spark=spark, load_settings=settings)
    bootstrap_bronze_suppliers(spark=spark, load_settings=settings)

    logger.info("bootstrap run complete | env=%s", args.env)


if __name__ == '__main__':
    run_bootstrap()

    