import argparse
import logging
import sys
from pathlib import Path


def run_ingest_bronze_products() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument("--repo_root", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from pyspark.sql import SparkSession

    from src.verdanta.common.config import load_settings
    from src.verdanta.common.logger import setup_log
    from src.verdanta.pipelines.bronze.bronze_products import ingest_bronze_products

    setup_log(name="INFO")
    logger = logging.getLogger(__name__)

    settings = load_settings(args.env)
    spark = SparkSession.builder.getOrCreate()

    logger.info("bronze products run start | env=%s", args.env)
    ingest_bronze_products(spark=spark, load_settings=settings)
    logger.info("bronze products run complete | env=%s", args.env)

if __name__ == '__main__':
    run_ingest_bronze_products()