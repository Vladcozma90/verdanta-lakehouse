import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def run_land_partition() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")
    parser.add_argument(
        "--ingest_date",
        default=datetime.now(tz=ZoneInfo("Europe/Bucharest")).date().isoformat(),
    )
    parser.add_argument("--repo_root", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from src.verdanta.common.logger import setup_log
    from src.verdanta.extract.land import land_partition

    setup_log(name="INFO")
    logger = logging.getLogger(__name__)

    ingest_date = datetime.strptime(args.ingest_date, "%Y-%m-%d").date()

    logger.info("land run start | env=%s ingest_date=%s", args.env, ingest_date)
    count = land_partition(ingest_date=ingest_date, env=args.env)
    logger.info("land run complete | env=%s ingest_date=%s landed=%d files", args.env, ingest_date, count)

    if count == 0:
        logger.warning("Zero files landed - check source_root and ingest_date")

if __name__ == '__main__':
    run_land_partition()

