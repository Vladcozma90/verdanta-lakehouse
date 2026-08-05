import logging
import shutil
from datetime import date
from pathlib import Path

from verdanta.common.config import load_settings
from verdanta.common.paths import SOURCES, landing_file

logger = logging.getLogger(__name__)


def land_partition(ingest_date: date, env: str | None = None) -> int:
    """Push one day's source extracts into the landing zone. Idempotent per date."""
    cfg = load_settings(env)
    landed = 0

    for system, entities in SOURCES.items():
        for entity, ext in entities:
            local = (Path(cfg.source_root) / system / entity
                     / f"ingest_date={ingest_date:%Y-%m-%d}"
                     / f"{entity}_{ingest_date:%Y%m%d}.{ext}")

            if not local.exists():
                logger.info("no extract for %s/%s on %s — skipping", system, entity, ingest_date)
                continue

            target = landing_file(cfg.landing_root, system, entity, ext, ingest_date)
            _put(local, target)
            logger.info("landed %s (%d bytes)", target, local.stat().st_size)
            landed += 1

    return landed


def _put(local: Path, target: str) -> None:
    if target.startswith("abfss://"):
        from verdanta.extract.adls import upload
        upload(local, target)
    else:
        dest = Path(target.removeprefix("file://"))
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local, dest)