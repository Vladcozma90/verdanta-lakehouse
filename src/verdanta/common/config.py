import os
from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

CONF_DIR = Path(__file__).resolve().parents[3] / "conf"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="VERDANTA_", extra="ignore")

    env: str = "local"
    landing_root: str
    source_root: Path
    catalog: str
    checkpoint_base_path: str


@lru_cache
def load_settings(env: str | None = None) -> Settings:
    env = env or os.getenv("VERDANTA_ENV", "local")
    yaml_cfg = yaml.safe_load((CONF_DIR / f"{env}.yml").read_text())
    merged = {
        "env": env,
        "landing_root": os.getenv("VERDANTA_LANDING_ROOT", yaml_cfg.get("landing_root")),
        "source_root": os.getenv("VERDANTA_SOURCE_ROOT", yaml_cfg.get("source_root")),
        "catalog": os.getenv("VERDANTA_CATALOG", yaml_cfg.get("catalog")),
        "checkpoint_base_path": os.getenv("VERDANTA_CHECKPOINT_BASE_PATH", yaml_cfg.get("checkpoint_base_path")),
    }
    return Settings(**merged)