import logging, logging.config
import os
import yaml
from pathlib import Path

from .config import OrthantConfig

ENV_ORTHANT_CONFIG = "ORTHANT_CONFIG_PATH"
LOGGING_CONFIG_PATH = "orthant-logging.yaml"
ORTHANT_CONFIG_PATH = "orthant.yaml"


_config_path = os.getenv(ENV_ORTHANT_CONFIG)
if _config_path is None:
    _config_path = Path(os.getcwd())
else:
    _config_path = Path(_config_path)


def initialize_logging():
    with open(_config_path / LOGGING_CONFIG_PATH, "r", encoding="utf-8") as f:
        logging_config = yaml.safe_load(f)
    logging.config.dictConfig(logging_config)
    logging.info("Orthant logging initialized.")

def load_orthant_config() -> OrthantConfig:
    with open(_config_path / ORTHANT_CONFIG_PATH, "r", encoding="utf-8") as f:
        orthant_config = yaml.safe_load(f)
    return OrthantConfig(**orthant_config)

def initialize_storage(orthant_config: OrthantConfig):
    os.makedirs(orthant_config.storage_dir, exist_ok=True)
    logging.info(f"Storage directory '{orthant_config.storage_dir}' initialized.")
