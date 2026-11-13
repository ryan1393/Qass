"""Filesystem locations, overridable through environment variables."""

import os
from pathlib import Path


def get_base_dir() -> Path:
    """Directory containing the code (/app inside the container)."""
    return Path(__file__).resolve().parent


BASE_DIR = get_base_dir()
LOG_DIR = Path(os.getenv("QASS_LOG_DIR", BASE_DIR / "logs"))
STATE_DIR = Path(os.getenv("QASS_STATE_DIR", BASE_DIR / "state"))
CONFIG_DIR = BASE_DIR / "configs"


def state_db_path(config_path: Path) -> Path:
    """Per-machine state file in STATE_DIR, e.g. state_config_machine1.db.

    All containers share one state volume, so the name must be unique per
    machine; the config file name is.
    """
    return STATE_DIR / f"state_{Path(config_path).stem}.db"
