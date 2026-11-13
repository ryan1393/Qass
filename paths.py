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
