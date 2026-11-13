from pathlib import Path
import os

def get_base_dir():
    # If running in Docker, /app is WORKDIR
    # If running locally, this resolves correctly
    return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
LOG_DIR = Path(os.getenv("QASS_LOG_DIR", BASE_DIR / "logs"))
STATE_DIR = Path(os.getenv("QASS_STATE_DIR", BASE_DIR / "state"))
CONFIG_DIR = BASE_DIR / "configs"