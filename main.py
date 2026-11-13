import sys
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import yaml
import os
from worker_async import AsyncWorker
from pathlib import Path
from paths import LOG_DIR
from paths import CONFIG_DIR

async def main():
    # Allow config file to be passed via CLI (Docker / systemd safe)
    
    config_arg = sys.argv[1] if len(sys.argv) > 1 else "configs/default.yaml"
    config_path = Path(config_arg)
    
    if not config_path.is_absolute():
        config_path = CONFIG_DIR / config_path.name

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    shutdown_event = asyncio.Event()
    worker = AsyncWorker(cfg)

    try:
        await worker.start(shutdown_event)
    except KeyboardInterrupt:
        logging.info("Shutdown requested")
        shutdown_event.set()
    finally:
        await worker.shutdown()

if __name__ == "__main__":    
    
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = os.getenv("QASS_LOG_FILE", "qass.log")
    log_path = LOG_DIR / log_file

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    asyncio.run(main())
