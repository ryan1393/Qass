"""Entry point: ``python3 main.py [config]``.

Relative config paths resolve to ``configs/<basename>`` (default
``configs/default.yaml``); absolute paths are used as-is.

Environment:
  QASS_LOG_FILE  log file name inside QASS_LOG_DIR (default qass.log)
  QASS_LOG_DIR   log directory (default ./logs next to the code)
  QASS_STATE_DIR state directory (default ./state next to the code)
  HEALTH_PORT    only used by the Dockerfile HEALTHCHECK; the service listens
                 on health.port from the config
"""

import asyncio
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from config import ConfigError, load_config, resolve_config_path
from paths import LOG_DIR, state_db_path
from worker_async import AsyncWorker

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5


async def main() -> None:
    """Load the config, run the worker until shutdown, then clean up."""
    config_path = resolve_config_path(sys.argv[1] if len(sys.argv) > 1 else None)

    try:
        cfg = load_config(config_path)
    except ConfigError as e:
        logging.critical("Invalid config: %s", e)
        raise SystemExit(1)

    state_path = state_db_path(config_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Stroke state file: %s", state_path)

    shutdown_event = asyncio.Event()
    worker = AsyncWorker(cfg, state_path)

    try:
        await worker.start(shutdown_event)
    except KeyboardInterrupt:
        logging.info("Shutdown requested")
        shutdown_event.set()
    finally:
        await worker.shutdown()


def setup_logging() -> None:
    """Root logger at INFO: rotating file (10 MB x 5) + console, same format."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_file = os.getenv("QASS_LOG_FILE", "qass.log")
    log_path = LOG_DIR / log_file

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())
