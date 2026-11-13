import sys
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
from worker_async import AsyncWorker
from config import ConfigError, load_config, resolve_config_path
from paths import LOG_DIR

async def main():
    # Allow config file to be passed via CLI (Docker / systemd safe)
    config_path = resolve_config_path(sys.argv[1] if len(sys.argv) > 1 else None)

    try:
        cfg = load_config(config_path)
    except ConfigError as e:
        logging.critical("Invalid config: %s", e)
        raise SystemExit(1)

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
