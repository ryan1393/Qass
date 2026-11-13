"""Loading and validation of the per-machine YAML config.

Validation only checks that every key the service reads at startup is
present, so a broken config fails immediately with one readable message
instead of a ``KeyError`` traceback. It does not add, rename or default any
key: the optional keys keep their defaults where they are used
(``plc.enable_writes`` -> True, ``qass.disconnect_timeout_sec`` -> 60).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping

import yaml

from paths import CONFIG_DIR

DEFAULT_CONFIG = "configs/default.yaml"

# Optional keys and their defaults (applied by the worker, documented here).
DEFAULT_ENABLE_WRITES = True
DEFAULT_DISCONNECT_TIMEOUT_SEC = 60

SUPPORTED_QASS_TYPES = ("auto", "manual")

# Keys read at startup, per section.
REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "qass": ("ip", "port", "type"),
    "plc": (
        "ip",
        "serial_tag",
        "break_tag",
        "crack_tag",
        "heartbeat_tag",
        "service_ok_tag",
        "qass_ok_tag",
        "plc_ok_tag",
        "last_stroke_tag",
        "error_code_tag",
    ),
    "health": ("port",),
    "reconnect": ("initial_delay", "max_delay"),
}


class ConfigError(ValueError):
    """The config file is missing or lacks required keys."""


def resolve_config_path(arg: str | None) -> Path:
    """Map the ``main.py`` argument to a config file.

    Absolute paths are used as-is. Relative paths always resolve to
    ``configs/<basename>`` next to the code, whatever directory they name,
    so ``configs/config_machine1.yaml`` and ``config_machine1.yaml`` are the
    same file.
    """
    path = Path(arg if arg else DEFAULT_CONFIG)
    if not path.is_absolute():
        path = CONFIG_DIR / path.name
    return path


def validate_config(cfg: Any, source: str = "config") -> dict:
    """Raise ConfigError listing every missing key; return ``cfg`` unchanged."""
    if not isinstance(cfg, Mapping):
        raise ConfigError(f"{source}: expected a YAML mapping, got {type(cfg).__name__}")

    missing: list[str] = []
    for section, keys in REQUIRED_KEYS.items():
        values = cfg.get(section)
        if not isinstance(values, Mapping):
            missing.append(f"{section} (section)")
            continue
        missing.extend(f"{section}.{k}" for k in keys if k not in values)

    qass = cfg.get("qass") if isinstance(cfg.get("qass"), Mapping) else {}
    plc = cfg.get("plc") if isinstance(cfg.get("plc"), Mapping) else {}
    if qass.get("type") == "auto" and "current_tag" not in plc:
        missing.append("plc.current_tag (required when qass.type is 'auto')")

    if missing:
        raise ConfigError(f"{source}: missing required key(s): {', '.join(missing)}")

    if qass.get("type") not in SUPPORTED_QASS_TYPES:
        # Unchanged behaviour: an unsupported type still raises when the first
        # stroke is written. Warn early so it shows up in the startup log.
        logging.warning(
            "%s: qass.type=%r is not one of %s; measurement writes will fail",
            source, qass.get("type"), "/".join(SUPPORTED_QASS_TYPES),
        )
    return cfg  # type: ignore[return-value]


def load_config(path: Path) -> dict:
    """Read and validate a YAML config file."""
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    return validate_config(cfg, str(path))
