"""Normalizes raw QASS TCP payloads into a list of JSON records."""

import json
from typing import Any


def split_and_parse_qass_json(raw: str) -> list[Any]:
    """Parse one socket read into records.

    * a single JSON value -> ``[value]`` (a JSON list is returned as-is)
    * concatenated objects ``{..}{..}`` -> ``[{..}, {..}]``
    * anything else (including a read that ends mid-object) -> ``[]``
    """
    raw = raw.strip()

    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        pass

    try:
        # Recover concatenated JSON objects: {}{}
        fixed = "[" + raw.replace("}{", "},{") + "]"
        return json.loads(fixed)
    except Exception:
        # Fail-safe: malformed payloads are dropped
        return []
