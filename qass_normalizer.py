# Normalizes QASS TCP payloads into valid JSON records

import json

def split_and_parse_qass_json(raw: str):
    # Remove whitespace and newlines
    raw = raw.strip()

    try:
        # Attempt direct JSON parse
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        pass

    try:
        # Recover concatenated JSON objects: {}{}
        fixed = "[" + raw.replace("}{", "},{") + "]"
        return json.loads(fixed)
    except Exception:
        # Fail-safe: return empty list for malformed payloads
        return []
