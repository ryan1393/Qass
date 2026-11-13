"""Normalizes raw QASS TCP payloads into a list of JSON records."""

import codecs
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


class QassStreamFramer:
    """Reassembles complete top-level JSON values from the QASS TCP byte stream.

    TCP has no message boundaries: one read can end in the middle of a record
    (or of a multi-byte UTF-8 character) and the next read carries the rest.
    ``feed()`` returns only complete ``{...}`` / ``[...]`` texts and keeps an
    unfinished value until more bytes arrive. Bytes outside a value (whitespace,
    newlines, stray characters) are skipped. Braces inside JSON strings are
    ignored. An unfinished value larger than ``max_pending`` characters is
    dropped (and reported via ``overflowed``) so a corrupt stream cannot grow
    memory without bound.
    """

    def __init__(self, max_pending: int = 1_000_000) -> None:
        self.max_pending = max_pending
        self.overflowed = 0  # number of times pending data was dropped
        self.reset()

    def reset(self) -> None:
        """Forget partial data (call on every new connection)."""
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="ignore")
        self._pending = ""

    def feed(self, data: bytes) -> list[str]:
        """Add one socket read; return the JSON value texts it completed."""
        buf = self._pending + self._decoder.decode(data)
        out: list[str] = []
        start = -1
        depth = 0
        in_string = False
        escaped = False

        for i, ch in enumerate(buf):
            if start < 0:
                if ch == "{" or ch == "[":
                    start, depth, in_string, escaped = i, 1, False, False
                continue
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
            elif ch == '"':
                in_string = True
            elif ch == "{" or ch == "[":
                depth += 1
            elif ch == "}" or ch == "]":
                depth -= 1
                if depth == 0:
                    out.append(buf[start:i + 1])
                    start = -1

        self._pending = buf[start:] if start >= 0 else ""
        if len(self._pending) > self.max_pending:
            self._pending = ""
            self.overflowed += 1
        return out
