"""In-process counters and timestamps reported by GET /metrics."""

from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock
from typing import Any

# Error codes written to the PLC error_code_tag and reported by /metrics.
# The PLC program reads these numbers: do not renumber them.
# Plain ints on purpose (the PLC write must receive an int, not an Enum).
ERROR_NONE = 0
ERROR_QASS_DISCONNECTED = 10
ERROR_QASS_NO_DATA = 11  # connected, but no "measuring" data for 10 s
ERROR_PLC_COMMS = 20

ERROR_NAMES = {
    ERROR_NONE: "NONE",
    ERROR_QASS_DISCONNECTED: "QASS_DISCONNECTED",
    ERROR_QASS_NO_DATA: "QASS_NO_DATA",
    ERROR_PLC_COMMS: "PLC_COMMS",
}


class Metrics:
    """Thread-safe service metrics. Updated by the worker, read by /metrics."""

    def __init__(self) -> None:
        self._lock = Lock()
        self.start_time = time.time()

        # Connectivity
        self.qass_connected = False
        self.qass_generation = 0
        self.plc_connected = False
        self.last_plc_write_ms: float = 0

        # Activity
        self.last_qass_message_ts: float | None = None  # any QASS payload
        self.last_plc_success_ts: float | None = None
        self.strokes_processed = 0

        # Errors
        self.error_count = 0
        self.last_error_code = 0
        self.last_error_ts: float | None = None
        self.error_counts: defaultdict[int, int] = defaultdict(int)

    # ---- helpers ----

    def uptime_seconds(self) -> int:
        return int(time.time() - self.start_time)

    def mark_qass_message(self) -> None:
        with self._lock:
            self.last_qass_message_ts = time.time()
            self.qass_connected = True

    def mark_qass_connected(self, generation: int) -> None:
        with self._lock:
            self.qass_connected = True
            self.qass_generation = generation

    def mark_qass_disconnected(self) -> None:
        with self._lock:
            self.qass_connected = False

    def mark_plc_success(self) -> None:
        with self._lock:
            self.last_plc_success_ts = time.time()
            self.plc_connected = True

    def mark_error(self, error_code: int) -> None:
        with self._lock:
            self.error_count += 1
            self.last_error_code = error_code
            self.last_error_ts = time.time()
            self.error_counts[error_code] += 1

    def mark_stroke_processed(self) -> None:
        with self._lock:
            self.strokes_processed += 1

    # ---- export ----

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "uptime_seconds": self.uptime_seconds(),
                "qass_connected": self.qass_connected,
                "qass_generation": self.qass_generation,
                "plc_connected": self.plc_connected,
                "strokes_processed": self.strokes_processed,
                "error_count": self.error_count,
                "last_error_code": self.last_error_code,
                "last_error_name": ERROR_NAMES.get(
                    self.last_error_code,
                    "UNKNOWN"
                ),
                "last_error_ts": self.last_error_ts,
                "error_counts": {
                    ERROR_NAMES.get(code, str(code)): count
                    for code, count in self.error_counts.items()
                },
            }
