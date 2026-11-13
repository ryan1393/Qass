import time
from threading import Lock
from collections import defaultdict

ERROR_NAMES = {
    0: "NONE",
    10: "QASS_DISCONNECTED",
    11: "QASS_NO_DATA",
    20: "PLC_COMMS",
}

class Metrics:
    def __init__(self):
        self._lock = Lock()
        self.start_time = time.time()

        # Connectivity
        self.qass_connected = False
        self.qass_generation = 0
        self.plc_connected = False
        self.last_plc_write_ms = 0

        # Activity
        self.last_qass_message_ts = None
        self.last_plc_success_ts = None
        self.strokes_processed = 0

        # Errors
        self.error_count = 0
        self.last_error_code = 0
        self.last_error_ts = None
        self.error_counts = defaultdict(int)

    # ---- helpers ----

    def uptime_seconds(self):
        return int(time.time() - self.start_time)

    def mark_qass_message(self):
        with self._lock:
            self.last_qass_message_ts = time.time()
            self.qass_connected = True

    def mark_plc_success(self):
        with self._lock:
            self.last_plc_success_ts = time.time()
            self.plc_connected = True
    
    def mark_error(self, error_code):
        with self._lock:
            self.error_count += 1
            self.last_error_code = error_code
            self.last_error_ts = time.time()
            self.error_counts[error_code] += 1
    
    def mark_stroke_processed(self):
        with self._lock:
            self.strokes_processed += 1

    # ---- export ----

    def snapshot(self) -> dict:
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
    