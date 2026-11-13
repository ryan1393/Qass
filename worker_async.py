"""Async worker: QASS TCP client, stroke processing, PLC I/O, watchdogs, HTTP.

One worker runs per machine (one Docker container each). Background tasks:

* ``_receiver``       reads raw QASS payloads and queues the ones with "measuring"
* ``_consumer``       parses queued payloads and detects strokes
* ``_serial_loop``    reads the part serial number from the PLC every second
* ``_plc_watchdog``   flags PLC comms as lost after 10 s without a successful call
* ``_qass_watchdog``  derives the error code and exits the process when QASS has
                      been silent for ``qass.disconnect_timeout_sec``
* ``_heartbeat``      writes heartbeat + status tags to the PLC every 5 s
* ``_health_server``  serves /health, /metrics and /logs

All PLC calls go through ``_plc_call``: one lock, blocking pylogix calls run in
the default thread-pool executor.

PLC contract: the tag names, values, write order and intervals below are read
by the PLC program. Do not change them without changing the PLC program.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from collections import deque
from typing import Any, Callable, TypeVar

from aiohttp import web
from pylogix import PLC

from config import DEFAULT_DISCONNECT_TIMEOUT_SEC, DEFAULT_ENABLE_WRITES
from metrics import (
    ERROR_NONE,
    ERROR_PLC_COMMS,
    ERROR_QASS_DISCONNECTED,
    ERROR_QASS_NO_DATA,
    Metrics,
)
from paths import LOG_DIR
from qass_normalizer import QassStreamFramer, split_and_parse_qass_json
from state_store import StrokeStateStore

T = TypeVar("T")


def monotonic() -> float:
    """Clock for every interval/timeout. Unaffected by NTP or manual clock steps."""
    return time.monotonic()

# QASS protocol
QASS_SUBSCRIBE_CMD = b'{"cmd":"sendreadings","p1":"true"}'
QASS_READ_SIZE = 12000  # max bytes per socket read (records may span reads)
QASS_RECONNECT_RESET_DELAY = 1  # backoff restarts here after a successful connect
QASS_NO_DATA_SEC = 10  # connected but no "measuring" payload -> ERROR_QASS_NO_DATA

# Timing (seconds) — part of the PLC contract
SERIAL_POLL_SEC = 1
WATCHDOG_INTERVAL_SEC = 2
HEARTBEAT_INTERVAL_SEC = 5
PLC_COMMS_TIMEOUT_SEC = 10  # plc_ok is False after this long without a successful call
PLC_ERROR_HOLD_SEC = 10  # error code stays PLC_COMMS this long after a failed PLC call

STATE_SAVE_EVERY = 25  # persist the stroke buffer every N samples
LOG_TAIL_LINES = 100  # lines returned by /logs
HTTP_BIND_RETRY_SEC = 10  # retry interval while health.port cannot be bound


# pylogix reports comms problems through Response.Status instead of raising.
# Tag-level errors (e.g. "Path destination unknown") mean the PLC answered, so
# they are not comms failures.
PLC_COMMS_FAILURE_STATUSES = frozenset({
    "Connection failure",
    "Connection lost",
    "Register session failed",
    "Forward open failed",
})


def _is_comms_failure(status: Any) -> bool:
    """True if a pylogix Status string means the PLC could not be reached."""
    text = str(status)
    if text in PLC_COMMS_FAILURE_STATUSES:
        return True
    # Socket errors surface as "Unknown error <exception text>"; an unknown CIP
    # code from a PLC that did answer is "Unknown error <number>".
    if text.startswith("Unknown error "):
        rest = text[len("Unknown error "):]
        return rest != "Success" and not rest.isdigit()
    return False


def _comms_failures(result: Any) -> list[str]:
    """Comms-failure statuses in a pylogix Response or a list of them."""
    responses = result if isinstance(result, list) else [result]
    return [
        str(r.Status) for r in responses
        if hasattr(r, "Status") and _is_comms_failure(r.Status)
    ]


NUMERIC_FIELDS = ("e_current", "e_max", "limitbreak", "limitcrack")


def _non_numeric_fields(record: dict) -> list[str]:
    """Fields that the stroke logic would fail to convert with int()."""
    bad = []
    for name in NUMERIC_FIELDS:
        try:
            int(record.get(name, 0))
        except (TypeError, ValueError):
            bad.append(name)
    return bad


class AsyncWorker:
    """Runs one machine: one QASS unit, one PLC. Must be created inside a running loop."""

    def __init__(self, cfg: dict, state_path: str | os.PathLike) -> None:
        """``state_path``: SQLite file for the stroke state (see paths.state_db_path)."""
        qass = cfg["qass"]
        plc = cfg["plc"]

        # ---------- Configuration ----------
        self.plc_lock = asyncio.Lock()

        # QASS TCP endpoint
        self.qass_ip: str = qass["ip"]
        self.qass_port: int = qass["port"]
        self.qass_type: str = qass["type"]  # "auto" or "manual"
        self.disconnect_timeout_sec: float = qass.get(
            "disconnect_timeout_sec", DEFAULT_DISCONNECT_TIMEOUT_SEC
        )

        # PLC connection and read tag
        # pylogix.PLC; None only until _connect_plc runs (a call on None raises
        # inside _plc_call and is handled like any other PLC error)
        self.plc_conn: Any = None
        self.plc_ip: str = plc["ip"]
        self.serial_tag: str = plc["serial_tag"]

        # PLC measurement tags (manual mode writes QASS_Measurement_<stroke> instead)
        self.break_tag: str = plc["break_tag"]
        self.crack_tag: str = plc["crack_tag"]
        self.current_tag: str | None = (
            plc["current_tag"] if self.qass_type == "auto" else None
        )

        # PLC heartbeat and status tags
        self.heartbeat_tag: str = plc["heartbeat_tag"]
        self.service_ok_tag: str = plc["service_ok_tag"]
        self.qass_ok_tag: str = plc["qass_ok_tag"]
        self.plc_ok_tag: str = plc["plc_ok_tag"]
        self.last_stroke_tag: str = plc["last_stroke_tag"]
        self.error_code_tag: str = plc["error_code_tag"]

        # Master PLC write switch (False: no PLC writes at all, reads continue)
        self.plc_writes_enabled: bool = plc.get("enable_writes", DEFAULT_ENABLE_WRITES)

        # HTTP health server port
        self.health_port: int = cfg["health"]["port"]
        self.metrics = Metrics()

        # QASS reconnect backoff
        self.reconnect_delay: float = cfg["reconnect"]["initial_delay"]
        self.reconnect_max: float = cfg["reconnect"]["max_delay"]

        # ---------- Runtime state ----------

        # Async TCP reader / writer for QASS
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        # Splits the TCP byte stream into complete JSON records
        self.framer = QassStreamFramer()

        # QASS record texts waiting for the consumer
        self.queue: asyncio.Queue[str] = asyncio.Queue()

        self.loop = asyncio.get_running_loop()
        self.shutdown_flag = False

        # Persistent stroke state
        self.state = StrokeStateStore(state_path)
        self.ident, self.stroke, self.stroke_buffer = self.state.load()
        self.serial_epoch = 0  # bumped on every serial change

        # PLC watchdog tracking
        self.last_plc_ok = monotonic()
        self.plc_comms_failing = False  # last PLC call hit a comms-failure status
        self.last_plc_error_ts = float("-inf")  # last failed PLC call

        # QASS tracking (monotonic times; None = never)
        self.last_qass_data: float | None = None  # last "measuring" record (or connect)
        self.last_qass_message: float | None = None  # last read of any kind
        self.qass_connected = False
        self.qass_generation = 0  # bumped on every successful connect

        # Heartbeat and error tracking
        self.heartbeat_value = 0
        self.last_error_code = ERROR_NONE

        self.tasks: list[asyncio.Task] = []

        logging.info(
            "PLC writes %s",
            "ENABLED" if self.plc_writes_enabled else "DISABLED",
        )

    # ---------- Lifecycle ----------

    async def start(self, shutdown_event: asyncio.Event) -> None:
        """Start all background tasks and block until ``shutdown_event`` is set.

        The caller is responsible for calling :meth:`shutdown` afterwards.
        """
        logging.info("Service started")
        await self._connect_plc()

        self.tasks = [
            asyncio.create_task(self._receiver()),
            asyncio.create_task(self._consumer()),
            asyncio.create_task(self._serial_loop()),
            asyncio.create_task(self._plc_watchdog()),
            asyncio.create_task(self._heartbeat()),
            asyncio.create_task(self._health_server()),
            asyncio.create_task(self._qass_watchdog()),
        ]

        await shutdown_event.wait()

    async def shutdown(self) -> None:
        """Cancel background tasks and close the QASS socket and PLC connection."""
        logging.info("Shutting down service")
        self.shutdown_flag = True

        for task in self.tasks:
            task.cancel()

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        if self.plc_conn:
            self.plc_conn.Close()

    # ---------- QASS communication ----------

    async def _connect_qass(self) -> None:
        """One connect attempt. On failure, sleep the backoff delay and double it."""
        try:
            logging.info("Connecting to QASS %s:%s", self.qass_ip, self.qass_port)

            self.reader, self.writer = await asyncio.open_connection(
                self.qass_ip, self.qass_port
            )

            # New connection generation: anything from the old socket is stale
            self.qass_generation += 1
            self.qass_connected = True
            self.metrics.mark_qass_connected(self.qass_generation)
            self.last_qass_data = monotonic()

            # A stroke cannot continue across a reconnect
            self.stroke_buffer.clear()
            self.framer.reset()  # partial data from the old socket is stale

            # Subscribe to measurements
            self.writer.write(QASS_SUBSCRIBE_CMD)
            await self.writer.drain()

            self.reconnect_delay = QASS_RECONNECT_RESET_DELAY
            logging.info("QASS connected")

        except Exception as e:
            self.qass_connected = False
            self.metrics.mark_qass_disconnected()
            self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
            # If the TCP connect worked but the subscribe failed, do not keep
            # reading from that half-set-up socket.
            self._close_qass_socket()
            logging.error("QASS connect failed: %s", e)
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.reconnect_max)

    def _close_qass_socket(self) -> None:
        """Forget the current QASS connection and close its socket (no waiting)."""
        writer = self.writer
        self.reader = self.writer = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    async def _receiver(self) -> None:
        """Read from QASS; queue each complete JSON record containing "measuring"."""
        local_generation = 0

        while not self.shutdown_flag:
            if not self.reader:
                await self._connect_qass()
                local_generation = self.qass_generation
                continue

            try:
                data = await self.reader.read(QASS_READ_SIZE)
                if not data:
                    raise ConnectionError("QASS disconnected")

                # Ignore stale reads after a reconnect
                if local_generation != self.qass_generation:
                    continue

                self.last_qass_message = monotonic()
                self.metrics.mark_qass_message()

                # Reassemble complete JSON records across reads, then enqueue
                # only measurement records.
                overflowed = self.framer.overflowed
                for record_text in self.framer.feed(data):
                    if '"measuring"' in record_text:
                        self.last_qass_data = monotonic()
                        await self.queue.put(record_text)
                if self.framer.overflowed != overflowed:
                    logging.warning(
                        "Dropped unterminated QASS data (> %s chars without a "
                        "complete JSON record)", self.framer.max_pending,
                    )

            except Exception as e:
                logging.error("QASS receive error: %s", e)
                self.qass_connected = False
                self.metrics.mark_qass_disconnected()
                self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
                self._close_qass_socket()

    # ---------- Stroke processing ----------

    async def _consumer(self) -> None:
        """Parse queued payloads and feed each record to the stroke logic."""
        while not self.shutdown_flag:
            payload = await self.queue.get()
            for record in split_and_parse_qass_json(payload):
                try:
                    await self._process_record(record)
                except Exception:
                    # Safety net: never let one record end stroke processing.
                    logging.exception("Failed to process QASS record: %r", record)

    async def _process_record(self, record: dict) -> None:
        """Buffer samples while measuring; on the first non-measuring record,
        write the sample with the highest e_current to the PLC."""
        if not isinstance(record, dict):
            logging.warning("Skipping QASS record that is not a JSON object: %r", record)
            return

        measuring = str(record.get("measuring")).lower() == "true"

        # Buffer samples during an active stroke
        if measuring:
            bad_fields = _non_numeric_fields(record)
            if bad_fields:
                # Never let one bad sample stop stroke processing.
                logging.warning(
                    "Skipping QASS record with non-numeric %s: %r",
                    ", ".join(bad_fields), record,
                )
                return
            self.stroke_buffer.append(record)
            if len(self.stroke_buffer) % STATE_SAVE_EVERY == 0:
                self.state.save(self.ident, self.stroke, self.stroke_buffer)
            return

        # End-of-stroke without data: nothing to report
        if not self.stroke_buffer:
            return

        # Peak sample (first one wins on ties)
        peak = max(self.stroke_buffer, key=lambda x: int(x.get("e_current", 0)))

        e_max = int(peak.get("e_max", 0))
        e_current = int(peak.get("e_current", 0))
        break_limit = int(peak.get("limitbreak", 0))
        crack_limit = int(peak.get("limitcrack", 0))

        # Pin the stroke to the part it was measured on. The write below waits
        # for the PLC lock, and a new serial may be read in the meantime.
        ident, stroke, serial_epoch = self.ident, self.stroke, self.serial_epoch

        logging.info(
            "Serial=%s Stroke=%s ECurrent=%s EMax=%s Break=%s Crack=%s",
            ident, stroke,
            e_current, e_max, break_limit, crack_limit,
        )

        try:
            await self._write_plc(e_current, break_limit, crack_limit, ident, stroke)
        except Exception:
            # The stroke happened even if the PLC write failed: still advance the
            # counter so later strokes keep their own numbers.
            logging.exception(
                "PLC measurement write failed: Serial=%s Stroke=%s",
                ident, stroke,
            )

        self.metrics.mark_stroke_processed()
        if self.serial_epoch != serial_epoch:
            # A new part arrived during the write; the serial loop already
            # reset the counter to 1 for it. Do not advance it.
            return

        # Prepare for the next stroke
        self.stroke_buffer.clear()
        self.stroke += 1
        self.state.save(self.ident, self.stroke, [])

    # ---------- PLC interaction ----------

    async def _connect_plc(self) -> None:
        """Create the PLC object and do one test read of the serial tag."""
        try:
            self.plc_conn = PLC(self.plc_ip)

            result = await self._plc_call(lambda: self.plc_conn.Read(self.serial_tag))

            if result.Status == "Success":
                logging.info("PLC connected")

        except Exception:
            logging.exception("PLC connection failed")

    async def _plc_call(self, fn: Callable[[], T]) -> T:
        """Run a blocking pylogix call in the executor, serialized by ``plc_lock``.

        On an exception the PLC object is closed and recreated, the error is
        recorded (ERROR_PLC_COMMS) and the exception is re-raised.

        ``fn`` returns a pylogix Response or a list of them. If any of them has a
        comms-failure Status, the call does not count as a PLC success (it does
        not refresh plc_ok / the watchdog) and ERROR_PLC_COMMS is recorded.
        """
        async with self.plc_lock:
            try:
                result = await self.loop.run_in_executor(None, fn)
            except Exception:
                self._record_plc_error()

                try:
                    self.plc_conn.Close()
                except Exception:
                    pass

                try:
                    self.plc_conn = PLC(self.plc_ip)
                except Exception:
                    pass

                raise

            failures = _comms_failures(result)
            if failures:
                # The call returned, but the PLC was not reached: not a success.
                self._record_plc_error()
                if not self.plc_comms_failing:
                    logging.error("PLC comms failure: %s", failures[0])
                self.plc_comms_failing = True
                return result

            if self.plc_comms_failing:
                logging.info("PLC comms restored")
            self.plc_comms_failing = False
            self.last_plc_ok = monotonic()
            self.metrics.mark_plc_success()
            return result

    def _write_tags(self, items: list[tuple[Any, Callable[[], Any]]]) -> list[Any]:
        """Write (tag, value) pairs in order; runs inside the executor.

        Each value is evaluated right before its own write. If a write returns a
        comms-failure status the PLC is unreachable: the remaining writes would
        each wait for pylogix's socket timeout (5 s) and fail too, so they are
        skipped and the PLC lock is released sooner.
        """
        responses = []
        for tag, value in items:
            response = self.plc_conn.Write(tag, value())
            responses.append(response)
            if _is_comms_failure(getattr(response, "Status", "Success")):
                break
        return responses

    def _record_plc_error(self) -> None:
        """Set ERROR_PLC_COMMS; the QASS watchdog keeps it for PLC_ERROR_HOLD_SEC."""
        self.last_error_code = ERROR_PLC_COMMS
        self.last_plc_error_ts = monotonic()
        self.metrics.mark_error(ERROR_PLC_COMMS)

    async def _write_plc(
        self,
        e_current: int,
        break_limit: int,
        crack_limit: int,
        ident: Any = None,
        stroke: int | None = None,
    ) -> None:
        """Write one stroke result. Order: break, crack, then the measurement tag.

        ``ident``/``stroke`` identify the stroke being written (default: current).
        """
        if stroke is None:
            ident, stroke = self.ident, self.stroke
        start = time.perf_counter()
        if not self.plc_writes_enabled:
            logging.info("PLC writes disabled — skipping measurement write")
            return

        if self.qass_type == "auto":
            measurement_tag = self.current_tag
        elif self.qass_type == "manual":
            measurement_tag = f"QASS_Measurement_{stroke}"
        else:
            raise ValueError(f"Unsupported qass type: {self.qass_type}")

        def write() -> list[Any]:
            responses = self._write_tags([
                (self.break_tag, lambda: break_limit),
                (self.crack_tag, lambda: crack_limit),
                (measurement_tag, lambda: e_current),
            ])
            logging.debug(
                "Write to PLC Serial=%s Stroke=%s ECurrent=%s Break=%s Crack=%s",
                ident, stroke,
                e_current, break_limit, crack_limit,
            )
            return responses

        responses = await self._plc_call(write)

        elapsed_ms = (time.perf_counter() - start) * 1000
        self.metrics.last_plc_write_ms = elapsed_ms
        logging.debug("PLC write took %.1f ms", elapsed_ms)

        rejected = [
            f"{r.TagName}={r.Status}" for r in responses
            if getattr(r, "Status", "Success") != "Success"
        ]
        written = len(responses)
        if written < 3:
            skipped = [self.break_tag, self.crack_tag, measurement_tag][written:]
            rejected += [f"{tag}=skipped" for tag in skipped]
        if rejected:
            logging.error(
                "PLC measurement write not confirmed: Serial=%s Stroke=%s %s",
                ident, stroke, ", ".join(rejected),
            )

    async def _serial_loop(self) -> None:
        """Read the serial tag every second; a new non-empty serial restarts at stroke 1."""

        def read_serial() -> Any:
            return self.plc_conn.Read(self.serial_tag)

        while not self.shutdown_flag:
            try:
                response = await self._plc_call(read_serial)

                if response.Status == "Success":
                    serial = response.Value
                    self.metrics.mark_plc_success()
                    if serial and serial != self.ident:
                        self.ident = serial
                        self.serial_epoch += 1
                        self.stroke = 1
                        self.stroke_buffer.clear()
                        self.state.save(self.ident, self.stroke, [])
                        logging.info("SN Tag Read: Value=%s", serial)
            except Exception as e:
                logging.error("PLC ident error: %s", e)

            await asyncio.sleep(SERIAL_POLL_SEC)

    # ---------- Watchdogs ----------

    async def _plc_watchdog(self) -> None:
        """Flag PLC comms as lost after PLC_COMMS_TIMEOUT_SEC without success."""
        comms_lost = False
        while not self.shutdown_flag:
            if monotonic() - self.last_plc_ok > PLC_COMMS_TIMEOUT_SEC:
                if not comms_lost:  # log the transition, not every 2 s
                    logging.error("PLC WATCHDOG: no successful comms")
                comms_lost = True
                self.metrics.plc_connected = False
            else:
                if comms_lost:
                    logging.info("PLC WATCHDOG: comms OK again")
                comms_lost = False
                self.metrics.plc_connected = True
            await asyncio.sleep(WATCHDOG_INTERVAL_SEC)

    async def _qass_watchdog(self) -> None:
        """Set the error code; exit if QASS has gone silent.

        Priority: QASS_DISCONNECTED > QASS_NO_DATA > PLC_COMMS (a PLC call failed
        in the last PLC_ERROR_HOLD_SEC) > NONE.
        """
        while not self.shutdown_flag:
            now = monotonic()

            if not self.qass_connected:
                self.last_error_code = ERROR_QASS_DISCONNECTED
                self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
            elif self.last_qass_data is None or now - self.last_qass_data > QASS_NO_DATA_SEC:
                self.last_error_code = ERROR_QASS_NO_DATA
                self.metrics.mark_error(ERROR_QASS_NO_DATA)
            elif now - self.last_plc_error_ts < PLC_ERROR_HOLD_SEC:
                # QASS is fine but a PLC call failed recently: keep reporting it
                # (already counted in metrics when it happened)
                self.last_error_code = ERROR_PLC_COMMS
            else:
                self.last_error_code = ERROR_NONE
            self._exit_if_qass_silent()
            await asyncio.sleep(WATCHDOG_INTERVAL_SEC)

    def _exit_if_qass_silent(self) -> None:
        """Exit with code 1 (Docker restarts the container) when no QASS message
        of any kind has arrived for ``disconnect_timeout_sec``. Never fires before
        the first message."""
        timeout = self.disconnect_timeout_sec
        last_msg = self.last_qass_message

        if last_msg is None:
            return

        if monotonic() - last_msg > timeout:
            logging.error(
                "QASS disconnected for %s seconds → exiting container",
                timeout,
            )
            sys.exit(1)

    # ---------- Heartbeat / health ----------

    async def _heartbeat(self) -> None:
        """Toggle the heartbeat and write the status tags every 5 s."""
        while not self.shutdown_flag:
            self.heartbeat_value ^= 1

            if self.plc_writes_enabled:
                try:
                    await self._write_plc_status()
                except Exception as e:
                    # _plc_call already recreated the PLC object; keep beating.
                    logging.error("PLC heartbeat write failed: %s", e)

            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

    async def _write_plc_status(self) -> None:
        """Write heartbeat and status tags, in this order, in one PLC transaction."""

        def write() -> list[Any]:
            return self._write_tags([
                (self.heartbeat_tag, lambda: self.heartbeat_value),
                (self.service_ok_tag, lambda: True),
                (self.qass_ok_tag, lambda: self.qass_connected),
                (self.plc_ok_tag,
                 lambda: monotonic() - self.last_plc_ok < PLC_COMMS_TIMEOUT_SEC),
                (self.last_stroke_tag, lambda: self.stroke),
                (self.error_code_tag, lambda: self.last_error_code),
            ])

        await self._plc_call(write)

    async def _health_server(self) -> None:
        """Start the HTTP server on 0.0.0.0:health.port (returns once listening).

        If the port cannot be bound, log it and retry every HTTP_BIND_RETRY_SEC."""

        async def health(_request: web.Request) -> web.Response:
            # Always 200: the Docker HEALTHCHECK only checks that the process answers.
            return web.json_response({
                "status": "ok",
                "qass_connected": self.qass_connected,
                "last_qass_data_s": (
                    # before the first connect: seconds since 1970, as always
                    int(time.time()) if self.last_qass_data is None
                    else int(monotonic() - self.last_qass_data)
                ),
                "serial": self.ident,
            })

        async def metrics(_request: web.Request) -> web.Response:
            healthy = True
            reason = "OK"

            if not self.qass_connected:
                healthy = False
                reason = "QASS disconnected"

            return web.json_response(
                {
                    "healthy": healthy,
                    "reason": reason,
                    "metrics": self.metrics.snapshot(),
                    "qass_queue_depth": self.queue.qsize(),
                    "plc_queue_depth": 0,  # kept for dashboards; no PLC queue exists
                    "last_plc_write_ms": self.metrics.last_plc_write_ms,
                },
                status=200 if healthy else 503,
            )

        def tail_log() -> list[str]:
            logfile = LOG_DIR / os.getenv("QASS_LOG_FILE", "qass.log")
            try:
                with open(logfile, "r", errors="replace") as f:
                    # streams the file instead of loading all of it (up to 10 MB)
                    return list(deque(f, maxlen=LOG_TAIL_LINES))
            except FileNotFoundError:
                return []

        async def logs(_request: web.Request) -> web.Response:
            # file I/O off the event loop so QASS/PLC tasks are not stalled
            lines = await self.loop.run_in_executor(None, tail_log)
            return web.json_response({"logs": lines})

        app = web.Application()
        app.add_routes([
            web.get("/health", health),
            web.get("/metrics", metrics),
            web.get("/logs", logs),
        ])
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        failed = False
        while not self.shutdown_flag:
            try:
                await web.TCPSite(runner, "0.0.0.0", port=self.health_port).start()
            except OSError as e:
                # Keep doing the QASS/PLC job; Docker shows the container as
                # unhealthy until HTTP is up. Retry until the port is free.
                if not failed:
                    logging.critical(
                        "Cannot serve HTTP on 0.0.0.0:%s: %s (retrying every %s s)",
                        self.health_port, e, HTTP_BIND_RETRY_SEC,
                    )
                failed = True
                await asyncio.sleep(HTTP_BIND_RETRY_SEC)
                continue
            if failed:
                logging.info("HTTP server listening on 0.0.0.0:%s", self.health_port)
            return
