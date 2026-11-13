"""Hardware-free stand-ins for a QASS unit and an Allen-Bradley PLC.

* ``PlcSim`` / ``FakePLC`` replace ``pylogix.PLC``. Every Read/Write/Close is
  recorded in order (tag, value, thread, simulated time) so tests can assert
  the exact tag contract the PLC program depends on.
* ``FakeQass`` is a real asyncio TCP server that behaves like a QASS unit:
  it accepts a connection, records what the client sends (the subscribe
  command) and lets the test push raw bytes, including split and
  concatenated JSON payloads.
* ``WarpClock`` speeds up time: ``asyncio.sleep(d)`` really sleeps
  ``d * scale`` and ``time.time()`` advances ``1/scale`` times faster, so the
  service's 1 s / 2 s / 5 s / 10 s timers run in milliseconds while still
  seeing consistent timestamps.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

# Captured before any test patches them.
REAL_SLEEP = asyncio.sleep
REAL_TIME = time.time
REAL_OPEN_CONNECTION = asyncio.open_connection


# --------------------------------------------------------------------------
# Time warp
# --------------------------------------------------------------------------


class WarpClock:
    """Simulated wall clock running ``1/scale`` times faster than real time."""

    def __init__(self, scale: float = 0.05) -> None:
        self.scale = scale
        self._real0 = REAL_TIME()
        # (task, requested_delay, simulated_time) for every asyncio.sleep call
        self.sleeps: list[tuple[Any, float, float]] = []

    def time(self) -> float:
        return self._real0 + (REAL_TIME() - self._real0) / self.scale

    async def sleep(self, delay: float, result: Any = None) -> Any:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        self.sleeps.append((task, delay, self.time()))
        return await REAL_SLEEP(delay * self.scale, result)

    async def real_sleep(self, seconds: float) -> None:
        await REAL_SLEEP(seconds)

    async def sim_sleep(self, sim_seconds: float) -> None:
        """Let ``sim_seconds`` of simulated time pass."""
        await REAL_SLEEP(sim_seconds * self.scale)


# --------------------------------------------------------------------------
# Fake PLC (pylogix.PLC replacement)
# --------------------------------------------------------------------------


@dataclass
class Response:
    """Mimics ``pylogix.lgx_response.Response``."""

    TagName: str
    Value: Any
    Status: str


UNREACHABLE = "Unknown error [Errno 111] Connection refused"


@dataclass
class PlcCall:
    kind: str  # "init" | "read" | "write" | "close"
    tag: str | None = None
    value: Any = None
    t: float = 0.0
    thread: str = ""
    instance: int = 0
    failed: bool = False  # this call raised an injected PlcFault


class PlcFault(Exception):
    """Raised by the fake PLC when a test injects a failure."""


@dataclass
class PlcSim:
    """Shared state behind every ``FakePLC`` instance the service creates."""

    clock: Callable[[], float] = REAL_TIME
    tags: dict[str, Any] = field(default_factory=dict)
    calls: list[PlcCall] = field(default_factory=list)
    fail_reads: int = 0  # raise on the next N Read() calls
    fail_writes: int = 0  # raise on the next N Write() calls
    fail_write_tags: dict = field(default_factory=dict)  # tag -> raise N times
    fail_all: bool = False  # raise on every Read/Write while True
    # return an error Status (what real pylogix does when the PLC is unreachable)
    status_fail: bool = False
    write_delay: float = 0.0005  # real seconds; widens race windows
    read_gate: Any = None  # threading.Event: Read() blocks until it is set
    blocked_reads: int = 0
    instances: int = 0
    max_inflight: int = 0
    _inflight: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # ---- recording helpers ----

    def _record(self, kind: str, tag=None, value=None, instance=0) -> None:
        with self._lock:
            self.calls.append(
                PlcCall(
                    kind,
                    tag,
                    value,
                    self.clock(),
                    threading.current_thread().name,
                    instance,
                )
            )

    def _enter(self) -> None:
        with self._lock:
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)

    def _exit(self) -> None:
        with self._lock:
            self._inflight -= 1

    # ---- views ----

    def writes(self) -> list[tuple[str, Any]]:
        return [(c.tag, c.value) for c in self.calls if c.kind == "write"]

    def reads(self) -> list[str]:
        return [c.tag for c in self.calls if c.kind == "read"]

    def factory(self) -> type:
        sim = self

        class FakePLC:
            def __init__(self, ip: str = "", *args: Any, **kwargs: Any) -> None:
                with sim._lock:
                    sim.instances += 1
                    self._id = sim.instances
                self.IPAddress = ip
                sim._record("init", ip, None, self._id)

            def Read(self, tag: str, *args: Any, **kwargs: Any) -> Response:
                sim._enter()
                try:
                    sim._record("read", tag, None, self._id)
                    time.sleep(sim.write_delay)
                    gate = sim.read_gate
                    if gate is not None and not gate.is_set():
                        sim.blocked_reads += 1
                        gate.wait(5)
                    if sim.fail_all or sim.fail_reads > 0:
                        sim.fail_reads = max(0, sim.fail_reads - 1)
                        sim.calls[-1].failed = True
                        raise PlcFault(f"injected read failure on {tag}")
                    if sim.status_fail:
                        return Response(tag, None, UNREACHABLE)
                    if tag in sim.tags:
                        return Response(tag, sim.tags[tag], "Success")
                    return Response(tag, None, "Path destination unknown")
                finally:
                    sim._exit()

            def Write(self, tag: str, value: Any, *args: Any, **kwargs: Any) -> Response:
                sim._enter()
                try:
                    sim._record("write", tag, value, self._id)
                    time.sleep(sim.write_delay)
                    if sim.fail_all or sim.fail_writes > 0:
                        sim.fail_writes = max(0, sim.fail_writes - 1)
                        sim.calls[-1].failed = True
                        raise PlcFault(f"injected write failure on {tag}")
                    if sim.fail_write_tags.get(tag, 0) > 0:
                        sim.fail_write_tags[tag] -= 1
                        sim.calls[-1].failed = True
                        raise PlcFault(f"injected write failure on {tag}")
                    if sim.status_fail:
                        return Response(tag, value, UNREACHABLE)
                    return Response(tag, value, "Success")
                finally:
                    sim._exit()

            def Close(self) -> None:
                sim._record("close", None, None, self._id)

        return FakePLC


# --------------------------------------------------------------------------
# Fake QASS unit (TCP server)
# --------------------------------------------------------------------------


class FakeQass:
    """Minimal QASS look-alike TCP server on 127.0.0.1."""

    def __init__(self) -> None:
        self.server: asyncio.base_events.Server | None = None
        self.port: int = 0
        self.connections: int = 0
        self.received: list[bytes] = []  # bytes received, one entry per connection
        self._writer: asyncio.StreamWriter | None = None
        self._connected = asyncio.Event()

    async def start(self, port: int = 0) -> "FakeQass":
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        idx = len(self.received)
        self.received.append(b"")
        self._writer = writer
        self._connected.set()
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                self.received[idx] += data
        except (ConnectionError, asyncio.CancelledError):
            pass

    async def wait_connections(self, n: int, timeout: float = 5.0) -> None:
        await wait_until(lambda: self.connections >= n and self._writer is not None, timeout)
        # wait for the subscribe command of that connection
        await wait_until(lambda: len(self.received) >= n and self.received[n - 1], timeout)

    async def send(self, data: bytes | str, gap: float = 0.03) -> None:
        """Send raw bytes in a single write, then pause so it arrives as its own read."""
        if isinstance(data, str):
            data = data.encode()
        assert self._writer is not None, "no QASS client connected"
        self._writer.write(data)
        await self._writer.drain()
        await REAL_SLEEP(gap)

    async def send_records(self, *records: dict, gap: float = 0.03) -> None:
        """Send each record as its own TCP write (one JSON object per read)."""
        for r in records:
            await self.send(json.dumps(r), gap=gap)

    async def drop_client(self) -> None:
        """Close the current client connection (QASS reboot / cable pull)."""
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None

    async def stop(self) -> None:
        """Stop listening (further connects are refused) and drop the client."""
        await self.drop_client()
        if self.server is not None:
            self.server.close()
            try:
                await asyncio.wait_for(self.server.wait_closed(), 1)
            except Exception:
                pass
            self.server = None


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


async def wait_until(cond: Callable[[], Any], timeout: float = 5.0, interval: float = 0.002) -> None:
    """Poll ``cond`` (real time) until truthy or fail the test."""
    deadline = REAL_TIME() + timeout
    while True:
        if cond():
            return
        if REAL_TIME() > deadline:
            raise AssertionError(f"condition not met within {timeout}s: {cond}")
        await REAL_SLEEP(interval)


def rec(measuring: Any, e_current: Any = 0, e_max: Any = 0, limitbreak: Any = 0,
        limitcrack: Any = 0, **extra: Any) -> dict:
    """Build a QASS reading shaped like the unit's ``sendreadings`` output."""
    r = {
        "measuring": measuring,
        "e_current": e_current,
        "e_max": e_max,
        "limitbreak": limitbreak,
        "limitcrack": limitcrack,
    }
    r.update(extra)
    return r
