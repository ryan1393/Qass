"""Shared fixtures: fake PLC, fake QASS unit, time warp and a service runner."""

from __future__ import annotations

import asyncio
import copy
import os
import socket
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import pytest

# Must be set before ``paths`` is imported anywhere.
_SESSION_DIR = Path(tempfile.mkdtemp(prefix="qass-tests-"))
os.environ.setdefault("QASS_LOG_DIR", str(_SESSION_DIR / "logs"))
os.environ.setdefault("QASS_STATE_DIR", str(_SESSION_DIR / "state"))
(_SESSION_DIR / "logs").mkdir(exist_ok=True)
(_SESSION_DIR / "state").mkdir(exist_ok=True)

import state_store  # noqa: E402
import worker_async  # noqa: E402
from fakes import REAL_OPEN_CONNECTION, FakeQass, PlcSim, WarpClock  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent

# Status tags, in the order the PLC program expects them on every heartbeat.
STATUS_TAGS = [
    "QASS_Heartbeat",
    "QASS_ServiceOK",
    "QASS_QassConnected",
    "QASS_PlcCommOK",
    "QASS_LastStroke",
    "QASS_LastErrorCode",
]

SERIAL_TAG = "PROGRAM:PC_COMM.COM_TT200_From_PC.Serial_Num"
BREAK_TAG = "PROGRAM:PC_COMM.COM_TT200_From_PC.Qass_Break_Limit"
CRACK_TAG = "PROGRAM:PC_COMM.COM_TT200_From_PC.Qass_Crack_Limit"
CURRENT_TAG = "PROGRAM:PC_COMM.COM_TT250_From_PC.Qass_Measurement"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def make_cfg(qass_port: int, health_port: int, qass_type: str = "auto",
             enable_writes: bool = True, **sections: Any) -> dict:
    """A config shaped exactly like configs/config_machineN.yaml."""
    cfg = {
        "qass": {
            "ip": "127.0.0.1",
            "port": qass_port,
            "type": qass_type,
            # effectively disabled for in-process tests (it calls sys.exit)
            "disconnect_timeout_sec": 10**7,
        },
        "plc": {
            "ip": "10.99.0.1",
            "serial_tag": SERIAL_TAG,
            "break_tag": BREAK_TAG,
            "crack_tag": CRACK_TAG,
            "current_tag": CURRENT_TAG,
            "heartbeat_tag": "QASS_Heartbeat",
            "service_ok_tag": "QASS_ServiceOK",
            "qass_ok_tag": "QASS_QassConnected",
            "plc_ok_tag": "QASS_PlcCommOK",
            "last_stroke_tag": "QASS_LastStroke",
            "error_code_tag": "QASS_LastErrorCode",
            "enable_writes": enable_writes,
        },
        "health": {"port": health_port},
        "reconnect": {"initial_delay": 1, "max_delay": 30},
    }
    for name, values in sections.items():
        cfg[name].update(values)
    return cfg


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> WarpClock:
    # real seconds per simulated second; raise it on slow machines (CI uses 0.1)
    c = WarpClock(scale=float(os.getenv("QASS_TEST_TIME_SCALE", "0.05")))
    monkeypatch.setattr(time, "time", c.time)
    # the service measures intervals with worker_async.monotonic()
    monkeypatch.setattr(worker_async, "monotonic", c.mono)
    monkeypatch.setattr(asyncio, "sleep", c.sleep)
    return c


@pytest.fixture
def plc(monkeypatch: pytest.MonkeyPatch, clock: WarpClock) -> PlcSim:
    sim = PlcSim(clock=clock.time)
    monkeypatch.setattr(worker_async, "PLC", sim.factory())
    return sim


class ConnRecorder:
    """Wraps asyncio.open_connection: records attempts, can refuse them."""

    def __init__(self, clock: WarpClock) -> None:
        self.clock = clock
        self.attempts: list[tuple[Any, float, bool]] = []  # (task, sim_time, ok)
        self.refuse = False

    async def __call__(self, *args: Any, **kwargs: Any):
        task = asyncio.current_task()
        if self.refuse:
            self.attempts.append((task, self.clock.time(), False))
            raise ConnectionRefusedError("injected: QASS unreachable")
        try:
            result = await REAL_OPEN_CONNECTION(*args, **kwargs)
        except Exception:
            self.attempts.append((task, self.clock.time(), False))
            raise
        self.attempts.append((task, self.clock.time(), True))
        return result


@pytest.fixture
def conns(monkeypatch: pytest.MonkeyPatch, clock: WarpClock) -> ConnRecorder:
    rec = ConnRecorder(clock)
    monkeypatch.setattr(asyncio, "open_connection", rec)
    return rec


@pytest.fixture
async def qass() -> AsyncIterator[FakeQass]:
    server = await FakeQass().start()
    yield server
    await server.stop()


@pytest.fixture
def saves(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, int, list]]:
    """Record every StrokeStateStore.save(ident, stroke, buffer) call."""
    calls: list[tuple[Any, int, list]] = []
    orig = state_store.StrokeStateStore.save

    def spy(self, ident, stroke, buffer):
        calls.append((ident, stroke, copy.deepcopy(list(buffer))))
        return orig(self, ident, stroke, buffer)

    monkeypatch.setattr(state_store.StrokeStateStore, "save", spy)
    return calls


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate files the service writes relative to CWD or the state dir."""
    monkeypatch.chdir(tmp_path)
    state = tmp_path / "state"
    logs = tmp_path / "logs"
    state.mkdir()
    logs.mkdir()
    monkeypatch.setenv("QASS_STATE_DIR", str(state))
    monkeypatch.setenv("QASS_LOG_DIR", str(logs))
    return tmp_path


class Service:
    def __init__(self, worker: Any, task: asyncio.Task, event: asyncio.Event) -> None:
        self.worker = worker
        self.task = task
        self.event = event

    async def stop(self) -> None:
        self.event.set()
        try:
            await asyncio.wait_for(self.task, 5)
        finally:
            tasks = [t for t in getattr(self.worker, "tasks", []) if not t.done()]
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def run_service(workdir: Path, plc: PlcSim, conns: ConnRecorder, saves):
    """Start AsyncWorker exactly like main.py does and stop it afterwards."""
    started: list[Service] = []

    @asynccontextmanager
    async def _run(cfg: dict) -> AsyncIterator[Service]:
        # mirrors main.main(): per-machine state file in the state dir
        worker = worker_async.AsyncWorker(cfg, workdir / "state" / "state_test.db")
        event = asyncio.Event()

        async def main_like() -> None:
            # mirrors main.main()
            try:
                await worker.start(event)
            finally:
                await worker.shutdown()

        svc = Service(worker, asyncio.create_task(main_like()), event)
        started.append(svc)
        try:
            yield svc
        finally:
            await svc.stop()

    return _run
