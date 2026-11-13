"""HTTP endpoints: /health, /metrics, /logs."""

from __future__ import annotations

import os
import sys

import aiohttp
import pytest

import paths
from conftest import SERIAL_TAG, free_port, make_cfg
from fakes import REAL_SLEEP, rec, wait_until

HEALTH_KEYS = {"status", "qass_connected", "last_qass_data_s", "serial"}
METRICS_KEYS = {"healthy", "reason", "metrics", "qass_queue_depth",
                "plc_queue_depth", "last_plc_write_ms"}
SNAPSHOT_KEYS = {"uptime_seconds", "qass_connected", "qass_generation", "plc_connected",
                 "strokes_processed", "error_count", "last_error_code", "last_error_name",
                 "last_error_ts", "error_counts"}


async def get(port: int, path: str):
    async with aiohttp.ClientSession() as s:
        async with s.get(f"http://127.0.0.1:{port}{path}") as r:
            return r.status, await r.json()


async def wait_http(port: int) -> None:
    async def ok():
        try:
            await get(port, "/health")
            return True
        except aiohttp.ClientError:
            return False

    for _ in range(500):
        if await ok():
            return
        await REAL_SLEEP(0.01)
    raise AssertionError("health server never came up")


async def test_health_and_metrics_when_connected(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN7"
    port = free_port()
    async with run_service(make_cfg(qass.port, port)) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN7")
        await qass.send_records(rec("true", 3, 0, 1, 1), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        await wait_http(port)

        status, body = await get(port, "/health")
        assert status == 200
        assert set(body) == HEALTH_KEYS
        assert body["status"] == "ok"
        assert body["qass_connected"] is True
        assert body["serial"] == "SN7"
        assert isinstance(body["last_qass_data_s"], int)

        status, body = await get(port, "/metrics")
        assert status == 200
        assert set(body) == METRICS_KEYS
        assert set(body["metrics"]) == SNAPSHOT_KEYS
        assert body["healthy"] is True and body["reason"] == "OK"
        assert body["metrics"]["strokes_processed"] == 1
        assert body["qass_queue_depth"] == 0
        assert body["plc_queue_depth"] == 0
        assert isinstance(body["last_plc_write_ms"], (int, float))


async def test_health_200_and_metrics_503_when_qass_down(run_service, plc, qass, conns):
    plc.tags[SERIAL_TAG] = "SN7"
    conns.refuse = True
    port = free_port()
    async with run_service(make_cfg(qass.port, port)):
        await wait_http(port)
        await wait_until(lambda: len(conns.attempts) >= 1)

        status, body = await get(port, "/health")
        assert status == 200
        assert set(body) == HEALTH_KEYS
        assert body["qass_connected"] is False

        status, body = await get(port, "/metrics")
        assert status == 503
        assert set(body) == METRICS_KEYS
        assert body["healthy"] is False
        assert body["reason"] == "QASS disconnected"
        assert body["metrics"]["error_counts"].get("QASS_DISCONNECTED", 0) >= 1


async def test_logs_returns_last_100_lines(run_service, plc, qass, monkeypatch):
    monkeypatch.setenv("QASS_LOG_FILE", "qass_test_http.log")
    log_dir = paths.LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    lines = [f"line {i}\n" for i in range(150)]
    (log_dir / "qass_test_http.log").write_text("".join(lines))

    port = free_port()
    async with run_service(make_cfg(qass.port, port)):
        await wait_http(port)
        status, body = await get(port, "/logs")
    assert status == 200
    assert body == {"logs": lines[-100:]}


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc/net/tcp")
async def test_health_server_binds_all_interfaces(run_service, plc, qass):
    port = free_port()
    async with run_service(make_cfg(qass.port, port)):
        await wait_http(port)
        listening = set()
        for table in ("/proc/net/tcp", "/proc/net/tcp6"):
            if not os.path.exists(table):
                continue
            for row in open(table).read().splitlines()[1:]:
                local, _, st = row.split()[1:4]
                addr, p = local.split(":")
                if st == "0A" and int(p, 16) == port:  # LISTEN
                    listening.add(addr)
    assert listening and all(set(a) == {"0"} for a in listening), listening
