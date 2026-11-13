"""End-to-end: run main.py as a real process (fake PLC, fake QASS server).

Covers config-path resolution, the "exit non-zero when QASS is silent" restart
contract, and the log file setup (path, format, rotation).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sys
from pathlib import Path

import aiohttp
import pytest
import yaml

from conftest import BREAK_TAG, CRACK_TAG, CURRENT_TAG, REPO_ROOT, SERIAL_TAG, free_port, make_cfg
from fakes import REAL_SLEEP, FakeQass, rec

LAUNCHER = Path(__file__).with_name("launcher.py")
LOG_FMT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LINE_RE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} (INFO|ERROR|WARNING|DEBUG) \S+ .*")


def copy_app(dst: Path) -> Path:
    app = dst / "app"
    app.mkdir()
    for py in REPO_ROOT.glob("*.py"):
        shutil.copy(py, app / py.name)
    shutil.copytree(REPO_ROOT / "configs", app / "configs")
    return app


async def http_up(port: int, timeout: float = 10) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"http://127.0.0.1:{port}/health") as r:
                    return await r.json()
        except aiohttp.ClientError:
            if asyncio.get_running_loop().time() > deadline:
                raise
            await REAL_SLEEP(0.05)


async def run_main(tmp_path: Path, qass: FakeQass, cfg: dict, cfg_name: str | None,
                   arg: str | None, log_file: str = "qass_proc.log"):
    app = copy_app(tmp_path)
    if cfg_name:
        (app / "configs" / cfg_name).write_text(yaml.safe_dump(cfg))
    out = tmp_path / "out.json"
    state, logs = tmp_path / "state", tmp_path / "logs"
    env = dict(os.environ, QASS_LOG_FILE=log_file, QASS_LOG_DIR=str(logs),
               QASS_STATE_DIR=str(state), FAKE_SERIAL_TAG=SERIAL_TAG,
               PYTHONUNBUFFERED="1")
    args = [sys.executable, str(LAUNCHER), str(app), str(out), "SN42"]
    if arg is not None:
        args.append(arg.replace("{app}", str(app)))
    proc = await asyncio.create_subprocess_exec(
        *args, env=env, cwd=str(tmp_path),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    return app, proc, out, logs / log_file


async def drive_one_stroke_then_go_silent(qass: FakeQass, port: int, proc) -> int:
    await qass.wait_connections(1, timeout=10)
    body = await http_up(port)
    assert body["status"] == "ok"
    await REAL_SLEEP(1.5)  # let the 1 s serial loop read SN42
    await qass.send_records(rec("true", 12, 34, 56, 78), rec("false"))
    # now stay silent: disconnect_timeout_sec=2 -> exit within ~4 s
    return await asyncio.wait_for(proc.wait(), timeout=20)


@pytest.mark.parametrize("arg", ["somewhere/else/proc_cfg.yaml", "proc_cfg.yaml",
                                 "{app}/configs/proc_cfg.yaml"])
async def test_config_path_resolution(tmp_path, qass, arg):
    port = free_port()
    cfg = make_cfg(qass.port, port, qass={"disconnect_timeout_sec": 2})
    app, proc, out, _ = await run_main(tmp_path, qass, cfg, "proc_cfg.yaml", arg)
    try:
        assert await drive_one_stroke_then_go_silent(qass, port, proc) != 0
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


async def test_default_config_is_configs_default_yaml(tmp_path, qass):
    port = free_port()
    cfg = make_cfg(qass.port, port, qass={"disconnect_timeout_sec": 2})
    app, proc, out, _ = await run_main(tmp_path, qass, cfg, "default.yaml", None)
    try:
        assert await drive_one_stroke_then_go_silent(qass, port, proc) != 0
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


async def test_exit_on_silence_logging_and_plc_writes(tmp_path, qass):
    port = free_port()
    cfg = make_cfg(qass.port, port, qass={"disconnect_timeout_sec": 2})
    app, proc, out, log_path = await run_main(
        tmp_path, qass, cfg, "proc_cfg.yaml", "configs/proc_cfg.yaml", "qass_machine9.log")
    try:
        code = await drive_one_stroke_then_go_silent(qass, port, proc)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    console = (await proc.stdout.read()).decode()
    assert code == 1, console

    dump = json.loads(out.read_text())
    writes = [(t, v) for k, t, v in dump["calls"] if k == "write"]
    meas = [(t, v) for t, v in writes if not t.startswith("QASS_")]
    assert meas == [(BREAK_TAG, 56), (CRACK_TAG, 78), (CURRENT_TAG, 12)]

    # log handlers: rotating 10 MB x 5 + console, identical format
    handlers = {h["cls"]: h for h in dump["handlers"]}
    assert set(handlers) == {"RotatingFileHandler", "StreamHandler"}
    rot = handlers["RotatingFileHandler"]
    assert rot["maxBytes"] == 10 * 1024 * 1024 and rot["backupCount"] == 5
    assert rot["fmt"] == LOG_FMT and handlers["StreamHandler"]["fmt"] == LOG_FMT
    assert Path(rot["file"]) == log_path
    assert dump["level"] == 20  # INFO

    text = log_path.read_text()
    assert "Serial=SN42 Stroke=1 ECurrent=12 EMax=34 Break=56 Crack=78" in text
    assert "QASS disconnected for 2 seconds → exiting container" in text
    first = text.splitlines()[0]
    assert LINE_RE.match(first), first


async def test_invalid_config_exits_with_clear_message(tmp_path, qass):
    cfg = make_cfg(qass.port, free_port())
    del cfg["plc"]["serial_tag"]
    app, proc, out, log_path = await run_main(tmp_path, qass, cfg, "bad.yaml", "bad.yaml")
    code = await asyncio.wait_for(proc.wait(), timeout=20)
    console = (await proc.stdout.read()).decode()
    assert code == 1
    assert "Invalid config:" in console and "plc.serial_tag" in console
    assert "Traceback" not in console


async def test_state_file_is_per_machine_in_state_dir(tmp_path, qass):
    """Fixed bug: state.db used to be written to the working dir (/app), which
    is lost when the container is rebuilt. It now lives in QASS_STATE_DIR (the
    qass_state volume) with a per-machine name."""
    import sqlite3

    port = free_port()
    cfg = make_cfg(qass.port, port, qass={"disconnect_timeout_sec": 2})
    app, proc, out, _ = await run_main(
        tmp_path, qass, cfg, "config_machine7.yaml", "configs/config_machine7.yaml")
    try:
        await drive_one_stroke_then_go_silent(qass, port, proc)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    db = tmp_path / "state" / "state_config_machine7.db"
    assert db.exists()
    assert not (app / "state.db").exists()
    row = sqlite3.connect(db).execute("SELECT ident, stroke, buffer FROM stroke_state").fetchone()
    assert row == ("SN42", 2, "[]")

