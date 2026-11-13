"""PLC call serialization, heartbeat/status contract, watchdogs and error codes."""

from __future__ import annotations

import threading

import pytest

from conftest import BREAK_TAG, SERIAL_TAG, STATUS_TAGS, free_port, make_cfg
from fakes import rec, wait_until


def heartbeat_blocks(plc):
    """Split the call log into heartbeat transactions; each must be the 6 status
    writes, contiguous and in contract order."""
    calls = [c for c in plc.calls if c.kind in ("read", "write")]
    blocks = []
    i = 0
    while i < len(calls):
        c = calls[i]
        if c.failed:  # transaction aborted by an injected fault
            i += 1
            continue
        if c.kind == "write" and c.tag == STATUS_TAGS[0]:
            block = calls[i:i + 6]
            if len(block) < 6:
                break  # in-flight at the end of the test
            assert [b.tag for b in block] == STATUS_TAGS, [b.tag for b in block]
            assert all(b.kind == "write" for b in block)
            blocks.append(block)
            i += 6
        else:
            assert not (c.kind == "write" and c.tag in STATUS_TAGS), "stray status write"
            i += 1
    return blocks


def values(block):
    return [c.value for c in block]


async def test_startup_reads_serial_first(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())):
        await wait_until(lambda: len(plc.calls) >= 2)
    assert (plc.calls[0].kind, plc.calls[0].tag) == ("init", "10.99.0.1")
    assert (plc.calls[1].kind, plc.calls[1].tag) == ("read", SERIAL_TAG)


async def test_plc_calls_run_in_executor_and_never_overlap(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        for _ in range(5):
            await qass.send_records(rec("true", 3, 0, 1, 1), rec("false"), gap=0.005)
        await clock.sim_sleep(12)
    main = threading.main_thread().name
    io_calls = [c for c in plc.calls if c.kind in ("read", "write")]
    assert io_calls and all(c.thread != main for c in io_calls)
    assert plc.max_inflight == 1


async def test_heartbeat_contract(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        await qass.send_records(rec("true", 3, 0, 1, 1), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        for _ in range(8):  # keep QASS data fresh -> error code 0
            await qass.send_records(rec("false"))
            await clock.sim_sleep(2)
        blocks = heartbeat_blocks(plc)

    assert len(blocks) >= 3
    hb = [values(b)[0] for b in blocks]
    assert hb == [1, 0, 1, 0, 1, 0, 1, 0][: len(hb)]
    last = values(blocks[-1])
    assert last[1] is True                   # service_ok
    assert last[2] is True                   # qass_ok
    assert last[3] is True                   # plc_ok
    assert last[4] == 2                      # last_stroke = current stroke counter
    assert last[5] == 0 and type(last[5]) is int  # error code NONE
    # heartbeat period 5 s
    starts = [b[0].t for b in blocks]
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert all(abs(g - 5) < 0.75 for g in gaps), gaps


async def test_timer_intervals(run_service, plc, qass, clock, conns):
    """ident loop 1 s, PLC watchdog 2 s, QASS watchdog 2 s, heartbeat 5 s."""
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())):
        await qass.wait_connections(1)
        await clock.sim_sleep(11)
    receiver = conns.attempts[0][0]
    per_task: dict = {}
    for task, delay, _ in clock.sleeps:
        if task is receiver or delay == 0:
            continue
        per_task.setdefault(task, set()).add(delay)
    assert sorted(sorted(s) for s in per_task.values()) == [[1], [2], [2], [5]]

    serial_reads = [c.t for c in plc.calls if c.kind == "read"]
    gaps = [b - a for a, b in zip(serial_reads[1:], serial_reads[2:])]
    assert all(abs(g - 1) < 0.5 for g in gaps), gaps


async def test_plc_exception_recreates_connection_and_sets_error(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        errors_before = svc.worker.metrics.error_count
        plc.fail_reads = 1
        await wait_until(lambda: svc.worker.last_error_code == 20, timeout=3, interval=0.0005)
        await wait_until(lambda: plc.instances == 2)
        assert svc.worker.metrics.error_count == errors_before + 1
        assert svc.worker.metrics.error_counts[20] == 1
        assert svc.worker.metrics.snapshot()["error_counts"]["PLC_COMMS"] == 1
        n = len(plc.calls)
        await clock.sim_sleep(3)

    kinds = [(c.kind, c.instance) for c in plc.calls]
    fail_idx = next(i for i, c in enumerate(plc.calls) if c.kind == "close" and c.instance == 1)
    assert plc.calls[fail_idx - 1].kind == "read"           # the failing call
    assert (plc.calls[fail_idx + 1].kind, plc.calls[fail_idx + 1].tag) == ("init", "10.99.0.1")
    # traffic continues on the new object
    assert any(k == ("read", 2) for k in kinds[n:])


async def test_plc_watchdog_threshold(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), enable_writes=False)
    async with run_service(cfg) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        await clock.sim_sleep(3)
        assert svc.worker.metrics.plc_connected is True
        plc.fail_all = True
        await clock.sim_sleep(6)
        assert svc.worker.metrics.plc_connected is True   # < 10 s without success
        await clock.sim_sleep(8)
        assert svc.worker.metrics.plc_connected is False  # > 10 s
        plc.fail_all = False
        await clock.sim_sleep(4)
        assert svc.worker.metrics.plc_connected is True


async def test_error_codes_written_to_plc(run_service, plc, qass, clock, conns):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        for _ in range(4):
            await qass.send_records(rec("false"))
            await clock.sim_sleep(2)
        n_ok = len(heartbeat_blocks(plc))

        # QASS connected but silent > 10 s -> 11 QASS_NO_DATA
        await clock.sim_sleep(17)
        n_nodata = len(heartbeat_blocks(plc))

        # QASS gone -> 10 QASS_DISCONNECTED
        conns.refuse = True
        await qass.drop_client()
        await clock.sim_sleep(8)
        blocks = heartbeat_blocks(plc)

    ok = [values(b) for b in blocks[:n_ok]]
    nodata = [values(b) for b in blocks[n_ok:n_nodata]]
    gone = [values(b) for b in blocks[n_nodata:]]
    assert ok[-1][5] == 0 and ok[-1][2] is True
    assert nodata[-1][5] == 11 and nodata[-1][2] is True
    assert gone[-1][5] == 10 and gone[-1][2] is False
    assert {v[5] for v in ok + nodata + gone} <= {0, 10, 11}


# ---------------------------------------------------------------------------
# Known bugs (strict xfail)
# ---------------------------------------------------------------------------


async def test_heartbeat_survives_a_plc_exception(run_service, plc, qass, clock, caplog):
    """Fixed bug: one raising heartbeat write used to stop the heartbeat forever."""
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: len(heartbeat_blocks(plc)) >= 1)
        n = len(heartbeat_blocks(plc))
        plc.fail_writes = 1
        await clock.sim_sleep(16)
        assert svc.worker.metrics.plc_connected is True
    assert len(heartbeat_blocks(plc)) >= n + 2
    assert "PLC heartbeat write failed" in caplog.text
    # the toggle keeps alternating across the failed cycle
    hb = [values(b)[0] for b in heartbeat_blocks(plc)]
    assert all(v in (0, 1) for v in hb)


@pytest.mark.parametrize("enable_writes", [False, True])
async def test_unreachable_plc_trips_watchdog(run_service, plc, qass, clock, caplog, enable_writes):
    """Fixed bug: pylogix returns an error Status (it does not raise) when the PLC
    is unreachable, and that used to count as a successful call."""
    caplog.set_level("INFO")
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), enable_writes=enable_writes)
    async with run_service(cfg) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        errors_before = svc.worker.metrics.error_counts[20]
        plc.status_fail = True  # what real pylogix does when the PLC is down
        await clock.sim_sleep(6)
        assert svc.worker.metrics.plc_connected is True   # < 10 s
        await clock.sim_sleep(8)
        assert svc.worker.metrics.plc_connected is False  # > 10 s
        assert svc.worker.metrics.error_counts[20] > errors_before
        plc.status_fail = False
        await clock.sim_sleep(4)
        assert svc.worker.metrics.plc_connected is True
    assert caplog.text.count("PLC comms failure: Unknown error [Errno 111]") == 1
    assert "PLC comms restored" in caplog.text


@pytest.mark.parametrize("status,is_failure", [
    ("Success", False),
    ("Path destination unknown", False),   # PLC answered: tag problem, not comms
    ("Unknown error 255", False),          # PLC answered with an unknown CIP code
    ("Unknown error Success", False),
    ("Connection failure", True),
    ("Connection lost", True),
    ("Register session failed", True),
    ("Forward open failed", True),
    ("Unknown error [Errno 111] Connection refused", True),
    ("Unknown error timed out", True),
])
def test_comms_failure_classification(status, is_failure):
    from worker_async import _is_comms_failure
    assert _is_comms_failure(status) is is_failure


async def test_missing_tag_is_not_a_comms_failure(run_service, plc, qass, clock):
    # serial tag not defined in the PLC -> "Path destination unknown" on every read
    cfg = make_cfg(qass.port, free_port(), enable_writes=False)
    async with run_service(cfg) as svc:
        await qass.wait_connections(1)
        await clock.sim_sleep(14)
        assert svc.worker.metrics.plc_connected is True
        assert svc.worker.ident is None


async def test_shutdown_closes_plc_once(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())):
        await qass.wait_connections(1)
    assert [c.kind for c in plc.calls].count("close") == 1


async def test_plc_comms_error_code_is_held_for_10s(run_service, plc, qass, clock):
    """Fixed bug: error 20 used to be overwritten by the QASS watchdog within 2 s,
    so the PLC almost never saw it."""
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        for _ in range(3):
            await qass.send_records(rec("false"))
            await clock.sim_sleep(2)
        n_before = len(heartbeat_blocks(plc))
        plc.fail_reads = 1                      # one failed serial read
        for _ in range(12):                     # keep QASS data fresh (no code 11)
            await qass.send_records(rec("false"))
            await clock.sim_sleep(2)
        blocks = heartbeat_blocks(plc)
    codes = [values(b)[5] for b in blocks[n_before:]]
    assert codes[0] == 20 and codes[1] == 20, codes      # written at least twice
    assert codes[-1] == 0 and codes[-2] == 0, codes      # cleared after ~10 s
    assert set(codes) == {0, 20}


async def test_qass_codes_take_priority_over_plc_code(run_service, plc, qass, clock, conns):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        conns.refuse = True
        await qass.drop_client()
        plc.fail_reads = 1
        await clock.sim_sleep(5)
        assert svc.worker.last_error_code == 10



async def test_unreachable_plc_aborts_rest_of_transaction(run_service, plc, qass, clock):
    """With the PLC unreachable every pylogix call waits for its socket timeout
    (5 s), so after the first comms failure the rest of a heartbeat or
    measurement transaction is skipped instead of holding the lock ~30 s."""
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        n = len(plc.calls)
        plc.status_fail = True
        await qass.send_records(rec("true", 10, 0, 1, 1), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        await clock.sim_sleep(11)
        down = [(c.kind, c.tag) for c in plc.calls[n:] if c.kind == "write"]
        plc.status_fail = False
        m = len(plc.calls)
        await clock.sim_sleep(6)
        after = [c.tag for c in plc.calls[m:] if c.kind == "write"]

    status_writes = [t for k, t in down if t in STATUS_TAGS]
    assert status_writes and set(status_writes) == {"QASS_Heartbeat"}  # 1 per heartbeat
    meas = [t for k, t in down if t not in STATUS_TAGS]
    assert meas == [BREAK_TAG]                                       # crack/current skipped
    i = after.index(STATUS_TAGS[0])                                  # full blocks again
    assert after[i:i + 6] == STATUS_TAGS                             # after recovery
