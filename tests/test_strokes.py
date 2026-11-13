"""Stroke detection and the measurement-write contract with the PLC."""

from __future__ import annotations

import json
import logging

import pytest

from conftest import (BREAK_TAG, CRACK_TAG, CURRENT_TAG, SERIAL_TAG, STATUS_TAGS,
                      free_port, make_cfg)
from fakes import rec, wait_until


def measurement_writes(plc):
    return [(t, v) for t, v in plc.writes() if t not in STATUS_TAGS]


def assert_blocks_contiguous(plc, block_len: int) -> None:
    """Measurement writes for one stroke must be one uninterrupted PLC transaction."""
    calls = [c for c in plc.calls if c.kind in ("read", "write")]
    idx = [i for i, c in enumerate(calls) if c.kind == "write" and c.tag not in STATUS_TAGS]
    assert len(idx) % block_len == 0
    for k in range(0, len(idx), block_len):
        block = idx[k:k + block_len]
        assert block == list(range(block[0], block[0] + block_len)), block


async def ready(svc, qass, serial: str = "SN1") -> None:
    await qass.wait_connections(1)
    await wait_until(lambda: svc.worker.ident == serial)


# ---------------------------------------------------------------------------
# auto / manual golden sequences
# ---------------------------------------------------------------------------


async def test_auto_mode_golden_write_sequence(run_service, plc, qass, caplog):
    caplog.set_level(logging.INFO)
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), qass_type="auto")
    async with run_service(cfg) as svc:
        await ready(svc, qass)

        # stroke 1: highest e_current sample is the 2nd one
        await qass.send_records(
            rec("true", 10, 100, 500, 400),
            rec("true", 30, 120, 510, 410),
            rec("true", 20, 130, 520, 420),
            rec("false", 0, 0, 0, 0),
        )
        await wait_until(lambda: len(measurement_writes(plc)) == 3)

        # stroke 2: string values, mixed-case flags, missing optional keys
        await qass.send_records(
            {"measuring": "TRUE", "e_current": "7", "e_max": "70",
             "limitbreak": "900", "limitcrack": "800"},
            {"measuring": "True", "e_current": "8"},
            {"measuring": "False"},
        )
        await wait_until(lambda: len(measurement_writes(plc)) == 6)
        assert svc.worker.stroke == 3

    assert measurement_writes(plc) == [
        (BREAK_TAG, 510), (CRACK_TAG, 410), (CURRENT_TAG, 30),
        # max e_current=8 has no limit keys -> defaults of 0
        (BREAK_TAG, 0), (CRACK_TAG, 0), (CURRENT_TAG, 8),
    ]
    assert_blocks_contiguous(plc, 3)
    # values are ints, not strings
    assert all(type(v) is int for _, v in measurement_writes(plc))

    lines = [r.getMessage() for r in caplog.records]
    assert "Serial=SN1 Stroke=1 ECurrent=30 EMax=120 Break=510 Crack=410" in lines
    assert "Serial=SN1 Stroke=2 ECurrent=8 EMax=0 Break=0 Crack=0" in lines


async def test_manual_mode_golden_write_sequence(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), qass_type="manual")
    del cfg["plc"]["current_tag"]  # manual configs have no current_tag
    async with run_service(cfg) as svc:
        await ready(svc, qass)
        for stroke in range(1, 4):
            await qass.send_records(
                rec("true", stroke * 10, 0, 100 + stroke, 200 + stroke),
                rec("false"),
            )
            await wait_until(lambda: len(measurement_writes(plc)) == 3 * stroke)

    assert measurement_writes(plc) == [
        (BREAK_TAG, 101), (CRACK_TAG, 201), ("QASS_Measurement_1", 10),
        (BREAK_TAG, 102), (CRACK_TAG, 202), ("QASS_Measurement_2", 20),
        (BREAK_TAG, 103), (CRACK_TAG, 203), ("QASS_Measurement_3", 30),
    ]
    assert_blocks_contiguous(plc, 3)


async def test_ties_pick_first_sample_with_highest_e_current(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(
            rec("true", 50, 0, 1, 1), rec("true", 50, 0, 2, 2), rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 3)
    assert measurement_writes(plc) == [(BREAK_TAG, 1), (CRACK_TAG, 1), (CURRENT_TAG, 50)]


async def test_non_measuring_without_buffer_writes_nothing(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("false"), rec("false"), rec(False))
        await clock.sim_sleep(3)
        assert svc.worker.stroke == 1
    assert measurement_writes(plc) == []


async def test_concatenated_payload_in_one_read(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        blob = "".join(json.dumps(r) for r in (
            rec("true", 1, 0, 11, 12), rec("true", 9, 0, 13, 14),
            rec("true", 3, 0, 15, 16), rec("false")))
        await qass.send(blob)
        await wait_until(lambda: len(measurement_writes(plc)) == 3)
    assert measurement_writes(plc) == [(BREAK_TAG, 13), (CRACK_TAG, 14), (CURRENT_TAG, 9)]


async def test_payload_without_measuring_key_is_ignored(run_service, plc, qass, clock):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(
            rec("true", 10, 0, 1, 1),
            {"e_current": 999, "status": "idle"},  # would end the stroke if queued
            rec("true", 20, 0, 2, 2),
            rec("false"),
        )
        await wait_until(lambda: len(measurement_writes(plc)) == 3)
        await clock.sim_sleep(2)
        assert svc.worker.stroke == 2
    assert measurement_writes(plc) == [(BREAK_TAG, 2), (CRACK_TAG, 2), (CURRENT_TAG, 20)]


async def test_unsupported_type_raises_on_write(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), qass_type="semi")
    async with run_service(cfg) as svc:
        await ready(svc, qass)
        with pytest.raises(ValueError, match="Unsupported qass type: semi"):
            await svc.worker._write_plc(1, 2, 3)
    assert measurement_writes(plc) == []


# ---------------------------------------------------------------------------
# enable_writes, serial changes, state saves
# ---------------------------------------------------------------------------


async def test_enable_writes_false_skips_every_plc_write(run_service, plc, qass, clock, caplog):
    caplog.set_level(logging.INFO)
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), enable_writes=False)
    async with run_service(cfg) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("true", 5, 6, 7, 8), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        await clock.sim_sleep(12)  # > 2 heartbeat periods
    assert plc.writes() == []
    assert plc.reads().count(SERIAL_TAG) >= 8  # reads still happen
    msgs = [r.getMessage() for r in caplog.records]
    assert "PLC writes DISABLED" in msgs
    assert "Serial=SN1 Stroke=1 ECurrent=5 EMax=6 Break=7 Crack=8" in msgs
    assert "PLC writes disabled — skipping measurement write" in msgs


async def test_serial_change_resets_stroke_and_buffer(run_service, plc, qass, saves):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), qass_type="manual")
    async with run_service(cfg) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("true", 10, 0, 1, 1), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)

        # half a stroke, then a new part arrives
        await qass.send_records(rec("true", 99, 0, 9, 9), rec("true", 98, 0, 9, 9))
        await wait_until(lambda: len(svc.worker.stroke_buffer) == 2)
        plc.tags[SERIAL_TAG] = "SN2"
        await wait_until(lambda: svc.worker.ident == "SN2")
        assert svc.worker.stroke == 1
        assert svc.worker.stroke_buffer == []

        # the half stroke is gone: end marker alone writes nothing
        await qass.send_records(rec("false"))
        await qass.send_records(rec("true", 20, 0, 2, 2), rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 6)

    assert measurement_writes(plc) == [
        (BREAK_TAG, 1), (CRACK_TAG, 1), ("QASS_Measurement_1", 10),
        (BREAK_TAG, 2), (CRACK_TAG, 2), ("QASS_Measurement_1", 20),
    ]
    assert ("SN2", 1, []) in saves


@pytest.mark.parametrize("value", ["", None])
async def test_empty_serial_is_ignored(run_service, plc, qass, clock, value):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("true", 10), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        plc.tags[SERIAL_TAG] = value
        await clock.sim_sleep(4)
        assert svc.worker.ident == "SN1"
        assert svc.worker.stroke == 2


async def test_same_serial_does_not_reset(run_service, plc, qass, clock, saves):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("true", 10), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)
        await clock.sim_sleep(4)
        assert svc.worker.stroke == 2
    assert [s for s in saves if s[1] == 1 and s[2] == []] == [("SN1", 1, [])]


async def test_state_saved_every_25_samples_and_after_stroke(run_service, plc, qass, saves):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        samples = [rec("true", i, 0, i, i) for i in range(60)]
        for k in range(0, 60, 10):  # 10 objects per TCP write
            await qass.send("".join(json.dumps(r) for r in samples[k:k + 10]))
        await wait_until(lambda: len(svc.worker.stroke_buffer) == 60)
        await qass.send_records(rec("false"))
        await wait_until(lambda: svc.worker.stroke == 2)

    assert [(i, s, len(b)) for i, s, b in saves] == [
        ("SN1", 1, 0),   # serial read at startup
        ("SN1", 1, 25),
        ("SN1", 1, 50),
        ("SN1", 2, 0),   # after the stroke
    ]
    assert saves[1][2] == samples[:25]
    assert measurement_writes(plc) == [(BREAK_TAG, 59), (CRACK_TAG, 59), (CURRENT_TAG, 59)]


async def test_state_is_restored_on_start(run_service, plc, qass, workdir):
    """A restart continues the stroke counter of the same serial."""
    import state_store

    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), qass_type="manual")
    async with run_service(cfg) as svc:
        await ready(svc, qass)
        for _ in range(2):
            await qass.send_records(rec("true", 10), rec("false"))
        await wait_until(lambda: svc.worker.stroke == 3)
        path = svc.worker.state.path

    reopened = state_store.StrokeStateStore(path)
    assert reopened.load() == ("SN1", 3, [])

    cfg["health"]["port"] = free_port()
    async with run_service(cfg) as svc:
        assert svc.worker.stroke == 3 and svc.worker.ident == "SN1"
        await qass.wait_connections(2)
        await qass.send_records(rec("true", 44), rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 9)
    assert measurement_writes(plc)[-1] == ("QASS_Measurement_3", 44)


# ---------------------------------------------------------------------------
# Known bugs (strict xfail: these flip to "XPASS -> failure" once fixed)
# ---------------------------------------------------------------------------


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="BUG: non-numeric e_current kills the consumer task")
async def test_bad_numeric_record_is_skipped(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(
            rec("true", "abc", 0, 1, 1), rec("true", 30, 0, 2, 2), rec("false"))
        await qass.send_records(rec("true", 40, 0, 3, 3), rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 6, timeout=1.5)
    assert measurement_writes(plc) == [
        (BREAK_TAG, 2), (CRACK_TAG, 2), (CURRENT_TAG, 30),
        (BREAK_TAG, 3), (CRACK_TAG, 3), (CURRENT_TAG, 40),
    ]


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="BUG: a record split across two TCP reads is lost")
async def test_record_split_across_reads(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        await qass.send_records(rec("true", 10, 0, 1, 1))
        peak = json.dumps(rec("true", 99, 0, 9, 9))
        await qass.send(peak[:20], gap=0.1)   # arrives as its own read
        await qass.send(peak[20:], gap=0.1)
        await qass.send_records(rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 3, timeout=1.5)
    assert measurement_writes(plc) == [(BREAK_TAG, 9), (CRACK_TAG, 9), (CURRENT_TAG, 99)]


@pytest.mark.known_bug
@pytest.mark.xfail(strict=True, reason="BUG: a read ending mid-object drops the whole read")
async def test_complete_records_before_a_split_are_lost(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await ready(svc, qass)
        whole = json.dumps(rec("true", 99, 0, 9, 9)) + json.dumps(rec("true", 50, 0, 5, 5))
        tail = json.dumps(rec("false"))
        # read 1: two complete measuring samples + first half of the end marker
        await qass.send(whole + tail[:10], gap=0.1)
        await qass.send(tail[10:], gap=0.1)
        await wait_until(lambda: len(measurement_writes(plc)) == 3, timeout=1.5)
    assert measurement_writes(plc) == [(BREAK_TAG, 9), (CRACK_TAG, 9), (CURRENT_TAG, 99)]
