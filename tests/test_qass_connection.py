"""QASS TCP subscribe, reconnect backoff, generation counter and buffer reset."""

from __future__ import annotations

from conftest import BREAK_TAG, CRACK_TAG, CURRENT_TAG, SERIAL_TAG, STATUS_TAGS, free_port, make_cfg
from fakes import rec, wait_until

SUBSCRIBE = b'{"cmd":"sendreadings","p1":"true"}'


def measurement_writes(plc):
    return [(t, v) for t, v in plc.writes() if t not in STATUS_TAGS]


def receiver_sleeps(clock, conns):
    task = conns.attempts[0][0]
    return [d for t, d, _ in clock.sleeps if t is task and d != 0]


async def test_subscribe_command_is_exact(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())):
        await qass.wait_connections(1)
        await wait_until(lambda: qass.received[0] == SUBSCRIBE)
    assert qass.received == [SUBSCRIBE]


async def test_reconnect_resubscribes_bumps_generation_and_clears_buffer(run_service, plc, qass):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.ident == "SN1")
        assert svc.worker.qass_generation == 1

        # half a stroke, then QASS drops the connection
        await qass.send_records(rec("true", 99, 0, 9, 9), rec("true", 98, 0, 9, 9))
        await wait_until(lambda: len(svc.worker.stroke_buffer) == 2)
        await qass.drop_client()

        await qass.wait_connections(2)
        await wait_until(lambda: svc.worker.qass_generation == 2)
        assert svc.worker.stroke_buffer == []

        # end marker alone does nothing; a fresh stroke is processed normally
        await qass.send_records(rec("false"))
        await qass.send_records(rec("true", 5, 0, 1, 2), rec("false"))
        await wait_until(lambda: len(measurement_writes(plc)) == 3)
        assert svc.worker.stroke == 2
    assert qass.received == [SUBSCRIBE, SUBSCRIBE]
    assert measurement_writes(plc) == [(BREAK_TAG, 1), (CRACK_TAG, 2), (CURRENT_TAG, 5)]


async def test_backoff_doubles_to_max_and_resets_to_1(run_service, plc, qass, clock, conns):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), reconnect={"initial_delay": 3, "max_delay": 20})
    conns.refuse = True
    async with run_service(cfg) as svc:
        await wait_until(lambda: len(conns.attempts) >= 5)
        conns.refuse = False
        await qass.wait_connections(1)
        assert receiver_sleeps(clock, conns)[:4] == [3, 6, 12, 20]

        # successful connect resets the delay to 1 (not to initial_delay)
        n_before = len(receiver_sleeps(clock, conns))
        conns.refuse = True
        await qass.drop_client()
        await wait_until(lambda: len(receiver_sleeps(clock, conns)) >= n_before + 4)
        assert receiver_sleeps(clock, conns)[n_before:n_before + 4] == [1, 2, 4, 8]
        assert svc.worker.qass_generation == 1  # failed attempts do not bump it

    ok = [a for a in conns.attempts if a[2]]
    assert len(ok) == 1


async def test_backoff_timing_between_attempts(run_service, plc, qass, clock, conns):
    plc.tags[SERIAL_TAG] = "SN1"
    cfg = make_cfg(qass.port, free_port(), reconnect={"initial_delay": 1, "max_delay": 4})
    conns.refuse = True
    async with run_service(cfg):
        await wait_until(lambda: len(conns.attempts) >= 6)
    times = [t for _, t, _ in conns.attempts[:6]]
    gaps = [b - a for a, b in zip(times, times[1:])]
    for got, want in zip(gaps, [1, 2, 4, 4, 4]):
        assert abs(got - want) < 0.6, gaps


async def test_qass_disconnect_marks_not_connected(run_service, plc, qass, conns):
    plc.tags[SERIAL_TAG] = "SN1"
    async with run_service(make_cfg(qass.port, free_port())) as svc:
        await qass.wait_connections(1)
        await wait_until(lambda: svc.worker.qass_connected)
        conns.refuse = True
        await qass.drop_client()
        await wait_until(lambda: not svc.worker.qass_connected)
        assert svc.worker.metrics.error_counts[10] >= 1  # keyed by numeric code
