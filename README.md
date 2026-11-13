# QASS service

Reads crack/break measurements from QASS units over TCP and writes the result
of every press stroke to an Allen-Bradley PLC (via `pylogix`). One container
runs per machine (`qass-machine1` … `qass-machine4`).

> **The PLC program depends on every tag write, value, order and timing below.**
> Treat them as a contract: change them only together with the PLC program.

## What it does

1. Connects to `qass.ip:qass.port` and sends exactly
   `{"cmd":"sendreadings","p1":"true"}`.
2. Reassembles complete JSON records from the TCP stream: a record may be split
   across socket reads, or several may arrive in one read. Only records that
   contain `"measuring"` are queued; unparseable data is dropped.
3. While `measuring` is `"true"` (case-insensitive string compare) records are
   buffered. On the first non-measuring record with a non-empty buffer it takes
   the sample with the highest `e_current` (first one wins on ties) and uses its
   `e_current`, `limitbreak`, `limitcrack` (ints, default 0). It writes them to the
   PLC, then increments the stroke counter, even if the PLC write fails (that is
   logged). A stroke still measuring after 60 s is finished the same way, with a
   warning, and later samples start a new stroke. Records whose numeric fields are
   not convertible with `int()` are logged and skipped.
4. Reads the part serial (`plc.serial_tag`) every second. A new non-empty serial
   resets the stroke counter to 1 and drops any half-finished stroke.
5. Writes a heartbeat and status tags every 5 s.
6. Exits with code 1 if no QASS message arrives for `qass.disconnect_timeout_sec`
   seconds, so Docker (`restart: always`) restarts it.

Each log line for a stroke looks like this, and downstream tools grep for it:

```
Serial=<serial> Stroke=<n> ECurrent=<e> EMax=<e_max> Break=<limitbreak> Crack=<limitcrack>
```

## PLC tag contract

| When | Tag | Value |
|---|---|---|
| startup, then every 1 s | read `plc.serial_tag` | part serial |
| end of stroke, `qass.type: auto` | `break_tag`, `crack_tag`, `current_tag` (in that order) | limitbreak, limitcrack, e_current |
| end of stroke, `qass.type: manual` | `break_tag`, `crack_tag`, `QASS_Measurement_<stroke>` | limitbreak, limitcrack, e_current |
| every 5 s, in this order | `heartbeat_tag` | toggles 1, 0, 1, … |
| | `service_ok_tag` | `True` |
| | `qass_ok_tag` | QASS TCP connected (bool) |
| | `plc_ok_tag` | last successful PLC call < 10 s ago (bool) |
| | `last_stroke_tag` | current stroke counter |
| | `error_code_tag` | error code (below) |

Any other `qass.type` makes the measurement write raise. With
`plc.enable_writes: false` **no** tag is written (neither measurements nor
status), but the serial is still read.

All PLC calls are serialized by one lock and run in a thread-pool executor. If a
call raises, the PLC object is closed and recreated and the error re-raised.
pylogix reports an unreachable PLC as a `Status` string (for example
`Connection failure` or `Unknown error [Errno 111] …`) rather than an exception.
Such calls are treated as failed: they don't refresh `plc_ok`, and they set error
20. Tag errors such as `Path destination unknown` still count as "PLC reachable".
A failed heartbeat or measurement write is logged and the service carries on.

### Error codes (`error_code_tag`, `/metrics`)

| Code | Name | Meaning |
|---|---|---|
| 0 | `NONE` | OK |
| 10 | `QASS_DISCONNECTED` | no TCP connection to QASS |
| 11 | `QASS_NO_DATA` | connected, but no `measuring` data for 10 s |
| 20 | `PLC_COMMS` | a PLC call failed (raised or unreachable) in the last 10 s |

The code is re-evaluated every 2 s. Priority: 10 > 11 > 20 > 0.

## Configuration

`python3 main.py <config>`. A relative path always resolves to
`configs/<basename>`, and an absolute path is used as-is. With no argument the
service looks for `configs/default.yaml`. That file is not shipped, so always pass
a machine config (compose does). A missing file or key gives one
`CRITICAL Invalid config: …` line and exit code 1. The config is validated at startup, and the error lists every missing key.

| Key | Required | Notes |
|---|---|---|
| `qass.ip`, `qass.port` | yes | QASS TCP endpoint |
| `qass.type` | yes | `auto` or `manual` |
| `qass.disconnect_timeout_sec` | no (60) | exit after this long without any QASS message |
| `plc.ip` | yes | |
| `plc.serial_tag` | yes | read every 1 s |
| `plc.break_tag`, `plc.crack_tag` | yes | |
| `plc.current_tag` | if `auto` | |
| `plc.heartbeat_tag`, `service_ok_tag`, `qass_ok_tag`, `plc_ok_tag`, `last_stroke_tag`, `error_code_tag` | yes | status tags |
| `plc.enable_writes` | no (true) | master switch for all PLC writes |
| `health.port` | yes | HTTP port |
| `reconnect.initial_delay`, `reconnect.max_delay` | yes | QASS reconnect backoff: starts at `initial_delay`, doubles up to `max_delay`, restarts at 1 after a successful connect |

Production uses `config_machine1..4.yaml`.

### Environment variables

| Variable | Default | Used for |
|---|---|---|
| `HEALTH_PORT` | 8081 | Docker `HEALTHCHECK` URL only; the service listens on `health.port` |
| `QASS_LOG_FILE` | `qass.log` | log file name (also served by `/logs`) |
| `QASS_LOG_DIR` | `<app>/logs` | log directory |
| `QASS_STATE_DIR` | `<app>/state` | stroke-state directory |

Logs rotate at 10 MB and keep 5 backups. The format is
`%(asctime)s %(levelname)s %(name)s %(message)s`, written to the file and to stdout.

Stroke state (serial, stroke counter, partial buffer) is saved to
`$QASS_STATE_DIR/state_<config name>.db`, e.g. `/app/state/state_config_machine1.db`
on the shared `qass_state` volume. It is saved every 25 buffered samples and
after each stroke, so the counter survives restarts and rebuilds.

## HTTP endpoints (bound to 0.0.0.0:`health.port`)

| Endpoint | Status | Body |
|---|---|---|
| `GET /health` | always 200 | `status`, `qass_connected`, `last_qass_data_s`, `serial` |
| `GET /metrics` | 200, or 503 when QASS is disconnected | `healthy`, `reason`, `metrics` (`uptime_seconds`, `qass_connected`, `qass_generation`, `plc_connected`, `strokes_processed`, `error_count`, `last_error_code`, `last_error_name`, `last_error_ts`, `error_counts`), `qass_queue_depth`, `plc_queue_depth` (always 0), `last_plc_write_ms` |
| `GET /logs` | 200 | `{"logs": [last 100 lines]}` (`[]` if the file doesn't exist yet) |

Ports: machine1 8081, machine2 8082, machine3 8083, machine4 8084.

## Build, deploy, roll back

On the Docker host, in this directory:

```bash
docker compose up -d --build                    # all four machines
docker compose up -d --build qass-machine4      # one machine
docker compose logs -f --tail 100 qass-machine4
curl -s localhost:8084/health
```

The image is tagged `qass-service:13.0`, and `--build` overwrites that tag.
Before deploying, keep the running image so you can roll back:

```bash
docker tag qass-service:13.0 qass-service:13.0-prev
```

To roll back:

```bash
docker tag qass-service:13.0-prev qass-service:13.0
docker compose up -d --no-build --force-recreate
```

You can also check out the previous commit and run `docker compose up -d --build` again.

Safe first rollout: set `plc.enable_writes: false` in one machine's config,
deploy only that service, and compare its stroke log lines with the old
container's.

## Tests

The tests need no hardware. They use a fake QASS TCP server and a fake `pylogix`
PLC that records every Read/Write (tag, value, order, thread). A warped clock
runs the 1/2/5/10 s timers in milliseconds. One test runs `main.py` as a real
subprocess.

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

GitHub Actions (`.github/workflows/tests.yml`) runs the suite on Python 3.11 for
every push and pull request. Simulated time is slowed with
`QASS_TEST_TIME_SCALE=0.1` there; set the same locally if timing tests flake on a
slow machine.

Tests marked `known_bug` are strict `xfail`. They document confirmed bugs that
are not fixed yet. When one is fixed, the test starts passing, which fails the
run: remove the marker then.

The tests also snapshot `configs/*.yaml`, the effective `docker-compose.yml`
services and the Dockerfile essentials (`tests/golden/`). An accidental change
to tags, IPs or ports fails the run.

## Known issues (not fixed yet)

- When the PLC host is down, each pylogix call still waits for its 5 s socket
  timeout. A transaction stops at its first comms failure, so the PLC lock is
  held for about 5 s per heartbeat or stroke, not 30 s.
- A measurement write that fails is not retried, so that stroke's values never
  reach the PLC (it is logged). This is deliberate: a late write could be applied
  to the wrong part. Retries would need a handshake defined on the PLC side.
