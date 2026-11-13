# QASS service

Reads crack/break measurements from QASS units over TCP and writes the result
of every press stroke to an Allen-Bradley PLC (via `pylogix`). One container
runs per machine (`qass-machine1` … `qass-machine4`).

> **The PLC program depends on every tag write, value, order and timing below.**
> Treat them as a contract: change them only together with the PLC program.

## What it does

1. Connects to `qass.ip:qass.port` and sends exactly
   `{"cmd":"sendreadings","p1":"true"}`.
2. Queues only socket reads that contain `"measuring"`. Each read is parsed as
   JSON; concatenated objects (`{..}{..}`) are split; unparseable reads are dropped.
3. While `measuring` is `"true"` (case-insensitive string compare) records are
   buffered. On the first non-measuring record with a non-empty buffer it takes
   the sample with the highest `e_current` (first one wins on ties) and uses its
   `e_current`, `limitbreak`, `limitcrack` (ints, default 0). It writes them to the
   PLC, then increments the stroke counter. Records whose numeric fields are not
   convertible with `int()` are logged and skipped.
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

### Error codes (`error_code_tag`, `/metrics`)

| Code | Name | Meaning |
|---|---|---|
| 0 | `NONE` | OK |
| 10 | `QASS_DISCONNECTED` | no TCP connection to QASS |
| 11 | `QASS_NO_DATA` | connected, but no `measuring` data for 10 s |
| 20 | `PLC_COMMS` | a PLC call raised (re-evaluated by the QASS watchdog every 2 s) |

## Configuration

`python3 main.py <config>`. A relative path always resolves to
`configs/<basename>`; with no argument `configs/default.yaml` is used. An absolute
path is used as-is. The config is validated at startup: a missing key gives one
`CRITICAL Invalid config: …` line listing every missing key, then exit code 1.

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

`configs/default.yaml` is `type: auto` without `plc.current_tag`, so it does
not start as-is. Production uses `config_machine1..4.yaml`.

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
| `GET /logs` | 200 | `{"logs": [last 100 lines]}` |

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

Tests marked `known_bug` are strict `xfail`. They document confirmed bugs that
are not fixed yet. When one is fixed, the test starts passing, which fails the
run: remove the marker then.

The tests also snapshot `configs/*.yaml`, the effective `docker-compose.yml`
services and the Dockerfile essentials (`tests/golden/`). An accidental change
to tags, IPs or ports fails the run.

## Known issues (not fixed yet)

Each item below has a strict-`xfail` test unless noted otherwise.

- **A record split across two TCP reads is lost.** Every 12000-byte read is
  parsed on its own. If a JSON object straddles two reads, both halves fail to
  parse and are dropped, together with any complete records in the same read.
  A lost end-of-stroke record merges two strokes into one.
- **pylogix returns an error `Status` instead of raising when the PLC is
  unreachable.** The service counts that as a successful call, so `plc_ok`,
  `/metrics plc_connected` and the PLC watchdog do not notice a dead PLC.
- **One raising PLC call stops the heartbeat task**, and one raising
  measurement write stops stroke processing. `/health` keeps returning 200.
- Error code 20 (`PLC_COMMS`) is overwritten by the QASS watchdog within 2 s,
  so the PLC rarely sees it. (no test)
- `/metrics` → `metrics.qass_connected` never goes back to `false`, and
  `metrics.qass_generation` is always 0. The top-level `healthy` and `reason`
  are correct. (no test)
- After a receive error the old QASS socket is dropped without being closed.
  (no test)
- `/logs` returns HTTP 500 if the log file does not exist yet. (no test)
