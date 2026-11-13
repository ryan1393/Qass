# QASS service

Reads crack/break measurements from QASS units over TCP and writes the result
of every press stroke to an Allen-Bradley PLC (via `pylogix`). One container
runs per machine (`qass-machine1` … `qass-machine4`).

> **The PLC program depends on every tag write, value, order and timing below.**
> Treat them as a contract: change them only together with the PLC program.

| Machine | Container | `qass.type` | QASS unit | PLC | HTTP port | Config |
|---|---|---|---|---|---|---|
| 1 | `qass-machine1` | manual | 10.16.81.163 | 10.16.81.164 | 8081 | `configs/config_machine1.yaml` |
| 2 | `qass-machine2` | manual | 10.16.81.150 | 10.16.81.236 | 8082 | `configs/config_machine2.yaml` |
| 3 | `qass-machine3` | auto | 10.16.82.99 | 10.16.82.70 | 8083 | `configs/config_machine3.yaml` |
| 4 | `qass-machine4` | auto | 10.16.82.98 | 10.16.82.64 | 8084 | `configs/config_machine4.yaml` |

All four run the same image (`qass-service:13.0`) on one Docker host with `network_mode: host`.
The QASS units listen on TCP port 17000.

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
6. Exits with code 1 if no QASS message of any kind arrives for
   `qass.disconnect_timeout_sec` seconds (30 in production), so Docker
   (`restart: always`) restarts it. The timer starts with the first message: a
   service that has never reached its QASS unit keeps retrying instead.

Every timeout and interval is measured on a monotonic clock, so a change of the
host's clock (an NTP correction, for example) never trips a watchdog or restarts
the service.

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

When a write in a transaction (the 3 measurement writes, or the 6 status writes)
returns such a comms failure, the rest of that transaction is skipped: those
writes would each wait 5 s and fail too. While the PLC is reachable, every write
is attempted in the order above. A failed heartbeat or measurement write is
logged and the service carries on. A stroke counts even if its write failed, so
later strokes keep their own numbers.

### Error codes (`error_code_tag`)

| Code | Name | Meaning |
|---|---|---|
| 0 | `NONE` | OK |
| 10 | `QASS_DISCONNECTED` | no TCP connection to QASS |
| 11 | `QASS_NO_DATA` | connected, but no `measuring` data for 10 s |
| 20 | `PLC_COMMS` | a PLC call failed (raised or unreachable) in the last 10 s |

The code is re-evaluated every 2 s. Priority: 10 > 11 > 20 > 0. This is the
current state the PLC sees. `/metrics` `last_error_code` is something else: the
last error *recorded* since the service started (see HTTP endpoints).

## Configuration

`python3 main.py <config>`. A relative path always resolves to
`configs/<basename>`, and an absolute path is used as-is. With no argument the
service looks for `configs/default.yaml`. That file is not shipped, so always pass
a machine config (Compose does).

The config is validated at startup. A missing file or key gives one
`CRITICAL Invalid config: …` line listing every missing key, then exit code 1.
An unsupported `qass.type` is only warned about at startup and fails at the
first measurement write.

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
| `QASS_ENABLE_WRITES` | unset | `false` turns all PLC writes off (overrides `plc.enable_writes`), `true` forces them on; unset = use the config |

Logs rotate at 10 MB and keep 5 backups. The format is
`%(asctime)s %(levelname)s %(name)s %(message)s`, written to the file and to stdout.

Stroke state (serial, stroke counter, partial buffer) is saved to
`$QASS_STATE_DIR/state_<config name>.db`, e.g. `/app/state/state_config_machine1.db`
on the shared `qass_state` volume. It is saved every 25 buffered samples and
after each stroke, so the serial and the stroke counter survive restarts and
rebuilds. A half-finished stroke does not: the buffer is cleared on every QASS
(re)connect.

## HTTP endpoints (bound to 0.0.0.0:`health.port`)

| Endpoint | Status | Body |
|---|---|---|
| `GET /health` | always 200 | `status`, `qass_connected`, `last_qass_data_s` (seconds since 1970 before the first connect), `serial` |
| `GET /metrics` | 200, or 503 when QASS is disconnected | `healthy`, `reason`, `metrics` (`uptime_seconds`, `qass_connected`, `qass_generation`, `plc_connected`, `strokes_processed`, `error_count`, `last_error_code`, `last_error_name`, `last_error_ts`, `error_counts`), `qass_queue_depth`, `plc_queue_depth` (always 0), `last_plc_write_ms` |
| `GET /logs` | 200 | `{"logs": [last 100 lines]}` (`[]` if the file doesn't exist yet) |

Ports: machine1 8081, machine2 8082, machine3 8083, machine4 8084. There is no
authentication. The endpoints are read-only.

The Docker `HEALTHCHECK` calls `/health`, which answers 200 whenever the process
runs. "Healthy" in `docker ps` therefore means "alive", not "QASS and PLC OK". Use
`/metrics` or the dashboard for that.

If `health.port` cannot be bound (port already in use), the service logs one
`CRITICAL Cannot serve HTTP on 0.0.0.0:<port>` line and retries every 10 s. QASS
and PLC work carry on in the meantime.

`metrics.last_error_code` / `last_error_name` / `last_error_ts` describe the last error *recorded*
since the service started; they are never reset to 0. For the current state use `/health`
(`qass_connected`, `last_qass_data_s`) and `metrics.plc_connected`.

## Dashboard (RyanWRLD)

These endpoints feed the **QASS dashboard** in [RyanWRLD](https://github.com/ryan1393/RyanWRLD)
(MeshForge, `apps/qass-dashboard`, port 4205). The MeshForge API polls `/health` and `/metrics`
every 5 s and reads `/logs` on request. It only sends GET requests, so it never changes anything
here or on a PLC. The dashboard requires a MeshForge sign-in.

To connect it, set these on the MeshForge server (its `.env` or Admin → Server Settings → QASS
dashboard), with `<qass-host>` being this Docker host:

```sh
QASS_MONITOR_ENABLED=true
QASS_MACHINES=machine1=http://<qass-host>:8081,machine2=http://<qass-host>:8082,machine3=http://<qass-host>:8083,machine4=http://<qass-host>:8084
```

The MeshForge API must be able to reach ports 8081–8084 on this host. Nothing changes on the QASS
side: the dashboard works with any image that serves these endpoints, including `13.0`. If you
rename a machine, add one or change a port, update `QASS_MACHINES` to match.

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

Safe first rollout: turn PLC writes off for one machine without touching its
config or rebuilding twice. Create `docker-compose.override.yml` next to
`docker-compose.yml`; Compose merges it automatically, and it is git-ignored:

```yaml
services:
  qass-machine4:
    environment:
      QASS_ENABLE_WRITES: "false"
```

Then `docker compose up -d --build qass-machine4`. The log shows
`QASS_ENABLE_WRITES=false overrides plc.enable_writes=True` and
`PLC writes DISABLED`. The service still reads the serial and logs every
`Serial=… Stroke=…` line, but writes nothing to the PLC: no measurements and
no heartbeat or status tags.

> **Heads-up:** the heartbeat is a PLC write too, so it stops while writes are
> off. If the PLC program alarms or stops the machine on a lost heartbeat, warn
> the people running that machine first. While the override file exists the
> machine stays muted, including on later full deploys. `PLC writes DISABLED` at
> startup tells you it is still there.

To send to the PLC again, delete the override file (or set it to `"true"`) and
run `docker compose up -d qass-machine4`. No rebuild is needed; Compose recreates
the container with the new environment.

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
are not fixed yet; there are none at the moment. When one is fixed, the test
starts passing, which fails the run: remove the marker then.

The tests also snapshot `configs/*.yaml`, the effective `docker-compose.yml`
services and the Dockerfile essentials (`tests/golden/`). An accidental change
to tags, IPs or ports fails the run.

## Files

| Path | What |
|---|---|
| `main.py` | Entry point: config path, logging, runs the worker |
| `worker_async.py` | QASS client, stroke logic, PLC I/O, watchdogs, heartbeat, HTTP |
| `qass_normalizer.py` | Splits the TCP stream into JSON records |
| `config.py` | Config loading/validation, `QASS_ENABLE_WRITES` |
| `metrics.py` | Error codes and the `/metrics` counters |
| `state_store.py` | SQLite stroke state |
| `paths.py` | Log/state/config directories |
| `configs/` | One YAML per machine |
| `tests/` | The test suite, fakes and golden snapshots |

## Known issues (not fixed yet)

- When the PLC host is down, each pylogix call still waits for its 5 s socket
  timeout. A transaction stops at its first comms failure, so the PLC lock is
  held for about 5 s per heartbeat or stroke, not 30 s.
- A measurement write that fails is not retried, so that stroke's values never
  reach the PLC (it is logged). This is deliberate: a late write could be applied
  to the wrong part. Retries would need a handshake defined on the PLC side.
- `requirements.txt` is not pinned, so each build installs the newest pylogix,
  aiohttp and PyYAML. The suite passes with pylogix 1.1.7, aiohttp 3.14.5 and
  PyYAML 6.0.3. Pin them (`pip freeze` from a known-good image) to make builds
  reproducible.
