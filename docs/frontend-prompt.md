Build a read-only monitoring dashboard for the QASS service, inside the RyanWRLD repo, with the same stack and conventions RyanWRLD already uses: an Angular frontend, a NestJS backend, and dana-ui components.

## Start here
1. Read RyanWRLD first. Find how it lays out apps (monorepo/Nx or separate folders), how Angular and NestJS apps are structured, how dana-ui is imported and themed, how config and env vars are handled, and how apps are built and deployed (Dockerfiles, compose). Copy those patterns. Don't introduce new frameworks, state libraries or UI kits; if something is missing from dana-ui, build it from dana-ui primitives.
2. Do not touch the QASS service (repo ryan1393/Qass). It runs in production and a PLC program depends on it. The dashboard only reads its HTTP endpoints. Never add anything that writes to a PLC or to QASS.
3. Before writing code, give me a short plan: where the files go, the module and component list, and anything you had to assume.

## What exists (the data source)
One Docker host runs 4 containers with `network_mode: host`. Each serves plain JSON over HTTP, with no auth and no CORS headers:

| Machine | URL |
|---|---|
| machine1 | http://<qass-host>:8081 |
| machine2 | http://<qass-host>:8082 |
| machine3 | http://<qass-host>:8083 |
| machine4 | http://<qass-host>:8084 |

Machines 1 and 2 are `manual` type; machines 3 and 4 are `auto`.

**GET /health**: always HTTP 200.
```json
{"status": "ok", "qass_connected": true, "last_qass_data_s": 3, "serial": "SN123"}
```
- `last_qass_data_s` is seconds since the last measuring payload. It is huge before the first connect.
- `serial` can be `null`.

**GET /metrics**: HTTP 200 when healthy, **503 when QASS is disconnected**. The 503 still carries the full JSON body; treat it as data, not as a failure.
```json
{
  "healthy": true, "reason": "OK",
  "metrics": {
    "uptime_seconds": 1234, "qass_connected": true, "qass_generation": 0,
    "plc_connected": true, "strokes_processed": 57, "error_count": 3,
    "last_error_code": 11, "last_error_name": "QASS_NO_DATA",
    "last_error_ts": 1731470000.12, "error_counts": {"QASS_NO_DATA": 3}
  },
  "qass_queue_depth": 0, "plc_queue_depth": 0, "last_plc_write_ms": 12.4
}
```
- Error codes: 0 NONE, 10 QASS_DISCONNECTED, 11 QASS_NO_DATA, 20 PLC_COMMS.
- `last_error_ts` is Unix seconds (float) or null.

**GET /logs**: `{"logs": ["<line>\n", ...]}`, the last 100 lines. An empty list means no logs yet; older builds return HTTP 500 instead, so handle both.
- Line format: `2026-11-13 08:15:02,123 INFO root <message>`.
- Stroke lines look exactly like `Serial=SN1 Stroke=4 ECurrent=30 EMax=120 Break=510 Crack=410`.

**Notes:**
- `plc_queue_depth` is always 0; ignore it.
- `metrics.plc_connected` becomes false after 10 s without a reachable PLC.
- Error code 20 means a PLC call failed in the last 10 s.

## Backend (NestJS)
- A `qass` module. Machine list from config/env (`QASS_HOST` plus a name→port map, defaulting to the 4 machines above), following RyanWRLD's config pattern.
- Poll each machine's /health and /metrics every 5 s on the server, with a 2 s timeout, keeping the last result in memory. One unreachable machine must not block the others. Record per machine: last successful poll time, last error, and an "unreachable" state.
- Fetch /logs only on request; don't poll it.
- Parse stroke lines from /logs into `{time, serial, stroke, eCurrent, eMax, break, crack}`.
- REST:
  - `GET /api/qass/machines`: a summary for all machines.
  - `GET /api/qass/machines/:name`: health + metrics + poll status for one machine.
  - `GET /api/qass/machines/:name/logs`: raw lines and parsed strokes.
- Live updates over SSE or WebSocket, whichever RyanWRLD already uses; otherwise the frontend polls every 5 s.
- The browser never calls the QASS ports directly (no CORS, internal IPs).
- Unit tests: mock the QASS HTTP responses, including 503 with a body, timeouts, an empty or 500 /logs, and a `null` serial.

## Frontend (Angular + dana-ui)
- **Overview:** one card per machine, showing:
  - Status badge: green when healthy; amber when `last_error_code` is 11; red when 10 or the machine is unreachable.
  - Serial, strokes processed, uptime, last data age, last error name and time, last PLC write ms.
- **Machine detail:** the same fields, `error_counts` as a small table, a recent-strokes table parsed from logs (newest first), and a log viewer with a refresh button and a text filter.
- Show "stale" when the backend's last successful poll is older than 15 s.
- Read-only: no buttons that restart, write or configure anything.
- Follow RyanWRLD's routing, theming, dark mode and responsive conventions. Component tests in the style RyanWRLD uses.

## Done when
- Backend and frontend build and their tests pass with RyanWRLD's own commands.
- It runs locally against a mock: add a tiny mock QASS server (4 ports, canned JSON including the 503 and 500 cases) as a dev script.
- There is a Dockerfile/compose entry matching RyanWRLD's deploy pattern. The backend needs network access to the QASS host's ports 8081–8084.
- A short README section covers config, running locally against the mock, and deployment.
- Work on a new branch, and don't open a pull request until I ask.
