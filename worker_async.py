# Async worker that handles:
# - QASS TCP communication
# - Stroke processing
# - PLC reads/writes
# - Watchdogs
# - Heartbeat
# - HTTP health endpoint


import asyncio
import logging
import os
import time
import sys
from aiohttp import web
from pylogix import PLC

from paths import LOG_DIR
from state_store import StrokeStateStore
from qass_normalizer import split_and_parse_qass_json
from metrics import Metrics


# Numeric error codes for PLC / monitoring
ERROR_NONE = 0
ERROR_QASS_DISCONNECTED = 10
ERROR_QASS_NO_DATA = 11
ERROR_PLC_COMMS = 20


class AsyncWorker:
    def __init__(self, cfg):
        # ---------- Configuration ----------
        self.plc_lock = asyncio.Lock()
        
        # QASS TCP endpoint
        self.qass_ip = cfg["qass"]["ip"]
        self.qass_port = cfg["qass"]["port"]

        # PLC configuration
        self.plc_conn = None
        plc = cfg["plc"]
        self.plc_ip = plc["ip"]
        self.sn_tag = plc["serial_tag"]

        # PLC measurement tags
        self.type_tag = cfg["qass"]["type"] 
        self.break_tag = plc["break_tag"]
        self.crack_tag = plc["crack_tag"]   
        if self.type_tag == "auto":                
            self.current_tag = plc["current_tag"] 
        # PLC heartbeat and status tags
        self.heartbeat_tag = plc["heartbeat_tag"]
        self.service_ok_tag = plc["service_ok_tag"]
        self.qass_ok_tag = plc["qass_ok_tag"]
        self.plc_ok_tag = plc["plc_ok_tag"]
        self.last_stroke_tag = plc["last_stroke_tag"]
        self.error_code_tag = plc["error_code_tag"]

        # Master PLC write enable switch
        self.plc_writes_enabled = plc.get("enable_writes", True)

        # HTTP health server port
        self.health_port = cfg["health"]["port"]        
        self.cfg = cfg
        self.metrics = Metrics()


        # QASS reconnect backoff parameters
        self.reconnect_delay = cfg["reconnect"]["initial_delay"]
        self.reconnect_max = cfg["reconnect"]["max_delay"]

        # ---------- Runtime State ----------

        # Async TCP reader / writer for QASS
        self.reader = None
        self.writer = None

        # Queue separating socket reading from processing
        self.queue = asyncio.Queue()

        # Reference to the active event loop
        self.loop = asyncio.get_running_loop()

        # Shutdown coordination flag
        self.shutdown_flag = False

        # Persistent stroke state
        self.state = StrokeStateStore()
        self.ident, self.stroke, self.stroke_buffer = self.state.load()

        # PLC watchdog tracking
        self.last_plc_ok = time.time()

        # QASS tracking
        self.last_qass_data = 0.0
        self.qass_connected = False
        self.qass_generation = 0

        # Heartbeat and error tracking
        self.hb_value = 0
        self.last_error_code = ERROR_NONE
        
        self.tasks = []

        logging.info(
            "PLC writes %s",
            "ENABLED" if self.plc_writes_enabled else "DISABLED",
        )

    # ---------- Lifecycle ----------

    async def start(self, shutdown_event):
        # Start all background tasks
        logging.info("Service started")
        await self._connect_plc()

        self.tasks = [
            asyncio.create_task(self._receiver()),
            asyncio.create_task(self._consumer()),
            asyncio.create_task(self._ident_loop()),
            asyncio.create_task(self._plc_watchdog()),
            asyncio.create_task(self._heartbeat()),
            asyncio.create_task(self._health_server()),
            asyncio.create_task(self._qass_watchdog()),
        ]

        # Block until shutdown is requested
        await shutdown_event.wait()
        await self.shutdown()

    async def shutdown(self):
        # Clean shutdown of all tasks and sockets
        logging.info("Shutting down service")
        self.shutdown_flag = True

        for t in self.tasks:
            t.cancel()

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            
        if self.plc_conn:    
            self.plc_conn.Close()

    # ---------- QASS Communication ----------

    async def _connect(self):
        # Attempt TCP connection to QASS
        try:
            logging.info("Connecting to QASS %s:%s", self.qass_ip, self.qass_port)

            self.reader, self.writer = await asyncio.open_connection(
                self.qass_ip, self.qass_port
            )

            # Mark new connection generation
            self.qass_generation += 1
            self.qass_connected = True
            self.last_qass_data = time.time()

            # Clear any partial stroke data
            self.stroke_buffer.clear()

            # Subscribe to measurements
            self.writer.write(b'{"cmd":"sendreadings","p1":"true"}')
            await self.writer.drain()

            self.reconnect_delay = 1
            logging.info("QASS connected")

        except Exception as e:
            self.qass_connected = False
            self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
            logging.error("QASS connect failed: %s", e)
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.reconnect_max)

    async def _receiver(self):
        # Receives raw payloads from QASS and queues them
        local_gen = 0

        while not self.shutdown_flag:
            if not self.reader:
                await self._connect()
                local_gen = self.qass_generation
                continue

            try:
                data = await self.reader.read(12000)
                if not data:
                    raise ConnectionError("QASS disconnected")

                # Ignore stale reads after reconnect
                if local_gen != self.qass_generation:
                    continue

                payload = data.decode(errors="ignore")
                self.metrics.mark_qass_message()
                
                # Only enqueue measurement payloads
                if '"measuring"' in payload:
                    self.last_qass_data = time.time()
                    await self.queue.put(payload)

            except Exception as e:
                logging.error("QASS receive error: %s", e)
                self.qass_connected = False
                self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
                self.reader = self.writer = None

    # ---------- Processing ----------

    async def _consumer(self):
        # Process queued QASS payloads
        while not self.shutdown_flag:
            payload = await self.queue.get()
            for r in split_and_parse_qass_json(payload):
                await self._process_record(r)

    async def _process_record(self, r):
        # Determine measuring state
        measuring = str(r.get("measuring")).lower() == "true"

        # Buffer samples during active stroke
        if measuring:
            self.stroke_buffer.append(r)
            if len(self.stroke_buffer) % 25 == 0:
                self.state.save(
                    self.ident,
                    self.stroke,
                    self.stroke_buffer
                )
            return
        # Ignore end-of-stroke without data
        if not self.stroke_buffer:
            return

        # Determine maximum energy sample
        max_pt = max(
            self.stroke_buffer,
            key=lambda x: int(x.get("e_current", 0))
        )

        e_max = int(max_pt.get("e_max", 0))
        e_current = int(max_pt.get("e_current", 0))
        break_l = int(max_pt.get("limitbreak", 0))
        crack_l = int(max_pt.get("limitcrack", 0))

        logging.info(
            "Serial=%s Stroke=%s ECurrent=%s EMax=%s Break=%s Crack=%s",
            self.ident, self.stroke,
            e_current, e_max, break_l, crack_l
        )

        # Write results to PLC (if enabled)
        await self._write_plc(e_current, break_l, crack_l)
        
        # Prepare for next stroke
        self.stroke_buffer.clear()
        self.stroke += 1
        self.metrics.mark_stroke_processed()
        self.state.save(self.ident, self.stroke, [])

    # ---------- PLC Interaction ----------
    
    async def _connect_plc(self):
        try:
            self.plc_conn = PLC(self.plc_ip)

            # Test connection
            result = await self._plc_call(
                lambda: self.plc_conn.Read(self.sn_tag)
            )

            if result.Status == "Success":
                logging.info("PLC connected")

        except Exception:
            logging.exception("PLC connection failed")
            
    async def _plc_call(self, fn):
        # Run blocking PLC calls in thread executor
        async with self.plc_lock:
            try:
                result = await self.loop.run_in_executor(None, fn)
                self.last_plc_ok = time.time()
                self.metrics.mark_plc_success()
                return result
            except Exception:
                self.last_error_code = ERROR_PLC_COMMS
                self.metrics.error_count += 1

                try:
                    self.plc_conn.Close()
                except Exception:
                    pass

                try:
                    self.plc_conn = PLC(self.plc_ip)
                except Exception:
                    pass

                raise

    async def _write_plc(self, e_current, break_l, crack_l):
        start = time.perf_counter()
        # Skip PLC writes if disabled
        if not self.plc_writes_enabled:
            logging.info("PLC writes disabled — skipping measurement write")
            return
        if self.type_tag == "auto":
            def write():
                self.plc_conn.Write(self.break_tag, break_l)
                self.plc_conn.Write(self.crack_tag, crack_l)
                self.plc_conn.Write(self.current_tag, e_current)
                logging.debug("Write to PLC Serial=%s Stroke=%s ECurrent=%s Break=%s Crack=%s",
                    self.ident, self.stroke,
                    e_current, break_l, crack_l
                )
        elif self.type_tag == "manual":
            def write():
                self.plc_conn.Write(self.break_tag, break_l)
                self.plc_conn.Write(self.crack_tag, crack_l)
                self.plc_conn.Write(f"QASS_Measurement_{self.stroke}", e_current)
                logging.debug("Write to PLC Serial=%s Stroke=%s ECurrent=%s Break=%s Crack=%s",
                    self.ident, self.stroke,
                    e_current, break_l, crack_l
                )
        else:
            raise ValueError(
                f"Unsupported qass type: {self.type_tag}"
            )
        
        await self._plc_call(write)
        
        elapsed_ms = (time.perf_counter() - start) * 1000
        self.metrics.last_plc_write_ms = elapsed_ms
        logging.debug("PLC write took %.1f ms",    elapsed_ms)
        self.metrics.mark_plc_success()

    async def _ident_loop(self):
        # Periodically read serial number from PLC
        def read_sn():
            return self.plc_conn.Read(self.sn_tag)

        while not self.shutdown_flag:
            try:
                r = await self._plc_call(read_sn)
                
                if r.Status == "Success":
                    sn = r.Value
                    self.metrics.mark_plc_success()
                    if sn and sn != self.ident:
                        self.ident = sn
                        self.stroke = 1
                        self.stroke_buffer.clear()
                        self.state.save(self.ident, self.stroke, [])
                        logging.info("SN Tag Read: Value=%s", sn)
            except Exception as e:
                logging.error("PLC ident error: %s", e)

            await asyncio.sleep(1)

    # ---------- Watchdogs ----------
    
    async def _plc_watchdog(self):
        # Detect PLC communication loss
        while not self.shutdown_flag:
            if time.time() - self.last_plc_ok > 10:
                logging.error("PLC WATCHDOG: no successful comms")
                self.metrics.plc_connected = False
            else:
                self.metrics.plc_connected = True
            await asyncio.sleep(2)

    async def _qass_watchdog(self):
        # Detect QASS communication or data loss
        while not self.shutdown_flag:
            now = time.time()
            
            if not self.qass_connected:
                self.last_error_code = ERROR_QASS_DISCONNECTED
                self.metrics.mark_error(ERROR_QASS_DISCONNECTED)
            elif now - self.last_qass_data > 10:
                self.last_error_code = ERROR_QASS_NO_DATA
                self.metrics.mark_error(ERROR_QASS_NO_DATA)
            else:
                self.last_error_code = ERROR_NONE
            self.check_qass_disconnect_and_exit()
            await asyncio.sleep(2)
         
    def check_qass_disconnect_and_exit(self):
        timeout = self.cfg["qass"].get("disconnect_timeout_sec", 60)
        last_msg = self.metrics.last_qass_message_ts

        if last_msg is None:
            return

        if time.time() - last_msg > timeout:
            logging.error(
                "QASS disconnected for %s seconds → exiting container",
                timeout
            )
            # Clean exit → Docker will restart
            sys.exit(1)

    # ---------- Heartbeat / Health ----------

    async def _heartbeat(self):
        # Periodic heartbeat and PLC status update
        while not self.shutdown_flag:
            self.hb_value ^= 1

            if self.plc_writes_enabled:
                await self._write_plc_status()

            # logging.info("Heartbeat OK")
            await asyncio.sleep(5)

    async def _write_plc_status(self):
        # Write heartbeat and status tags to PLC
        def write():
            self.plc_conn.Write(self.heartbeat_tag, self.hb_value)
            self.plc_conn.Write(self.service_ok_tag, True)
            self.plc_conn.Write(self.qass_ok_tag, self.qass_connected)
            self.plc_conn.Write(self.plc_ok_tag, time.time() - self.last_plc_ok < 10)
            self.plc_conn.Write(self.last_stroke_tag, self.stroke)
            self.plc_conn.Write(self.error_code_tag, self.last_error_code)

        await self._plc_call(write)

    async def _health_server(self):
        # HTTP health endpoint
        async def health(_):
            return web.json_response({
                "status": "ok",
                "qass_connected": self.qass_connected,
                "last_qass_data_s": int(time.time() - self.last_qass_data),
                "serial": self.ident,
            })    
        async def _metrics(request):
            
            healthy = True
            reason = "OK"

            if not self.qass_connected:
                healthy = False
                reason = "QASS disconnected"

            return web.json_response(
                {
                    "healthy": healthy,
                    "reason": reason,
                    "metrics": self.metrics.snapshot(),
                    "qass_queue_depth": self.queue.qsize(),
                    "plc_queue_depth": 0,  # kept for dashboards; no PLC queue exists
                    "last_plc_write_ms": self.metrics.last_plc_write_ms,
                },
                status=200 if healthy else 503,
            )
        async def _logs_handler(request):
            logfile = LOG_DIR / os.getenv(
                "QASS_LOG_FILE",
                "qass.log"
            )

            with open(logfile, "r") as f:
                lines = f.readlines()[-100:]

            return web.json_response({
                "logs": lines
            })
        app = web.Application()
        app.add_routes([web.get("/health", health)])   
        app.add_routes([web.get("/metrics", _metrics)])
        app.add_routes([web.get("/logs", _logs_handler)])
        runner = web.AppRunner(app)
        runner = web.AppRunner(
            app,
            access_log=None
        )
        await runner.setup()
        logging.info(self.cfg)
        await web.TCPSite(runner, "0.0.0.0", port=self.health_port).start()
    
    
