import sys
import os
import time
import threading
import yaml
import sqlite3
import zmq
import signal
import argparse
import logging
from logging.handlers import RotatingFileHandler

import onlutils
import onlconsts


def _setup_logging(log_file, debug=False, daemon=False):
    level = logging.DEBUG if debug else logging.INFO
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handlers = [RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)]
    if not daemon:
        handlers.append(logging.StreamHandler())

    root = logging.getLogger()
    root.setLevel(level)
    for h in handlers:
        h.setFormatter(formatter)
        root.addHandler(h)


log = logging.getLogger("DAQMON")


class DAQMonitorServer:
    def __init__(self, cmd_port=None, pub_port=None, pid_file=None):
        self.cmd_port = cmd_port or onlconsts.kDAQMON_CMD_PORT
        self.pub_port = pub_port or onlconsts.kDAQMON_PUB_PORT
        self.pid_file = pid_file

        self.context = zmq.Context()

        self.cmd_socket = self.context.socket(zmq.REP)
        self.cmd_socket.bind(f"tcp://*:{self.cmd_port}")

        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://*:{self.pub_port}")

        self._shared_data = {
            "RunNumber":   -1,
            "RunType":     "",
            "Shift":       "",
            "DaqtimeBase": 0.0,  # sum of daqtime for completed runs of same type
            "RunStats":          {},
            "ModuleConnected":   {},
            "SubRunNumber":      0,
            "StartTime":         0,
            "EndTime":           0,
            "MonNames":          [],
            "CurrentTime":       0.0,
        }
        # Cached by BOOT_RUN so _monitor_loop avoids repeated DB reads
        self._current_run_number = -1
        self._current_config_file = ""
        self._data_lock = threading.Lock()
        self._db_write_lock = threading.Lock()
        self._hw_lock = threading.Lock()

        self.running = True
        self._daq_connected = False
        self.error_count = 0
        self.start_time = time.monotonic()
        self._fallback_modules_loaded = False

        self.monitor_thread = None
        self.reconnect_thread = None
        self._reconnect_event = threading.Event()  # set by NOTIFY_DAQ_STARTED

    # ------------------------------------------------------------------
    # DAQ server connection
    # ------------------------------------------------------------------

    def _connect_daq(self) -> bool:
        try:
            sock = onlutils.get_connection(onlconsts.kDAQSERVER_ADDR)
            reply = onlutils.send_daq_cmd(
                sock, onlconsts.kQUERYDAQSTATUS, timeout_ms=2000)
            sock.close()
            success = reply is not None
            with self._hw_lock:
                self._daq_connected = success
            if success:
                log.info("TCB reachable at %s", onlconsts.kDAQSERVER_ADDR)
            else:
                log.warning("TCB not responding at %s", onlconsts.kDAQSERVER_ADDR)
            return success
        except Exception as e:
            with self._hw_lock:
                self._daq_connected = False
            log.warning("TCB connection failed: %s", e)
            return False

    def _try_load_fallback_modules(self):
        """Load last run's metadata and module list from DB/config for degraded-mode publishing."""
        try:
            with self._db_write_lock:
                conn = sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0)
                try:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT runnum, runtype, shift FROM runcatalog"
                        " ORDER BY runnum DESC LIMIT 1")
                    record = cursor.fetchone()
                    if not record:
                        return

                    runnum   = record['runnum']
                    runtype  = record['runtype'] or ""
                    shift    = record['shift'] or ""

                    # DaqtimeBase = sum of daqtime for all completed physics runs
                    daqtime_base = 0.0
                    if runtype == "physics":
                        cursor.execute("PRAGMA table_info(runcatalog)")
                        time_cols = [row[1] for row in cursor.fetchall()
                                     if row[1].startswith('t') and row[2].upper() == 'REAL']
                        if time_cols:
                            col_expr = ", ".join(f"COALESCE({c}, 0.0)" for c in time_cols)
                            cursor.execute(f"""
                                SELECT {col_expr} FROM runcatalog
                                WHERE runtype = 'physics'
                                  AND etime IS NOT NULL AND etime != ''
                            """)
                            daqtime_base = sum(max(row) for row in cursor.fetchall())
                finally:
                    conn.close()

            config_path = os.path.join(
                onlconsts.kRAWDATA_DIR, 'CONFIG', f'{runnum:06d}.yml')
            if not os.path.isfile(config_path):
                log.warning("Fallback config not found: %s", config_path)
                return
            with open(config_path, 'r', encoding='utf-8') as fp:
                config_data = yaml.safe_load(fp) or {}
            mon_names = [
                str(item.get('NAME', ''))
                for item in config_data.get('DAQ', [])
                if 'TCB' not in str(item.get('NAME', ''))
            ]
            with self._data_lock:
                self._shared_data['RunNumber']   = runnum
                self._shared_data['RunType']     = runtype
                self._shared_data['Shift']       = shift
                self._shared_data['DaqtimeBase'] = daqtime_base
                self._shared_data['MonNames']    = mon_names
                self._shared_data['RunStats']    = {
                    name: {'n': 0, 'dn': 0, 't': 0.0, 'dt': 0.0, 'ar': 0.0, 'sr': 0.0}
                    for name in mon_names
                }
            log.info("Fallback: run %d (%s) loaded, daqtime_base=%.0fs, modules=%s",
                     runnum, runtype, daqtime_base, mon_names)
        except Exception as e:
            log.warning("Failed to load fallback module list: %s", e)

    def _reconnect_loop(self):
        """Retry TCB connection when disconnected.
        Publishes kDOWN heartbeats at 1Hz when the monitor loop is not running.
        Reconnection is attempted every kDAQMON_RECONNECT_INTERVAL seconds,
        or immediately when _reconnect_event is set (NOTIFY_DAQ_STARTED)."""
        elapsed = 0
        while self.running:
            woke_early = self._reconnect_event.wait(timeout=1.0)
            if not self.running:
                break

            with self._hw_lock:
                connected = self._daq_connected
            monitor_alive = (self.monitor_thread is not None
                             and self.monitor_thread.is_alive())

            if not connected and not monitor_alive:
                if not self._fallback_modules_loaded:
                    self._fallback_modules_loaded = True
                    with self._data_lock:
                        mon_names_empty = not self._shared_data['MonNames']
                    if mon_names_empty:
                        self._try_load_fallback_modules()
                self._publish(onlconsts.kDOWN, time.time())

            if woke_early or elapsed >= onlconsts.kDAQMON_RECONNECT_INTERVAL:
                self._reconnect_event.clear()
                elapsed = 0
                if not connected:
                    log.info("Retrying TCB connection...")
                    if self._connect_daq():
                        if not monitor_alive:
                            self.monitor_thread = threading.Thread(
                                target=self._monitor_loop, daemon=True)
                            self.monitor_thread.start()
                            log.info("Monitor thread restarted after reconnection.")
            else:
                elapsed += 1

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self):
        log.info("DAQ Monitor Server starting...")
        log.info("Command interface (REP) on port %d", self.cmd_port)
        log.info("Telemetry broadcast (PUB) on port %d", self.pub_port)

        if self._connect_daq():
            self.monitor_thread = threading.Thread(
                target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            log.info("DAQ Monitor Server fully operational.")
        else:
            log.warning(
                "Starting in DEGRADED mode — DAQ server not reachable. "
                "Will retry every 10s."
            )

        self.reconnect_thread = threading.Thread(
            target=self._reconnect_loop, daemon=True)
        self.reconnect_thread.start()

        self._command_loop()

    # ------------------------------------------------------------------
    # Monitor loop (1Hz polling + PUB broadcast)
    # ------------------------------------------------------------------

    def _monitor_loop(self):
        log.info("Monitor loop started.")

        last_run_number = -1
        last_run_state = -1
        last_active_time = time.time()
        fallback_loaded = False  # True after attempting to load module list from last run config

        mon_list = []
        run_stats = {}
        mon_names = []

        # Single socket to TCB: handles both kQUERYDAQSTATUS and kQUERYRUNINFO
        tcb_sock = None
        run_ending = False  # set when kPROCENDED or kERROR detected
        last_db_update_time = 0.0

        def _close_all_sockets():
            nonlocal tcb_sock
            if tcb_sock:
                try:
                    tcb_sock.close()
                except Exception:
                    pass
                tcb_sock = None
            for mon in mon_list:
                if mon['sock']:
                    try:
                        mon['sock'].close()
                    except Exception:
                        pass
                    mon['sock'] = None

        while self.running:
            time.sleep(1.0)
            current_time = time.time()

            if current_time - last_active_time > onlconsts.kDAQMON_IDLE_TIMEOUT:
                log.warning(
                    "Monitor idle for %d seconds. Entering degraded mode.",
                    onlconsts.kDAQMON_IDLE_TIMEOUT)
                with self._hw_lock:
                    self._daq_connected = False
                _close_all_sockets()
                break

            run_state = onlconsts.kDOWN

            try:
                if tcb_sock is None:
                    tcb_sock = onlutils.get_connection(onlconsts.kDAQSERVER_ADDR)

                # TCB → overall DAQ state
                reply = onlutils.send_daq_cmd(tcb_sock, onlconsts.kQUERYDAQSTATUS)

                if reply is None:
                    if run_ending:
                        log.info("TCB shut down as expected after run end.")
                    else:
                        log.warning("Unexpected TCB disconnect.")
                    try:
                        tcb_sock.close()
                    except Exception:
                        pass
                    tcb_sock = None
                    with self._hw_lock:
                        self._daq_connected = False
                    self._publish(run_state, current_time)
                    _close_all_sockets()
                    break

                run_state = reply.get("run_status", onlconsts.kDOWN)

                if run_state != last_run_state:
                    log.info("DAQ State changed: %s -> %s",
                             last_run_state, run_state)
                    last_run_state = run_state

                # Run end conditions: normal end (kPROCENDED) or error
                if not run_ending and (
                        onlutils.check_state(run_state, onlconsts.kPROCENDED) or
                        onlutils.check_error(run_state)):
                    run_ending = True
                    log.info("Run end detected (state=%s). Closing module sockets.", run_state)
                    for mon in mon_list:
                        if mon['sock']:
                            try:
                                mon['sock'].close()
                            except Exception:
                                pass
                            mon['sock'] = None

                if run_state != onlconsts.kDOWN:
                    last_active_time = current_time

                is_active = (onlutils.check_state(run_state, onlconsts.kRUNNING) or
                             onlutils.check_state(run_state, onlconsts.kRUNENDED))

                if is_active:
                    # Use run info cached by BOOT_RUN; fall back to DB only if missing
                    current_run_number = self._current_run_number
                    config_file = self._current_config_file

                    if current_run_number == -1:
                        with self._db_write_lock:
                            conn = sqlite3.connect(
                                onlconsts.kRUNCATALOGDBFILE, timeout=5.0)
                            try:
                                conn.row_factory = sqlite3.Row
                                cursor = conn.cursor()
                                cursor.execute(
                                    "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                                record = cursor.fetchone()
                            finally:
                                conn.close()
                        if not record:
                            self._publish(run_state, current_time)
                            continue
                        current_run_number = record['runnum']
                        config_file = record['config']
                        self._current_run_number = current_run_number
                        self._current_config_file = config_file
                        with self._data_lock:
                            self._shared_data['RunNumber'] = current_run_number
                            self._shared_data['RunType']   = record['runtype'] or ""
                            self._shared_data['Shift']     = record['shift'] or ""

                    if current_run_number != last_run_number:
                        if last_run_number != -1:
                            log.info("New run %d detected. Re-initializing modules.",
                                     current_run_number)

                        for mon in mon_list:
                            if mon['sock']:
                                try:
                                    mon['sock'].close()
                                except Exception:
                                    pass

                        mon_list.clear()
                        run_stats.clear()
                        mon_names.clear()

                        if os.path.isfile(config_file):
                            with open(config_file, 'r', encoding='utf-8') as fp:
                                config_data = yaml.safe_load(fp) or {}

                            for item in config_data.get('DAQ', []):
                                name = str(item.get('NAME', ''))
                                ip = str(item.get('IP', ''))
                                port = int(item.get('PORT', 0))
                                if 'TCB' in name:
                                    continue  # state/run info comes from TCB, not here
                                mon_list.append(
                                    {'name': name, 'ip': ip, 'port': port, 'sock': None})
                                mon_names.append(name)
                                run_stats[name] = {
                                    'n': 0, 'dn': 0, 't': 0.0,
                                    'dt': 0.0, 'ar': 0.0, 'sr': 0.0}

                        with self._data_lock:
                            self._shared_data['MonNames'] = list(mon_names)
                            self._shared_data['RunStats'] = {
                                k: dict(v) for k, v in run_stats.items()}
                            self._shared_data['StartTime'] = 0
                            self._shared_data['EndTime'] = 0
                            self._shared_data['SubRunNumber'] = 0

                        last_run_number = current_run_number

                    # TCB → run info (start/end time, subrun number)
                    info_reply = onlutils.send_daq_cmd(tcb_sock, onlconsts.kQUERYRUNINFO)
                    if info_reply:
                        with self._data_lock:
                            self._shared_data['SubRunNumber'] = info_reply.get(
                                "subrun_number", 0)
                            self._shared_data['StartTime'] = info_reply.get(
                                "start_time", 0)
                            self._shared_data['EndTime'] = info_reply.get(
                                "end_time", 0)
                    else:
                        # TCB timeout on run info — reset socket for next cycle
                        try:
                            tcb_sock.close()
                        except Exception:
                            pass
                        tcb_sock = None

                # On first DOWN cycle with empty mon_list, try loading last run's config
                if not mon_list and not is_active and not fallback_loaded:
                    fallback_loaded = True
                    try:
                        with self._db_write_lock:
                            conn = sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0)
                            try:
                                conn.row_factory = sqlite3.Row
                                cursor = conn.cursor()
                                cursor.execute(
                                    "SELECT runnum FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                                record = cursor.fetchone()
                            finally:
                                conn.close()
                        if record:
                            runnum = record['runnum']
                            config_path = os.path.join(
                                onlconsts.kRAWDATA_DIR, 'CONFIG', f'{runnum:06d}.yml')
                            if os.path.isfile(config_path):
                                with open(config_path, 'r', encoding='utf-8') as fp:
                                    config_data = yaml.safe_load(fp) or {}
                                for item in config_data.get('DAQ', []):
                                    name = str(item.get('NAME', ''))
                                    ip = str(item.get('IP', ''))
                                    port = int(item.get('PORT', 0))
                                    if 'TCB' in name:
                                        continue
                                    mon_list.append(
                                        {'name': name, 'ip': ip, 'port': port, 'sock': None})
                                    mon_names.append(name)
                                    run_stats[name] = {
                                        'n': 0, 'dn': 0, 't': 0.0,
                                        'dt': 0.0, 'ar': 0.0, 'sr': 0.0}
                                with self._data_lock:
                                    self._shared_data['MonNames'] = list(mon_names)
                                    self._shared_data['RunStats'] = {
                                        k: dict(v) for k, v in run_stats.items()}
                                log.info("Loaded module list from run %d config for DOWN-state polling.",
                                         runnum)
                            else:
                                log.warning("Fallback config not found: %s", config_path)
                    except Exception as e:
                        log.warning("Failed to load fallback module list: %s", e)

                # Module polling — connection check in all states, stats only when active
                if mon_list:
                    local_stats = {}
                    set_clauses = []
                    update_params = []
                    connected_names = set()

                    for mon in mon_list:
                        name = mon['name']

                        if mon['sock'] is None:
                            endpoint = f"tcp://{mon['ip']}:{mon['port']}"
                            log.debug("Connecting to module %s at %s", name, endpoint)
                            mon['sock'] = onlutils.get_connection(endpoint)

                            init_reply = onlutils.send_daq_cmd(
                                mon['sock'], onlconsts.kQUERYMONITOR)
                            if init_reply is None:
                                log.warning("Module %s init timeout.", name)
                                try:
                                    mon['sock'].close()
                                except Exception:
                                    pass
                                mon['sock'] = None
                                continue

                        if mon['sock']:
                            try:
                                trg_info = onlutils.send_daq_cmd(
                                    mon['sock'], onlconsts.kQUERYTRGINFO)
                                if trg_info is None:
                                    raise Exception("Empty response")

                                connected_names.add(name)

                                if is_active:
                                    n = run_stats[name]['n'] = trg_info.get("nevent", 0)
                                    t_ns = trg_info.get("trgtime", 0)
                                    t = run_stats[name]['t'] = t_ns / 1_000_000_000.0

                                    if t > 0:
                                        run_stats[name]['ar'] = n / t
                                    dt = t - run_stats[name]['dt']
                                    dn = n - run_stats[name]['dn']
                                    if dt > 0:
                                        run_stats[name]['sr'] = dn / dt
                                    run_stats[name]['dt'] = t
                                    run_stats[name]['dn'] = n

                                    local_stats[name] = dict(run_stats[name])

                                    if 'AADC' in name:
                                        set_clauses.extend(["naadc=?", "taadc=?"])
                                    elif 'FADC' in name:
                                        set_clauses.extend(["nfadc=?", "tfadc=?"])
                                    elif 'SADC' in name:
                                        set_clauses.extend(["nsadc=?", "tsadc=?"])
                                    elif 'IADC' in name:
                                        set_clauses.extend(["niadc=?", "tiadc=?"])
                                    update_params.extend([n, t])

                            except Exception as e:
                                log.error("Polling module %s failed: %s", name, e)
                                try:
                                    mon['sock'].close()
                                except Exception:
                                    pass
                                mon['sock'] = None

                    module_connected = {name: (name in connected_names) for name in mon_names}
                    with self._data_lock:
                        if is_active:
                            self._shared_data['RunStats'].update(local_stats)
                        self._shared_data['ModuleConnected'] = module_connected
                        self._shared_data['CurrentTime'] = current_time

                    # DB update — throttled to kSTATSREPORTINTERVAL (only while RUNNING)
                    if (is_active
                            and set_clauses
                            and onlutils.check_state(run_state, onlconsts.kRUNNING)
                            and current_time - last_db_update_time
                                >= onlconsts.kSTATSREPORTINTERVAL):
                        q = ("UPDATE runcatalog SET "
                             + ", ".join(set_clauses)
                             + " WHERE runnum=?")
                        update_params.append(current_run_number)
                        try:
                            with self._db_write_lock:
                                conn = sqlite3.connect(
                                    onlconsts.kRUNCATALOGDBFILE, timeout=5.0)
                                try:
                                    conn.execute(q, tuple(update_params))
                                    conn.commit()
                                finally:
                                    conn.close()
                            last_db_update_time = current_time
                        except Exception as e:
                            log.error("DB UPDATE failed: %s", e, exc_info=True)

            except Exception as e:
                log.critical("Monitor loop crashed: %s", e, exc_info=True)
                if tcb_sock:
                    try:
                        tcb_sock.close()
                    except Exception:
                        pass
                    tcb_sock = None

            self._publish(run_state, current_time)

        log.info("Monitor loop exited.")

    def _publish(self, run_state, current_time):
        """Publish a snapshot of shared_data via the PUB socket."""
        with self._data_lock:
            payload = {
                "RunNumber":   self._shared_data['RunNumber'],
                "RunType":     self._shared_data['RunType'],
                "Shift":       self._shared_data['Shift'],
                "DaqtimeBase": self._shared_data['DaqtimeBase'],
                "RunStats":          dict(self._shared_data['RunStats']),
                "ModuleConnected":   dict(self._shared_data['ModuleConnected']),
                "SubRunNumber":      self._shared_data['SubRunNumber'],
                "StartTime":         self._shared_data['StartTime'],
                "EndTime":           self._shared_data['EndTime'],
                "MonNames":          list(self._shared_data['MonNames']),
                "CurrentTime":       current_time,
                "RunState":          run_state,
            }
        try:
            self.pub_socket.send_json(payload, flags=zmq.NOBLOCK)
        except Exception as e:
            log.debug("PUB send failed: %s", e)

    # ------------------------------------------------------------------
    # Command loop (main thread)
    # ------------------------------------------------------------------

    def _command_loop(self):
        poller = zmq.Poller()
        poller.register(self.cmd_socket, zmq.POLLIN)
        log.info("Command loop ready on port %d", self.cmd_port)

        while self.running:
            if poller.poll(500):
                try:
                    request = self.cmd_socket.recv_json()
                    cmd = request.get("cmd")
                    if cmd != "PING":
                        log.debug("CMD: %s", cmd)
                    response = self._handle_request(cmd, request)
                    self.cmd_socket.send_json(response)
                except Exception as e:
                    log.error("Command loop error: %s", e, exc_info=True)
                    try:
                        self.cmd_socket.send_json(
                            {"status": "error", "message": str(e)})
                    except Exception:
                        pass

    def _handle_request(self, cmd, request):
        if cmd is None:
            return {"status": "error", "message": "Missing 'cmd' field"}

        # Always-available commands
        if cmd == "PING":
            return {"status": "ok"}

        if cmd == "NOTIFY_DAQ_STARTED":
            log.info("DAQ boot notification received. Triggering immediate reconnect.")
            self._reconnect_event.set()
            return {"status": "ok"}

        if cmd == "GET_SERVER_HEALTH":
            with self._hw_lock:
                daq_connected = self._daq_connected
            return {
                "uptime_s":     time.monotonic() - self.start_time,
                "daq_connected": daq_connected,
                "error_count":  self.error_count,
            }

        # DB commands
        try:
            with self._db_write_lock:
                conn = sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0)
                try:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    result = None

                    if cmd == "BOOT_RUN":
                        config  = request.get("config", "")
                        runtype = request.get("runtype", "")
                        cursor.execute(
                            "INSERT INTO runcatalog "
                            "(shift, runtype, rundesc, config) VALUES (?, ?, ?, ?)",
                            (request.get("shift", ""),
                             runtype,
                             request.get("rundesc", ""),
                             config)
                        )
                        conn.commit()
                        run_num = cursor.lastrowid

                        # Sum daqtime of completed physics runs (excl. current)
                        historical = 0.0
                        if runtype == "physics":
                            cursor.execute("PRAGMA table_info(runcatalog)")
                            time_cols = [row[1] for row in cursor.fetchall()
                                         if row[1].startswith('t') and row[2].upper() == 'REAL']
                            if time_cols:
                                col_expr = ", ".join(f"COALESCE({c}, 0.0)" for c in time_cols)
                                cursor.execute(f"""
                                    SELECT {col_expr} FROM runcatalog
                                    WHERE runtype = 'physics'
                                      AND etime IS NOT NULL AND etime != ''
                                      AND runnum != ?
                                """, (run_num,))
                                historical = sum(max(row) for row in cursor.fetchall())

                        self._current_run_number = run_num
                        self._current_config_file = config
                        with self._data_lock:
                            self._shared_data['RunNumber']   = run_num
                            self._shared_data['RunType']     = runtype
                            self._shared_data['Shift']       = request.get("shift", "")
                            self._shared_data['DaqtimeBase'] = historical
                        log.info("BOOT_RUN: Run %d  runtype=%s  historical_daqtime=%.0fs",
                                 run_num, runtype, historical)
                        result = {"run_num": run_num}

                    elif cmd == "SYNC_LATEST":
                        cursor.execute(
                            "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                        record = cursor.fetchone()
                        result = dict(record) if record else {}

                    elif cmd == "TAG_GOODRUN":
                        run_num   = request.get("run_num")
                        onlbit    = request.get("onlbit")
                        stime_str = request.get("stime_str")
                        etime_str = request.get("etime_str")
                        log.info("TAG_GOODRUN: RunNum %s, goodrun=%s",
                                 run_num, bool(onlbit))

                        q = "UPDATE runcatalog SET stime=?, etime=?, onlbit=?"
                        params = [stime_str, etime_str, onlbit]

                        for daqname, stats in request.get("final_stats", {}).items():
                            n_val = stats.get('n', 0)
                            t_val = stats.get('t', 0.0)
                            if 'AADC' in daqname:
                                q += ", naadc=?, taadc=?"
                            elif 'FADC' in daqname:
                                q += ", nfadc=?, tfadc=?"
                            elif 'SADC' in daqname:
                                q += ", nsadc=?, tsadc=?"
                            elif 'IADC' in daqname:
                                q += ", niadc=?, tiadc=?"
                            else:
                                continue
                            params.extend([n_val, t_val])

                        q += " WHERE runnum=?"
                        params.append(run_num)
                        cursor.execute(q, tuple(params))
                        conn.commit()
                        result = {"status": "ok"}

                finally:
                    conn.close()

            if result is not None:
                return result

        except Exception as e:
            log.error("DB operation failed for cmd '%s': %s", cmd, e,
                      exc_info=True)
            return {"status": "error", "message": str(e)}

        log.warning("Unknown command: %s", cmd)
        return {"status": "error", "message": f"Unknown command: {cmd}"}

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self, reason=""):
        if not self.running:
            return

        log.info("Shutting down. Reason: %s", reason)
        self.running = False

        try:
            self.pub_socket.send_json(
                {"type": "shutdown", "reason": reason}, flags=zmq.NOBLOCK)
            time.sleep(0.3)
        except Exception:
            pass

        try:
            self.cmd_socket.close()
            self.pub_socket.close()
            self.context.term()
        except Exception as e:
            log.error("ZMQ cleanup error: %s", e)

        if self.pid_file and os.path.exists(self.pid_file):
            try:
                with open(self.pid_file, 'r') as f:
                    if int(f.read().strip()) == os.getpid():
                        os.remove(self.pid_file)
            except Exception:
                pass

        log.info("DAQ Monitor Server terminated.")
        sys.exit(0)


# ------------------------------------------------------------------
# Daemonization (double-fork)
# ------------------------------------------------------------------

def daemonize():
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"Fork #1 failed: {e}\n")
        sys.exit(1)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"Fork #2 failed: {e}\n")
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as f:
        os.dup2(f.fileno(), sys.stdin.fileno())
    with open('/dev/null', 'a+') as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
    with open('/dev/null', 'a+') as f:
        os.dup2(f.fileno(), sys.stderr.fileno())


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CUP DAQ Monitor Server")
    parser.add_argument(
        "--daemon", action="store_true",
        help="Run in background (daemonize)")
    parser.add_argument(
        "--log", default=onlconsts.kDAQMON_LOG_FILE,
        help=f"Log file path (default: {onlconsts.kDAQMON_LOG_FILE})")
    parser.add_argument(
        "--pid", default=onlconsts.kDAQMON_PID_FILE,
        help=f"PID file path (default: {onlconsts.kDAQMON_PID_FILE})")
    parser.add_argument(
        "--debug", action="store_true",
        help="Enable debug logging")
    args = parser.parse_args()

    # Resolve to absolute paths before daemonize() calls os.chdir("/")
    args.log = os.path.abspath(args.log)
    args.pid = os.path.abspath(args.pid)

    # Check for duplicate instance via PID file
    if os.path.exists(args.pid):
        try:
            with open(args.pid, 'r') as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)
            print(f"Server already running with PID {old_pid}. Exiting.")
            sys.exit(1)
        except (OSError, ValueError):
            print("Stale PID file found. Cleaning up.")
            os.remove(args.pid)

    if args.daemon:
        daemonize()

    _setup_logging(args.log, debug=args.debug, daemon=args.daemon)

    with open(args.pid, 'w') as f:
        f.write(str(os.getpid()))

    server = DAQMonitorServer(pid_file=args.pid)

    def signal_handler(sig, frame):
        reason = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
        server.shutdown(reason)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server.start()


if __name__ == "__main__":
    main()
