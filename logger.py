import onlutils
import onlconsts
from datetime import datetime
import threading
import json
import zmq
import yaml
import sqlite3
import time
import sys
import os

# Ensure the current directory is in the path to avoid ModuleNotFoundError when running via nohup
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Shared data in memory for real-time communication with RC
shared_data = {
    "RunStats": {},
    "SubRunNumber": 0,
    "StartTime": 0,
    "EndTime": 0,
    "MonNames": []
}
data_lock = threading.Lock()

# Automatically terminate logger if DAQ is DOWN for this many seconds
IDLE_TIMEOUT_SEC = 3600


def handle_rc_requests():
    """
    Local ZMQ REP server thread handling RC commands (DB insert, tag save, state query).
    """
    context = zmq.Context.instance()
    sock = context.socket(zmq.REP)
    sock.bind(f"tcp://*:{onlconsts.kLOGGERPORT}")

    while True:
        try:
            request = sock.recv_json()
            cmd = request.get("cmd")
            response = {"status": "error"}

            with sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0) as db_conn:
                db_conn.row_factory = sqlite3.Row
                cursor = db_conn.cursor()

                if cmd == "BOOT_RUN":
                    shift = request.get("shift", "")
                    runtype = request.get("runtype", "")
                    rundesc = request.get("rundesc", "")
                    config = request.get("config", "")

                    cursor.execute(
                        "INSERT INTO runcatalog (shift, runtype, rundesc, config) VALUES (?, ?, ?, ?)",
                        (shift, runtype, rundesc, config)
                    )
                    db_conn.commit()
                    response = {"run_num": cursor.lastrowid}

                elif cmd == "SYNC_LATEST":
                    cursor.execute(
                        "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                    record = cursor.fetchone()
                    if record:
                        response = dict(record)
                    else:
                        response = {}

                elif cmd == "GET_STATS":
                    with data_lock:
                        response = shared_data.copy()

                elif cmd == "TAG_GOODRUN":
                    run_num = request.get("run_num")
                    onlbit = request.get("onlbit")
                    stime_str = request.get("stime_str")
                    etime_str = request.get("etime_str")

                    update_query = "UPDATE runcatalog SET stime=?, etime=?, onlbit=?"
                    update_params = [stime_str, etime_str, onlbit]

                    final_stats = request.get("final_stats", {})
                    for daqname, stats in final_stats.items():
                        n_val = stats.get('n', 0)
                        t_val = stats.get('t', 0.0)
                        if 'AADC' in daqname:
                            update_query += ", naadc=?, taadc=?"
                            update_params.extend([n_val, t_val])
                        elif 'FADC' in daqname:
                            update_query += ", nfadc=?, tfadc=?"
                            update_params.extend([n_val, t_val])
                        elif 'SADC' in daqname:
                            update_query += ", nsadc=?, tsadc=?"
                            update_params.extend([n_val, t_val])
                        elif 'IADC' in daqname:
                            update_query += ", niadc=?, tiadc=?"
                            update_params.extend([n_val, t_val])

                    update_query += " WHERE runnum=?"
                    update_params.append(run_num)

                    cursor.execute(update_query, tuple(update_params))
                    db_conn.commit()
                    response = {"status": "ok"}

            sock.send_json(response)
        except Exception as e:
            try:
                sock.send_json({"status": "error", "message": str(e)})
            except:
                pass


def run_logger():
    """
    Monitor DAQ state and perform real-time DB logging at 1Hz using ZMQ JSON.
    """
    print(f"[{datetime.now()}] DAQ Logger daemon started.")
    last_run_number = -1
    last_run_state = -1

    mon_list = []
    run_stats = {}
    mon_names = []
    last_active_time = time.time()

    daq_state_sock = None
    daq_info_sock = None

    while True:
        time.sleep(1.0)
        current_time = time.time()

        if current_time - last_active_time > IDLE_TIMEOUT_SEC:
            print(
                f"[{datetime.now()}] Logger idle for {IDLE_TIMEOUT_SEC} seconds. Auto-terminating.")
            break

        try:
            # Query DAQ State
            if daq_state_sock is None:
                daq_state_sock = onlutils.get_connection(
                    onlconsts.kDAQSERVER_ADDR)

            reply = onlutils.execute_command(
                daq_state_sock, onlconsts.kQUERYDAQSTATUS)
            if reply is None:
                try:
                    daq_state_sock.close()
                except:
                    pass
                daq_state_sock = None
                continue

            run_state = reply.get("status", onlconsts.kDOWN)

            if run_state != last_run_state:
                if onlutils.check_state(run_state, onlconsts.kRUNNING):
                    print(f"[{datetime.now()}] DAQ State changed to RUNNING.")
                elif onlutils.check_state(run_state, onlconsts.kRUNENDED):
                    print(f"[{datetime.now()}] DAQ State changed to RUNENDED.")
                elif onlutils.check_state(run_state, onlconsts.kDOWN):
                    print(f"[{datetime.now()}] DAQ State changed to DOWN.")
                last_run_state = run_state

            if not onlutils.check_state(run_state, onlconsts.kDOWN):
                last_active_time = current_time

            # Handle monitoring and DB writing
            if onlutils.check_state(run_state, onlconsts.kRUNNING) or onlutils.check_state(run_state, onlconsts.kRUNENDED):
                with sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                    record = cursor.fetchone()

                if not record:
                    continue

                current_run_number = record['runnum']
                config_file = record['config']

                # New Run Initialization
                if current_run_number != last_run_number:
                    if last_run_number != -1:
                        print(
                            f"[{datetime.now()}] New Run {current_run_number} detected.")

                    for mon in mon_list:
                        if mon['sock']:
                            try:
                                mon['sock'].close()
                            except:
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
                                continue

                            mon_list.append(
                                {'name': name, 'ip': ip, 'port': port, 'sock': None})
                            mon_names.append(name)
                            run_stats[name] = {
                                'n': 0, 'dn': 0, 't': 0.0, 'dt': 0.0, 'ar': 0.0, 'sr': 0.0}

                    with data_lock:
                        shared_data['MonNames'] = mon_names
                        shared_data['RunStats'] = run_stats
                        shared_data['StartTime'] = 0
                        shared_data['EndTime'] = 0
                        shared_data['SubRunNumber'] = 0
                    last_run_number = current_run_number

                # Query DAQ Info
                if daq_info_sock is None:
                    daq_info_sock = onlutils.get_connection(
                        onlconsts.kDAQSERVER_ADDR)

                info_reply = onlutils.execute_command(
                    daq_info_sock, onlconsts.kQUERYRUNINFO)
                if info_reply:
                    with data_lock:
                        shared_data['SubRunNumber'] = info_reply.get(
                            "subrun", 0)
                        shared_data['StartTime'] = info_reply.get(
                            "starttime", 0)
                        shared_data['EndTime'] = info_reply.get("endtime", 0)
                else:
                    try:
                        daq_info_sock.close()
                    except:
                        pass
                    daq_info_sock = None

                # Polling Monitor Modules
                update_query = "UPDATE runcatalog SET "
                update_params = []
                set_clauses = []

                with data_lock:
                    for mon in mon_list:
                        name = mon['name']

                        if mon['sock'] is None:
                            endpoint = f"tcp://{mon['ip']}:{mon['port']}"
                            mon['sock'] = onlutils.get_connection(endpoint)
                            # Assuming kQUERYMONITOR initializes the module
                            onlutils.execute_command(
                                mon['sock'], onlconsts.kQUERYMONITOR)

                        if mon['sock']:
                            try:
                                trg_info = onlutils.execute_command(
                                    mon['sock'], onlconsts.kQUERYTRGINFO)
                                if trg_info is None:
                                    raise Exception("Recv empty")

                                # Adapting to JSON structure, assuming keys "events" or "n" and "time" or "t"
                                n = run_stats[name]['n'] = trg_info.get(
                                    "events", trg_info.get("n", 0))
                                t_ns = trg_info.get(
                                    "time", trg_info.get("t", 0))
                                t = run_stats[name]['t'] = t_ns / 1000000000.0

                                if t > 0:
                                    run_stats[name]['ar'] = n / t
                                dt = t - run_stats[name]['dt']
                                dn = n - run_stats[name]['dn']
                                if dt > 0:
                                    run_stats[name]['sr'] = dn / dt

                                run_stats[name]['dt'] = t
                                run_stats[name]['dn'] = n

                                if 'AADC' in name:
                                    set_clauses.extend(["naadc=?", "taadc=?"])
                                elif 'FADC' in name:
                                    set_clauses.extend(["nfadc=?", "tfadc=?"])
                                elif 'SADC' in name:
                                    set_clauses.extend(["nsadc=?", "tsadc=?"])
                                elif 'IADC' in name:
                                    set_clauses.extend(["niadc=?", "tiadc=?"])
                                update_params.extend([n, t])

                            except Exception:
                                try:
                                    mon['sock'].close()
                                except:
                                    pass
                                mon['sock'] = None

                # Safe DB Execution
                if set_clauses and onlutils.check_state(run_state, onlconsts.kRUNNING):
                    update_query += ", ".join(set_clauses) + " WHERE runnum=?"
                    update_params.append(current_run_number)
                    try:
                        with sqlite3.connect(onlconsts.kRUNCATALOGDBFILE, timeout=5.0) as conn:
                            cursor = conn.cursor()
                            cursor.execute(update_query, tuple(update_params))
                            conn.commit()
                    except Exception as db_e:
                        pass

        except Exception as e:
            pass


if __name__ == '__main__':
    api_thread = threading.Thread(target=handle_rc_requests, daemon=True)
    api_thread.start()
    run_logger()
