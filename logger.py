import sys
import time
import os
import sqlite3
import yaml
import socket
import json
import threading
from datetime import datetime

import onlconsts
import onlutils

# Shared data in memory for real-time communication with RC
shared_data = {
    "RunStats": {},
    "SubRunNumber": 0,
    "StartTime": 0,
    "EndTime": 0,
    "MonNames": []
}
data_lock = threading.Lock()


def handle_rc_requests():
    """Local server thread handling RC commands (DB insert, tag save, state query)"""
    host = '127.0.0.1'
    port = 9999

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(5)

    while True:
        try:
            conn, addr = server.accept()
            data = conn.recv(8192).decode('utf-8')
            if not data:
                conn.close()
                continue

            request = json.loads(data)
            cmd = request.get("cmd")

            # All DB accesses are safely executed in this single thread to prevent SQLite locks
            with sqlite3.connect(onlconsts.kRUNCATALOGDBFILE) as db_conn:
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
                    conn.sendall(json.dumps(
                        {"run_num": cursor.lastrowid}).encode('utf-8'))

                elif cmd == "SYNC_LATEST":
                    cursor.execute(
                        "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                    record = cursor.fetchone()
                    if record:
                        resp = {
                            "runnum": record['runnum'],
                            "shift": record['shift'],
                            "runtype": record['runtype'],
                            "rundesc": record['rundesc'],
                            "config": record['config']
                        }
                    else:
                        resp = {}
                    conn.sendall(json.dumps(resp).encode('utf-8'))

                elif cmd == "GET_STATS":
                    with data_lock:
                        resp = shared_data.copy()
                    conn.sendall(json.dumps(resp).encode('utf-8'))

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
                    conn.sendall(json.dumps({"status": "ok"}).encode('utf-8'))

            conn.close()
        except Exception as e:
            if 'conn' in locals():
                conn.close()


def run_logger():
    """Monitor DAQ state and perform real-time DB logging at 1Hz"""
    print("DAQ Logger daemon started.")
    last_run_number = -1
    mon_list = []
    run_stats = {}
    mon_names = []

    while True:
        time.sleep(1.0)

        try:
            run_state, _ = onlutils.query_runstate(
                onlconsts.kDAQSERVER_ADDR, None)

            if onlutils.check_state(run_state, onlconsts.kRUNNING) or onlutils.check_state(run_state, onlconsts.kRUNENDED):
                with sqlite3.connect(onlconsts.kRUNCATALOGDBFILE) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT * FROM runcatalog ORDER BY runnum DESC LIMIT 1")
                    record = cursor.fetchone()

                    if not record:
                        continue

                    current_run_number = record['runnum']
                    config_file = record['config']

                    if current_run_number != last_run_number:
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

                                try:
                                    sock = onlutils.get_connection((ip, port))
                                    onlutils.send_command(
                                        sock, onlconsts.kQUERYMONITOR)
                                    mess = []
                                    onlutils.recv_message(sock, mess)
                                    if mess and mess[0] > 0:
                                        mon_list.append((name, sock))
                                        mon_names.append(name)
                                        run_stats[name] = {
                                            'n': 0, 'dn': 0, 't': 0.0, 'dt': 0.0, 'ar': 0.0, 'sr': 0.0}
                                except Exception as e:
                                    pass

                        with data_lock:
                            shared_data['MonNames'] = mon_names
                            shared_data['RunStats'] = run_stats
                        last_run_number = current_run_number

                    try:
                        daq_sock = onlutils.get_connection(
                            onlconsts.kDAQSERVER_ADDR)
                        onlutils.send_command(
                            daq_sock, onlconsts.kQUERYRUNINFO)
                        mess = []
                        onlutils.recv_message(daq_sock, mess)
                        if mess:
                            with data_lock:
                                shared_data['SubRunNumber'] = mess[1]
                                shared_data['StartTime'] = mess[2]
                                if len(mess) > 3:
                                    shared_data['EndTime'] = mess[3]
                        daq_sock.close()
                    except:
                        pass

                    update_query = "UPDATE runcatalog SET "
                    update_params = []
                    set_clauses = []

                    with data_lock:
                        for name, sock in mon_list:
                            try:
                                onlutils.send_command(
                                    sock, onlconsts.kQUERYTRGINFO)
                                mess = []
                                onlutils.recv_message(sock, mess)
                                if mess:
                                    n = run_stats[name]['n'] = mess[0]
                                    t = run_stats[name]['t'] = mess[1] / \
                                        1000000000.
                                    if t > 0:
                                        run_stats[name]['ar'] = n/t
                                    dt = t - run_stats[name]['dt']
                                    dn = n - run_stats[name]['dn']
                                    if dt > 0:
                                        run_stats[name]['sr'] = dn/dt
                                    run_stats[name]['dt'] = t
                                    run_stats[name]['dn'] = n

                                    if 'AADC' in name:
                                        set_clauses.extend(
                                            ["naadc=?", "taadc=?"])
                                    elif 'FADC' in name:
                                        set_clauses.extend(
                                            ["nfadc=?", "tfadc=?"])
                                    elif 'SADC' in name:
                                        set_clauses.extend(
                                            ["nsadc=?", "tsadc=?"])
                                    elif 'IADC' in name:
                                        set_clauses.extend(
                                            ["niadc=?", "tiadc=?"])
                                    update_params.extend([n, t])
                            except Exception:
                                pass

                    if set_clauses and onlutils.check_state(run_state, onlconsts.kRUNNING):
                        update_query += ", ".join(set_clauses) + \
                            " WHERE runnum=?"
                        update_params.append(current_run_number)
                        cursor.execute(update_query, tuple(update_params))
                        conn.commit()
        except Exception:
            pass


if __name__ == '__main__':
    api_thread = threading.Thread(target=handle_rc_requests, daemon=True)
    api_thread.start()
    run_logger()
