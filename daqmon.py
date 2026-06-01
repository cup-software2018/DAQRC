#!/usr/bin/env python3
"""
daqmon.py - One-shot DAQ status output for Telegraf exec input plugin.

Connects to daqmon_server, polls one snapshot of server health and run
telemetry, then prints InfluxDB line protocol to stdout and exits.

Telegraf config example:
    [[inputs.exec]]
      commands = ["python /path/to/daqmon.py"]
      timeout = "10s"
      data_format = "influx"
      interval = "30s"

Usage:
    python daqmon.py [--host HOST] [--debug]
"""

import sys
import time
import math
import argparse
import zmq

import onlconsts
import onlutils


def _line(measurement, tags, fields, ts_ns):
    """Format one InfluxDB line protocol record."""
    field_parts = []
    for k, v in fields.items():
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                continue
            field_parts.append(f"{k}={v}")
        elif isinstance(v, int):
            field_parts.append(f"{k}={v}i")
        elif isinstance(v, bool):
            field_parts.append(f"{k}={int(v)}i")
        else:
            field_parts.append(f'{k}="{v}"')

    if not field_parts:
        return None

    prefix = measurement
    if tags:
        prefix += "," + ",".join(f"{k}={v}" for k, v in tags.items())

    return f"{prefix} {','.join(field_parts)} {ts_ns}"



def _send_cmd(sock, cmd, timeout_ms=3000):
    """Send a daqmon_server command ({"cmd": ...} format) and return reply."""
    try:
        sock.send_json({"cmd": cmd})
        if sock.poll(timeout=timeout_ms) == 0:
            return None
        return sock.recv_json()
    except Exception:
        return None


def _drain_latest(sock, timeout_ms=2000):
    """
    Wait up to timeout_ms for the first PUB message, then drain the
    buffer and return the most recent payload.
    """
    if sock.poll(timeout=timeout_ms) == 0:
        return None
    latest = None
    while sock.poll(timeout=0) != 0:
        try:
            latest = sock.recv_json()
        except Exception:
            break
    return latest


def main():
    parser = argparse.ArgumentParser(
        description="One-shot DAQ monitor for Telegraf exec plugin (InfluxDB line protocol)")
    parser.add_argument(
        "--host", default=onlconsts.kDAQMON_IP,
        help=f"daqmon_server host (default: {onlconsts.kDAQMON_IP})")
    parser.add_argument(
        "--debug", action="store_true",
        help="Print debug info to stderr")
    args = parser.parse_args()

    cmd_addr = f"tcp://{args.host}:{onlconsts.kDAQMON_CMD_PORT}"
    pub_addr = f"tcp://{args.host}:{onlconsts.kDAQMON_PUB_PORT}"

    ctx = zmq.Context()
    ts_ns = time.time_ns()
    lines = []

    # ------------------------------------------------------------------
    # CMD socket — TCB connection status via daqmon_server health
    # ------------------------------------------------------------------
    cmd_sock = ctx.socket(zmq.REQ)
    cmd_sock.setsockopt(zmq.LINGER, 0)
    cmd_sock.connect(cmd_addr)

    server_alive = False
    try:
        reply = _send_cmd(cmd_sock, "PING", timeout_ms=3000)
        server_alive = reply is not None and reply.get("status") == "ok"

        if server_alive:
            health = _send_cmd(cmd_sock, "GET_SERVER_HEALTH", timeout_ms=3000) or {}
            if args.debug:
                print(f"# health: {health}", file=sys.stderr)
            tcb_connected = int(bool(health.get("daq_connected", False)))
        else:
            tcb_connected = 0

        rec = _line("tcb", {}, {"connected": tcb_connected}, ts_ns)
        if rec:
            lines.append(rec)

    except Exception as e:
        print(f"# ERROR: server health: {e}", file=sys.stderr)
        rec = _line("tcb", {}, {"connected": 0}, ts_ns)
        if rec:
            lines.append(rec)

    finally:
        cmd_sock.close()

    # ------------------------------------------------------------------
    # PUB socket — run telemetry
    # ------------------------------------------------------------------
    if server_alive:
        pub_sock = ctx.socket(zmq.SUB)
        pub_sock.setsockopt(zmq.LINGER, 0)
        pub_sock.setsockopt(zmq.SUBSCRIBE, b"")
        pub_sock.connect(pub_addr)

        try:
            snapshot = _drain_latest(pub_sock, timeout_ms=2000)

            if args.debug:
                print(f"# snapshot: {snapshot}", file=sys.stderr)

            if snapshot and snapshot.get("type") != "shutdown":
                run_state  = snapshot.get("RunState", onlconsts.kDOWN)
                run_number = snapshot.get("RunNumber", -1)
                run_type   = snapshot.get("RunType", "")
                shift      = snapshot.get("Shift", "")
                run_stats  = snapshot.get("RunStats", {})
                mon_names  = snapshot.get("MonNames", [])

                # daq_run — always write to track state without gaps
                run_fields = {"run_state": int(run_state)}
                if run_number >= 0:
                    run_fields["run_number"] = int(run_number)
                if run_type:
                    run_fields["run_type"] = run_type
                if shift:
                    run_fields["shift"] = shift
                run_fields["subrun_number"] = int(snapshot.get("SubRunNumber", 0))
                start_time = snapshot.get("StartTime", 0)
                end_time   = snapshot.get("EndTime", 0)
                if start_time:
                    run_fields["start_time"] = float(start_time)
                if end_time:
                    run_fields["end_time"] = float(end_time)
                rec = _line("daq_run", {}, run_fields, ts_ns)
                if rec:
                    lines.append(rec)

                # daq_total — any time a physics run exists (including DOWN between runs)
                if run_type == "physics" and run_number >= 0:
                    current_daqtime = max(
                        (run_stats.get(n, {}).get("t", 0.0) for n in mon_names),
                        default=0.0
                    )
                    total_daqtime = snapshot.get("DaqtimeBase", 0.0) + current_daqtime
                    rec = _line("daq_total", {}, {
                        "run_type":        run_type,
                        "total_daqtime_s": total_daqtime,
                    }, ts_ns)
                    if rec:
                        lines.append(rec)

                # daq_module — write for every known module
                # when DOWN: only emit connected=0, skip stats
                mod_connected = snapshot.get("ModuleConnected", {})
                is_active = run_state != onlconsts.kDOWN
                for name in mon_names:
                    if is_active:
                        s = run_stats.get(name, {})
                        mod_fields = {
                            "connected":  int(bool(mod_connected.get(name, False))),
                            "nevent":     int(s.get("n", 0)),
                            "daq_time_s": float(s.get("t", 0.0)),
                        }
                    else:
                        mod_fields = {"connected": 0}
                    rec = _line("daq_module", {"module": name}, mod_fields, ts_ns)
                    if rec:
                        lines.append(rec)

        except Exception as e:
            print(f"# ERROR: run telemetry: {e}", file=sys.stderr)

        finally:
            pub_sock.close()

    ctx.term()

    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
