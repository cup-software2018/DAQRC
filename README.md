# CUP DAQ Run Control System (DAQRC)

Data Acquisition (DAQ) control and real-time monitoring system for the CUP experiment. Separates the PySide6 GUI frontend (`rc.py`) from a background monitoring server (`daqmon_server.py`) that handles SQLite database logging and hardware status polling.

## Core Architecture

```
rc.py  ──── REQ ────►  daqmon_server.py (port 7030)   BOOT_RUN / TAG_GOODRUN / SYNC_LATEST
       ◄─── SUB ────   daqmon_server.py (port 7031)   1Hz telemetry (RunState + RunStats)
       ──── REQ ────►  TCB (port 7100)                CONFIG / START / END / EXIT

daqmon_server.py ────►  TCB          kQUERYDAQSTATUS, kQUERYRUNINFO  (1Hz)
                 ────►  DAQ modules  kQUERYTRGINFO                   (1Hz)
                 ────►  runcatalog.db                                 (10s)

daqmon.py  ──── REQ ────►  daqmon_server.py   GET_SERVER_HEALTH
           ◄─── SUB ────   daqmon_server.py   latest PUB snapshot
           stdout ────►  Telegraf (InfluxDB line protocol)
```

### `rc.py` — Frontend / Commander

- GUI controls for shift, run type, and config file selection
- Sends Boot/Config/Start/End/Exit commands to TCB via SSH + ZMQ
- Subscribes to `daqmon_server` PUB socket for live state and statistics
- Sends `NOTIFY_DAQ_STARTED` after boot so the monitor reconnects immediately
- Watchdog: auto-starts `daqmon_server` with `--daemon` if not running

### `daqmon_server.py` — Backend / Monitor Server

- Runs as a daemon, independent of the GUI
- Polls TCB for DAQ state (`kQUERYDAQSTATUS`, `kQUERYRUNINFO`) at 1Hz
- Polls each DAQ module for trigger statistics (`kQUERYTRGINFO`) at 1Hz
- Broadcasts telemetry via ZMQ PUB socket at 1Hz (RunState, RunStats, RunNumber, etc.)
- Writes stats to `runcatalog.db` every `kSTATSREPORTINTERVAL` seconds
- Handles commands over ZMQ REP socket: `BOOT_RUN`, `TAG_GOODRUN`, `SYNC_LATEST`, `PING`, `NOTIFY_DAQ_STARTED`
- Detects run end (kPROCENDED / kERROR) and closes module sockets cleanly
- Reconnects automatically when TCB restarts after a new boot

### `daqmon.py` — InfluxDB Reporter (Telegraf exec plugin)

## Configuration (`onlconsts.py`)

```bash
cp onlconsts.py.example onlconsts.py
```

Key variables to set:

| Variable | Description |
|---|---|
| `kDAQSERVER_IP` | TCB IP address |
| `kDAQSERVER_PORT` | TCB ZMQ port (default 7100) |
| `kONLDAQ_DIR` | DAQ software directory on the server |
| `kRAWDATA_DIR` | Output data directory (RAW, LOG, CONFIG) |
| `kRUNCATALOGDBFILE` | Path to `runcatalog.db` |
| `kDAQMON_IP` | daqmon_server host (default localhost) |
| `kDAQMON_CMD_PORT` | Command interface port (default 7030) |
| `kDAQMON_PUB_PORT` | Telemetry broadcast port (default 7031) |
| `kSTATSREPORTINTERVAL` | DB write interval in seconds (default 10) |

## Usage

### System Startup
`daqmon_server` must be running before `rc.py` is launched.

```bash
# Step 1: Start the monitor server
python daqmon_server.py --daemon

# Step 2: Launch the Run Control GUI
python rc.py
```

If `daqmon_server` is not running when `rc.py` starts, RC will show a critical error and exit.

### Manual daqmon_server control
```bash
# Start in foreground (for testing)
python daqmon_server.py

# Start as daemon
python daqmon_server.py --daemon

# Stop
kill $(cat /tmp/cupdaq_daqmon_server.pid)
```

### InfluxDB reporter (one-shot test)
```bash
python daqmon.py
python daqmon.py --debug
```

## File Structure

| File | Description |
|---|---|
| `rc.py` | Run Control GUI (Frontend) |
| `daqmon_server.py` | Monitor server daemon (Backend) |
| `daqmon.py` | Telegraf exec plugin for InfluxDB |
| `onlutils.py` | Shared utilities: ZMQ, SSH, logging |
| `onlconsts.py` | System constants and configuration |
| `rcui.py` / `rc.ui` | PySide6 UI layout |
| `create_runcatalog_db.py` | Initialize SQLite run catalog schema |
| `killrun.py` | Emergency run termination script |

**Log files:**
- `/tmp/cupdaq_rc.log` — rc.py
- `/tmp/cupdaq_daqmon_server.log` — daqmon_server.py
- `/tmp/cupdaq_daqmon_server.pid` — daqmon_server PID file

## DAQ State Machine

| State | Value | Description |
|---|---|---|
| Down | 0 | No DAQ activity |
| Booted | 1 | TCB and DAQ processes started |
| Configured | 2 | Configuration loaded |
| Running | 3 | Run in progress |
| RunEnding | 4 | Stop signal sent |
| RunEnded | 5 | Data collection finished |
| ProcEnded | 6 | All processing complete, TCB shutting down |
| Error | 8 | Error state |

## Troubleshooting

**`qt.qpa.plugin: Could not load the Qt platform plugin "xcb"`**
```bash
sudo dnf install xcb-util-cursor
```

**`ModuleNotFoundError: No module named 'onlconsts'`**
```bash
cp onlconsts.py.example onlconsts.py
# Edit onlconsts.py with your site settings
```

**daqmon_server not responding**
```bash
# Check logs
cat /tmp/cupdaq_daqmon_server.log

# Force restart
kill $(cat /tmp/cupdaq_daqmon_server.pid) 2>/dev/null
python daqmon_server.py --daemon
```
