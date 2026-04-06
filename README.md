# CUP DAQ Run Control System (DAQRC)

This project is a Data Acquisition (DAQ) control and real-time monitoring system for the CUP experiment. It utilizes a decoupled architecture, separating the PyQt5-based GUI frontend (`rc.py`) from a background monitoring daemon (`daq_monitor.py`) that handles SQLite database logging and hardware status polling.

## 🌟 Core Architecture (Frontend-Backend Separation)

The system is split into two main components to ensure that logging and statistics collection continue even if the GUI is closed or the network connection is interrupted.

*   **`rc.py` (Frontend / Commander)**
    *   **GUI Control**: Handles user inputs for shift details, run types, and configuration files.
    *   **Command Execution**: Sends remote execution commands (Boot, Start, End) to DAQ devices (TCB, MERGER, ADC) via SSH.
    *   **IPC Communication**: Communicates with the background monitor via ZeroMQ (ZMQ) on port 7030 to fetch real-time statistics.
    *   **Watchdog Function**: Automatically detects if the monitor daemon (`daq_monitor.py`) is running; if not, it launches it in the background using `nohup`.

*   **`daq_monitor.py` (Backend / Scribe)**
    *   **Daemon Operation**: Runs 24/7 as a background process, independent of the GUI.
    *   **Hardware Polling**: Queries DAQ device status and event statistics every second (1Hz).
    *   **Database Logging**: Safely updates `runcatalog.db` in real-time, preventing SQLite DB locks by centralizing all writes.
    *   **API Server**: Provides a ZMQ-based REP interface for the frontend to query stats and sync run details.
    *   **Auto-termination**: Terminates itself if the DAQ remains in a DOWN state for more than 1 hour (3600 seconds) to save resources.

## ✨ Key Features

1.  **Uninterruptible Logging**: Database updates and statistical data collection continue in the background even if the user accidentally closes the RC window.
2.  **ZMQ-Based Watchdog**: `rc.py` uses a ZMQ "Ping" to determine if the monitor is alive, automatically reviving it if necessary.
3.  **Real-Time Statistics**: Frontend displays live event counts, trigger rates (Hz), and DAQ times by polling the monitor daemon.
4.  **Safe Termination**: Prevents accidental exit while a run is active and ensures all remote processes are notified upon a clean exit.

## ⚙️ Configuration (`onlconsts.py`)

Before running the system, you must configure your environment. An example file is provided as `onlconsts.py.example`.

**Step 1. Create the configuration file**
```bash
cp onlconsts.py.example onlconsts.py
```

**Step 2. Edit `onlconsts.py`**
Modify the following key variables:
*   `kDAQSERVER_IP`: IP address of the main DAQ server.
*   `kONLDAQ_DIR`: Path to the DAQ software installation on the server.
*   `kRAWDATA_DIR`: Path where output data (RAW, LOG, CONFIG) is stored.
*   `kRUNCATALOGDBFILE`: Path to the SQLite `runcatalog.db` file.
*   `kMONITORPORT`: Set to `7030` (used for frontend-backend communication).

> 🛑 **Warning**: Do not modify values below the `# Do not modify from here!!!` line, as these are critical internal status flags and command definitions.

## 🚀 Usage

### 1. System Startup
Run the main GUI script. It will automatically start the background monitor if it is not already running.
```bash
python rc.py
```

### 2. Manual Termination
If you need to kill the background monitor manually:
```bash
pkill -f 'daq_monitor.py'
```

## 📂 File Structure

*   `rc.py`: Main Run Control GUI (Frontend).
*   `daq_monitor.py`: Background monitoring and DB logging daemon (Backend).
*   `onlthreads.py`: Threading helpers for non-blocking GUI polling.
*   `onlutils.py`: Shared utilities for ZMQ, SSH, and logging.
*   `onlconsts.py`: System constants and configurations.
*   `rcui.py` / `rc.ui`: PyQt5 UI layout definitions.
*   `create_runcatalog_db.py`: Script to initialize the SQLite database schema.
*   `/tmp/cupdaq_rc.log`: Frontend logs.
*   `/tmp/cupdaq_monitor_daemon.log`: Backend monitor logs.