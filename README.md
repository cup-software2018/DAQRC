# CUP DAQ Run Control System

This project is a Data Acquisition (DAQ) control and real-time monitoring system for the CUP experiment. It adopts a perfectly decoupled MVC architecture, separating the PyQt5-based GUI frontend (`rc.py`) from the background daemon (`logger.py`) dedicated to SQLite database logging.

## 🌟 Core Architecture (Frontend-Backend Separation)

To overcome the limitations of the previous single-script approach (where logging halted when the GUI was closed, and SQLite DB Lock errors occurred frequently), the control and recording responsibilities have been strictly separated.

* **`rc.py` (Frontend / Commander)**
    * Handles user inputs (Shift, Run Type, etc.) and renders the GUI (status, statistics).
    * Sends execution commands (Boot, Start, End) to DAQ devices (TCB, MERGER, ADC).
    * Instructs the background logger to record data into the DB via local communication (Port 9999).
    * **Watchdog Function:** Upon execution, it monitors the background logger daemon's status via a "Port Ping" and automatically revives it if it is dead.

* **`logger.py` (Backend / Scribe)**
    * Operates 24/7 as a background daemon completely independent of the GUI using `nohup`.
    * Queries DAQ device status and event statistics every second (1Hz) to update `runcatalog.db` in real-time, fundamentally preventing SQLite DB Locks.
    * Logs state changes (RUNNING, DOWN, etc.) along with timestamps to `/tmp/cup_logger.log`.
    * **Auto-termination:** If the DAQ remains in the DOWN state for over 1 hour (3600 seconds), it terminates itself to free up system resources.

## ✨ Key Features & Resolved Issues

1. **Uninterruptible Logging:** Even if the user accidentally closes the RC window or the laptop disconnects, database updates and statistical data collection continue safely in the background.
2. **Port Ping Watchdog Logic:** To prevent the false-positive process recognition issue of the `pgrep` command, `rc.py` directly attempts a socket connection to port 9999 to determine the logger's survival with 100% certainty.
3. **Ghost Data Prevention:** When booting a new run, both the frontend (GUI) and backend (shared memory) perfectly initialize previous run statistics and time (`EndTime`) to `0`, ensuring no residual data is displayed.
4. **Safe Exit Prevention:** If the user attempts to exit the program while a run is active (e.g., RUNNING state), a warning pop-up is displayed to prevent abnormal termination.

## ⚙️ Configuration (`onlconsts.py`)

Before running the system for the first time, you must configure the environment variables. An example configuration file is provided as `onlconsts.py.example`. 

**Step 1. Create the actual configuration file**
Copy or rename the example file to `onlconsts.py`:
```bash
cp onlconsts.py.example onlconsts.py
```

**Step 2. Edit `onlconsts.py`**
Open the newly created `onlconsts.py` with your preferred text editor and fill in the blanks according to your server environment. 

Below is an explanation of the key variables you need to modify:

* **`kDAQSERVER_IP` & `kDAQSERVER_PORT`**: 
  The IP address and port number of the main DAQ server (the machine where the TCB server process will be running).
* **`kISREMOTEDAQ`**: 
  Set to `True` if you are running this `rc.py` GUI on a different machine than the DAQ server. Set to `False` if they are on the same machine.
* **`kONLDAQ_DIR`**: 
  The absolute path to the DAQ software installation directory on the DAQ server.
* **`kRAWDATA_DIR`**: 
  The absolute path where the output files will be saved on the DAQ server. 
  *(⚠️ **Important:** This directory MUST contain three subdirectories named `RAW`, `LOG`, and `CONFIG` before you start booting runs.)*
* **`kRUNCATALOGDBFILE`**: 
  The absolute path to the SQLite database file (e.g., `/path/to/runcatalog.db`) on the machine running `rc.py`.
* **`kDEFAULTCONFIGDIR`**: 
  The default directory path where your YAML configuration files are stored. This is the path that will open when you click the "Load" button in the GUI.
* **`kRUNTYPELIST`**: 
  You can append additional run types here if needed (e.g., `['', 'physics', 'calibration', 'test', 'pedestal']`).
* **`kOUTPUTFILEFORMAT`**: 
  Choose the output data format. Set it to either `'hdf5'` or `'root'`.

> 🛑 **Warning:** Do NOT modify any variables or constants below the line `# Do not modify from here!!!`. Those are essential internal status flags, communication commands, and port configurations (like `kLOGGERPORT = 9999`) used by the frontend-backend IPC architecture.

## 🚀 Usage

### 1. System Startup
Simply run `rc.py` in the terminal, and the system will automatically set up everything.

```bash
python rc.py
```

> **Note:** The moment `rc.py` starts, it internally checks port 9999. If the logger is not responding, it automatically launches the daemon in the background using the `nohup python logger.py &` command.

### 2. Manual Termination of the Background Logger
If you need to completely kill the logger daemon for maintenance or to resolve port conflicts, use the command below. (**Make sure to close the `rc.py` window first.**)

```bash
pkill -f 'logger.py'
```

## 📂 File Structure

* `rc.py`: Main Run Control GUI executable (Frontend).
* `logger.py`: Background monitoring and DB logging daemon (Backend).
* `rcui.py`: PyQt5 UI layout class file.
* `onlconsts.py`: Definitions for port numbers, directory paths, and DAQ state constants.
* `onlutils.py`: Utility functions for socket communication, process control, and time formatting.
* `runcatalog.db`: SQLite database where run statistics and configurations are recorded.
* `/tmp/cup_logger.log`: Real-time activity and error log of the logger daemon.

## 🛠 Troubleshooting

**Q. I get an `[Errno 111] Connection refused` error.**
* **Cause:** It is highly likely that the `logger.py` daemon crashed due to an error during startup before it could open port 9999.
* **Solution:** Open the `/tmp/cup_logger.log` file to identify the exact cause of the error (e.g., `ModuleNotFoundError`, `SyntaxError`).

**Q. I get an `[Errno 98] Address already in use` error.**
* **Cause:** A previously abnormally terminated logger process (zombie) is still holding port 9999.
* **Solution:** Force kill the zombie process using the `pkill -9 -f 'logger.py'` command, and then run `rc.py` again.