import time
from PySide6.QtCore import QThread, Signal
import onlutils
import onlconsts


class DAQStatePollerThread(QThread):
    """
    Background thread to poll DAQ state continuously.
    Prevents the PySide6 UI from freezing during ZeroMQ timeouts.
    """
    state_received = Signal(int, object)

    def __init__(self, endpoint, parent=None):
        super().__init__(parent)
        self.endpoint = endpoint
        self.active = True
        self.sock = None

    def run(self):
        while self.active:
            if self.sock is None:
                self.sock = onlutils.get_connection(self.endpoint)

            reply = onlutils.send_daq_cmd(
                self.sock, onlconsts.kQUERYDAQSTATUS, timeout_ms=1000)

            if reply is None or reply.get("status") != "ok":
                if self.sock:
                    self.sock.close()
                    self.sock = None
                current_state = onlconsts.kDOWN
                reply_dict = {}
            else:
                current_state = reply.get("run_status", onlconsts.kDOWN)
                reply_dict = reply

            self.state_received.emit(current_state, reply_dict)
            self.msleep(500)

    def stop(self):
        self.active = False
        if self.sock:
            self.sock.close()
            self.sock = None
        self.wait()


class MonitorPollerThread(QThread):
    """
    Background thread to poll monitor daemon for run stats.
    Prevents GUI freeze during ZeroMQ timeouts on monitor communication.
    Note: daq_monitor uses {"cmd": ...} format, not {"command": ...}.
    """
    stats_received = Signal(object)

    def __init__(self, monitor_endpoint, parent=None):
        super().__init__(parent)
        self.monitor_endpoint = monitor_endpoint
        self.active = True
        self.sock = None
        self.polling_enabled = False

    def run(self):
        while self.active:
            if self.polling_enabled:
                if self.sock is None:
                    self.sock = onlutils.get_connection(self.monitor_endpoint)

                try:
                    self.sock.send_json({"cmd": "GET_STATS"})

                    if self.sock.poll(timeout=1000) == 0:
                        self.sock.close()
                        self.sock = None
                        self.stats_received.emit({})
                    else:
                        reply = self.sock.recv_json()
                        self.stats_received.emit(reply)

                except Exception as e:
                    if self.sock:
                        self.sock.close()
                        self.sock = None
                    self.stats_received.emit({})

            self.msleep(1000)

    def enable(self):
        self.polling_enabled = True

    def disable(self):
        self.polling_enabled = False

    def stop(self):
        self.active = False
        if self.sock:
            self.sock.close()
            self.sock = None
        self.wait()
