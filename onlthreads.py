import time
from PyQt5.QtCore import QThread, pyqtSignal
import onlutils
import onlconsts


class DAQStatePollerThread(QThread):
    """
    Background thread to poll DAQ state continuously.
    Prevents the PyQt GUI from freezing during ZeroMQ timeouts.
    """
    # Signal to emit state back to the Main GUI thread safely
    # Signature: (run_state_integer, reply_dictionary)
    state_received = pyqtSignal(int, dict)

    def __init__(self, endpoint, parent=None):
        super().__init__(parent)
        self.endpoint = endpoint
        self.active = True
        self.sock = None

    def run(self):
        """
        Main loop of the background thread.
        Never manipulate GUI elements directly from here!
        """
        while self.active:
            if self.sock is None:
                self.sock = onlutils.get_connection(self.endpoint)

            # Polling DAQ status with a generous 1000ms timeout
            # It will not freeze the GUI because it runs in this background thread.
            reply = onlutils.send_daq_cmd(
                self.sock, onlconsts.kQUERYDAQSTATUS, timeout_ms=1000)

            if reply is None or reply.get("status") != "ok":
                # Lazy Pirate Pattern: Destroy and recreate socket on timeout
                if self.sock:
                    self.sock.close()
                    self.sock = None
                current_state = onlconsts.kDOWN
                reply_dict = {}
            else:
                current_state = reply.get("run_status", onlconsts.kDOWN)
                reply_dict = reply

            # Emit the result safely to the Main Thread
            self.state_received.emit(current_state, reply_dict)

            # Wait 500ms before the next poll
            self.msleep(500)

    def stop(self):
        """Gracefully terminate the polling thread."""
        self.active = False
        self.wait()
