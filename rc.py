import os
import sys
import time
import yaml
import zmq
import logging
from datetime import datetime
from PySide6.QtCore import Signal, Slot, Qt, QObject, QTimer, QThread
from PySide6.QtGui import QFont, QColor, QScreen, QGuiApplication
from PySide6.QtWidgets import QMainWindow, QApplication, QVBoxLayout, QHBoxLayout, QMessageBox, QFileDialog, QTextEdit
from rcui import Ui_MainWindow

import onlconsts
import onlutils


class MonitorPollerThread(QThread):
    """
    Subscribes to daqmon_server's PUB socket (1Hz).
    Receives RunState + RunStats in a single payload.
    """
    stats_received = Signal(object)

    def __init__(self, pub_endpoint, parent=None):
        super().__init__(parent)
        self.pub_endpoint = pub_endpoint
        self.active = True
        self.sock = None

    def _connect(self):
        sock = onlutils._ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.connect(self.pub_endpoint)
        return sock

    def run(self):
        while self.active:
            if self.sock is None:
                self.sock = self._connect()
            try:
                if self.sock.poll(timeout=1500) != 0:
                    reply = self.sock.recv_json()
                    self.stats_received.emit(reply)
                else:
                    self.stats_received.emit({})
            except Exception:
                if self.sock:
                    self.sock.close()
                    self.sock = None
                self.stats_received.emit({})

    def stop(self):
        self.active = False
        if self.sock:
            self.sock.close()
            self.sock = None
        self.wait()

log = onlutils.get_logger("RC", "/tmp/cupdaq_rc.log")


def sortfunc(e):
    return e[0]


class LogSignaller(QObject):
    new_log = Signal(str, str)


class GuiLogHandler(logging.Handler):
    def __init__(self, signaller):
        super().__init__()
        self.signaller = signaller
        self.setFormatter(logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record):
        msg = self.format(record)
        self.signaller.new_log.emit(msg, record.levelname)


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setupUi(self)

        self.Bindir = onlconsts.kONLDAQ_DIR + '/bin/'

        self.RunNumber = 0
        self.Shift = None
        self.RunType = None
        self.RunDesc = None
        self.ConfigFile = None

        self.RunState = onlconsts.kDOWN
        self.RunSocket = None
        self.OnThisRC = False

        self.SubRunNumber = 0
        self.StartTime = 0
        self.EndTime = 0
        self.MonNames = []
        self.RunStats = {}

        self._is_asking_goodrun = False

        self.center()
        self.RunTypeConfig.addItems(onlconsts.kRUNTYPELIST)

        font = QFont()
        font.setPointSize(14)
        self.RunStatsTextEdit = QTextEdit()
        self.RunStatsTextEdit.setFont(font)
        self.RunStatsTextEdit.setEnabled(False)

        layout = QVBoxLayout()
        layout.addWidget(self.RunStatsTextEdit)
        self.RunStatsBox.setLayout(layout)

        self.ConfigFileButton.clicked.connect(self.load_config)
        self.BootButton.clicked.connect(self.boot_run)
        self.ConfigButton.clicked.connect(self.config_run)
        self.StartButton.clicked.connect(self.start_run)
        self.EndButton.clicked.connect(self.end_run)
        self.ExitButton.clicked.connect(self.exit_run)
        self.ExitButton.setEnabled(False)

        self.daq_endpoint = onlconsts.kDAQSERVER_ADDR
        self.MonitorSocket = None

        self.log_signaller = LogSignaller()
        self.log_signaller.new_log.connect(self.append_log)

        self.gui_handler = GuiLogHandler(self.log_signaller)
        self.gui_handler.setLevel(logging.INFO)

        log.addHandler(self.gui_handler)
        onlutils.log.addHandler(self.gui_handler)

        log.info("Starting Run Control GUI...")

        self.check_and_start_monitor()

        self.monitor_poller_thread = MonitorPollerThread(
            onlconsts.kDAQMON_PUB_ADDR)
        self.monitor_poller_thread.stats_received.connect(
            self.on_stats_received)
        self.monitor_poller_thread.start()

    def closeEvent(self, event):
        log.info("Closing RC GUI and stopping background threads...")

        if hasattr(self, 'monitor_poller_thread'):
            self.monitor_poller_thread.stop()

        if self.RunSocket:
            self.RunSocket.close()
        if self.MonitorSocket:
            self.MonitorSocket.close()

        event.accept()

    def _get_run_socket(self):
        if self.RunSocket is None:
            self.RunSocket = onlutils.get_connection(self.daq_endpoint)
        return self.RunSocket

    @Slot(str, str)
    def append_log(self, msg, level):
        color = "black"
        if level in ["ERROR", "CRITICAL"]:
            color = "red"
        elif level == "WARNING":
            color = "#FF8C00"
        elif level == "DEBUG":
            color = "gray"
        elif level == "INFO":
            color = "blue"

        html_msg = f'<span style="color:{color};">{msg}</span>'
        self.LogViewer.append(html_msg)

    def check_and_start_monitor(self):
        reply = self.send_monitor_cmd({"cmd": "PING"})

        if reply:
            log.info("Monitor daemon running on port %d.",
                     onlconsts.kDAQMON_CMD_PORT)
            return

        msg = (f"daqmon_server is not running on port {onlconsts.kDAQMON_CMD_PORT}.\n\n"
               f"Please start it before launching RC:\n\n"
               f"  python daqmon_server.py --daemon")
        log.critical("Monitor daemon not found. %s", msg)
        self.msgbox_error(msg)
        sys.exit(1)

    def send_monitor_cmd(self, req_data):
        if self.MonitorSocket is None:
            self.MonitorSocket = onlutils.get_connection(
                onlconsts.kDAQMON_CMD_ADDR)

        cmd = req_data.get("cmd")
        reply = onlutils.send_daq_cmd(self.MonitorSocket, cmd, req_data)

        if reply is None:
            log.error("Monitor timeout on command '%s'. Resetting socket.", cmd)
            self.MonitorSocket.close()
            self.MonitorSocket = None
            return {}

        return reply

    def load_config(self):
        result = QFileDialog.getOpenFileName(
            self, 'Load Configuration File', onlconsts.kDEFAULTCONFIGDIR,
            'Configuration File (*.yml);;All Files (*)')
        self.ConfigFile = str(result[0])
        if self.ConfigFile:
            configfile = os.path.basename(self.ConfigFile)
            msg = '<font color="blue"><b>%s</b></font> loaded' % configfile
            self.ConfigFileLabel.setText(msg)
            log.info("Loaded configuration file: %s", self.ConfigFile)

    def boot_run(self):
        log.info("Initiating BOOT_RUN sequence...")

        self.Shift = str(self.ShiftConfig.text())
        if not self.Shift:
            return self.msgbox_error('Shift crew missing!')

        if not self.ConfigFile:
            return self.msgbox_error('Run configuration file missing!')

        self.RunType = str(self.RunTypeConfig.currentText())
        if not self.RunType:
            return self.msgbox_error('Run type missing!')

        self.RunDesc = str(self.RunDescConfig.toPlainText())
        configfile_basename = os.path.basename(self.ConfigFile)

        msg = f'<pre>Shift      : {self.Shift}<br>Run type   : {self.RunType}<br>Config file: {configfile_basename}\n<br><b>Do you want to boot this run?</b></pre>'
        reply = self.msgbox_question(msg)
        if reply.clickedButton() is reply.button(QMessageBox.No):
            log.info("BOOT_RUN cancelled by user.")
            return

        self.BootButton.setEnabled(False)
        self.BootButton.setStyleSheet("background-color: yellow")

        self.RunStats.clear()
        self.MonNames.clear()
        self.SubRunNumber = 0
        self.StartTime = 0
        self.EndTime = 0
        self.RunStatsTextEdit.clear()
        self.LogViewer.clear()
        self._is_asking_goodrun = False

        req = {
            "cmd": "BOOT_RUN",
            "shift": self.Shift,
            "runtype": self.RunType,
            "rundesc": self.RunDesc,
            "config": self.ConfigFile
        }

        resp = self.send_monitor_cmd(req)
        if "run_num" in resp:
            self.RunNumber = resp["run_num"]
            log.info("BOOT_RUN successful. Assigned Run Number: %06d",
                     self.RunNumber)
        else:
            log.error("Monitor failed to boot run (DB error). Response: %s", resp)
            return self.msgbox_error("Monitor failed to boot run (DB error).")

        run_number = self.RunNumber
        config_file = self.ConfigFile
        onldaq_dir = onlconsts.kONLDAQ_DIR
        rawdata_dir = onlconsts.kRAWDATA_DIR

        target_config = '%s/CONFIG/%06d.yml' % (rawdata_dir, run_number)
        merged_local_config = '/tmp/amore_run_%06d_merged.yml' % run_number

        daqlist = []
        try:
            def merge_dicts(base, update):
                for key, val in update.items():
                    if key in base:
                        if isinstance(base[key], dict) and isinstance(val, dict):
                            merge_dicts(base[key], val)
                        elif isinstance(base[key], list) and isinstance(val, list):
                            base[key].extend(val)
                        else:
                            base[key] = val
                    else:
                        base[key] = val
                return base

            with open(config_file, 'r', encoding='utf-8') as fp:
                main_config = yaml.safe_load(fp) or {}

            if 'Include' in main_config and isinstance(main_config['Include'], list):
                config_dir = os.path.dirname(os.path.abspath(config_file))
                for inc_file in main_config['Include']:
                    if not os.path.isabs(inc_file):
                        inc_file = os.path.join(config_dir, inc_file)
                    with open(inc_file, 'r', encoding='utf-8') as inc_fp:
                        inc_data = yaml.safe_load(inc_fp) or {}
                        merge_dicts(main_config, inc_data)
                del main_config['Include']

            config_data = main_config

            with open(merged_local_config, 'w', encoding='utf-8') as out_fp:
                yaml.dump(main_config, out_fp,
                          default_flow_style=None, sort_keys=False)

            cmd = 'scp -q %s %s:%s' % (merged_local_config,
                                       onlconsts.kDAQSERVER_IP, target_config)
            os.system(cmd)
            log.info("Merged config SCP copied to target: %s", target_config)

            if os.path.exists(merged_local_config):
                os.remove(merged_local_config)

            for item in config_data.get('DAQ', []):
                dnum = int(item.get('ID', 0))
                name = str(item.get('NAME', ''))
                ip = str(item.get('IP', ''))
                port = int(item.get('PORT', 0))

                if 'TCB' in name:
                    mode = 0
                elif 'MERGER' in name:
                    mode = 2
                else:
                    mode = 1
                daqlist.append((mode, dnum, name, ip, port))

        except Exception as e:
            log.error("Failed to load or merge YAML config: %s",
                      e, exc_info=True)
            return self.msgbox_error('Failed to load or merge YAML config:\n%s' % e)

        fformat = '-b' if getattr(onlconsts,
                                  'kOUTPUTFILEFORMAT', 'hdf5') == 'hdf5' else '-a'
        optlist = []
        for daq in daqlist:
            mode, dnum, name, ip, port = daq
            topt = name[0].lower()

            if mode == 0:
                sopt = '-t -r %d -n %s ' % (run_number, name)
                dopt = '-d 0 -r %d -c %s -p %d ' % (
                    run_number, target_config, onlconsts.kOUTPUTSPLITTIME)
            elif mode == 2:
                sopt = '-m -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d -q %d ' % (
                    topt, dnum, target_config, run_number, onlconsts.kSTATSREPORTINTERVAL)
            else:
                sopt = '-d -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d -q %d ' % (
                    topt, dnum, target_config, run_number, onlconsts.kSTATSREPORTINTERVAL)
                adc = name[0:4]
                for dd in daqlist:
                    if dd[0] == 2 and adc in dd[2]:
                        dopt += '-x '
                        break

            dopt += ' ' + fformat + ' '
            optlist.append((mode, sopt, dopt, ip, port))

        optlist.sort(key=sortfunc)
        optlist.append(optlist.pop(0))

        onldaqdiropt = '--onldaqdir=%s ' % onldaq_dir
        rawdatadiropt = '--rawdatadir=%s ' % rawdata_dir

        for daq in optlist:
            mode = daq[0]
            if mode > 0:
                cmd = self.Bindir + '%s %s%s -o "%s"' % (
                    onlconsts.kEXESCRIPT, daq[1], onldaqdiropt + rawdatadiropt, daq[2])
                log.info("Executing remote DAQ command via SSH on %s", daq[3])
                success, output = onlutils.run_ssh_cmd(cmd, daq[3])
                if not success:
                    log.error("Execution failed on %s: %s", daq[3], output)

        time.sleep(1)

        tcb = optlist[-1]
        cmd = self.Bindir + '%s %s%s -o "%s"' % (
            onlconsts.kEXESCRIPT, tcb[1], onldaqdiropt + rawdatadiropt, tcb[2])
        log.info("Executing TCB remote command via SSH on %s", tcb[3])
        success, output = onlutils.run_ssh_cmd(cmd, tcb[3])
        if not success:
            log.error("TCB Execution failed on %s: %s", tcb[3], output)

        self.OnThisRC = True
        self.StartTime = 0
        self.EndTime = 0

        # Notify daqmon_server that TCB and DAQ ZMQ servers are now running
        self.send_monitor_cmd({"cmd": "NOTIFY_DAQ_STARTED"})
        log.info("Boot sequence completed.")

    def config_run(self):
        log.info("User requested CONFIG_RUN.")
        reply = onlutils.send_daq_cmd(
            self._get_run_socket(), onlconsts.kCONFIGRUN, timeout_ms=2000)

        if reply is None and self.RunSocket:
            self.RunSocket.close()
            self.RunSocket = None

        self.ConfigButton.setStyleSheet("background-color: yellow")

    def start_run(self):
        log.info("User requested START_RUN.")
        reply = onlutils.send_daq_cmd(
            self._get_run_socket(), onlconsts.kSTARTRUN, timeout_ms=1000)

        if reply is None and self.RunSocket:
            self.RunSocket.close()
            self.RunSocket = None

        self.StartButton.setStyleSheet("background-color: yellow")

    def end_run(self):
        msg = '<pre><b>Run %06d running now.<br>Do you want to quit this run?</b></pre>' % self.RunNumber
        reply = self.msgbox_question(msg)
        if reply.clickedButton() is reply.button(QMessageBox.No):
            return

        log.info("User requested END_RUN.")
        reply = onlutils.send_daq_cmd(
            self._get_run_socket(), onlconsts.kENDRUN, timeout_ms=1000)

        if reply is None and self.RunSocket:
            self.RunSocket.close()
            self.RunSocket = None

        self.EndButton.setStyleSheet("background-color: yellow")

    def exit_run(self):
        is_safe_state = (
            self.RunState == onlconsts.kDOWN or
            onlutils.check_state(self.RunState, onlconsts.kPROCENDED) or
            onlutils.check_state(self.RunState, onlconsts.kRUNENDED)
        )
        if not is_safe_state:
            daqstate = onlutils.get_state(self.RunState)
            if onlutils.check_error(self.RunState):
                daqstate = onlconsts.kERROR
            state_str = onlconsts.kDAQSTATE[daqstate] if daqstate < len(
                onlconsts.kDAQSTATE) else "UNKNOWN"
            msg = '<pre><b>Run %06d is currently active (State: %s).<br>Are you sure to FORCE exit without ending properly?</b></pre>' % (
                self.RunNumber, state_str)
            reply = self.msgbox_question(msg)
            if reply.clickedButton() is reply.button(QMessageBox.No):
                return
            log.warning("User requested FORCE EXIT.")

        if self._get_run_socket():
            reply = onlutils.send_daq_cmd(
                self.RunSocket, onlconsts.kEXIT, timeout_ms=1000)
            if reply is None:
                self.RunSocket.close()
                self.RunSocket = None

    def on_stats_received(self, stats):
        if not stats:
            self.update_run_stats_display()
            return

        # RunState is included in the daqmon_server PUB payload
        new_state = stats.get("RunState", onlconsts.kDOWN)
        old_state = self.RunState
        self.RunState = new_state

        if old_state != self.RunState:
            raw_val = onlutils.get_state(self.RunState)
            state_str = onlconsts.kDAQSTATE[raw_val] if raw_val < len(
                onlconsts.kDAQSTATE) else "UNKNOWN"
            log.info("DAQ State changed: %s (Raw value: %s)",
                     state_str, self.RunState)
            if onlutils.check_error(self.RunState):
                log.error("DAQ has entered ERROR state!")

        self.set_runstate(self.RunState)

        self.RunStats = stats.get("RunStats", {})
        self.SubRunNumber = stats.get("SubRunNumber", 0)
        self.StartTime = stats.get("StartTime", 0)
        self.MonNames = stats.get("MonNames", [])
        self.EndTime = stats.get("EndTime", 0)

        if not self.OnThisRC and self.RunState != onlconsts.kDOWN and old_state != self.RunState:
            resp = self.send_monitor_cmd({"cmd": "SYNC_LATEST"})
            if resp and "runnum" in resp:
                self.RunNumber = resp["runnum"]
                self.Shift = resp["shift"]
                self.RunType = resp["runtype"]
                self.RunDesc = resp["rundesc"]
                self.ConfigFile = resp["config"]

                self.ShiftConfig.setText(self.Shift)
                index = self.RunTypeConfig.findText(
                    self.RunType, Qt.MatchFixedString)
                self.RunTypeConfig.setCurrentIndex(index)
                self.RunDescConfig.setText(self.RunDesc)

                configfile = os.path.basename(self.ConfigFile)
                self.ConfigFileLabel.setText(
                    '<font color="blue"><b>%s</b></font> loaded' % configfile)
                self.OnThisRC = True
                log.info(
                    "Synced latest run details from monitor: RunNum %06d", self.RunNumber)

        if onlutils.check_state(self.RunState, onlconsts.kRUNENDED):
            if not self._is_asking_goodrun:
                self._is_asking_goodrun = True

                onlbit = 0
                msg = 'Tag run %06d as GOODRUN?' % self.RunNumber
                reply = self.msgbox_question(msg)
                if reply.clickedButton() is reply.button(QMessageBox.Yes):
                    onlbit = 1
                    log.info("Run %06d tagged as GOODRUN.", self.RunNumber)
                else:
                    log.info("Run %06d NOT tagged as GOODRUN.", self.RunNumber)

                stime_str = datetime.fromtimestamp(self.StartTime).strftime(
                    "%Y-%m-%d %H:%M:%S") if self.StartTime else ""
                etime_str = datetime.fromtimestamp(int(self.EndTime)).strftime(
                    "%Y-%m-%d %H:%M:%S") if self.EndTime else ""

                req = {
                    "cmd": "TAG_GOODRUN",
                    "run_num": self.RunNumber,
                    "onlbit": onlbit,
                    "stime_str": stime_str,
                    "etime_str": etime_str,
                    "final_stats": self.RunStats
                }
                self.send_monitor_cmd(req)

        self.update_run_stats_display()

    def update_run_stats_display(self):
        """Update the RunStats text display. Called from on_stats_received."""
        curtime = time.strftime("%Y-%m-%d %H:%M:%S")
        stime = datetime.fromtimestamp(self.StartTime).strftime(
            "%Y-%m-%d %H:%M:%S") if self.StartTime > 0 else ''
        etime = datetime.fromtimestamp(int(self.EndTime)).strftime(
            "%Y-%m-%d %H:%M:%S") if self.EndTime > 0 else ''

        daqtime = ''
        if self.MonNames and self.MonNames[0] in self.RunStats:
            daqtime = onlutils.HMSFormatter(
                self.RunStats[self.MonNames[0]].get('t', 0))

        daqstate = onlutils.get_state(self.RunState)
        if onlutils.check_error(self.RunState):
            daqstate = onlconsts.kERROR

        summary = '<pre><font color="blue"><br>'
        summary += '  <b>Current Time</b>: %s<br><br>' % curtime
        summary += '    <b>Run Number</b>: %06d/%d<br>' % (
            self.RunNumber, self.SubRunNumber)
        summary += '     <b>DAQ State</b>: %s<br>' % (
            onlconsts.kDAQSTATE[daqstate] if daqstate < len(onlconsts.kDAQSTATE) else "UNKNOWN")
        summary += '    <b>Start Time</b>: %s<br>' % stime
        summary += '      <b>End Time</b>: %s<br>' % etime
        summary += '      <b>DAQ Time</b>: %s<br><br>' % daqtime

        for daq_name in self.MonNames:
            if daq_name in self.RunStats:
                n = self.RunStats[daq_name].get('n', 0)
                ar = self.RunStats[daq_name].get('ar', 0.0)
                sr = self.RunStats[daq_name].get('sr', 0.0)
                stat = '%10d [%6.1f %6.1f Hz]' % (n, sr, ar)
                summary += '%s' % (' ' * (14 - len(daq_name)))
                summary += '<b>%s</b>: %s<br>' % (daq_name, stat)

        summary += '</font></pre>'
        self.RunStatsTextEdit.setText(summary)

    def set_runstate(self, state):
        if state == onlconsts.kDOWN:
            self.ShiftConfig.setEnabled(True)
            self.RunTypeConfig.setEnabled(True)
            self.RunDescConfig.setEnabled(True)
            self.ConfigFileButton.setEnabled(True)
        else:
            self.ShiftConfig.setEnabled(False)
            self.RunTypeConfig.setEnabled(False)
            self.RunDescConfig.setEnabled(False)
            self.ConfigFileButton.setEnabled(False)

        self.ExitButton.setEnabled(False)

        if state == onlconsts.kDOWN:
            self.BootButton.setEnabled(True)
            self.BootButton.setStyleSheet("background-color: none")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: none")
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: none")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: none")
        elif onlutils.check_state(self.RunState, onlconsts.kBOOTED):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: blue")
            self.ConfigButton.setEnabled(True)
            self.ExitButton.setEnabled(True)
        elif onlutils.check_state(self.RunState, onlconsts.kCONFIGURED):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: none")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: blue")
            self.StartButton.setEnabled(True)
            self.ExitButton.setEnabled(True)
        elif onlutils.check_state(self.RunState, onlconsts.kRUNNING):
            self.BootButton.setEnabled(False)
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: none")
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: blue")
            self.EndButton.setEnabled(True)
        elif onlutils.check_state(self.RunState, onlconsts.kRUNENDED):
            self.BootButton.setEnabled(False)
            self.ConfigButton.setEnabled(False)
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: none")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: yellow")
        elif onlutils.check_state(self.RunState, onlconsts.kPROCENDED):
            self.BootButton.setEnabled(False)
            self.ConfigButton.setEnabled(False)
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: none")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: blue")
            self.ExitButton.setEnabled(True)

        if onlutils.check_error(state):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: red")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: red")
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: red")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: red")
            self.ExitButton.setEnabled(True)

    def center(self):
        qr = self.frameGeometry()
        screen = QGuiApplication.primaryScreen()
        cp = screen.availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def msgbox_error(self, message):
        log.error("GUI Error: %s", message)
        font = QFont()
        font.setPointSize(12)
        box = QMessageBox()
        box.setWindowTitle('Error')
        box.setFont(font)
        box.setIcon(QMessageBox.Critical)
        box.setText(message)
        box.exec()

    def msgbox_question(self, message):
        font = QFont()
        font.setPointSize(12)
        box = QMessageBox()
        box.setWindowTitle('Question')
        box.setFont(font)
        box.setIcon(QMessageBox.Question)
        box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        box.setText(message)
        box.exec()
        return box


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
