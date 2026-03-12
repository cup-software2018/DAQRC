import os
import sys
import time
import socket
import json
import yaml
from datetime import datetime
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from rcui import Ui_MainWindow
import onlconsts
import onlutils


def sortfunc(e):
    # Ensure TCB(mode=0) gets sorted to the front before moving to the end
    return e[0]


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setupUi(self)

        self.Bindir = onlconsts.kONLDAQ_DIR + '/bin/'

        # Launch the background logger daemon if it's dead when RC starts
        self.check_and_start_logger()

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

        timer = QTimer(self)
        timer.timeout.connect(self.update_runstate)
        timer.setInterval(100)
        timer.start()

    def check_and_start_logger(self):
        """
        1. Try connecting directly to port 9999 to verify the logger is alive with 100% certainty.
        2. If there is no response (Connection refused), forcefully launch the logger in the background.
        """
        is_running = False

        # 1. Knock on port 9999 with a 0.5-second timeout
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.5)
            try:
                sock.connect((onlconsts.kLOGGERIPADDR, onlconsts.kLOGGERPORT))
                is_running = True  # Connection successful! Logger is alive.
            except (socket.timeout, ConnectionRefusedError):
                is_running = False  # Connection failed! Logger is dead.

        # 2. Force start if the logger is dead
        if not is_running:
            print("Logger daemon is not responding. Starting logger.py in background...")

            logger_script = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), 'logger.py')
            log_file = '/tmp/cupdaq_logger.log'

            # Python execution path (default to 'python' if sys.executable is empty)
            python_exe = sys.executable if sys.executable else 'python'

            # Completely detach as a background process using nohup
            start_cmd = f"nohup {python_exe} {logger_script} > {log_file} 2>&1 &"
            os.system(start_cmd)

            # Wait 1 second for the logger to start and open port 9999 (Crucial!)
            time.sleep(1.0)
        else:
            print("Logger daemon is already running on port %d." %
                  onlconsts.kLOGGERPORT)

    def send_logger_cmd(self, req_data):
        """Send JSON request to Logger's port 9999 and receive response"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((onlconsts.kLOGGERIPADDR, onlconsts.kLOGGERPORT))
                sock.sendall(json.dumps(req_data).encode('utf-8'))
                resp = sock.recv(1024*1024).decode('utf-8')
                if resp:
                    return json.loads(resp)
        except Exception as e:
            print(f"Logger comm error: {e}")
        return {}

    def load_config(self):
        result = QFileDialog.getOpenFileName(self, 'Load Configuration File', onlconsts.kDEFAULTCONFIGDIR,
                                             'Configuration File (*.yml);;All Files (*)')
        self.ConfigFile = str(result[0])
        if self.ConfigFile:
            configfile = os.path.basename(self.ConfigFile)
            msg = '<font color="blue"><b>%s</b></font> loaded' % configfile
            self.ConfigFileLabel.setText(msg)

    def boot_run(self):
        self.check_and_start_logger()  # Re-check logger survival before booting

        self.Shift = str(self.ShiftConfig.text())
        if not self.Shift:
            return self.msgbox_error('Shift crew missing!')
        if not self.ConfigFile:
            return self.msgbox_error('Run configuration file missing!')

        self.RunType = str(self.RunTypeConfig.currentText())
        if not self.RunType:
            return self.msgbox_error('Run type missing!')

        self.RunDesc = str(self.RunDescConfig.toPlainText())
        configfile = os.path.basename(self.ConfigFile)

        msg = f'<pre>Shift      : {self.Shift}<br>Run type   : {self.RunType}<br>Config file: {configfile}\n<br><b>Do you want to boot this run?</b></pre>'
        reply = self.msgbox_question(msg)
        if reply.clickedButton() is reply.button(QMessageBox.No):
            return

        self.RunStats.clear()
        self.MonNames.clear()
        self.SubRunNumber = 0
        self.StartTime = 0
        self.EndTime = 0
        self.RunStatsTextEdit.clear()

        # Request DB Insert to Logger
        req = {
            "cmd": "BOOT_RUN",
            "shift": self.Shift,
            "runtype": self.RunType,
            "rundesc": self.RunDesc,
            "config": self.ConfigFile
        }
        resp = self.send_logger_cmd(req)
        if "run_num" in resp:
            self.RunNumber = resp["run_num"]
        else:
            return self.msgbox_error("Logger failed to boot run (DB error).")

        run_number = self.RunNumber
        config_file = self.ConfigFile
        onldaq_dir = onlconsts.kONLDAQ_DIR
        rawdata_dir = onlconsts.kRAWDATA_DIR

        target_config = '%s/CONFIG/%06d.yml' % (rawdata_dir, run_number)
        cmd = 'scp %s %s:%s' % (
            config_file, onlconsts.kDAQSERVER_IP, target_config)
        os.system(cmd)

        daqlist = []
        try:
            with open(config_file, 'r', encoding='utf-8') as fp:
                config_data = yaml.safe_load(fp) or {}

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
            return self.msgbox_error('Failed to load YAML config:\n%s' % e)

        fformat = '-b' if onlconsts.kOUTPUTFILEFORMAT == 'hdf5' else '-a'
        optlist = []
        for daq in daqlist:
            mode, dnum, name, ip, port = daq
            topt = name[0].lower()

            if mode == 0:
                sopt = '-t -r %d -n %s ' % (run_number, name)
                dopt = '-d 0 -r %d -c %s' % (run_number, target_config)
            elif mode == 2:
                sopt = '-m -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d ' % (topt,
                                                   dnum, target_config, run_number)
            else:
                sopt = '-d -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d ' % (topt,
                                                   dnum, target_config, run_number)
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
                cmd = self.Bindir + \
                    '%s %s%s -o "%s"' % (onlconsts.kEXESCRIPT,
                                         daq[1], onldaqdiropt+rawdatadiropt, daq[2])
                onlutils.execute_cmd(cmd, daq[3])
        time.sleep(1)

        tcb = optlist[-1]
        cmd = self.Bindir + \
            '%s %s%s -o "%s"' % (onlconsts.kEXESCRIPT,
                                 tcb[1], onldaqdiropt+rawdatadiropt, tcb[2])
        onlutils.execute_cmd(cmd, tcb[3])

        self.OnThisRC = True
        self.StartTime = 0
        self.EndTime = 0

    def config_run(self):
        onlutils.send_command(self.RunSocket, onlconsts.kCONFIGRUN)
        self.ConfigButton.setStyleSheet("background-color: yellow")

    def start_run(self):
        onlutils.send_command(self.RunSocket, onlconsts.kSTARTRUN)
        self.StartButton.setStyleSheet("background-color: yellow")

    def end_run(self):
        msg = '<pre><b>Run %06d running now.<br>Do you want to quit this run?</b></pre>' % self.RunNumber
        reply = self.msgbox_question(msg)
        if reply.clickedButton() is reply.button(QMessageBox.No):
            return
        onlutils.send_command(self.RunSocket, onlconsts.kENDRUN)
        self.EndButton.setStyleSheet("background-color: yellow")

    def exit_run(self):
        is_safe_state = (
            self.RunState in (onlconsts.kDOWN, onlconsts.kPROCENDED) or
            onlutils.check_state(self.RunState, onlconsts.kRUNENDED)
        )
        if not is_safe_state:
            daqstate = onlutils.get_state(self.RunState)
            if onlutils.check_error(self.RunState):
                daqstate = onlconsts.kERROR
            state_str = onlconsts.kDAQSTATE[daqstate]
            msg = '<pre><b>Run %06d is currently active (State: %s).<br>Are you sure to FORCE exit without ending properly?</b></pre>' % (
                self.RunNumber, state_str)
            reply = self.msgbox_question(msg)
            if reply.clickedButton() is reply.button(QMessageBox.No):
                return

        if self.RunSocket:
            onlutils.send_command(self.RunSocket, onlconsts.kEXIT)

    def update_runstate(self):
        self.RunState, self.RunSocket = onlutils.query_runstate(
            onlconsts.kDAQSERVER_ADDR, self.RunSocket)
        self.set_runstate(self.RunState)

        # 1. Sync with Logger
        if not self.OnThisRC and self.RunState is not onlconsts.kDOWN:
            resp = self.send_logger_cmd({"cmd": "SYNC_LATEST"})
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

        # 2. Get Stats from Logger
        if onlutils.check_state(self.RunState, onlconsts.kRUNNING) or onlutils.check_state(self.RunState, onlconsts.kRUNENDED):
            resp = self.send_logger_cmd({"cmd": "GET_STATS"})
            if resp:
                self.RunStats = resp.get("RunStats", {})
                self.SubRunNumber = resp.get("SubRunNumber", 0)
                self.StartTime = resp.get("StartTime", 0)
                self.MonNames = resp.get("MonNames", [])
                self.EndTime = resp.get("EndTime", 0)

        # 3. Handle End of Run Tagging
        if onlutils.check_state(self.RunState, onlconsts.kRUNENDED):
            if not getattr(self, '_is_asking_goodrun', False):
                self._is_asking_goodrun = True

                # Fetch endtime directly just in case logger hasn't yet
                if not self.EndTime:
                    onlutils.send_command(
                        self.RunSocket, onlconsts.kQUERYRUNINFO)
                    mess = []
                    onlutils.recv_message(self.RunSocket, mess)
                    if len(mess) > 3:
                        self.EndTime = mess[3]

                onlbit = 0
                msg = 'Tag run %06d as GOODRUN?' % self.RunNumber
                reply = self.msgbox_question(msg)
                if reply.clickedButton() is reply.button(QMessageBox.Yes):
                    onlbit = 1

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
                self.send_logger_cmd(req)

        # 4. Format Output Text
        curtime = time.strftime("%Y-%m-%d %H:%M:%S")
        stime = datetime.fromtimestamp(self.StartTime).strftime(
            "%Y-%m-%d %H:%M:%S") if self.StartTime > 0 else ''
        etime = datetime.fromtimestamp(int(self.EndTime)).strftime(
            "%Y-%m-%d %H:%M:%S") if self.EndTime > 0 else ''

        daqtime = ''
        if self.MonNames and self.MonNames[0] in self.RunStats:
            daqtime = onlutils.HMSFormatter(
                self.RunStats[self.MonNames[0]]['t'])

        daqstate = onlutils.get_state(self.RunState)
        if onlutils.check_error(self.RunState):
            daqstate = onlconsts.kERROR

        summary = '<pre><font color="blue"><br>'
        summary += '  <b>Current Time</b>: %s<br><br>' % curtime
        summary += '    <b>Run Number</b>: %06d/%d<br>' % (
            self.RunNumber, self.SubRunNumber)
        summary += '     <b>DAQ State</b>: %s<br>' % onlconsts.kDAQSTATE[daqstate]
        summary += '    <b>Start Time</b>: %s<br>' % stime
        summary += '      <b>End Time</b>: %s<br>' % etime
        summary += '      <b>DAQ Time</b>: %s<br><br>' % daqtime

        for daq_name in self.MonNames:
            if daq_name in self.RunStats:
                n = self.RunStats[daq_name]['n']
                ar = self.RunStats[daq_name]['ar']
                sr = self.RunStats[daq_name]['sr']
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

        if state == onlconsts.kDOWN:
            self.BootButton.setEnabled(True)
            self.BootButton.setStyleSheet("background-color: none")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: none")
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: none")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: none")
            self.ExitButton.setEnabled(True)
        elif onlutils.check_state(self.RunState, onlconsts.kBOOTED):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: blue")
            self.ConfigButton.setEnabled(True)
        elif onlutils.check_state(self.RunState, onlconsts.kCONFIGURED):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: none")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: blue")
            self.StartButton.setEnabled(True)
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
        if onlutils.check_error(state):
            self.BootButton.setEnabled(False)
            self.BootButton.setStyleSheet("background-color: red")
            self.ConfigButton.setEnabled(False)
            self.ConfigButton.setStyleSheet("background-color: red")
            self.StartButton.setEnabled(False)
            self.StartButton.setStyleSheet("background-color: red")
            self.EndButton.setEnabled(False)
            self.EndButton.setStyleSheet("background-color: red")

    def center(self):
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())

    def msgbox_error(self, message):
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
