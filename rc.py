import os
import sys
import time
from datetime import datetime
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from pydblite.sqlite import Database, Table
from rcui import Ui_MainWindow
import onlconsts
import onlutils


def sortfunc(e):
    return e[2]


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setupUi(self)

        # DAQ software
        #BFARCH = os.getenv('BFARCH')
        BFARCH = 'Linux5.14-GCC_11_4'
        self.Bindir = onlconsts.kONLDAQ_DIR + '/bin/' + BFARCH + '/'

        # Run configuration variables
        self.RunNumber = 0
        self.Shift = None
        self.RunType = None
        self.RunDesc = None
        self.ConfigFile = None

        # Run control variables
        self.RunState = onlconsts.kDOWN
        self.RunSocket = None
        self.OnThisRC = False

        # Run monitoring variables
        self.SubRunNumber = 0
        self.StartTime = 0
        self.EndTime = 0
        self.IsMonSet = False
        self.MonList = []

        # Run statistics variables
        self.RunStats = {}

        #
        # Run catalog database
        #
        if not os.path.isfile(onlconsts.kRUNCATALOGDBFILE):
            msg = 'No run catalog DB file!\n(%s)' % onlconsts.kRUNCATALOGDBFILE
            self.msgbox_error(msg)
            exit()
        self.RunCatalog = Database(onlconsts.kRUNCATALOGDBFILE)
        self.RunCatalogTable = Table('runcatalog', self.RunCatalog)

        #
        # Setup UI
        #
        self.center()

        # Run type
        self.RunTypeConfig.addItems(onlconsts.kRUNTYPELIST)

        # Run statistics
        font = QFont()
        font.setPointSize(14)

        self.RunStatsTextEdit = QTextEdit()
        self.RunStatsTextEdit.setFont(font)
        self.RunStatsTextEdit.setEnabled(False)

        layout = QVBoxLayout()
        layout.addWidget(self.RunStatsTextEdit)

        self.RunStatsBox.setLayout(layout)

        #
        # Signals and slots
        #
        self.ConfigFileButton.clicked.connect(self.load_config)
        self.BootButton.clicked.connect(self.boot_run)
        self.ConfigButton.clicked.connect(self.config_run)
        self.StartButton.clicked.connect(self.start_run)
        self.EndButton.clicked.connect(self.end_run)
        self.ExitButton.clicked.connect(self.exit_run)

        #
        # QTimer
        #
        timer = QTimer(self)
        timer.timeout.connect(self.update_runstate)
        timer.setInterval(100)
        timer.start()

    def load_config(self):
        result = QFileDialog.getOpenFileName(self, 'Load Configuration File', onlconsts.kDEFAULTCONFIGDIR,
                                             'Configuration File (*.config);;All Files (*)')
        self.ConfigFile = str(result[0])
        if self.ConfigFile:
            configfile = os.path.basename(self.ConfigFile)
            msg = '<font color="blue"><b>%s</b></font> loaded' % configfile
            self.ConfigFileLabel.setText(msg)

    def boot_run(self):
        self.IsMonSet = False
        self.Shift = str(self.ShiftConfig.text())
        if not self.Shift:
            self.msgbox_error('Shift crew missing!')
            return

        if not self.ConfigFile:
            self.msgbox_error('Run configuration file missing!')
            return

        self.RunType = str(self.RunTypeConfig.currentText())
        if not self.RunType:
            self.msgbox_error('Run type missing!')
            return

        self.RunDesc = str(self.RunDescConfig.toPlainText())
        configfile = os.path.basename(self.ConfigFile)

        msg = '<pre>'
        msg1 = 'Shift      : %s<br>' % self.Shift
        msg2 = 'Run type   : %s<br>' % self.RunType
        msg3 = 'Config file: %s\n<br>' % configfile
        msg += msg1 + msg2 + msg3
        msg += '<b>Do you want to boot this run?</b>'
        msg += '</pre>'

        reply = self.msgbox_question(msg)
        if reply.clickedButton() is reply.button(QMessageBox.No):
            return

        # get run number from run catalog
        self.RunNumber = self.RunCatalogTable.insert(shift=self.Shift,
                                                     runtype=self.RunType,
                                                     rundesc=self.RunDesc,
                                                     config=self.ConfigFile)
        self.RunCatalogTable.commit()

        run_number = self.RunNumber
        config_file = self.ConfigFile

        version = onlconsts.kSOFTWARE_VER
        onldaq_dir = onlconsts.kONLDAQ_DIR
        rawdata_dir = onlconsts.kRAWDATA_DIR

        config = '%s/CONFIG/%06d.config' % (rawdata_dir, run_number)
        cmd = 'scp %s %s:%s' % (config_file, onlconsts.kDAQSERVER_IP, config)
        os.system(cmd)
                
        daqlist = []
        fp = open(config_file)
        for line in fp:
            line = line.strip()
            if 'SERVER' in line and '#' not in line:
                sline = line.split()
                dnum = int(sline[1])
                name = sline[2]
                ip = sline[3]
                port = int(sline[4])
                if 'TCB' in sline[2]:
                    mode = 0
                elif 'MERGER' in sline[2]:
                    mode = 2
                else:
                    mode = 1
                daq = (mode, dnum, name, ip, port)
                daqlist.append(daq)

        optlist = []
        for daq in daqlist:
            mode = daq[0]
            dnum = daq[1]
            name = daq[2]
            topt = daq[2][0].lower()
            if mode == 0:
                sopt = '-t -r %d -n %s ' % (run_number, name)
                dopt = '-d 0 -r %d -c %s' % (run_number, config)
            elif mode == 2:
                sopt = '-m -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d ' % (topt,
                                                   dnum, config, run_number)
            else:
                sopt = '-d -r %d -n %s ' % (run_number, name)
                dopt = '-%s -d %d -c %s -r %d ' % (topt,
                                                   dnum, config, run_number)
                adc = name[0:4]
                for dd in daqlist:
                    if dd[0] == 2 and adc in dd[2]:
                        dopt += '-x '
                        break
            dopt = (mode, sopt, dopt, daq[3], daq[4])
            optlist.append(dopt)

        optlist.sort(key=sortfunc)
        optlist.append(optlist.pop(0))

        versionopt = '--version=%s ' % version
        onldaqdiropt = '--onldaqdir=%s ' % onldaq_dir
        rawdatadiropt = '--rawdatadir=%s ' % rawdata_dir

        # execute daq first
        for daq in optlist:
            mode = daq[0]
            daq_ip = daq[3]
            shell_option = daq[1]
            shell_option += versionopt
            shell_option += onldaqdiropt
            shell_option += rawdatadiropt
            daq_option = daq[2]
            if mode > 0:
                cmd = self.Bindir + '%s %s -o "%s"' % (onlconsts.kEXESCRIPT,
                                                       shell_option, daq_option)
                result = onlutils.execute_cmd(cmd, daq_ip)
        time.sleep(1)

        # execute tcb
        tcb = optlist[-1]
        tcb_ip = tcb[3]
        tcb_port = tcb[4]
        shell_option = tcb[1]
        shell_option += versionopt
        shell_option += onldaqdiropt
        shell_option += rawdatadiropt
        tcb_option = tcb[2]
        cmd = self.Bindir + '%s %s -o "%s"' % (onlconsts.kEXESCRIPT,
                                               shell_option, tcb_option)
        result = onlutils.execute_cmd(cmd, tcb_ip)
        
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
        if onlutils.check_state(self.RunState, onlconsts.kRUNNING):
            msg = '<pre><b>Run %06d running now.<br>Are you sure to exit this run?</b></pre>' % self.RunNumber
            reply = self.msgbox_question(msg)
            if reply.clickedButton() is reply.button(QMessageBox.No):
                return

        if self.RunSocket:
            onlutils.send_command(self.RunSocket, onlconsts.kEXIT)

    def update_runstate(self):
        self.RunState, self.RunSocket = onlutils.query_runstate(
            onlconsts.kDAQSERVER_ADDR, self.RunSocket)
        self.set_runstate(self.RunState)

        mess = []

        if not self.OnThisRC and self.RunState is not onlconsts.kDOWN:
            self.RunNumber = self.RunCatalogTable.__len__()
            record = self.RunCatalogTable[self.RunNumber]
            self.Shift = record['shift']
            self.RunType = record['runtype']
            self.RunDesc = record['rundesc']
            self.ConfigFile = record['config']
            self.ShiftConfig.setText(self.Shift)
            index = self.RunTypeConfig.findText(
                self.RunType, Qt.MatchFixedString)
            self.RunTypeConfig.setCurrentIndex(index)
            self.RunDescConfig.setText(self.RunDesc)
            configfile = os.path.basename(self.ConfigFile)
            msg = '<font color="blue"><b>%s</b></font> loaded' % configfile
            self.ConfigFileLabel.setText(msg)
            self.OnThisRC = True

        if not self.IsMonSet and self.RunState is not onlconsts.kDOWN:
            self.MonList.clear()
            fp = open(self.ConfigFile)
            for line in fp:
                line = line.strip()
                if 'SERVER' in line and '#' not in line:
                    sline = line.split()
                    name = sline[2]
                    ip = sline[3]
                    port = int(sline[4])
                    if 'TCB' in sline[2]:
                        continue
                    socket = onlutils.get_connection((ip, port))
                    onlutils.send_command(socket, onlconsts.kQUERYMONITOR)
                    onlutils.recv_message(socket, mess)
                    if mess[0] > 0:
                        daq = (name, socket)
                        self.MonList.append(daq)
                        self.RunStats[name] = {}
                        self.RunStats[name]['n'] = 0
                        self.RunStats[name]['dn'] = 0
                        self.RunStats[name]['t'] = 0.0
                        self.RunStats[name]['dt'] = 0.0
                        self.RunStats[name]['ar'] = 0.0
                        self.RunStats[name]['sr'] = 0.0
            self.IsMonSet = True

        if onlutils.check_state(self.RunState, onlconsts.kRUNNING):
            onlutils.send_command(self.RunSocket, onlconsts.kQUERYRUNINFO)
            onlutils.recv_message(self.RunSocket, mess)
            self.SubRunNumber = mess[1]
            self.StartTime = mess[2]

            for daq in self.MonList:
                name = daq[0]
                socket = daq[1]
                onlutils.send_command(socket, onlconsts.kQUERYTRGINFO)
                onlutils.recv_message(socket, mess)
                n = self.RunStats[name]['n'] = mess[0]
                t = self.RunStats[name]['t'] = mess[1]/1000000000.
                if t > 0:
                    self.RunStats[name]['ar'] = n/t
                dt = t - self.RunStats[name]['dt']
                dn = n - self.RunStats[name]['dn']
                if dt > 0:
                    self.RunStats[name]['sr'] = dn/dt
                self.RunStats[name]['dt'] = t
                self.RunStats[name]['dn'] = n

        if onlutils.check_state(self.RunState, onlconsts.kRUNENDED):
            record = self.RunCatalogTable[self.RunNumber]
            etime = record['etime']
            if not etime:
                onlutils.send_command(self.RunSocket, onlconsts.kQUERYRUNINFO)
                onlutils.recv_message(self.RunSocket, mess)
                self.EndTime = mess[3]

                for daq in self.MonList:
                    name = daq[0]
                    socket = daq[1]
                    onlutils.send_command(socket, onlconsts.kQUERYTRGINFO)
                    onlutils.recv_message(socket, mess)
                    self.RunStats[name]['n'] = mess[0]
                    self.RunStats[name]['t'] = mess[1]/1000000000.

                onlbit = 0
                msg = 'Tag run %06d as GOODRUN?' % self.RunNumber
                reply = self.msgbox_question(msg)
                if reply.clickedButton() is reply.button(QMessageBox.Yes):
                    onlbit = 1

                stime = datetime.fromtimestamp(
                    self.StartTime).strftime("%Y-%m-%d %H:%M:%S")
                etime = datetime.fromtimestamp(
                    int(self.EndTime)).strftime("%Y-%m-%d %H:%M:%S")

                self.RunCatalogTable.update(
                    record, stime=stime, etime=etime, onlbit=onlbit)
                for daq in self.MonList:
                    daqname = daq[0]
                    if 'AADC' in daqname:
                        self.RunCatalogTable.update(
                            record, naadc=self.RunStats[daqname]['n'])
                        self.RunCatalogTable.update(
                            record, taadc=self.RunStats[daqname]['t'])
                    elif 'FADC' in daqname:
                        self.RunCatalogTable.update(
                            record, nfadc=self.RunStats[daqname]['n'])
                        self.RunCatalogTable.update(
                            record, tfadc=self.RunStats[daqname]['t'])
                    elif 'SADC' in daqname:
                        self.RunCatalogTable.update(
                            record, nsadc=self.RunStats[daqname]['n'])
                        self.RunCatalogTable.update(
                            record, tsadc=self.RunStats[daqname]['t'])
                    elif 'IADC' in daqname:
                        self.RunCatalogTable.update(
                            record, niadc=self.RunStats[daqname]['n'])
                        self.RunCatalogTable.update(
                            record, tiadc=self.RunStats[daqname]['t'])
                self.RunCatalogTable.commit()

        curtime = time.strftime("%Y-%m-%d %H:%M:%S")
        stime = ''
        if self.StartTime > 0:
            stime = datetime.fromtimestamp(
                self.StartTime).strftime("%Y-%m-%d %H:%M:%S")
        etime = ''
        if self.EndTime > 0:
            etime = datetime.fromtimestamp(
                int(self.EndTime)).strftime("%Y-%m-%d %H:%M:%S")
        daqtime = ''
        if self.IsMonSet:
            daqtime = onlutils.HMSFormatter(
                self.RunStats[self.MonList[0][0]]['t'])
        
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

        if self.IsMonSet:
            for daq in self.MonList:
                n = self.RunStats[daq[0]]['n']
                ar = self.RunStats[daq[0]]['ar']
                sr = self.RunStats[daq[0]]['sr']
                stat = '%10d [%6.1f %6.1f Hz]' % (n, sr, ar)
                summary += '%s' % (' ' * (14 - len(daq[0])))
                summary += '<b>%s</b>: %s<br>' % (daq[0], stat)

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

    # create the main window
    window = MainWindow()
    window.show()

    # start the event loop
    sys.exit(app.exec())
