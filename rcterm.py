import os
import time
import onlconsts
import onlutils
from argparse import ArgumentParser


def sortfunc(e):
    return e[2]


parser = ArgumentParser()
parser.add_argument('-r', '--runnum', type=int, required=True)
parser.add_argument('-c', '--config', type=str, required=True)

args = parser.parse_args()

run_number = args.runnum
config_file = args.config

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
    if 'SERVER' in line:
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
        dopt = '-%s -d %d -c %s -r %d ' % (topt, dnum, config, run_number)
    else:
        sopt = '-d -r %d -n %s ' % (run_number, name)
        dopt = '-%s -d %d -c %s -r %d ' % (topt, dnum, config, run_number)
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
        cmd = onldaq_dir + \
            '/bin/Linux5.14-GCC_11_3/executenulldaq.sh %s -o "%s"' % (
                shell_option, daq_option)
        # print(cmd)
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
cmd = onldaq_dir + \
    '/bin/Linux5.14-GCC_11_3/executenulldaq.sh %s -o "%s"' % (
        shell_option, tcb_option)
# print(cmd)
result = onlutils.execute_cmd(cmd, tcb_ip)

time.sleep(1)

message = []

# tcb connection
tcb_ipaddr = (tcb_ip, tcb_port)
tcb_con = onlutils.get_connection(tcb_ipaddr)

result = onlutils.wait_runstate(tcb_con, onlconsts.kBOOTED)
if not result:
    print('error!!!')
    onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
    exit()
print('run=%06d booted' % run_number)

value = input("Do you want to configure run? (Yes/No): ")
while value != 'Yes' and value != 'No':
    value = input("Do you want to configure run? (Yes/No): ")

if value == 'Yes':
    onlutils.send_message(tcb_con, 0, onlconsts.kCONFIGRUN)
    time.sleep(1)
else:
    onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
    exit()

result = onlutils.wait_runstate(tcb_con, onlconsts.kCONFIGURED)
if not result:
    print('error!!!')
    onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
    exit()
print('run=%06d configured' % run_number)

value = input("Do you want to start run? (Yes/No): ")
while value != 'Yes' and value != 'No':
    value = input("Do you want to start run? (Yes/No): ")

if value == 'Yes':
    onlutils.send_message(tcb_con, 0, onlconsts.kSTARTRUN)
    time.sleep(5)
else:
    onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
    exit()

LINE_UP = '\033[1A'
LINE_CLEAR = '\x1b[2K'

try:
    while True:
        print('If you want to stop this run, press ctrl+c')
        if not onlutils.check_runstate(tcb_con, onlconsts.kRUNNING):
            print('error!!!')
            break
        time.sleep(0.1)
        print(LINE_UP, end=LINE_CLEAR)
except KeyboardInterrupt:
    print('run %06d will be ended ....' % run_number)

onlutils.send_message(tcb_con, 0, onlconsts.kENDRUN)
time.sleep(1)

result = onlutils.wait_runstate(tcb_con, onlconsts.kRUNENDED)
if not result:
    print('error!!!')
    onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
    exit()
print('run=%06d ended' % run_number)

onlutils.wait_runstate(tcb_con, onlconsts.kPROCENDED)
onlutils.send_message(tcb_con, 0, onlconsts.kEXIT)
