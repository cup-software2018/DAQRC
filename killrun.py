#!/usr/bin/env python

import sys
import onlconsts
import onlutils

runnum = int(sys.argv[1])
config = '%s/CONFIG/%06d.config' % (onlconsts.kRAWDATA_DIR, runnum)

cmd = "kill -9 `ps -ef | grep %s | grep -v grep | awk '{print $2}'`"

fp = open(config)
for line in fp:
    line = line.strip()
    if 'SERVER' in line:
        sline = line.split()
        name = sline[2]
        ip = sline[3]
        if 'TCB' in sline[2]:
            onlutils.execute_cmd(cmd%'tcb', ip)
        elif 'MERGER' in sline[2]:
            onlutils.execute_cmd(cmd%'merger', ip)
        else:
            onlutils.execute_cmd(cmd%'daq', ip)
