#!/usr/bin/env python
# coding=utf-8
import sys
import os
import yaml
import onlconsts
import onlutils

# Check if run number is provided
if len(sys.argv) < 2:
    print("Usage: python kill_daq.py <runnum>")
    sys.exit(1)

runnum = int(sys.argv[1])
# Strict use of .yml extension as specified
config_file = '%s/CONFIG/%06d.yml' % (onlconsts.kRAWDATA_DIR, runnum)

# Verify file existence
if not os.path.exists(config_file):
    print("Error: Configuration file not found -> %s" % config_file)
    sys.exit(1)

# Base kill command template
cmd = "kill -9 `ps -ef | grep %s | grep -v grep | awk '{print $2}'`"

# Parse the YAML configuration
try:
    with open(config_file, 'r', encoding='utf-8') as fp:
        config_data = yaml.safe_load(fp) or {}
except Exception as e:
    print("Error: Failed to parse YAML config -> %s" % str(e))
    sys.exit(1)

# Extract DAQ nodes from the parsed configuration
nodes = config_data.get('DAQ', [])

# Dispatch kill commands to each node based on its type
for item in nodes:
    name = str(item.get('NAME', '')).upper()
    ip = str(item.get('IP', ''))

    if not name or not ip:
        continue

    # Execute remote kill command via SSH
    if 'TCB' in name:
        onlutils.run_ssh_cmd(cmd % 'tcb', ip)
    elif 'MERGER' in name:
        onlutils.run_ssh_cmd(cmd % 'merger', ip)
    else:
        # Default to killing 'daq' for AADC, FADC, SADC, IADC, etc.
        onlutils.run_ssh_cmd(cmd % 'daq', ip)

print("Kill command dispatched successfully for run %06d." % runnum)