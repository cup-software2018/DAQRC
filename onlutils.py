# coding=utf-8
import sys
import socket
import time
from subprocess import Popen, PIPE

import onlconsts


def get_state(status):
    for n in range(1, 16):
        if status & (1 << n):
            return n
    return 0


def check_state(status, state):
    if status & (1 << state):
        return True
    return False


def check_error(status):
    if status & (1 << onlconsts.kERROR):
        return True
    return False


def encode_message(mess1, mess2 = 0, mess3 = 0, mess4 = 0):
    data = bytearray()

    for i in range(8):
        data.append((mess1 >> 8*i) & 0xFF)
    for i in range(8):
        data.append((mess2 >> 8*i) & 0xFF)
    for i in range(8):
        data.append((mess3 >> 8*i) & 0xFF)
    for i in range(8):
        data.append((mess4 >> 8*i) & 0xFF)                        

    return data


def decode_message(data):
    mess1 = 0
    mess2 = 0
    mess3 = 0
    mess4 = 0

    for i in range(8):
        mess1 += data[i] << 8*i
        mess2 += data[i+8] << 8*i
        mess3 += data[i+16] << 8*i
        mess4 += data[i+24] << 8*i

    return mess1, mess2, mess3, mess4


def get_connection(ipaddr):
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection.connect(ipaddr)
    except Exception as e:
        return None
    return connection


def send_command(connection, cmd):
    sendbuf = encode_message(cmd)
    try:
        connection.send(sendbuf)
    except socket.error as msg:
        return False
    return True


def recv_message(connection, message):
    del message[:]
    try:
        recvbuf = connection.recv(onlconsts.kMESSLEN)
    except socket.error as msg:
        return False
    else:
        if recvbuf:
            mess1, mess2, mess3, mess4 = decode_message(bytearray(recvbuf))
            message.append(mess1)
            message.append(mess2)
            message.append(mess3)
            message.append(mess4)
        else:
            message.append(0)
            message.append(0)
            message.append(0)
            message.append(0)
    return True


def query_runstate(ipaddr, connection):
    status = onlconsts.kDOWN
    message = []

    if not connection:
        connection = get_connection(ipaddr)

    if connection:
        if not send_command(connection, onlconsts.kQUERYDAQSTATUS):
            connection = None
            status = onlconsts.kDOWN
        else:
            if not recv_message(connection, message):
                connection = None
                status = onlconsts.kDOWN
            else:
                status = message[0]

    if status is None:
        status = onlconsts.kDOWN
        
    return status, connection


def wait_runstate(connection, state):
    mess = []
    while True:
        send_command(connection, onlconsts.kQUERYDAQSTATUS)
        recv_message(connection, mess)
        if check_state(mess[0], state):
            return True
        if check_error(mess[0]):
            return False
        time.sleep(0.1)


def check_runstate(connection, state):
    mess = []
    send_command(connection, onlconsts.kQUERYDAQSTATUS)
    recv_message(connection, mess)
    if not check_state(mess[0], state) or check_error(mess[0]):
        return False
    return True


def execute_cmd(cmd, host='localhost'):
    ssh = Popen(['ssh', '%s' % host, cmd],
                shell=False, stdout=PIPE, stderr=PIPE)

    result = ssh.stdout.readlines()
    if not result:
        return None

    result = [str(s).replace("b'", "") for s in result]
    result = [str(s).replace("\\n'", "") for s in result]

    return result


def HMSFormatter(value):
    h = value // 3600
    m = (value - h * 3600) // 60
    s = value % 60
    return "%02d:%02d:%02d" % (h,m,s)
