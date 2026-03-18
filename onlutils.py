import sys
import time
import zmq
import json
from subprocess import Popen, PIPE
import onlconsts

# Reuse global ZeroMQ Context for better efficiency
_ctx = zmq.Context.instance()


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


def get_connection(endpoint, sock_type=zmq.REQ):
    """
    Creates and connects a ZeroMQ socket.
    Timeout is now handled by poll() in execute_command.
    """
    sock = _ctx.socket(sock_type)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(endpoint)
    return sock


def execute_command(sock, cmd_string, extra_data=None, timeout_ms=200):
    """
    Sends a JSON command and uses poll() to prevent GUI freezing.
    Returns parsed JSON or None if timeout/error occurs.
    """
    if sock is None:
        return None

    req = {"command": cmd_string}
    if extra_data:
        req.update(extra_data)

    try:
        sock.send_json(req)

        if sock.poll(timeout=timeout_ms) == 0:
            return None

        reply = sock.recv_json()
        return reply
    except Exception as e:
        return None


def query_runstate(endpoint, sock=None):
    """
    Queries the DAQ server for its current status using JSON over ZMQ.
    Returns: (status_code, socket_object)
    """
    if sock is None:
        sock = get_connection(endpoint)

    reply = execute_command(sock, onlconsts.kQUERYDAQSTATUS)

    if reply is None or "status" not in reply:
        sock.close()
        return onlconsts.kDOWN, None

    return reply["status"], sock


def execute_cmd(cmd, host='localhost'):
    """
    Executes a shell command on a remote host via SSH.
    """
    ssh = Popen(['ssh', '%s' % host, cmd],
                shell=False, stdout=PIPE, stderr=PIPE)

    result = ssh.stdout.readlines()
    if not result:
        return None

    result = [str(s).replace("b'", "").replace("\\n'", "") for s in result]
    return result


def HMSFormatter(value):
    """
    Formats seconds into HH:MM:SS.
    """
    h = value // 3600
    m = (value - h * 3600) // 60
    s = value % 60
    return "%02d:%02d:%02d" % (h, m, s)
