import sys
import time
import zmq
import json
import logging
from logging.handlers import RotatingFileHandler
from subprocess import Popen, PIPE
import onlconsts

# Reuse global ZeroMQ Context for better efficiency
_ctx = zmq.Context.instance()


def get_logger(name, log_file=None):
    """
    Creates and configures a standard Python logger.
    Supports both terminal output and file saving simultaneously.
    """
    logger = logging.getLogger(name)

    # Prevent adding multiple handlers if the logger already exists
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Format: [YYYY-MM-DD HH:MM:SS] [MODULE_NAME] [LOG_LEVEL] Message (Seconds only)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 1. Console Handler (for terminal output)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. File Handler (for saving to file, max 5MB per file with 3 backups)
        if log_file:
            file_handler = RotatingFileHandler(
                log_file, maxBytes=5*1024*1024, backupCount=3)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


# Create an internal logger for onlutils itself
log = get_logger("ONLUTILS")


def get_state(status):
    """
    Extracts the current state from the bitmask.
    Matches C++ RUNSTATE::GetState logic.
    """
    try:
        status = int(status)
    except (ValueError, TypeError):
        return 0

    for n in range(1, 17):
        if status & (1 << n):
            return n
    return 0


def check_state(status, state):
    """
    Checks if a specific state bit is set.
    Matches C++ RUNSTATE::CheckState logic.
    """
    try:
        status = int(status)
    except (ValueError, TypeError):
        return False

    return bool(status & (1 << state))


def check_error(status):
    """
    Checks if the error bit is set.
    Matches C++ RUNSTATE::CheckError logic.
    """
    try:
        status = int(status)
    except (ValueError, TypeError):
        return False

    return bool(status & (1 << onlconsts.kERROR))


def get_connection(endpoint, sock_type=zmq.REQ):
    """
    Creates and connects a ZeroMQ socket.
    Timeout is now handled by poll() in send_daq_cmd.
    """
    sock = _ctx.socket(sock_type)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(endpoint)
    return sock


def send_daq_cmd(sock, cmd_string, extra_data=None, timeout_ms=200):
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

        # Timeout check
        if sock.poll(timeout=timeout_ms) == 0:
            # Keep it at the DEBUG level since frequent polling timeouts can spam the logs.
            #log.debug("ZMQ poll timeout (%d ms) on command: %s",
            #          timeout_ms, cmd_string)
            return None

        reply = sock.recv_json()
        return reply
    except Exception as e:
        log.error("ZMQ send/recv exception on command '%s': %s", cmd_string, e)
        return None


def query_runstate(endpoint, sock=None):
    """
    Queries the DAQ server for its current status using JSON over ZMQ.
    Returns: (status_code, socket_object)
    """
    if sock is None:
        sock = get_connection(endpoint)

    reply = send_daq_cmd(sock, onlconsts.kQUERYDAQSTATUS)

    # 1. Check if communication failed or status is not "ok"
    if reply is None or reply.get("status") != "ok":
        if sock:
            sock.close()
        return onlconsts.kDOWN, None

    # 2. Extract the actual bitmask state using the "run_status" key
    run_status = reply.get("run_status", onlconsts.kDOWN)

    return run_status, sock


def run_ssh_cmd(cmd, host='localhost'):
    """
    Executes a shell command on a remote host via SSH.
    """
    log.debug("Executing SSH command on %s: %s", host, cmd)
    ssh = Popen(['ssh', '%s' % host, cmd],
                shell=False, stdout=PIPE, stderr=PIPE)

    result = ssh.stdout.readlines()
    if not result:
        log.warning("SSH command returned empty result on %s", host)
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
