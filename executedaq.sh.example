#!/usr/bin/env bash

export MALLOC_CHECK_=0

# Parse options
OPTS=$(getopt -o r:o:n:dmt --long rawdatadir: -n 'executedaq.sh' -- "$@")
if [ $? != 0 ]; then
    echo "Error: Argument parsing failed." >&2
    exit 1
fi
eval set -- "$OPTS"

RUNNUM=
DAQOPT=""
DAQNAME=""
EXE=""
RAWDATADIR=""

while true; do
    case "$1" in
        -r) RUNNUM="$2";  shift 2 ;;
        -o) DAQOPT="$2";  shift 2 ;;
        -n) DAQNAME="$2"; shift 2 ;;
        -d) EXE="daq";    shift ;;
        -m) EXE="merger"; shift ;;
        -t) EXE="tcb";    shift ;;
        --rawdatadir) RAWDATADIR="$2"; shift 2 ;;
        --) shift; break ;;
        *) break ;;
    esac
done

# =============================================================================
# Environment Setup — modify for your site
# =============================================================================
# source /path/to/root/bin/thisroot.sh
# source /path/to/cupdaq/setup_cupdaq.sh
# =============================================================================

# Apply RAWDATA_DIR and create LOG directory
if [ -n "$RAWDATADIR" ]; then
    export RAWDATA_DIR="$RAWDATADIR"
    mkdir -p "${RAWDATA_DIR}/LOG"
fi

if [ -z "$EXE" ]; then
    echo "Error: Execution mode (-d, -m, -t) not specified." >&2
    exit 1
fi

if [ -z "$CUPDAQ_DIR" ]; then
    echo "Error: CUPDAQ_DIR is not set. Check your environment setup." >&2
    exit 1
fi

RUNNUMSTR=$(printf "%06d" "$RUNNUM")
LOGFILE="${RAWDATA_DIR}/LOG/${DAQNAME}_${RUNNUMSTR}.log"

"${CUPDAQ_DIR}/bin/$EXE" $DAQOPT > "$LOGFILE" 2>&1 &

exit 0
