#!/usr/bin/env bash 

export MALLOC_CHECK_=0

# Parse options
OPTS=$(getopt -o r:o:n:dmt --long onldaqdir:,rawdatadir: -n 'executedaq.sh' -- "$@")
if [ $? != 0 ]; then
    echo "Error: Argument parsing failed." >&2
    exit 1
fi
eval set -- "$OPTS"

RUNNUM=
DAQOPT=""
DAQNAME=""
EXE=""
USER_ONLDAQDIR=""
RAWDATADIR=""

while true; do
    case "$1" in
        -r) RUNNUM="$2"; shift 2 ;;
        -o) DAQOPT="$2"; shift 2 ;;
        -n) DAQNAME="$2"; shift 2 ;;
        -d) EXE="daq"; shift ;;
        -m) EXE="merger"; shift ;;
        -t) EXE="tcb"; shift ;;
        --onldaqdir) USER_ONLDAQDIR="$2"; shift 2 ;;
        --rawdatadir) RAWDATADIR="$2"; shift 2 ;;
        --) shift; break ;;
        *) break ;;
    esac
done

# -----------------------------------------------------------------------------
# Environment Logic
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# 1. External Infrastructure Setup (ROOT, python, etc.)
# -----------------------------------------------------------------------------
# [IMPORTANT] Modify this section to source your system's environment 
# e.g., source /path/to/your/root/bin/thisroot.sh
# -----------------------------------------------------------------------------

# Example: Default IBS CUP setup
# if [ -f ~cupsoft/prod_setup.sh ]; then
#     source ~cupsoft/prod_setup.sh 3.0
# fi


# -----------------------------------------------------------------------------
# 2. CUPDAQ Environment Setup
# -----------------------------------------------------------------------------

# Determine CUPDAQ directory
if [ -n "$USER_ONLDAQDIR" ]; then
    TARGET_DIR="$USER_ONLDAQDIR"
else
    # Auto-detect based on this script's location (bin/executedaq.sh)
    TARGET_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fi

# Source CUPDAQ specific environment
if [ -f "$TARGET_DIR/setup_cupdaq.sh" ]; then
    source "$TARGET_DIR/setup_cupdaq.sh"
else
    echo "Error: Cannot find setup_cupdaq.sh in $TARGET_DIR" >&2
    exit 1
fi

# Apply RAWDATA_DIR and Create LOG directory
if [ -n "$RAWDATADIR" ]; then
    export RAWDATA_DIR="$RAWDATADIR"
    mkdir -p "${RAWDATA_DIR}/LOG"
fi

# -----------------------------------------------------------------------------
# Execution
# -----------------------------------------------------------------------------

if [ -z "$EXE" ]; then
    echo "Error: Execution mode (-d, -m, -t) not specified." >&2
    exit 1
fi

RUNNUMSTR=$(printf "%06d" "$RUNNUM")
LOGFILE="${RAWDATA_DIR}/LOG/${DAQNAME}_${RUNNUMSTR}.log"

# Use the full path from the newly sourced environment to run the binary
"${CUPDAQ_DIR}/bin/$EXE" $DAQOPT > "$LOGFILE" 2>&1 &

exit 0