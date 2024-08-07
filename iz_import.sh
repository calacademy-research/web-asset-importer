#!/usr/bin/bash
LOCKFILE=/tmp/iz_import.lock


cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    echo "IZ import is already running. Exiting."
    exit 1
else
    # Create the lock file
    touch "$LOCKFILE"

    # trap to ensure lock file is removed on script exit
    trap cleanup EXIT

    # Main import process
    cd "$(dirname "$0")" || exit
    source ./venv/bin/activate
    python3 ./client_tools.py -vvvv IZ import >& iz_import_log.txt &
    PYTHON_PID=$!

    # wait for process to complete
    wait $PYTHON_PID
    PYTHON_EXIT_CODE=$?

    # Exit with the Python script's exit code
    exit $PYTHON_EXIT_CODE
fi
