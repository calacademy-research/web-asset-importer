#!/usr/bin/bash
LOCKFILE=/tmp/botany_import.lock

cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    echo "Botany import is already running. Exiting."
    exit 1
else
    # Create the lock file
    touch "$LOCKFILE"

    # trap to ensure lock file is removed on script exit
    trap cleanup EXIT

    # Main import process
    cd "$(dirname "$0")" || exit
    source ./venv/bin/activate
    python3 ./client_tools.py -vvvv Botany import >& botany_import_log.txt &
fi