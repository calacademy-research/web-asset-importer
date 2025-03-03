#!/bin/bash

LOCKFILE=/tmp/botany_sync.lock

cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    echo "Botany sync is already running. Exiting."
    exit 0
else
    # Create the lock file and trap cleanup
    touch "$LOCKFILE"
    trap cleanup EXIT
    # Running nightly sync
    cd "$(dirname "$0")"
    source ./env/bin/activate
    git pull
    git submodule update --init --recursive

    # Run nightly_sync.py and capture its exit status
    python3 ./nightly_sync.py Botany
    sync_exit_code=$?

    # If nightly_sync.py fails, exit with status 1
    if [ "$sync_exit_code" -ne 0 ]; then
        echo "Sync failed. Check botany_sync_log.txt for details."
        exit 1
    fi
fi

