#!/bin/bash

LOCKFILE=/tmp/botany_sync.lock

cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    echo "Botany sync is already running. Exiting."
    exit 1
else
    # Create the lock file and trap cleanup
    touch "$LOCKFILE"
    trap cleanup EXIT
    # running nightly sync
    cd "$(dirname "$0")"
    source ./env/bin/activate
    git pull
    git submodule update --init --recursive
    python3 ./nightly_sync.py Botany >& botany_sync_log.txt
fi
