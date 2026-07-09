#!/bin/bash

LOCKFILE=/tmp/iz_sync.lock
SUCCESS_MARKER=/admin/web-asset-importer/.iz_sync_last_success

cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    OLD_PID=$(cat "$LOCKFILE" 2>/dev/null)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "IZ sync is already running (pid $OLD_PID). Exiting."
        exit 0
    else
        echo "Stale lockfile (pid $OLD_PID no longer running). Removing and proceeding."
        rm -f "$LOCKFILE"
    fi
fi

# Create the lock file with our PID and trap cleanup
echo $$ > "$LOCKFILE"
trap cleanup EXIT

# Running nightly sync
cd "$(dirname "$0")"
source ./venv/bin/activate
git pull
git submodule update --init --recursive

# Run nightly_sync.py and capture its exit status
python3 ./nightly_sync.py IZ
sync_exit_code=$?

# If nightly_sync.py fails, exit with status 1
if [ "$sync_exit_code" -ne 0 ]; then
    echo "Sync failed. Check iz_sync_log.txt for details."
    exit 1
fi

# Record success timestamp
date +%s > "$SUCCESS_MARKER"
echo "Sync completed successfully at $(date)"
