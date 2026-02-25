#!/bin/bash

LOCKFILE=/tmp/iz_sync.lock
STALE_SECONDS=86400
SUCCESS_MARKER=/admin/web-asset-importer/.iz_sync_last_success

cleanup() {
    rm -f "$LOCKFILE"
}

if [ -e "$LOCKFILE" ]; then
    # Check if lockfile is stale (older than 24 hours)
    lock_age=$(( $(date +%s) - $(stat -c %Y "$LOCKFILE" 2>/dev/null || echo 0) ))
    if [ "$lock_age" -gt "$STALE_SECONDS" ]; then
        echo "Stale lockfile detected (age: ${lock_age}s). Removing and proceeding."
        rm -f "$LOCKFILE"
    else
        echo "IZ sync is already running. Exiting."
        exit 0
    fi
fi

# Create the lock file and trap cleanup
touch "$LOCKFILE"
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
