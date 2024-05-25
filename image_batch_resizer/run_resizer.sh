#!/bin/bash

pushd ..

while true; do
    python3 -u image_batch_resizer/img_batch_resizer.py > image_batch_resizer/resizer_log.txt 2>&1
    exit_code=$?
    if [ $exit_code -eq 0 ]; then
        break
    fi
    echo "Script exited with code $exit_code. Restarting in 5 minutes..." >> image_batch_resizer/resizer_log.txt
    sleep 300  # Wait for 5 minutes before restarting
done &

popd || exit
