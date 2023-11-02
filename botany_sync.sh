#!/bin/bash
cd "$(dirname "$0")"
source ./env/bin/activate
git pull
export PYTHONPATH='/admin/image_server'
python3 ./nightly_sync.py Botany >& botany_sync_log.txt &
