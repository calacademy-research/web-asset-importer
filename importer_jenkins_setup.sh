#!/bin/bash

source ./importer_jenkins_config.sh

#cleaning up any residual containers from leftover tests
setup
# cleanup lockfile on exit
trap cleanup ERR

# checking lock
exec 200>$lockfile
flock -n 200 || { echo "Another instance of the script is already running. Exiting."; exit 1; }


unset PYTHONPATH

export PYTHONPATH=$python_path:$PYTHONPATH

echo $PYTHONPATH

rm -r venv

sleep 5

python3 -m venv venv

source venv/bin/activate

# Print Python version
echo "Python version:"
python --version

# Upgrade pip and install requirements

pip install --upgrade pip

git config --global --add safe.directory "$(pwd)"

git submodule update --init --remote --force

# waiting for submodule to populate
timeout=300
interval=2
elapsed=0

while [ ! -f ${metadata_requirements_path} ]; do
  if [ $elapsed -ge $timeout ]; then
    echo "Timeout reached: ${metadata_requirements_path} not found"
    exit 1
  fi
  echo "Waiting for ${metadata_requirements_path} to exist..."
  sleep $interval
  elapsed=$((elapsed + interval))
done

echo "${metadata_requirements_path} found"

pip install -r requirements.txt

pip install -r metadata_tools/requirements.txt

# creating databases

docker run --name mariadb-specify -e MARIADB_ROOT_PASSWORD=password -d -p 3310:3306 mariadb:latest

echo "specify db running"

(
cd PIC_dbcreate || exit
./run_picdb.sh

echo "Picbatch db running"
)

sleep 10

docker exec -i mariadb-specify mariadb -u root -ppassword  < ../jenkins_ddls/specify_jenkins_ddl.sql

sleep 10

echo "specify db populated"

# tests

pytest --ignore="metadata_tools/tests"

exit_code=$?

if [ $exit_code -ne 0 ]; then
  echo "Pytest reported some failed tests"
  exit $exit_code
fi

echo "All tests passed"
