#!/bin/bash
image_password="test_password"
metadata_requirements_path="metadata_tools/requirements.txt"
python_path=$(pwd)
lockfile="/tmp/$(basename "$0").lock"


# Function to remove the lockfile

setup() {
    docker stop mariadb-specify
    docker rm mariadb-specify
    docker stop picbatch-mysql
    docker rm picbatch-mysql
}

cleanup() {
    rm -f "$lockfile"
    echo "Lockfile removed."
    docker stop mariadb-specify
    docker rm mariadb-specify
    docker stop picbatch-mysql
    docker rm picbatch-mysql
}

#cleaning up any residual containers from leftover tests
setup
# cleanup lockfile on exit
trap cleanup EXIT

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

git submodule update --init

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

docker exec -i mariadb-specify mariadb -u root -ppassword  < ../jenkins_ddls/specify_test_ddl.sql

sleep 10

echo "specify db populated"

pytest --ignore="metadata_tools/tests"

# Run botany import script and check its exit status
./botany_import.sh
if [ $? -ne 0 ]; then
  echo "botany_import.sh failed"
  exit 1
fi

# Run ichthyology import script and check its exit status
./ich_import.sh
if [ $? -ne 0 ]; then
  echo "ich_import.sh failed"
  exit 1
fi


# Run iz import script and check its exit status
./iz_import.sh
if [ $? -ne 0 ]; then
  echo "iz_import.sh failed"
  exit 1
fi

# deleting all image db records
echo "DELETE * FROM images;" | docker exec -i mysql-images -u root -p$image_password

# cleaning up mounted server attachments folder
find ../web-asset-server-ci/attachments -type f -delete